"""
Camada SILVER — modo INCREMENTAL via MERGE INTO (upsert).

Diferenças vs `silver.py` (full refresh):
  - Não reescreve a Silver inteira; aplica apenas o delta (linhas novas/alteradas).
  - Usa chave natural (VendorID + pickup + dropoff) para idempotência: rerodar a
    mesma janela de tempo NÃO gera duplicatas.
  - A condição do MERGE inclui a coluna de partição (`trip_month`): o Delta faz
    *partition pruning* e só lê/reescreve os meses presentes no source, em vez
    de escanear a tabela inteira a cada merge.
  - Habilita Change Data Feed (CDF) na escrita — Gold pode então propagar apenas
    o delta via `table_changes()`.
  - Suporta backfill de meses isolados sem reprocessar 5 meses inteiros.

Em produção real, o source seria filtrado por um watermark persistido (última
`_ingested_at` processada, guardada em uma tabela de controle) — aqui o source
é a Bronze completa e a idempotência fica por conta da chave natural do MERGE.

Pré-requisito: storage_format == "delta". Em Parquet puro não existe MERGE.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from ..config import Config
from ..quality import run_checks
from ..transformations import to_silver

logger = logging.getLogger(__name__)

# Chave natural escolhida para upsert. Não há ID único nos dados do NYC TLC,
# então combinamos campos que tornam a tupla quase-única na prática:
#   - VendorID + tpep_pickup_datetime  -> identifica o veículo no instante
#   - tpep_dropoff_datetime            -> distingue corridas em sequência
# (Em produção real, adicionaríamos PULocationID/DOLocationID se viessem na Silver.)
MERGE_KEYS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"]


def _table_exists(spark: SparkSession, path: str) -> bool:
    try:
        from delta.tables import DeltaTable  # type: ignore

        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _initial_write(spark: SparkSession, silver: DataFrame, cfg: Config) -> None:
    """Primeira escrita: cria a tabela Delta com CDF habilitado."""
    logger.info("[silver-inc] tabela inexistente — escrita inicial em %s", cfg.paths.silver)
    (
        silver.write.mode("overwrite")
        .format("delta")
        .partitionBy(cfg.partition_column)
        .option("delta.enableChangeDataFeed", "true")
        .save(cfg.paths.silver)
    )


def _merge_into(spark: SparkSession, silver_new: DataFrame, cfg: Config) -> dict[str, int]:
    """Executa MERGE INTO usando a chave natural; retorna estatísticas."""
    from delta.tables import DeltaTable  # type: ignore

    target = DeltaTable.forPath(spark, cfg.paths.silver)
    # A coluna de partição na condição permite ao Delta descartar (pruning) as
    # partições não afetadas — sem ela, todo MERGE escaneia a tabela inteira.
    keys = [*MERGE_KEYS, cfg.partition_column]
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    logger.info("[silver-inc] MERGE on %s", on_clause)
    (
        target.alias("t")
        .merge(silver_new.alias("s"), on_clause)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    # Métricas: o operationMetrics do Delta tem contadores precisos por operação.
    last_op = target.history(1).collect()[0]
    metrics = last_op["operationMetrics"] or {}
    stats = {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
        "source_rows": int(metrics.get("numSourceRows", 0)),
    }
    logger.info("[silver-inc] resultado MERGE: %s", stats)
    return stats


def run(spark: SparkSession, cfg: Config) -> dict[str, int]:
    """Executa o estágio Silver em modo incremental.

    Retorna estatísticas do MERGE para observabilidade.
    """
    if cfg.storage_format != "delta":
        raise RuntimeError(
            "Silver incremental exige storage_format=delta (MERGE INTO não existe em Parquet)."
        )

    logger.info("[silver-inc] lendo bronze: %s", cfg.paths.bronze)
    bronze = spark.read.format(cfg.storage_format).load(cfg.paths.bronze)

    start, end = cfg.window
    silver_new = to_silver(bronze, start=start, end=end)
    silver_new.persist()

    try:
        run_checks(silver_new, raise_on_fail=True)

        if not _table_exists(spark, cfg.paths.silver):
            _initial_write(spark, silver_new, cfg)
            n = silver_new.count()
            return {"inserted": n, "updated": 0, "deleted": 0, "source_rows": n}

        return _merge_into(spark, silver_new, cfg)
    finally:
        silver_new.unpersist()
