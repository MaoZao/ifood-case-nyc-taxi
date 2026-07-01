"""
Catalog — registro das tabelas Delta no metastore.

Hoje o pipeline grava em paths (``data/silver/``). Para que usuários consumam via
``SELECT * FROM ifood.silver_trips`` precisa registrar essas localizações como
tabelas no catálogo. Localmente usamos o Hive Metastore embedded (Derby);
em Databricks pluga-se ao Unity Catalog sem mudar este código (basta trocar o
``database`` por um catalog.schema do UC).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession

from .config import Config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableSpec:
    """Descrição mínima de uma tabela externa no catalog."""

    name: str
    location: str
    comment: str = ""


def ensure_database(spark: SparkSession, database: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    logger.info("[catalog] database garantido: %s", database)


def _normalize_location(location: str) -> str:
    """Spark SQL aceita só forward slashes em LOCATION (também no Windows)."""
    # Mantém esquemas (s3a://, file://, etc.) intactos; só troca \ por /.
    return location.replace("\\", "/")


def register_table(spark: SparkSession, db: str, spec: TableSpec, fmt: str = "delta") -> None:
    """Registra ``db.spec.name`` apontando para ``spec.location`` (tabela externa).

    Tabela externa = catalog guarda os metadados, mas os arquivos seguem no path
    original. Drop não apaga dados — adequado para Lakehouse.
    """
    qualified = f"{db}.{spec.name}"
    comment_sql = f" COMMENT '{spec.comment}'" if spec.comment else ""
    location = _normalize_location(spec.location)
    # DROP + CREATE (em vez de IF NOT EXISTS) torna o registro AUTORITATIVO: a
    # LOCATION passa a refletir SEMPRE o ambiente atual (local vs s3a://...),
    # corrigindo registros obsoletos — ex.: um metastore compartilhado entre host
    # e container com caminhos de outro SO. Tabela externa: DROP remove só os
    # metadados, nunca os arquivos.
    spark.sql(f"DROP TABLE IF EXISTS {qualified}")
    spark.sql(f"CREATE TABLE {qualified} USING {fmt}{comment_sql} LOCATION '{location}'")
    # MSCK / REFRESH garante que partições novas sejam descobertas.
    try:
        spark.sql(f"REFRESH TABLE {qualified}")
    except Exception:  # pragma: no cover - REFRESH em Parquet puro pode não existir
        pass
    logger.info("[catalog] registrada %s -> %s", qualified, spec.location)


def register_all(spark: SparkSession, cfg: Config) -> dict[str, str]:
    """Registra Bronze, Silver e Gold no catalog.

    Retorna mapa ``{nome_logico: tabela_qualificada}`` para uso em queries SQL.
    """
    ensure_database(spark, cfg.database)
    gold = cfg.paths.gold.rstrip("/")
    specs = [
        TableSpec("bronze_trips", cfg.paths.bronze, "Camada raw + lineage."),
        TableSpec(
            "silver_trips",
            cfg.paths.silver,
            "5 colunas obrigatórias, tipada, particionada por trip_month.",
        ),
        TableSpec(
            "gold_receita_mensal",
            f"{gold}/agg_receita_mensal",
            "Q1 — receita média por mês.",
        ),
        TableSpec(
            "gold_passageiros_hora_maio",
            f"{gold}/agg_passageiros_hora_maio",
            "Q2 — média de passageiros por hora (maio).",
        ),
        TableSpec(
            "gold_trips",
            f"{gold}/trips",
            "Fato granular (5 colunas + trip_month) para SQL ad-hoc.",
        ),
    ]

    registered: dict[str, str] = {}
    for spec in specs:
        try:
            register_table(spark, cfg.database, spec, cfg.storage_format)
            registered[spec.name] = f"{cfg.database}.{spec.name}"
        except Exception as exc:
            # Se o path ainda não existe (estágio não rodou), apenas avisa.
            logger.warning("[catalog] skip %s: %s", spec.name, exc)
    return registered


def run(spark: SparkSession, cfg: Config) -> dict[str, str]:
    """Entry point chamado pelo CLI: registra todas as tabelas existentes."""
    logger.info("=== iniciando registro no catalog ===")
    return register_all(spark, cfg)
