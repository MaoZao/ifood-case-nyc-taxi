"""
Camada BRONZE (Raw / Landing -> Trusted-raw).

Objetivo: ponto de entrada IMUTÁVEL. Lê os Parquet originais da landing zone
e persiste *as-is* (sem transformação), apenas adicionando metadados de
linhagem (arquivo de origem e timestamp de ingestão). Isso garante
rastreabilidade e permite reprocessamento total (backfill) caso uma regra da
Silver mude no futuro.
"""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from ..config import Config

logger = logging.getLogger(__name__)

# Os Parquet reais do NYC TLC NÃO têm schema estável entre meses: a mesma coluna
# aparece ora como INT32 ora como INT64 (VendorID, PULocationID...) e ora como
# BIGINT ora como DOUBLE (passenger_count, RatecodeID). Ler a pasta inteira de
# uma vez quebra (leitor vetorizado) ou recusa o merge (BIGINT vs INT). Por isso
# lemos arquivo a arquivo e forçamos um tipo canônico (o mais largo) em cada
# coluna divergente antes de unir — schema enforcement na ingestão.
_CANONICAL_TYPES = {
    "VendorID": "bigint",
    "passenger_count": "double",
    "RatecodeID": "double",
    "PULocationID": "bigint",
    "DOLocationID": "bigint",
    "payment_type": "bigint",
}


def _fix_nanos_timestamps(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Reconverte timestamps em nanossegundos lidos como long.

    Com ``spark.sql.legacy.parquet.nanosAsLong=true`` (ver spark.py), as colunas
    ``TIMESTAMP(NANOS)`` dos Parquet reais do NYC TLC chegam como ``long``
    (nanos desde a época, sem tz). Recuperamos o ``TimestampType`` preservando o
    *wall clock* original: nanos -> micros -> ``timestamp_micros`` (que assume
    UTC) e ``to_utc_timestamp`` reinterpreta esse relógio na timezone da sessão.

    Só toca colunas de data/hora que vieram como ``long`` — dados sintéticos
    (timestamps em micros) passam intactos, assim como demais colunas inteiras.
    """
    session_tz = spark.conf.get("spark.sql.session.timeZone") or "UTC"
    for fld in df.schema.fields:
        if "datetime" in fld.name.lower() and isinstance(fld.dataType, LongType):
            micros = (F.col(fld.name) / 1000).cast("long")
            df = df.withColumn(
                fld.name,
                F.to_utc_timestamp(F.timestamp_micros(micros), session_tz),
            )
            logger.info("[bronze] coluna %s convertida de nanos(long) -> timestamp", fld.name)
    return df


def _read_one(spark: SparkSession, path: str) -> DataFrame:
    """Lê um arquivo e normaliza as colunas de tipo instável ao canônico."""
    df = spark.read.parquet(path)
    for col, typ in _CANONICAL_TYPES.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))
    return df


def read_landing(spark: SparkSession, cfg: Config) -> DataFrame:
    base = cfg.paths.landing.rstrip("/")
    paths = [f"{base}/yellow_tripdata_{m}.parquet" for m in cfg.months]
    logger.info("[bronze] lendo %d arquivos da landing", len(paths))
    # unionByName (case-insensitive, allowMissingColumns) reconcilia também a
    # diferença de nome 'airport_fee' vs 'Airport_fee' entre meses.
    dfs = [_read_one(spark, p) for p in paths]
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    df = _fix_nanos_timestamps(df, spark)
    return df.withColumn("_source_file", F.input_file_name()).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def write_bronze(df: DataFrame, cfg: Config) -> None:
    writer = df.write.mode("overwrite").format(cfg.storage_format)
    logger.info("[bronze] gravando %s em %s", cfg.storage_format, cfg.paths.bronze)
    writer.save(cfg.paths.bronze)


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    df = read_landing(spark, cfg)
    write_bronze(df, cfg)
    logger.info("[bronze] concluída")
    return df
