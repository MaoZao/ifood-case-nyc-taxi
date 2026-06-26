"""
Camada SILVER (Trusted Zone).

Lê a Bronze, aplica as transformações puras (seleção de colunas exigidas,
tipagem, limpeza, derivação temporal), roda os contratos de Data Quality como
*gate*, e persiste em formato colunar particionado por `trip_month`.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from ..config import Config
from ..quality import run_checks
from ..transformations import to_silver

logger = logging.getLogger(__name__)


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    logger.info("[silver] lendo bronze: %s", cfg.paths.bronze)
    bronze = spark.read.format(cfg.storage_format).load(cfg.paths.bronze)

    silver = to_silver(bronze)

    # Gate de qualidade: nada sobe sem passar nos contratos.
    run_checks(silver, raise_on_fail=True)

    (
        silver.write.mode("overwrite")
        .format(cfg.storage_format)
        .partitionBy(cfg.partition_column)
        .save(cfg.paths.silver)
    )
    logger.info("[silver] gravada e particionada por %s em %s",
                cfg.partition_column, cfg.paths.silver)
    return silver
