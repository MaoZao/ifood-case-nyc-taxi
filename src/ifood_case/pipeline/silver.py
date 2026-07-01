"""
Camada SILVER (Trusted Zone).

Lê a Bronze, aplica as transformações puras (seleção de colunas exigidas,
tipagem, limpeza, derivação temporal), roda os contratos de Data Quality como
*gate*, e persiste em formato colunar particionado por `trip_month`.

Performance:
  - O DataFrame é persistido (memória+disco) antes do gate: o job único de DQ
    materializa o resultado e o write o REUTILIZA, em vez de recomputar toda a
    linhagem (incluindo o shuffle do dropDuplicates) uma segunda vez.
  - `repartition(partition_column)` antes do write alinha as partições de
    shuffle às partições físicas: 1 arquivo por mês, em vez de até
    (shuffle.partitions × meses) arquivos pequenos — leitura downstream e
    listagem de metadados muito mais baratas.
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

    start, end = cfg.window
    silver = to_silver(bronze, start=start, end=end).repartition(cfg.partition_column)
    silver.persist()

    try:
        # Gate de qualidade: nada sobe sem passar nos contratos (1 job único).
        run_checks(silver, raise_on_fail=True)

        (
            silver.write.mode("overwrite")
            .format(cfg.storage_format)
            .partitionBy(cfg.partition_column)
            .save(cfg.paths.silver)
        )
    finally:
        silver.unpersist()
    logger.info(
        "[silver] gravada e particionada por %s em %s", cfg.partition_column, cfg.paths.silver
    )
    return silver
