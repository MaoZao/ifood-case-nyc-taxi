"""
Camada GOLD (Consumption Zone).

Materializa tabelas de negócio prontas para BI/SQL, respondendo diretamente às
perguntas do case. Pré-agregar aqui acelera as consultas dos usuários finais e
desacopla o consumo do volume granular da Silver.

Tabelas geradas:
  - gold/agg_receita_mensal        -> Q1 (média de total_amount por mês)
  - gold/agg_passageiros_hora_maio -> Q2 (média de passageiros por hora, maio)
  - gold/trips (opcional)          -> fato granular exposto via SQL/Delta
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config

logger = logging.getLogger(__name__)


def receita_mensal(silver: DataFrame) -> DataFrame:
    """Q1: média (e total) de total_amount por mês, sobre toda a frota."""
    return (
        silver.groupBy("trip_month")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.avg("total_amount"), 2).alias("receita_media_usd"),
            F.round(F.sum("total_amount"), 2).alias("receita_total_usd"),
        )
        .orderBy("trip_month")
    )


def passageiros_por_hora_maio(silver: DataFrame) -> DataFrame:
    """Q2: média de passenger_count por hora do dia, apenas maio (mês 5)."""
    return (
        silver.filter(F.col("trip_month") == 5)
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.avg("passenger_count"), 3).alias("media_passageiros"),
        )
        .orderBy("pickup_hour")
    )


def _write(df: DataFrame, cfg: Config, name: str) -> None:
    path = os.path.join(cfg.paths.gold, name)
    df.write.mode("overwrite").format(cfg.storage_format).save(path)
    logger.info("[gold] %s -> %s", name, path)


def run(spark: SparkSession, cfg: Config) -> dict[str, DataFrame]:
    silver = spark.read.format(cfg.storage_format).load(cfg.paths.silver)

    tables = {
        "agg_receita_mensal": receita_mensal(silver),
        "agg_passageiros_hora_maio": passageiros_por_hora_maio(silver),
    }
    for name, df in tables.items():
        _write(df, cfg, name)

    # Fato granular com as 5 colunas exigidas, exposto para consumo SQL ad-hoc.
    _write(
        silver.select(
            "VendorID", "passenger_count", "total_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_month",
        ),
        cfg, "trips",
    )
    logger.info("[gold] concluída")
    return tables
