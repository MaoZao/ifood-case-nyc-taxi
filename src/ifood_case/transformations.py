"""
Transformações puras (DataFrame -> DataFrame).

Mantidas separadas da orquestração de I/O para serem testáveis de forma
unitária com SparkSession local, sem tocar em disco/cloud. Cada função faz
uma coisa só — facilita revisão, teste e reuso.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

from .config import REQUIRED_COLUMNS


def select_required_columns(df: DataFrame) -> DataFrame:
    """Projeção apenas das colunas exigidas pelo case.

    Usa .select() (não loop Python) para que o otimizador Catalyst aplique
    *column pruning* já na leitura do Parquet — evita descomprimir colunas
    descartadas e acelera drasticamente o scan.
    """
    return df.select(*REQUIRED_COLUMNS)


def cast_types(df: DataFrame) -> DataFrame:
    """Contrato de tipos: integridade matemática e temporal garantida."""
    return (
        df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    )


def clean(df: DataFrame, start: str = "2023-01-01", end: str = "2023-06-01") -> DataFrame:
    """Regras de qualidade da camada Silver.

    - Descarta nulos em campos essenciais.
    - Elimina anomalias que violam regras de negócio/física:
      receita não positiva, 0 passageiros, dropoff <= pickup.
    - Restringe à janela contratada (default Jan-Mai/2023; em produção o
      chamador deriva de ``Config.months`` — ver ``to_silver``), evitando
      vazamento de registros com datas erradas (existem no dataset real).
    - Remove duplicatas exatas (reprocessamento de terminal) por ÚLTIMO:
      o ``dropDuplicates`` exige um shuffle completo, então filtrar antes
      reduz o volume embaralhado (~5-8% de linhas a menos no dataset real).
    """
    return (
        df.dropna(subset=REQUIRED_COLUMNS)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        .filter(
            (F.col("tpep_pickup_datetime") >= F.lit(start))
            & (F.col("tpep_pickup_datetime") < F.lit(end))
        )
        .dropDuplicates()
    )


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Deriva colunas temporais para particionamento e analytics."""
    return df.withColumn("trip_month", F.month("tpep_pickup_datetime")).withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    )


def to_silver(df: DataFrame, start: str | None = None, end: str | None = None) -> DataFrame:
    """Pipeline Silver completo, composto pelas funções puras acima.

    ``start``/``end`` delimitam a janela de datas válida (ver ``clean``);
    quando omitidos valem os defaults do case. O orquestrador passa a janela
    derivada de ``Config.months`` — mudar os meses no YAML ajusta a limpeza
    automaticamente, sem editar código.
    """
    window_kwargs = {}
    if start is not None:
        window_kwargs["start"] = start
    if end is not None:
        window_kwargs["end"] = end
    return (
        df.transform(select_required_columns)
        .transform(cast_types)
        .transform(lambda d: clean(d, **window_kwargs))
        .transform(add_partition_columns)
    )
