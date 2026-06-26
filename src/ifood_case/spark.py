"""
Fábrica da SparkSession.

Centraliza a criação da sessão com as extensões do Delta Lake já configuradas.
Quando `delta-spark` não está instalado (ex.: ambiente mínimo de teste), cai
graciosamente para Parquet, mantendo o pipeline executável.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def build_spark(app_name: str = "ifood-nyc-taxi", delta: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # Snappy = bom equilíbrio compressão/CPU para colunar.
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Particionamento dinâmico evita reescrever partições intocadas.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # 200 é o default; explícito para deixar claro o ponto de tuning.
        .config("spark.sql.shuffle.partitions", "200").config(
            "spark.sql.session.timeZone", "America/New_York"
        )
        # Os Parquet reais do NYC TLC (2023+) gravam os timestamps com precisão
        # de NANOSSEGUNDOS (INT64 TIMESTAMP(NANOS)), que o Spark 3.5 recusa por
        # padrão. Esta flag lê o valor bruto como long; a Bronze o reconverte
        # para TimestampType (ver pipeline/bronze.py).
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    )

    if delta:
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore

            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            logger.info("SparkSession criada com suporte a Delta Lake.")
            return spark
        except Exception as exc:  # pragma: no cover - depende do ambiente
            logger.warning("Delta indisponível (%s). Usando Parquet.", exc)

    spark = builder.getOrCreate()
    return spark
