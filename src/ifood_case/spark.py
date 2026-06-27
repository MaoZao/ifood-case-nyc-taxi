"""
Fábrica da SparkSession.

Centraliza a criação da sessão com as extensões do Delta Lake já configuradas.
Quando `delta-spark` não está instalado (ex.: ambiente mínimo de teste), cai
graciosamente para Parquet, mantendo o pipeline executável.

Storage S3-compatível (MinIO/AWS S3): se a variável de ambiente
``IFOOD_S3_ENDPOINT`` estiver definida, a sessão é configurada com o conector
``s3a`` (credenciais via ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY``). Assim
caminhos ``s3a://bucket/...`` nos configs passam a ser lidos/gravados nativamente
pelo Spark, sem alterar o código do pipeline.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Conector S3A: versões alinhadas ao Hadoop do PySpark 3.5 (Hadoop 3.3.4).
_S3A_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


def _s3a_settings() -> tuple[dict[str, str], list[str]]:
    """Lê a configuração de S3/MinIO do ambiente.

    Retorna ``(configs, packages)`` para o conector ``s3a`` quando
    ``IFOOD_S3_ENDPOINT`` está definido; caso contrário, ``({}, [])`` — mantendo
    o comportamento local (filesystem) intacto, sem baixar jars desnecessários.
    """
    endpoint = os.getenv("IFOOD_S3_ENDPOINT")
    if not endpoint:
        return {}, []

    configs = {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        # path-style é obrigatório para MinIO (sem DNS por bucket).
        "spark.hadoop.fs.s3a.path.style.access": os.getenv("IFOOD_S3_PATH_STYLE", "true"),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }
    return configs, list(_S3A_PACKAGES)


def build_spark(
    app_name: str = "ifood-nyc-taxi",
    delta: bool = True,
    warehouse_dir: str | None = None,
) -> SparkSession:
    # Warehouse local p/ Hive Metastore embedded (Derby). Em Databricks, ignorado.
    warehouse = warehouse_dir or os.getenv("IFOOD_WAREHOUSE", "data/_warehouse")
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
        # Catalog/metastore para CREATE TABLE — permite SELECT * FROM ifood.silver_trips.
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.catalogImplementation", "hive")
    )

    # Storage S3-compatível (MinIO/S3): aplica configs do conector s3a quando
    # IFOOD_S3_ENDPOINT está definido (no-op no modo local/filesystem).
    s3_configs, s3_packages = _s3a_settings()
    for key, value in s3_configs.items():
        builder = builder.config(key, value)
    if s3_configs:
        logger.info("S3A habilitado (endpoint=%s).", s3_configs["spark.hadoop.fs.s3a.endpoint"])

    if delta:
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore

            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            try:
                spark = (
                    configure_spark_with_delta_pip(builder, extra_packages=s3_packages)
                    .enableHiveSupport()
                    .getOrCreate()
                )
            except Exception:  # pragma: no cover - Hive jars ausentes em alguns envs
                spark = configure_spark_with_delta_pip(
                    builder, extra_packages=s3_packages
                ).getOrCreate()
                logger.warning("Hive Metastore indisponível; usando catalog in-memory.")
            logger.info("SparkSession criada com suporte a Delta Lake. Warehouse=%s", warehouse)
            return spark
        except Exception as exc:  # pragma: no cover - depende do ambiente
            logger.warning("Delta indisponível (%s). Usando Parquet.", exc)

    if s3_packages:
        builder = builder.config("spark.jars.packages", ",".join(s3_packages))
    try:
        spark = builder.enableHiveSupport().getOrCreate()
    except Exception:  # pragma: no cover
        spark = builder.getOrCreate()
    return spark
