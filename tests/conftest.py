"""Fixtures de teste: SparkSession local (sem Delta para rapidez) e dados-mock."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    # Warehouse num tmp dir session-scoped: catalog/CREATE TABLE funcionam sem
    # poluir o repo nem precisar de Hive Metastore externo.
    warehouse = tmp_path_factory.mktemp("warehouse")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ifood-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # UTC + timestamps tz-aware no fixture tornam os testes determinísticos
        # em qualquer máquina. Com fuso != UTC, um datetime naive seria
        # interpretado no fuso do SO na escrita e relido noutro fuso por
        # F.hour/F.month, deslocando hora/mês (flaky conforme a máquina).
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def raw_df(spark):
    """Mini dataset com sujeira proposital para exercitar a limpeza:
    - linha boa
    - duplicata exata
    - total_amount negativo
    - passenger_count nulo
    - 0 passageiros
    - dropoff antes do pickup
    - coluna extra (deve ser descartada na projeção)
    """
    utc = dt.timezone.utc

    def ts(y, mo, d, h, mi):
        return dt.datetime(y, mo, d, h, mi, tzinfo=utc)

    rows = [
        # VendorID, passenger_count, total_amount, pickup, dropoff, extra
        (1, 1, 20.5, ts(2023, 1, 5, 8, 0), ts(2023, 1, 5, 8, 15), "x"),
        (1, 1, 20.5, ts(2023, 1, 5, 8, 0), ts(2023, 1, 5, 8, 15), "x"),  # dup
        (2, 2, -5.0, ts(2023, 2, 1, 9, 0), ts(2023, 2, 1, 9, 20), "x"),  # neg
        (2, None, 30.0, ts(2023, 3, 1, 10, 0), ts(2023, 3, 1, 10, 10), "x"),  # null
        (1, 0, 15.0, ts(2023, 4, 1, 11, 0), ts(2023, 4, 1, 11, 5), "x"),  # 0 pax
        (2, 3, 40.0, ts(2023, 5, 1, 12, 0), ts(2023, 5, 1, 11, 0), "x"),  # invertida
        (1, 4, 55.0, ts(2023, 5, 2, 18, 0), ts(2023, 5, 2, 18, 30), "x"),  # boa (maio)
    ]
    cols = [
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "extra_col",
    ]
    return spark.createDataFrame(rows, cols)
