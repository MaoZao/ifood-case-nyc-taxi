"""Testes do módulo catalog.

Verifica que o registro das tabelas é idempotente, que tabelas externas
preservam os dados após DROP e que nomes qualificados ficam disponíveis no
catalog default (in-memory) sem precisar de Hive Metastore embedded.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ifood_case.catalog import TableSpec, ensure_database, register_table
from ifood_case.config import Config, Paths


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(
        months=["2023-01"],
        base_url="http://example.com",
        paths=Paths(
            landing=str(tmp_path / "landing"),
            bronze=str(tmp_path / "bronze"),
            silver=str(tmp_path / "silver"),
            gold=str(tmp_path / "gold"),
        ),
        storage_format="parquet",
        database="ifood_test",
        warehouse_dir=str(tmp_path / "warehouse"),
    )


def _write_dummy_parquet(spark, path: str) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df.write.mode("overwrite").parquet(path)


def test_ensure_database_creates_when_missing(spark, cfg):
    ensure_database(spark, cfg.database)
    dbs = [r.namespace for r in spark.sql("SHOW DATABASES").collect()]
    assert cfg.database in dbs


def test_register_table_makes_it_queryable(spark, cfg, tmp_path):
    location = str(tmp_path / "silver_test")
    _write_dummy_parquet(spark, location)
    ensure_database(spark, cfg.database)

    spec = TableSpec("silver_trips_test", location, "Mini tabela de teste.")
    register_table(spark, cfg.database, spec, fmt="parquet")

    rows = spark.sql(f"SELECT * FROM {cfg.database}.silver_trips_test ORDER BY id").collect()
    assert [r.id for r in rows] == [1, 2]
    assert [r.name for r in rows] == ["a", "b"]


def test_register_table_is_idempotent(spark, cfg, tmp_path):
    location = str(tmp_path / "silver_idem")
    _write_dummy_parquet(spark, location)
    ensure_database(spark, cfg.database)

    spec = TableSpec("silver_idempotent", location)
    register_table(spark, cfg.database, spec, fmt="parquet")
    # Segunda chamada não pode falhar — CREATE TABLE IF NOT EXISTS.
    register_table(spark, cfg.database, spec, fmt="parquet")

    n = spark.sql(f"SELECT COUNT(*) AS c FROM {cfg.database}.silver_idempotent").first().c
    assert n == 2


def test_external_table_keeps_data_after_drop(spark, cfg, tmp_path):
    """Tabela externa: DROP apaga metadados, NÃO os arquivos físicos."""
    location = str(tmp_path / "silver_ext")
    _write_dummy_parquet(spark, location)
    ensure_database(spark, cfg.database)

    spec = TableSpec("silver_ext", location)
    register_table(spark, cfg.database, spec, fmt="parquet")
    spark.sql(f"DROP TABLE {cfg.database}.silver_ext")

    # Arquivos seguem no path original (essência de uma external table).
    assert any(os.scandir(location))
