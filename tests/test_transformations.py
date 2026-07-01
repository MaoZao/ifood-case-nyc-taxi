"""Testes unitários das transformações puras da camada Silver."""

from __future__ import annotations

from ifood_case.config import REQUIRED_COLUMNS
from ifood_case.transformations import (
    add_partition_columns,
    cast_types,
    clean,
    select_required_columns,
    to_silver,
)


def test_select_keeps_only_required_columns(raw_df):
    out = select_required_columns(raw_df)
    assert set(out.columns) == set(REQUIRED_COLUMNS)
    assert "extra_col" not in out.columns


def test_cast_types(raw_df):
    out = cast_types(select_required_columns(raw_df))
    dtypes = dict(out.dtypes)
    assert dtypes["passenger_count"] == "int"
    assert dtypes["total_amount"] == "double"
    assert dtypes["tpep_pickup_datetime"] == "timestamp"


def test_clean_removes_all_dirty_rows(raw_df):
    out = clean(cast_types(select_required_columns(raw_df)))
    rows = out.collect()
    # Das 7 linhas: dup, negativo, nulo, 0-pax e invertida saem -> sobram 2.
    assert out.count() == 2
    assert all(r["total_amount"] > 0 for r in rows)
    assert all(r["passenger_count"] > 0 for r in rows)
    assert all(r["tpep_dropoff_datetime"] > r["tpep_pickup_datetime"] for r in rows)


def test_clean_dedups(raw_df):
    out = clean(cast_types(select_required_columns(raw_df)))
    # A linha boa de janeiro aparece uma única vez (duplicata removida).
    jan = out.filter("month(tpep_pickup_datetime) = 1")
    assert jan.count() == 1


def test_add_partition_columns(raw_df):
    out = add_partition_columns(cast_types(select_required_columns(raw_df)))
    assert "trip_month" in out.columns
    assert "pickup_hour" in out.columns
    may = out.filter("trip_month = 5 and pickup_hour = 18").collect()
    assert len(may) >= 1


def test_to_silver_end_to_end(raw_df):
    out = to_silver(raw_df)
    assert out.count() == 2
    assert set(REQUIRED_COLUMNS).issubset(set(out.columns))
    assert {"trip_month", "pickup_hour"}.issubset(set(out.columns))


def test_to_silver_respects_custom_window(raw_df):
    # Janela restrita a janeiro: a corrida boa de maio deve ficar de fora.
    out = to_silver(raw_df, start="2023-01-01", end="2023-02-01")
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["trip_month"] == 1
