"""Testes unitários do Silver incremental.

Foco em invariantes que NÃO dependem do runtime Delta (ausente nos workers de CI):
  - chave de MERGE bem-definida e não-vazia.
  - guard contra storage_format=parquet.
  - função _table_exists retorna False para path inexistente.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ifood_case.config import Config, Paths
from ifood_case.pipeline import silver_incremental


def _cfg(tmp_path: Path, fmt: str = "delta") -> Config:
    return Config(
        months=["2023-01"],
        base_url="http://example.com",
        paths=Paths(
            landing=str(tmp_path / "landing"),
            bronze=str(tmp_path / "bronze"),
            silver=str(tmp_path / "silver"),
            gold=str(tmp_path / "gold"),
        ),
        storage_format=fmt,
    )


def test_merge_keys_are_defined():
    keys = silver_incremental.MERGE_KEYS
    assert len(keys) >= 2, "chave de merge precisa de pelo menos 2 colunas"
    assert "VendorID" in keys
    assert any("pickup" in k for k in keys), "deve incluir o instante de pickup"


def test_run_blocks_when_format_is_parquet(spark, tmp_path):
    cfg = _cfg(tmp_path, fmt="parquet")
    with pytest.raises(RuntimeError, match="storage_format=delta"):
        silver_incremental.run(spark, cfg)


def test_table_exists_returns_false_for_missing_path(spark, tmp_path):
    assert silver_incremental._table_exists(spark, str(tmp_path / "nope")) is False
