"""Testes dos contratos de Data Quality."""
from __future__ import annotations

import pytest

from ifood_case.quality import DataQualityError, run_checks
from ifood_case.transformations import to_silver


def test_silver_passes_all_checks(raw_df):
    silver = to_silver(raw_df)
    results = run_checks(silver, raise_on_fail=True)
    assert all(r.passed for r in results)


def test_checks_raise_on_dirty_data(raw_df):
    # Pulando a limpeza, os contratos devem falhar (dados sujos).
    from ifood_case.transformations import cast_types, select_required_columns

    dirty = cast_types(select_required_columns(raw_df))
    with pytest.raises(DataQualityError):
        run_checks(dirty, raise_on_fail=True)


def test_checks_report_without_raising(raw_df):
    from ifood_case.transformations import cast_types, select_required_columns

    dirty = cast_types(select_required_columns(raw_df))
    results = run_checks(dirty, raise_on_fail=False)
    assert any(not r.passed for r in results)
