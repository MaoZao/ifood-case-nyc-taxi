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


def test_missing_column_fails_schema_check_without_exception(raw_df):
    # Sem uma coluna obrigatória, o gate deve reportar a falha de schema —
    # e NÃO estourar AnalysisException tentando agregar a coluna ausente.
    df = raw_df.drop("total_amount")
    results = run_checks(df, raise_on_fail=False)
    schema_check = next(r for r in results if r.name == "colunas_obrigatorias_presentes")
    assert not schema_check.passed
    assert "total_amount" in schema_check.detail


def test_checks_run_in_single_job(raw_df):
    # As checagens de dados devem consolidar-se em UM único df.agg — o
    # resultado deve conter exatamente 1 checagem de schema + 1 por expectativa.
    from ifood_case.quality import SILVER_EXPECTATIONS
    from ifood_case.transformations import to_silver

    results = run_checks(to_silver(raw_df), raise_on_fail=True)
    assert len(results) == 1 + len(SILVER_EXPECTATIONS)
