"""
Data Quality — checagens declarativas no estilo Great Expectations, porém sem
dependência pesada (mantém o projeto leve e o CI rápido).

Cada `Expectation` retorna um resultado; o runner agrega tudo e levanta
`DataQualityError` se algo crítico falhar. Usado como *gate* entre camadas:
nenhum dado sobe para Gold sem passar pelos contratos da Silver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import REQUIRED_COLUMNS

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Levantada quando uma expectativa crítica falha."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    critical: bool = True


def expect_columns_present(df: DataFrame) -> CheckResult:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return CheckResult(
        "colunas_obrigatorias_presentes",
        not missing,
        "ok" if not missing else f"faltando: {missing}",
    )


def expect_no_nulls_in_required(df: DataFrame) -> CheckResult:
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLUMNS]
    first = df.select(exprs).first()
    row = first.asDict() if first else {}
    offenders = {k: v for k, v in row.items() if v and v > 0}
    return CheckResult(
        "sem_nulos_em_obrigatorias",
        not offenders,
        "ok" if not offenders else f"nulos: {offenders}",
    )


def expect_positive_total_amount(df: DataFrame) -> CheckResult:
    bad = df.filter(F.col("total_amount") <= 0).count()
    return CheckResult("total_amount_positivo", bad == 0, f"{bad} linhas <= 0")


def expect_positive_passengers(df: DataFrame) -> CheckResult:
    bad = df.filter(F.col("passenger_count") <= 0).count()
    return CheckResult("passenger_count_positivo", bad == 0, f"{bad} linhas <= 0")


def expect_chronological_trips(df: DataFrame) -> CheckResult:
    bad = df.filter(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).count()
    return CheckResult("dropoff_apos_pickup", bad == 0, f"{bad} viagens invertidas")


def expect_non_empty(df: DataFrame) -> CheckResult:
    n = df.limit(1).count()
    return CheckResult("dataset_nao_vazio", n > 0, f"linhas>0: {n>0}")


SILVER_CHECKS: List[Callable[[DataFrame], CheckResult]] = [
    expect_non_empty,
    expect_columns_present,
    expect_no_nulls_in_required,
    expect_positive_total_amount,
    expect_positive_passengers,
    expect_chronological_trips,
]


def run_checks(
    df: DataFrame, checks=SILVER_CHECKS, raise_on_fail: bool = True
) -> List[CheckResult]:
    results = [check(df) for check in checks]
    for r in results:
        level = logging.INFO if r.passed else logging.ERROR
        logger.log(level, "[DQ] %-32s %s | %s", r.name, "PASS" if r.passed else "FAIL", r.detail)
    failed = [r for r in results if not r.passed and r.critical]
    if failed and raise_on_fail:
        raise DataQualityError(
            f"{len(failed)} checagem(ns) crítica(s) falharam: " f"{[r.name for r in failed]}"
        )
    return results
