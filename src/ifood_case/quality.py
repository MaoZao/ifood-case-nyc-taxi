"""
Data Quality — checagens declarativas no estilo Great Expectations, porém sem
dependência pesada (mantém o projeto leve e o CI rápido).

Cada `Expectation` declara UMA expressão de agregação e um validador sobre o
resultado. O runner combina todas as expressões em UM único `df.agg(...)` —
uma única passada/job Spark sobre o DataFrame, em vez de um `count()` por
checagem (que recomputaria a linhagem inteira N vezes). Usado como *gate*
entre camadas: nenhum dado sobe para Gold sem passar pelos contratos da Silver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple

from pyspark.sql import Column, DataFrame
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


@dataclass(frozen=True)
class Expectation:
    """Uma checagem = fábrica de expressões de agregação + validador.

    ``exprs`` é uma FÁBRICA (não as Columns prontas): PySpark exige um
    SparkContext ativo para criar Column, então a construção é adiada até o
    ``run_checks``. Retorna {alias: Column de agregação}; ``validate`` recebe
    o dict {alias: valor} da linha agregada e devolve ``(passou, detalhe)``.
    """

    name: str
    exprs: Callable[[], Dict[str, Column]]
    validate: Callable[[dict], Tuple[bool, str]]
    critical: bool = True


def _count_where(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def _validate_no_nulls(row: dict) -> Tuple[bool, str]:
    offenders = {
        k.removeprefix("nulls__"): v
        for k, v in row.items()
        if k.startswith("nulls__") and v and v > 0
    }
    return (not offenders, "ok" if not offenders else f"nulos: {offenders}")


SILVER_EXPECTATIONS: List[Expectation] = [
    Expectation(
        "dataset_nao_vazio",
        lambda: {"total_rows": F.count(F.lit(1))},
        lambda row: (row["total_rows"] > 0, f"linhas: {row['total_rows']}"),
    ),
    Expectation(
        "sem_nulos_em_obrigatorias",
        lambda: {f"nulls__{c}": F.sum(F.col(c).isNull().cast("int")) for c in REQUIRED_COLUMNS},
        _validate_no_nulls,
    ),
    Expectation(
        "total_amount_positivo",
        lambda: {"bad_amount": _count_where(F.col("total_amount") <= 0)},
        lambda row: (not row["bad_amount"], f"{row['bad_amount'] or 0} linhas <= 0"),
    ),
    Expectation(
        "passenger_count_positivo",
        lambda: {"bad_pax": _count_where(F.col("passenger_count") <= 0)},
        lambda row: (not row["bad_pax"], f"{row['bad_pax'] or 0} linhas <= 0"),
    ),
    Expectation(
        "dropoff_apos_pickup",
        lambda: {
            "bad_chrono": _count_where(
                F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")
            )
        },
        lambda row: (not row["bad_chrono"], f"{row['bad_chrono'] or 0} viagens invertidas"),
    ),
]


def check_columns_present(df: DataFrame) -> CheckResult:
    """Checagem de schema — não precisa de job Spark, só de ``df.columns``."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return CheckResult(
        "colunas_obrigatorias_presentes",
        not missing,
        "ok" if not missing else f"faltando: {missing}",
    )


def run_checks(
    df: DataFrame,
    expectations: List[Expectation] | None = None,
    raise_on_fail: bool = True,
) -> List[CheckResult]:
    """Roda todas as checagens em UMA única passada sobre o DataFrame.

    1. Valida o schema (sem job). Se faltar coluna obrigatória, falha aqui —
       nem tenta agregar (evitaria AnalysisException confusa).
    2. Combina as expressões de todas as expectativas num único ``df.agg()``.
    3. Cada expectativa valida sua fatia do resultado agregado.
    """
    expectations = SILVER_EXPECTATIONS if expectations is None else expectations
    results = [check_columns_present(df)]

    if results[0].passed:
        built = [(e, e.exprs()) for e in expectations]
        all_exprs = [col.alias(alias) for _, exprs in built for alias, col in exprs.items()]
        row_obj = df.agg(*all_exprs).first()
        row = row_obj.asDict() if row_obj else {}
        for e, _ in built:
            passed, detail = e.validate(row)
            results.append(CheckResult(e.name, passed, detail, e.critical))

    for r in results:
        level = logging.INFO if r.passed else logging.ERROR
        logger.log(level, "[DQ] %-32s %s | %s", r.name, "PASS" if r.passed else "FAIL", r.detail)
    failed = [r for r in results if not r.passed and r.critical]
    if failed and raise_on_fail:
        raise DataQualityError(
            f"{len(failed)} checagem(ns) crítica(s) falharam: {[r.name for r in failed]}"
        )
    return results
