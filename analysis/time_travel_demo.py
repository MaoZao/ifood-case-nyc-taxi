"""
Demo de Time Travel sobre a tabela Silver (Delta Lake).

Mostra os 3 superpoderes do Delta que justificam o Lakehouse:
  1) DESCRIBE HISTORY        — quem mudou o que e quando (auditoria).
  2) VERSION AS OF / TIMESTAMP AS OF — consultar versões passadas (debug).
  3) Diff entre versões      — quanto a KPI mudou após um deploy.

Como rodar:
    python analysis/time_travel_demo.py                  # snapshot atual
    python analysis/time_travel_demo.py --version 2      # versão específica
    python analysis/time_travel_demo.py --diff 1 3       # delta entre v1 e v3
    python analysis/time_travel_demo.py --export dashboard/data/history.json

O export JSON alimenta o dashboard (aba "Time Travel"), permitindo ao usuário
explorar versões da Silver visualmente.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ifood_case.config import load_config
from ifood_case.spark import build_spark

logger = logging.getLogger(__name__)


def show_history(spark: SparkSession, path: str, limit: int = 20) -> list[dict]:
    """Lê o histórico Delta (operação, timestamp, métricas)."""
    history = (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        .select(
            "version",
            "timestamp",
            "operation",
            "operationMetrics",
            "userName",
        )
        .orderBy(F.desc("version"))
        .limit(limit)
    )
    rows = [r.asDict(recursive=True) for r in history.collect()]
    # timestamps -> ISO string para serializar
    for r in rows:
        if r.get("timestamp") is not None:
            r["timestamp"] = r["timestamp"].isoformat()
    return rows


def read_version(spark: SparkSession, path: str, version: int):
    """Lê o snapshot na versão dada — read-only, sem reprocessar nada."""
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def kpi_snapshot(df) -> dict:
    """Computa Q1 (receita média/mês) sobre o snapshot — KPI sintético."""
    agg = (
        df.groupBy("trip_month")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.avg("total_amount"), 2).alias("receita_media_usd"),
        )
        .orderBy("trip_month")
        .collect()
    )
    return {
        "linhas": df.count(),
        "por_mes": [r.asDict() for r in agg],
    }


def diff_versions(spark: SparkSession, path: str, v1: int, v2: int) -> dict:
    """Compara KPI Q1 entre duas versões."""
    k1 = kpi_snapshot(read_version(spark, path, v1))
    k2 = kpi_snapshot(read_version(spark, path, v2))
    return {"version_a": v1, "version_b": v2, "kpi_a": k1, "kpi_b": k2}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    ap = argparse.ArgumentParser(description="Time Travel demo sobre a Silver.")
    ap.add_argument("--config", default=None)
    ap.add_argument("--version", type=int, help="Lê apenas esta versão.")
    ap.add_argument(
        "--diff", type=int, nargs=2, metavar=("V1", "V2"), help="Compara duas versões."
    )
    ap.add_argument("--limit", type=int, default=20, help="Linhas do histórico.")
    ap.add_argument("--export", default=None, help="Path JSON p/ dashboard.")
    args = ap.parse_args()

    cfg = load_config(args.config)
    if cfg.storage_format != "delta":
        raise SystemExit("Time Travel exige storage_format=delta no pipeline.yaml.")

    spark = build_spark(delta=True)
    spark.sparkContext.setLogLevel("WARN")

    path = cfg.paths.silver
    out: dict = {"path": path, "history": show_history(spark, path, args.limit)}

    if args.version is not None:
        df = read_version(spark, path, args.version)
        out["snapshot"] = {"version": args.version, **kpi_snapshot(df)}

    if args.diff:
        out["diff"] = diff_versions(spark, path, args.diff[0], args.diff[1])

    if args.export:
        Path(args.export).parent.mkdir(parents=True, exist_ok=True)
        with open(args.export, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=2, ensure_ascii=False)
        logger.info("[time-travel] exportado para %s", args.export)
    else:
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
