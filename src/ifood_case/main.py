"""
Orquestrador CLI do pipeline Medallion.

Uso:
    python -m ifood_case.main --stage all      # bronze -> silver -> gold
    python -m ifood_case.main --stage silver   # apenas uma camada
    python -m ifood_case.main --stage gold --no-delta

Em produção, cada `--stage` vira uma task de um orquestrador (Airflow/Databricks
Workflows), com dependências bronze >> silver >> gold.
"""

from __future__ import annotations

import argparse
import logging
import time

from .config import load_config
from .pipeline import bronze, gold, silver
from .spark import build_spark

logger = logging.getLogger(__name__)

STAGES = ["bronze", "silver", "gold"]


def run_stage(stage: str, spark, cfg) -> None:
    t0 = time.time()
    logger.info("=== iniciando estágio: %s ===", stage)
    {"bronze": bronze.run, "silver": silver.run, "gold": gold.run}[stage](spark, cfg)
    logger.info("=== estágio %s concluído em %.1fs ===", stage, time.time() - t0)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    ap = argparse.ArgumentParser(description="Pipeline NYC Taxi (Medallion).")
    ap.add_argument("--stage", choices=["all", *STAGES], default="all")
    ap.add_argument("--config", default=None)
    ap.add_argument("--no-delta", action="store_true", help="Força Parquet em vez de Delta.")
    args = ap.parse_args()

    cfg = load_config(args.config)
    if args.no_delta:
        cfg.storage_format = "parquet"

    spark = build_spark(delta=not args.no_delta)
    spark.sparkContext.setLogLevel("WARN")

    stages = STAGES if args.stage == "all" else [args.stage]
    for stage in stages:
        run_stage(stage, spark, cfg)

    logger.info("Pipeline finalizado. Formato=%s | Gold em %s", cfg.storage_format, cfg.paths.gold)


if __name__ == "__main__":
    main()
