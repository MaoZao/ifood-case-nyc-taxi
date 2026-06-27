"""
Orquestrador CLI do pipeline Medallion.

Uso:
    python -m ifood_case.main --stage all                  # bronze -> silver -> gold
    python -m ifood_case.main --stage silver               # apenas uma camada
    python -m ifood_case.main --stage gold --no-delta
    python -m ifood_case.main --stage silver --mode incremental   # MERGE INTO
    python -m ifood_case.main --stage catalog              # registra tabelas no metastore
    python -m ifood_case.main --stage all --register-catalog       # tudo + catalog ao final

Em produção, cada `--stage` vira uma task de um orquestrador (Airflow/Databricks
Workflows), com dependências bronze >> silver >> gold >> catalog.
"""

from __future__ import annotations

import argparse
import logging
import time

from . import catalog
from .config import load_config
from .pipeline import bronze, gold, silver, silver_incremental
from .spark import build_spark

logger = logging.getLogger(__name__)

STAGES = ["bronze", "silver", "gold", "catalog"]


def run_stage(stage: str, spark, cfg, mode: str = "full") -> None:
    t0 = time.time()
    logger.info("=== iniciando estágio: %s (mode=%s) ===", stage, mode)

    if stage == "silver" and mode == "incremental":
        stats = silver_incremental.run(spark, cfg)
        logger.info("[silver-inc] stats: %s", stats)
    elif stage == "catalog":
        registered = catalog.run(spark, cfg)
        logger.info("[catalog] %d tabela(s) registrada(s): %s", len(registered), list(registered))
    else:
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
    ap.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Silver: full refresh ou MERGE incremental (exige Delta).",
    )
    ap.add_argument(
        "--register-catalog",
        action="store_true",
        help="Registra as tabelas no metastore ao final do pipeline.",
    )
    args = ap.parse_args()

    cfg = load_config(args.config)
    if args.no_delta:
        cfg.storage_format = "parquet"
        if args.mode == "incremental":
            raise SystemExit("--mode incremental exige Delta. Remova --no-delta.")

    spark = build_spark(delta=not args.no_delta, warehouse_dir=cfg.warehouse_dir)
    spark.sparkContext.setLogLevel("WARN")

    if args.stage == "all":
        stages = ["bronze", "silver", "gold"]
        if args.register_catalog:
            stages.append("catalog")
    else:
        stages = [args.stage]

    for stage in stages:
        run_stage(stage, spark, cfg, mode=args.mode)

    logger.info("Pipeline finalizado. Formato=%s | Gold em %s", cfg.storage_format, cfg.paths.gold)


if __name__ == "__main__":
    main()
