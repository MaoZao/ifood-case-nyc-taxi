"""
Respostas do case em PySpark (DataFrame API + Spark SQL), lado a lado.

Demonstra que a camada de consumo serve tanto usuários de SQL quanto de
PySpark — uma exigência explícita do case ("a escolha da linguagem de consulta
fica a seu critério"). Roda sobre a Silver/Gold e também exporta os resultados
para JSON, alimentando o dashboard.

Uso:
    python -m analysis.answers              # usa conf/pipeline.yaml
    python analysis/answers.py --export dashboard/data/kpis.json
"""

from __future__ import annotations

import argparse
import json
import logging

from pyspark.sql import functions as F

from ifood_case.config import load_config
from ifood_case.spark import build_spark

logger = logging.getLogger(__name__)
MESES = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai"}


def q1_dataframe_api(silver):
    """Q1 via DataFrame API."""
    return (
        silver.groupBy("trip_month")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.avg("total_amount"), 2).alias("receita_media_usd"),
        )
        .orderBy("trip_month")
    )


def q1_spark_sql(spark, silver):
    """Q1 via Spark SQL — mesmo resultado, sintaxe SQL pura."""
    silver.createOrReplaceTempView("silver_trips")
    return spark.sql(
        """
        SELECT trip_month AS mes,
               COUNT(*) AS qtd_corridas,
               ROUND(AVG(total_amount), 2) AS receita_media_usd
        FROM silver_trips
        GROUP BY trip_month
        ORDER BY trip_month
        """
    )


def q2_dataframe_api(silver):
    """Q2 via DataFrame API."""
    return (
        silver.filter(F.col("trip_month") == 5)
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.avg("passenger_count"), 3).alias("media_passageiros"),
        )
        .orderBy("pickup_hour")
    )


def run(config_path: str | None, export: str | None) -> None:
    cfg = load_config(config_path)
    spark = build_spark(delta=cfg.storage_format == "delta")
    spark.sparkContext.setLogLevel("WARN")

    # Cache: Q1, média global, equivalência SQL e Q2 varrem a mesma Silver —
    # materializa uma vez, reutiliza em todos os jobs da sessão.
    silver = spark.read.format(cfg.storage_format).load(cfg.paths.silver).cache()

    print("\n========== Q1: média de total_amount por mês ==========")
    q1 = q1_dataframe_api(silver)
    q1_rows = q1.collect()  # coletado UMA vez; reutilizado no assert e no export
    q1.show()
    media_global = silver.select(F.round(F.avg("total_amount"), 2)).first()[0]
    print(f">> Média global do período (Jan-Mai/2023): US$ {media_global}")

    # Confirma equivalência DataFrame API == Spark SQL.
    assert q1_rows == q1_spark_sql(spark, silver).collect(), "Divergência API vs SQL!"
    print(">> DataFrame API e Spark SQL retornam resultados idênticos. ✓")

    print("\n========== Q2: média de passageiros por hora (maio) ==========")
    q2 = q2_dataframe_api(silver)
    q2_rows = q2.collect()
    q2.show(24)

    if export:
        payload = {
            # Meta REAL da execução: sem ela, o dashboard exibiria o meta do
            # fallback sintético ao lado de KPIs reais (números incoerentes).
            "meta": {
                "linhas_silver": sum(r["qtd_corridas"] for r in q1_rows),
                "periodo": f"{min(cfg.months)} a {max(cfg.months)}",
                "nota": "KPIs gerados por analysis/answers.py sobre a camada Silver.",
            },
            "q1_receita_mensal": [
                {
                    "mes_num": r["trip_month"],
                    "mes": MESES.get(r["trip_month"], str(r["trip_month"])),
                    "corridas": r["qtd_corridas"],
                    "receita_media_usd": r["receita_media_usd"],
                }
                for r in q1_rows
            ],
            "q1_media_global_usd": media_global,
            "q2_passageiros_hora_maio": [
                {
                    "hora": r["pickup_hour"],
                    "corridas": r["qtd_corridas"],
                    "media_passageiros": r["media_passageiros"],
                }
                for r in q2_rows
            ],
        }
        with open(export, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        print(f"\n>> KPIs exportados para {export}")

    spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.WARN)
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--export", default=None, help="Caminho do JSON de KPIs (dashboard).")
    args = ap.parse_args()
    run(args.config, args.export)


if __name__ == "__main__":
    main()
