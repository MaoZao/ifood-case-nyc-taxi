# Databricks notebook source
# MAGIC %md
# MAGIC # iFood Case — Análise Exploratória (NYC Yellow Taxi 2023)
# MAGIC
# MAGIC Notebook compatível com **Databricks Community Edition** (importe via
# MAGIC *Workspace > Import*) e também executável localmente com `pyspark`.

# MAGIC
# MAGIC Fluxo: leitura da camada **Silver/Gold** → perfilagem → respostas do case
# MAGIC (Q1 e Q2) → gráficos. Decisões técnicas estão comentadas ao longo do caminho.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup
# MAGIC No Databricks, a SparkSession (`spark`) já existe. Localmente, criamos uma.

# COMMAND ----------
try:
    spark  # type: ignore  # noqa: F821 - injetada pelo Databricks
except NameError:
    import sys, os
    sys.path.append(os.path.abspath("../../src"))
    from ifood_case.spark import build_spark
    spark = build_spark()

from pyspark.sql import functions as F

# Ajuste o caminho conforme seu ambiente (DBFS, S3 ou local).
SILVER_PATH = os.getenv("IFOOD_SILVER", "../../data/silver")
FORMAT = os.getenv("IFOOD_FORMAT", "delta")

silver = spark.read.format(FORMAT).load(SILVER_PATH)
silver.createOrReplaceTempView("silver_trips")
print(f"Linhas na Silver: {silver.count():,}")
silver.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Perfilagem rápida
# MAGIC Distribuição por mês, sanidade de tipos e ausência de nulos (já tratados
# MAGIC na Silver). Confirmamos que a limpeza funcionou.

# COMMAND ----------
display(silver.groupBy("trip_month").count().orderBy("trip_month")) if "display" in dir() else \
    silver.groupBy("trip_month").count().orderBy("trip_month").show()

silver.select(
    F.min("total_amount").alias("min_amount"),
    F.max("total_amount").alias("max_amount"),
    F.round(F.avg("total_amount"), 2).alias("avg_amount"),
    F.min("passenger_count").alias("min_pax"),
    F.max("passenger_count").alias("max_pax"),
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1b. Outliers de `total_amount` — a média de Q1 é confiável?
# MAGIC O máximo observado (milhares de US$) sugere cauda direita longa. Os
# MAGIC percentis mostram o quão extremos são esses valores, e a comparação
# MAGIC média × mediana quantifica a sensibilidade da resposta de Q1 a eles.

# COMMAND ----------
pcts = silver.approxQuantile("total_amount", [0.5, 0.9, 0.99, 0.999], 0.001)
print(f"p50={pcts[0]:.2f}  p90={pcts[1]:.2f}  p99={pcts[2]:.2f}  p99.9={pcts[3]:.2f}")
# Média x mediana por mês: se divergirem muito, a média (pedida no case) deve
# ser lida com a cauda em mente — mantemos a média por ser o que o case pede,
# mas registramos a distorção.
spark.sql("""
    SELECT trip_month AS mes,
           ROUND(AVG(total_amount), 2)                              AS media,
           ROUND(PERCENTILE_APPROX(total_amount, 0.5), 2)           AS mediana,
           ROUND(AVG(total_amount) - PERCENTILE_APPROX(total_amount, 0.5), 2) AS distorcao_cauda
    FROM silver_trips GROUP BY trip_month ORDER BY trip_month
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1c. Ocupação por dia da semana × VendorID
# MAGIC Duas dimensões baratas que contextualizam Q2: fim de semana × dia útil
# MAGIC muda o perfil de ocupação, e o VendorID é a única dimensão categórica
# MAGIC disponível na Silver.

# COMMAND ----------
spark.sql("""
    SELECT dayofweek(tpep_pickup_datetime) AS dia_semana,     -- 1=Dom … 7=Sáb
           COUNT(*) AS corridas,
           ROUND(AVG(passenger_count), 3) AS media_pax
    FROM silver_trips WHERE trip_month = 5
    GROUP BY dia_semana ORDER BY dia_semana
""").show()

spark.sql("""
    SELECT VendorID, COUNT(*) AS corridas,
           ROUND(AVG(passenger_count), 3) AS media_pax,
           ROUND(AVG(total_amount), 2)    AS ticket_medio
    FROM silver_trips GROUP BY VendorID ORDER BY corridas DESC
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Q1 — Média de `total_amount` por mês (toda a frota)

# COMMAND ----------
q1 = spark.sql("""
    SELECT trip_month AS mes,
           COUNT(*) AS qtd_corridas,
           ROUND(AVG(total_amount), 2) AS receita_media_usd
    FROM silver_trips
    GROUP BY trip_month
    ORDER BY trip_month
""")
q1.show()
media_global = silver.select(F.round(F.avg("total_amount"), 2)).first()[0]
print(f"Média global Jan-Mai/2023: US$ {media_global}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Q2 — Média de passageiros por hora do dia (maio)

# COMMAND ----------
q2 = spark.sql("""
    SELECT pickup_hour AS hora,
           COUNT(*) AS qtd_corridas,
           ROUND(AVG(passenger_count), 3) AS media_passageiros
    FROM silver_trips
    WHERE trip_month = 5
    GROUP BY pickup_hour
    ORDER BY hora
""")
q2.show(24)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Visualização
# MAGIC No Databricks, use o botão de gráfico em `display()`. Localmente, matplotlib.

# COMMAND ----------
import matplotlib.pyplot as plt

p1 = q1.toPandas()
p2 = q2.toPandas()

fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].bar(p1["mes"], p1["receita_media_usd"], color="#EA1D2C")
ax[0].axhline(media_global, ls="--", color="#333", label=f"Média {media_global}")
ax[0].set_title("Q1 — Receita média por mês (US$)"); ax[0].legend()
ax[1].plot(p2["hora"], p2["media_passageiros"], marker="o", color="#EA1D2C")
ax[1].set_title("Q2 — Média de passageiros por hora (maio)")
ax[1].set_xlabel("Hora do dia")
plt.tight_layout()
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Conclusões (dados REAIS, Jan–Mai/2023 — ver outputs acima)
# MAGIC - **Q1:** o ticket médio sobe de Jan a Mai com um leve **recuo em
# MAGIC   fevereiro** — coerente com o mês mais curto e clima; a tendência de
# MAGIC   alta retoma a partir de março. A média mensal é puxada por uma cauda
# MAGIC   direita longa (p99.9 ≫ mediana, ver §1b): para previsão de receita
# MAGIC   vale acompanhar média E mediana.
# MAGIC - **Q2:** a ocupação por corrida varia ~**1,26–1,46** ao longo do dia,
# MAGIC   com padrão claro: **máximos de madrugada (0h–3h)** — grupos voltando
# MAGIC   de bares/eventos dividem o táxi — e **mínimo às 5h–6h** (~1,26),
# MAGIC   quando predominam commuters viajando sozinhos. O *volume* de corridas,
# MAGIC   por outro lado, tem pico no fim de tarde (17h–18h). Ocupação e demanda
# MAGIC   têm perfis diferentes — insumo distinto para pricing dinâmico
# MAGIC   (ocupação) vs dimensionamento de frota (volume).
