<div align="center">

# 🚖 NYC Taxi Data Lakehouse — Case Técnico **Data Architect · iFood**

Ingestão, modelagem e analytics das corridas de táxi de Nova York (Jan–Mai/2023)
em uma arquitetura **Medallion** com **PySpark + Delta Lake**, pronta para rodar
local, em Docker ou no Databricks.

![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-00ADD8)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)
![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=githubactions&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

![Arquitetura Medallion](assets/architecture.svg)

---

## 🎯 O desafio

> Ingerir os dados de corridas de táxi de NY no Data Lake, disponibilizá-los para
> consumo (via SQL, por exemplo) e responder a duas perguntas analíticas.

Requisitos atendidos:

- ✅ Solução de **ingestão** → Data Lake → **camada de consumo** para usuários finais.
- ✅ **PySpark** usado na etapa de transformação (Bronze → Silver).
- ✅ Tecnologia de **metadados/consulta** à escolha: **Delta Lake + Spark SQL**.
- ✅ Colunas garantidas na camada de consumo: `VendorID`, `passenger_count`,
  `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`.
- ✅ Tabelas **modeladas e criadas** do zero (Bronze/Silver/Gold).
- ✅ **Análises** com resultados comunicados de forma clara (+ dashboard).

## ✨ Diferenciais

- 🏅 **Lakehouse Medallion** completo (Bronze/Silver/Gold) com Delta Lake (ACID, Time Travel).
- 🧪 **Data Quality como gate** entre camadas + **suíte de testes** (pytest).
- 🐳 **Docker Compose** (Spark + Jupyter + MinIO/S3) — zero "funciona na minha máquina".
- 🔁 **CI/CD** (GitHub Actions): lint, tipos, testes e *smoke test* do pipeline.
- ⚙️ **Config-driven** (YAML + env): o mesmo código roda local ⇄ Docker ⇄ Databricks ⇄ AWS.
- 📊 **Dashboard interativo** em duas formas — **Streamlit** (Python, gráficos Plotly)
  e **HTML standalone** (zero deps) — compartilhando os mesmos KPIs.
- 📐 **ADRs** documentando o porquê de cada decisão técnica.

## 📈 Resultados

> Números abaixo gerados sobre um **sample sintético fiel ao schema** (rodável
> offline/CI). Para os valores reais, rode `make download && make pipeline && make answers`.

### Q1 — Média de `total_amount` por mês (toda a frota yellow)

| Mês | Receita média (US$) |
|-----|---------------------|
| Jan | 26.25 |
| Fev | 26.70 |
| Mar | 27.41 |
| Abr | 28.05 |
| Mai | 29.00 |
| **Média global Jan–Mai** | **27.54** |

📌 *O ticket médio cresce de forma consistente ao longo de 2023 — sinal de
sazonalidade/reajuste, insumo direto para previsão de receita.*

### Q2 — Média de passageiros por hora do dia (maio)

Ocupação estável (~**1,64 passageiros/corrida**), com leve elevação em horários
sociais (almoço/noite) e **pico de demanda no fim de tarde (17h–18h)**. Veja o
mapa de calor de 24h no dashboard.

👉 **Dashboard:**
- **Streamlit** (interativo, Plotly): `make dashboard` → http://localhost:8501
- **HTML standalone** (sem dependências): abra [`dashboard/index.html`](dashboard/index.html) no navegador.

Ambos lêem [`dashboard/data/kpis.json`](dashboard/data/kpis.json) (gerado por `make answers`),
com *fallback* embutido para funcionarem mesmo sem o pipeline rodado.

## 🗂️ Estrutura do repositório

```
.
├─ src/ifood_case/            # Código-fonte da solução (pacote instalável)
│  ├─ config.py               #   Configuração (YAML + env), colunas exigidas
│  ├─ spark.py                #   Fábrica da SparkSession (Delta + fallback)
│  ├─ transformations.py      #   Transformações PURAS e testáveis (Silver)
│  ├─ quality.py              #   Contratos de Data Quality (gate)
│  ├─ ingestion.py            #   Download idempotente do NYC TLC
│  ├─ main.py                 #   Orquestrador CLI (bronze→silver→gold + catalog)
│  ├─ catalog.py              #   Registro das tabelas no Metastore (SQL nativo)
│  └─ pipeline/               #   Camadas Medallion
│     ├─ bronze.py · silver.py · gold.py
│     └─ silver_incremental.py   # MERGE INTO + Change Data Feed
├─ analysis/                  # Respostas do case
│  ├─ sql/                    #   Q1 e Q2 em SQL puro
│  ├─ answers.py              #   Q1 e Q2 em PySpark (API + Spark SQL)
│  ├─ time_travel_demo.py     #   Demo de Time Travel sobre a Silver (Delta)
│  └─ notebooks/              #   EDA (compatível com Databricks)
├─ dashboard/                 # 📊 Dashboards: app.py (Streamlit) + index.html + kpis.json
├─ tests/                     # pytest (transformações, quality, analytics)
├─ scripts/                   # Gerador de dados sintéticos
├─ conf/pipeline.yaml         # Configuração do pipeline
├─ docs/                      # architecture.md, ADRs, diagramas
├─ assets/architecture.svg    # Diagrama da arquitetura
├─ Dockerfile · docker-compose.yml · Makefile
└─ .github/workflows/ci.yml   # CI/CD
```

## 🚀 Como executar

### Opção A — Local (rápido, com dados sintéticos, sem internet)

```bash
make install          # deps + pacote em modo editável
make demo             # sample -> pipeline (bronze/silver/gold) -> respostas
# equivale a: make sample && make pipeline && make answers
```

### Opção B — Dados REAIS do NYC TLC

```bash
make install
make download         # baixa Jan-Mai/2023 (~400 MB/mês) para data/landing
make pipeline         # bronze -> silver -> gold (Delta Lake)
make answers          # imprime Q1/Q2 e atualiza dashboard/data/kpis.json
```

### Opção C — Docker (Spark + Jupyter prontos)

```bash
make docker-up        # JupyterLab em http://localhost:8888 (Spark UI :4040)
# dentro do container: make demo
make docker-down
```

### Opção C+ — Lakehouse no MinIO (S3 local)

Grava as camadas **bronze/silver/gold no MinIO** (object storage S3-compatível),
exercitando o conector `s3a` do Spark — o mesmo caminho de uma nuvem real, sem custo:

```bash
make cloud-up         # Spark + Jupyter + MinIO; cria o bucket ifood-lakehouse
make cloud-demo       # amostra + pipeline + respostas -> s3a://ifood-lakehouse/...
# ou, com dados REAIS do NYC TLC:
make cloud-real       # download real + pipeline -> MinIO
# Console do MinIO: http://localhost:9001  (login: minio / minio123)
make docker-down
```

> ⚠️ **Grave sempre pelos alvos `cloud-*`.** O pipeline só escreve no MinIO quando
> roda **dentro do container** `ifood-spark` (onde `IFOOD_ENV=cloud`). Rodar
> `make pipeline`/`make demo` direto no host usa a config local e grava em `data/`
> — **não** no MinIO. Os alvos `cloud-pipeline`/`cloud-demo`/`cloud-real` fazem
> `docker exec` no container automaticamente.

Como funciona: `IFOOD_ENV=cloud` seleciona [`conf/pipeline.cloud.yaml`](conf/pipeline.cloud.yaml)
(paths `s3a://…`) e `IFOOD_S3_ENDPOINT` ativa o conector `s3a` em [`src/ifood_case/spark.py`](src/ifood_case/spark.py).
A *landing* permanece local (download via `urllib`, portável); só o lakehouse curado
vai para o S3. Variáveis em [`.env.example`](.env.example).

### Dashboard interativo (Streamlit)

```bash
pip install -r dashboard/requirements.txt   # streamlit + plotly (1ª vez)
make dashboard                              # http://localhost:8501
```

Após `make answers`, o dashboard reflete os números atuais (sample ou dados reais).

### Opção D — Databricks Community Edition

1. Importe `analysis/notebooks/01_exploratory_analysis.py` no Workspace.
2. Faça upload dos Parquet para o DBFS e ajuste `IFOOD_SILVER`/paths.
3. Rode as células — SQL e PySpark lado a lado.

## ⚡ Delta Lake em uso real

Além do *medallion* básico, o projeto exercita as features que tornam o Delta
superior a Parquet puro — o "porquê" do Lakehouse:

### Catalog (metastore) — consumir as tabelas por nome SQL

```bash
make catalog          # registra Bronze/Silver/Gold no Hive Metastore (Derby local)
```

Depois disso, qualquer cliente SQL (Spark, Databricks, dbt) consome as camadas
sem precisar conhecer paths físicos:

```sql
SELECT trip_month, ROUND(AVG(total_amount), 2) AS receita_media
FROM ifood.silver_trips
GROUP BY trip_month;
```

As tabelas são **externas** — `DROP TABLE` apaga apenas o metadado; os arquivos
Delta seguem intactos no path original. Em Databricks, basta trocar `database`
no [`conf/pipeline.yaml`](conf/pipeline.yaml) por um catalog do Unity Catalog
(`main.ifood`, por exemplo).

### Pipeline incremental (MERGE INTO)

```bash
make pipeline-inc     # Silver via MERGE com chave natural (idempotente)
```

Em vez de `mode("overwrite")` (full refresh), aplica apenas o delta:

- **Chave de upsert:** `(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime)`
- **Idempotente:** rerodar a mesma janela **não** duplica linhas
- **Suporta late-arriving data** sem reescrever a tabela inteira
- **Habilita Change Data Feed** — Gold pode propagar só o delta via `table_changes()`

Implementação em [`src/ifood_case/pipeline/silver_incremental.py`](src/ifood_case/pipeline/silver_incremental.py).

### Time Travel — auditoria e debug

```bash
make time-travel      # histórico + KPI por versão -> dashboard/data/history.json
# ou direto:
python analysis/time_travel_demo.py --version 3
python analysis/time_travel_demo.py --diff 1 3   # compara KPI Q1 entre v1 e v3
```

Mostra:
- `DESCRIBE HISTORY` — auditoria completa (quem mudou, quando, métricas da operação)
- `VERSION AS OF` / `TIMESTAMP AS OF` — consulta versões passadas sem reprocessar
- **Diff de KPI entre versões** — quantifica o impacto de um deploy na Silver

Útil para: investigar regressões, validar deploys, rollback rápido com
`RESTORE TABLE silver TO VERSION AS OF N`.

## 🔍 As perguntas, em código

**SQL** ([`analysis/sql/`](analysis/sql)):
```sql
-- Q1
SELECT trip_month AS mes, ROUND(AVG(total_amount),2) AS receita_media_usd
FROM ifood.silver_trips GROUP BY trip_month ORDER BY trip_month;

-- Q2
SELECT HOUR(tpep_pickup_datetime) AS hora, ROUND(AVG(passenger_count),3) AS media_pax
FROM ifood.silver_trips WHERE trip_month = 5
GROUP BY HOUR(tpep_pickup_datetime) ORDER BY hora;
```

**PySpark** ([`analysis/answers.py`](analysis/answers.py)) entrega o mesmo
resultado via DataFrame API **e** Spark SQL, com `assert` de equivalência.

## 🧪 Qualidade & testes

```bash
make test     # pytest + cobertura
make lint     # flake8 + mypy
make format   # black + isort
```


- **Transformações puras** testadas isoladamente com SparkSession local.
- **Gate de Data Quality**: colunas obrigatórias, sem nulos, `total_amount > 0`,
  `passenger_count > 0`, `dropoff > pickup` — nada sobe para a Gold sem passar.
  
- **CI** roda tudo + um *smoke test* do pipeline ponta-a-ponta a cada push/PR.

## 🔁 CI/CD & Ambientes (Dev / Hom / Prd)

Estratégia de **branches por ambiente** com **GitHub Environments** (segredos
isolados por ambiente, homologação espelhando produção e **aprovação manual**
antes do go-live).

| Branch | Ambiente | Deploy | Config |
|--------|----------|--------|--------|
| `develop` | **Dev** | automático | `conf/pipeline.dev.yaml` (Parquet) |
| `release/*` | **Hom** | automático (pós-CI) | `conf/pipeline.hom.yaml` (Delta) |
| `main` | **Prd** | **aprovação manual** | `conf/pipeline.prd.yaml` (Delta) |

```
feature/* ─PR→ develop ─→ DEV ─PR→ release/* ─→ HOM ─PR→ main ─aprovação→ PRD
```

O pipeline seleciona a config do ambiente automaticamente via `IFOOD_ENV`
(injetada pelo workflow). Rodar como um ambiente específico:

```bash
IFOOD_ENV=hom python -m ifood_case.main --stage all   # usa pipeline.hom.yaml
```

- **`ci.yml`** — lint, tipos, testes e smoke test em PRs/pushes das 3 branches.
- **`cd.yml`** — resolve a branch → vincula ao GitHub Environment (aplica as
  *protection rules*, incl. aprovação no `prd`) → deploy + *health check*.
- **`auto-pr.yml`** — ao dar `git push` numa branch `feature/**`, `fix/**`,
  `release/**` ou `hotfix/**`, abre a PR automaticamente (base `develop` para
  feature/fix; `main` para release/hotfix). Idempotente: não duplica PR.

### ⚙️ Setup do Auto-PR (uma vez)

Para o `auto-pr.yml` conseguir abrir a PR **e** fazer o CI rodar nela, é preciso
um **PAT fine-grained** guardado em secret (PRs abertas pelo `GITHUB_TOKEN`
padrão são bloqueadas e não disparam o CI):

1. **Criar o PAT** — GitHub → *Settings → Developer settings → Personal access
   tokens → Fine-grained*, com escopo neste repositório e permissões:
   - **Contents:** Read-only
   - **Pull requests:** Read and write
2. **Adicionar o secret** — repo → *Settings → Secrets and variables → Actions →
   New repository secret*, nome **`AUTO_PR_TOKEN`**, valor = o PAT.
3. **Propagar o workflow** — branches `feature/**` novas só trazem o `auto-pr.yml`
   se ele estiver em `develop`/`main` (de onde são cortadas); garanta o merge.

> Sem o `AUTO_PR_TOKEN` há fallback para o `GITHUB_TOKEN`, mas aí é obrigatório
> marcar *Settings → Actions → General → "Allow GitHub Actions to create and
> approve pull requests"* — e o CI **não** roda na PR criada.

📖 Setup completo (Environments, branch protection, CODEOWNERS): [`docs/cicd.md`](docs/cicd.md).

## 🧠 Decisões técnicas (ADRs)

| ADR | Decisão | Resumo |
|-----|---------|--------|
| [001](docs/adr/ADR-001-delta-lake.md) | **Delta Lake** | ACID + Time Travel + schema evolution; fallback Parquet |
| [002](docs/adr/ADR-002-medallion.md) | **Medallion** | Separação de responsabilidades, backfill seguro |
| [003](docs/adr/ADR-003-partitioning.md) | **Partição por `trip_month`** | Pruning eficiente; nota sobre Z-Order/Liquid Clustering em escala |
| [004](docs/adr/ADR-004-ambientes-git.md) | **Ambientes Dev/Hom/Prd** | Branches por ambiente + GitHub Environments, aprovação manual no prd |

Detalhes completos em [`docs/architecture.md`](docs/architecture.md).

## 📚 Fonte dos dados

[NYC TLC — Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(Yellow Taxi, Parquet, Jan–Mai/2023). O dataset real tem ~16M linhas/mês e
particularidades de qualidade (nulos em `passenger_count`, `total_amount`
negativos, outliers de distância) — todas tratadas na camada Silver.

---

<div align="center">
<sub>Construído para o case de <b>Data Architect — iFood</b> · PySpark · Delta Lake · Spark SQL</sub>
</div>
