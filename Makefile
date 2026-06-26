# ============================================================================
# iFood Case — NYC Taxi Lakehouse | atalhos de desenvolvimento
# ============================================================================
.PHONY: help install sample download pipeline answers dashboard test lint format \
        docker-up docker-down jupyter clean

help:  ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Instala dependências de dev e o pacote em modo editável
	pip install -r requirements-dev.txt && pip install -e .

sample:  ## Gera dados sintéticos na landing zone (rápido, sem internet)
	python scripts/generate_sample_data.py --out data/landing --scale 0.05

download:  ## Baixa os dados REAIS do NYC TLC (Jan-Mai/2023)
	python -m ifood_case.ingestion

pipeline:  ## Roda o pipeline completo (bronze -> silver -> gold)
	python -m ifood_case.main --stage all

answers:  ## Executa as respostas do case e exporta KPIs para o dashboard
	python analysis/answers.py --export dashboard/data/kpis.json

dashboard:  ## Sobe o dashboard interativo (Streamlit em :8501)
	streamlit run dashboard/app.py

test:  ## Roda a suíte de testes com cobertura
	pytest --cov=ifood_case --cov-report=term-missing

lint:  ## Checa estilo (flake8) e tipos (mypy)
	flake8 src tests analysis && mypy src

format:  ## Formata o código (black + isort)
	black src tests analysis scripts && isort src tests analysis scripts

docker-up:  ## Sobe o ambiente Docker (Spark + Jupyter em :8888)
	docker compose up --build -d

docker-down:  ## Derruba o ambiente Docker
	docker compose down

jupyter:  ## Abre o JupyterLab local
	jupyter lab

demo: sample pipeline answers  ## Pipeline ponta-a-ponta com dados sintéticos

clean:  ## Limpa as camadas de dados geradas
	rm -rf data/bronze data/silver data/gold data/landing/*.parquet

ci: lint test  ## Pipeline de CI (lint + testes)
