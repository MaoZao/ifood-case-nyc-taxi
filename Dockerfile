# Imagem base com Spark + Jupyter já compilados (evita instalar Java/Hadoop na mão).
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
WORKDIR /home/jovyan/work

# Dependências Python do projeto.
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Código fonte e configs.
COPY pyproject.toml ./
COPY src/ ./src/
COPY conf/ ./conf/
COPY analysis/ ./analysis/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

# Instala o pacote em modo editável para `import ifood_case` funcionar.
RUN pip install --no-cache-dir -e .

ENV PYTHONPATH=/home/jovyan/work/src:/home/jovyan/work
USER jovyan
