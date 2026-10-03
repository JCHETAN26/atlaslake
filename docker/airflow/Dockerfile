# Airflow (Phase 24) with the AtlasLake CLI available to BashOperator tasks.
#
# Airflow and AtlasLake live in SEPARATE Python environments: AtlasLake pins pyspark/pandas/dbt
# versions that would fight Airflow's constraints. Tasks call `atlas ...`, a wrapper that runs the
# repo (mounted at /opt/atlaslake by docker-compose) on /opt/atlas-venv.
FROM apache/airflow:slim-3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv

# AtlasLake runtime environment (dependencies only; source is mounted at runtime).
COPY pyproject.toml uv.lock /tmp/atlas/
RUN cd /tmp/atlas \
 && uv export --frozen --no-dev --all-extras --no-emit-project --no-hashes -o requirements.txt \
 && uv venv --python "$(command -v python3.12 || command -v python3)" /opt/atlas-venv \
 && VIRTUAL_ENV=/opt/atlas-venv uv pip install --no-cache -r requirements.txt \
 && rm -rf /tmp/atlas \
 && printf '#!/bin/sh\ncd /opt/atlaslake && PYTHONPATH=/opt/atlaslake ATLAS_SPARK_LOCAL_DIR=/tmp/spark-local exec /opt/atlas-venv/bin/python -m cli.main "$@"\n' > /usr/local/bin/atlas \
 && chmod +x /usr/local/bin/atlas \
 && chown -R airflow: /opt/atlas-venv

USER airflow
ARG AIRFLOW_VERSION=3.3.2
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-standard psycopg2-binary pytest \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
