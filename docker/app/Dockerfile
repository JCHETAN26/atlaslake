# AtlasLake application image: the `atlas` CLI with PySpark (JDK 17), dbt-duckdb and all clients.
#   docker build -f docker/app/Dockerfile -t atlaslake/app:local .
#   docker run --rm --network host atlaslake/app:local status
# bookworm ships openjdk-17-jre-headless (trixie does not)
FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/opt/venv/bin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv

WORKDIR /opt/atlaslake
# Dependencies first (cached layer), then the source.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --all-extras --no-install-project
COPY . .
RUN uv sync --frozen --no-dev --all-extras \
 && useradd --create-home --uid 1000 atlas && chown -R atlas /opt/atlaslake
USER atlas
ENV ATLAS_SPARK_LOCAL_DIR=/tmp/spark-local
ENTRYPOINT ["atlas"]
CMD ["--help"]
