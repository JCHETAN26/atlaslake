"""Platform configuration.

Every setting is read from an ``ATLAS_*`` environment variable with a default that matches the local
docker-compose stack, so a fresh clone works with no configuration and CI/AWS override via env.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(f"ATLAS_{name}", default)


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _env("PG_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_env("PG_PORT", "5432")))
    user: str = field(default_factory=lambda: _env("PG_USER", "atlas"))
    password: str = field(default_factory=lambda: _env("PG_PASSWORD", "atlas"))
    source_db: str = field(default_factory=lambda: _env("PG_SOURCE_DB", "commerce"))
    analytics_db: str = field(default_factory=lambda: _env("PG_ANALYTICS_DB", "analytics"))
    catalog_db: str = field(default_factory=lambda: _env("PG_CATALOG_DB", "iceberg_catalog"))

    def dsn(self, db: str | None = None) -> str:
        return (
            f"host={self.host} port={self.port} user={self.user} "
            f"password={self.password} dbname={db or self.source_db}"
        )

    def jdbc_url(self, db: str) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{db}"

    def sqlalchemy_url(self, db: str) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{db}"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    schema_registry_url: str = field(
        default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    partitions: int = field(default_factory=lambda: int(_env("KAFKA_PARTITIONS", "6")))


@dataclass(frozen=True)
class S3Config:
    endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "atlas"))
    secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "atlas-secret-key"))
    region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    bucket: str = field(default_factory=lambda: _env("S3_BUCKET", "atlaslake"))

    @property
    def warehouse(self) -> str:
        return f"s3://{self.bucket}/warehouse"

    @property
    def endpoint_host(self) -> str:
        return self.endpoint.split("://", 1)[-1]

    @property
    def use_ssl(self) -> bool:
        return self.endpoint.startswith("https")


@dataclass(frozen=True)
class Settings:
    env: str = field(default_factory=lambda: _env("ENV", "local"))
    pg: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    s3: S3Config = field(default_factory=S3Config)
    catalog_name: str = field(default_factory=lambda: _env("ICEBERG_CATALOG", "atlas"))
    data_dir: Path = field(default_factory=lambda: Path(_env("DATA_DIR", str(REPO_ROOT / "data"))))
    evaluation_dir: Path = field(
        default_factory=lambda: Path(_env("EVALUATION_DIR", str(REPO_ROOT / "evaluation")))
    )
    gold_duckdb: Path = field(
        default_factory=lambda: Path(_env("GOLD_DUCKDB", str(REPO_ROOT / "data" / "gold.duckdb")))
    )
    pushgateway: str = field(default_factory=lambda: _env("PUSHGATEWAY", "localhost:9091"))
    openlineage_url: str = field(default_factory=lambda: _env("OPENLINEAGE_URL", "http://localhost:5000"))
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "5g"))

    @property
    def checkpoint_dir(self) -> Path:
        return self.data_dir / "checkpoints"


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings()
