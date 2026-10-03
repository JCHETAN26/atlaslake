"""SparkSession factory: Iceberg (JDBC catalog in Postgres, S3FileIO on MinIO/S3) + Kafka connector."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from core.config import Settings, settings

SPARK_VERSION = "3.5.9"
ICEBERG_VERSION = os.environ.get("ATLAS_ICEBERG_VERSION", "1.10.0")

PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "org.postgresql:postgresql:42.7.4",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
]


def iceberg_catalog_conf(cfg: Settings) -> dict[str, str]:
    prefix = f"spark.sql.catalog.{cfg.catalog_name}"
    conf = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{prefix}.uri": cfg.pg.jdbc_url(cfg.pg.catalog_db),
        f"{prefix}.jdbc.user": cfg.pg.user,
        f"{prefix}.jdbc.password": cfg.pg.password,
        # V1 catalog schema: required for view resolution (MERGE ... USING <temp view>).
        f"{prefix}.jdbc.schema-version": "V1",
        f"{prefix}.warehouse": cfg.s3.warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.client.region": cfg.s3.region,
        f"{prefix}.cache-enabled": "false",
    }
    if cfg.s3.endpoint:  # MinIO locally; empty in AWS (real S3, virtual-hosted style)
        conf[f"{prefix}.s3.endpoint"] = cfg.s3.endpoint
        conf[f"{prefix}.s3.path-style-access"] = "true"
    if cfg.s3.access_key:  # empty in AWS => default credential chain (ECS task role)
        conf[f"{prefix}.s3.access-key-id"] = cfg.s3.access_key
        conf[f"{prefix}.s3.secret-access-key"] = cfg.s3.secret_key
    return conf


def get_spark(app_name: str = "atlaslake", extra_conf: dict[str, str] | None = None) -> SparkSession:
    cfg = settings()
    os.environ.setdefault("AWS_REGION", cfg.s3.region)
    # The platform runs in UTC end-to-end: Spark collects timestamps as naive *process-local* times.
    os.environ["TZ"] = "UTC"
    time.tzset()
    # Workers must run the driver's interpreter (and see the repo packages).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Scratch space must support hard links (RocksDB state store) => not the exFAT data drive.
    local_dir = Path(
        os.environ.get("ATLAS_SPARK_LOCAL_DIR", Path.home() / ".cache" / "atlaslake" / "spark-local")
    )
    local_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.driver.memory", cfg.spark_driver_memory)
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("ATLAS_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.defaultCatalog", cfg.catalog_name)
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    )
    for key, value in {**iceberg_catalog_conf(cfg), **(extra_conf or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("ATLAS_SPARK_LOG_LEVEL", "WARN"))
    return spark
