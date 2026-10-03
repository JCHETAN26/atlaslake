"""Batch ingestion of slowly-changing reference tables straight from Postgres (no CDC topic).

Stores have no event stream: they are extracted incrementally by ``updated_at`` over JDBC, using a
high-water mark kept in ``ops.batch_watermarks`` (advanced only after the Iceberg commit succeeds,
so a failed run simply re-reads the same window — the MERGE is idempotent).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.config import settings
from simulator.postgres_sink import connect
from spark.common.tables import create_all, fq

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _watermark(dataset: str) -> datetime:
    with connect() as conn:
        row = conn.execute(
            "SELECT watermark FROM ops.batch_watermarks WHERE dataset=%s", (dataset,)
        ).fetchone()
    return row[0] if row else EPOCH


def _set_watermark(dataset: str, wm: datetime) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO ops.batch_watermarks (dataset, watermark) VALUES (%s, %s)
                        ON CONFLICT (dataset) DO UPDATE SET watermark = excluded.watermark, updated_at = now()""",
            (dataset, wm),
        )


def ingest_stores(spark: SparkSession, full: bool = False) -> dict:
    cfg = settings()
    create_all(spark, only=["silver.stores"])
    t0 = time.time()
    wm = EPOCH if full else _watermark("stores")
    query = (
        f"(SELECT store_id, name, city, country, currency, opened_at, updated_at FROM commerce.stores "
        f"WHERE updated_at > '{wm.isoformat()}') q"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", cfg.pg.jdbc_url(cfg.pg.source_db))
        .option("dbtable", query)
        .option("user", cfg.pg.user)
        .option("password", cfg.pg.password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .withColumn("extracted_at", F.current_timestamp())
    ).persist()
    n = df.count()
    if n:
        df.createOrReplaceTempView("stores_extract")
        spark.sql(f"""MERGE INTO {fq("silver.stores")} t USING stores_extract s ON t.store_id = s.store_id
                      WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")
        new_wm = df.agg(F.max("updated_at")).collect()[0][0]
        _set_watermark("stores", new_wm.replace(tzinfo=UTC) if new_wm.tzinfo is None else new_wm)
    df.unpersist()
    return {
        "dataset": "stores",
        "rows_extracted": n,
        "from_watermark": wm.isoformat(),
        "runtime_s": round(time.time() - t0, 3),
    }
