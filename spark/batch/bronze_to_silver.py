"""Batch Bronze → Silver (Phase 9) — also the engine behind backfills (Phase 25) and re-processing.

Reads the raw bytes retained in Bronze and pushes them through the *same* SilverProcessor the
streaming job uses. Because every Silver write is idempotent, running any range any number of times
converges to the same state ("process January twice → same final logical state").
"""

from __future__ import annotations

import time
import uuid
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from contracts import events as ev
from spark.common.decode import RAW_SCHEMA
from spark.common.pipeline import BatchStats, SilverProcessor
from spark.common.tables import create_all, fq


def bronze_raw(
    spark: SparkSession,
    datasets: list[str],
    date_from: date | None = None,
    date_to: date | None = None,
    by: str = "event",
) -> DataFrame:
    """Union selected Bronze tables, optionally restricted to a date range (inclusive)."""
    frames = []
    for d in datasets:
        df = spark.table(fq(f"bronze.{d}"))
        col = "event_timestamp" if by == "event" else "ingestion_timestamp"
        if date_from:
            df = df.filter(F.col(col) >= F.lit(datetime.combine(date_from, datetime.min.time())))
        if date_to:
            df = df.filter(
                F.col(col) < F.lit(datetime.combine(date_to + timedelta(days=1), datetime.min.time()))
            )
        df = df.withColumn("header_event_id", F.col("event_id"))  # Bronze keeps the attributed id
        frames.append(df.select(*RAW_SCHEMA.fieldNames()))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def resolve_dlq(spark: SparkSession, processed: DataFrame) -> int:
    """Mark PENDING DLQ rows whose original offsets now decode + validate cleanly as REPROCESSED."""
    from core.config import settings
    from spark.common.decode import decode

    ok = (
        decode(processed, settings().kafka.schema_registry_url)
        .filter(F.col("error_codes").isNull())
        .select(F.concat_ws("/", "topic", "kafka_partition", "kafka_offset").alias("dlq_id"))
    )
    pending = spark.table(fq("ops.dlq")).filter("status = 'PENDING'").select("dlq_id")
    hits = ok.join(pending, "dlq_id").distinct()
    n = hits.count()
    if n:
        hits.createOrReplaceTempView("dlq_resolved")
        spark.sql(f"""MERGE INTO {fq("ops.dlq")} t USING dlq_resolved s ON t.dlq_id = s.dlq_id
                      WHEN MATCHED AND t.status = 'PENDING' THEN UPDATE SET
                        status = 'REPROCESSED', resolved_at = current_timestamp(),
                        resolution_note = 'record passes current validation on re-processing'""")
    return n


def run(
    spark: SparkSession,
    datasets: list[str] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    by: str = "event",
    query: str | None = None,
    reprocess_dlq: bool = True,
) -> dict:
    create_all(spark)
    datasets = datasets or list(ev.DATASETS)
    t0 = time.time()
    raw = bronze_raw(spark, datasets, date_from, date_to, by).persist()
    processor = SilverProcessor(spark, query=query or f"batch:{uuid.uuid4().hex[:8]}", write_bronze=False)
    stats: BatchStats = processor.process_raw(raw, 0)
    resolved = resolve_dlq(spark, raw) if reprocess_dlq else 0
    raw.unpersist()
    return {
        "datasets": datasets,
        "from": str(date_from) if date_from else None,
        "to": str(date_to) if date_to else None,
        "by": by,
        "dlq_reprocessed": resolved,
        "runtime_s": round(time.time() - t0, 3),
        **stats.as_dict(),
    }
