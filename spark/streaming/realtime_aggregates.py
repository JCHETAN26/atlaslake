"""Real-time order metrics with event-time windows, watermark and Spark-managed state (Phase 10/12).

    commerce.orders ─decode─► ORDER_CREATED ─withWatermark(10 min)─► dropDuplicatesWithinWatermark(event_id)
                    ─► window(1 min) × currency ─► append ─► silver.realtime_sales_1m (MERGE, idempotent)

Late-data policy (docs/LATE_DATA_POLICY.md):
* within the watermark (≤ 10 min behind the max event time seen)  → counted in its window
* beyond the watermark                                            → excluded from THIS real-time
  aggregate (Spark reports it as ``numRowsDroppedByWatermark``), but still accepted into Silver by
  the silver_ingest query and therefore included in every Gold (batch) model. Nothing is lost; the
  real-time view is explicitly approximate for very late data, Gold is exact.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from contracts import events as ev
from core.config import settings
from spark.common.decode import decode, standardize_kafka
from spark.common.tables import create_all, fq
from spark.common.transforms import record_schema

QUERY_NAME = "realtime_sales"
WATERMARK = "10 minutes"
WINDOW = "1 minute"


def build_query(
    spark: SparkSession,
    query: str = QUERY_NAME,
    available_now: bool = False,
    trigger_s: float = 10.0,
    watermark: str = WATERMARK,
    window: str = WINDOW,
) -> StreamingQuery:
    cfg = settings()
    create_all(spark, only=["silver.realtime_sales_1m"])
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap)
        .option("subscribe", ev.TOPIC_ORDERS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    decoded = decode(standardize_kafka(kafka), cfg.kafka.schema_registry_url)
    created = (
        decoded.filter(F.col("error_codes").isNull() & (F.col("event_type") == "ORDER_CREATED"))
        .select(
            "event_id",
            "event_timestamp",
            F.from_json("record_json", record_schema(ev.TOPIC_ORDERS)).alias("r"),
        )
        .select(
            "event_id",
            "event_timestamp",
            F.col("r.currency").alias("currency"),
            F.col("r.amount_minor").alias("amount_minor"),
        )
    )
    agg = (
        created.withWatermark("event_timestamp", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_timestamp", window), "currency")
        .agg(F.count("*").alias("orders"), F.sum("amount_minor").alias("revenue_minor"))
    )

    def upsert(df, batch_id: int) -> None:
        out = df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "currency",
            "orders",
            "revenue_minor",
            F.current_timestamp().alias("computed_at"),
        )
        out.createOrReplaceTempView("rt_batch")
        out.sparkSession.sql(f"""
            MERGE INTO {fq("silver.realtime_sales_1m")} t USING rt_batch s
            ON t.window_start = s.window_start AND t.currency = s.currency
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

    writer = (
        agg.writeStream.queryName(query)
        .outputMode("append")
        .foreachBatch(upsert)
        .option("checkpointLocation", str(cfg.checkpoint_dir / query))
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=f"{trigger_s} seconds")
    )
    return writer.start()


def dropped_by_watermark(progress: list[dict]) -> int:
    return sum(op.get("numRowsDroppedByWatermark", 0) for p in progress for op in p.get("stateOperators", []))
