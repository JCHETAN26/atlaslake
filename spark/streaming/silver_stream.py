"""Kafka → (Bronze, DLQ, Silver) with Spark Structured Streaming.

* checkpointing: Kafka offsets + batch ids in ``data/checkpoints/<query>`` (survives crashes)
* micro-batches bounded by ``maxOffsetsPerTrigger`` so backlog recovery is smooth
* ``available_now=True`` drains everything currently in Kafka and stops — the *same* code path is the
  batch ingestion used by Airflow (no separate batch implementation to drift)
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from contracts import events as ev
from core.config import settings
from monitoring.metrics import streaming_metrics
from spark.common.decode import standardize_kafka
from spark.common.pipeline import SilverProcessor
from spark.common.session import get_spark
from spark.common.tables import create_all

log = logging.getLogger(__name__)
QUERY_NAME = "silver_ingest"


class MetricsListener(StreamingQueryListener):
    """Export micro-batch progress to Prometheus (and keep the last progress for tests)."""

    def __init__(self) -> None:
        self.m = streaming_metrics()
        self.progress: list[dict] = []

    def onQueryStarted(self, event) -> None:  # noqa: N802 (Spark API)
        log.info("query started %s", event.name)

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = json.loads(event.progress.json)
        self.progress.append(p)
        self.progress = self.progress[-500:]
        q = p.get("name") or "unnamed"
        self.m.input_rows.labels(q).inc(p.get("numInputRows", 0))
        self.m.rows_per_second.labels(q).set(p.get("processedRowsPerSecond") or 0)
        self.m.batch_duration.labels(q).observe((p.get("durationMs", {}).get("triggerExecution", 0)) / 1000)
        self.m.batch_id.labels(q).set(p.get("batchId", 0))
        self.m.last_progress.labels(q).set(time.time())
        lag = consumer_lag_from_progress(p)
        if lag is not None:
            self.m.consumer_lag.labels(q).set(lag)
        wm = p.get("eventTime", {}).get("watermark")
        if wm:
            from datetime import datetime

            ts = datetime.fromisoformat(wm.replace("Z", "+00:00")).timestamp()
            self.m.watermark_lag.labels(q).set(max(0.0, time.time() - ts))
        for op in p.get("stateOperators", []):
            dropped = op.get("numRowsDroppedByWatermark", 0)
            if dropped:
                self.m.late_dropped.labels(ev.TOPIC_ORDERS).inc(dropped)

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            self.m.failed_batches.labels(str(event.id)).inc()
            log.error("query terminated with error: %s", event.exception)


def consumer_lag_from_progress(p: dict) -> int | None:
    """Kafka lag = latestOffset − endOffset summed over partitions (Spark doesn't commit to a group)."""
    total = 0
    found = False
    for src in p.get("sources", []):
        latest, end = src.get("latestOffset"), src.get("endOffset")
        if not latest or not end:
            continue
        latest = json.loads(latest) if isinstance(latest, str) else latest
        end = json.loads(end) if isinstance(end, str) else end
        for topic, parts in latest.items():
            for part, off in parts.items():
                total += max(0, int(off) - int(end.get(topic, {}).get(part, 0)))
                found = True
    return total if found else None


def checkpoint_path(query: str) -> Path:
    return settings().checkpoint_dir / query


def build_query(
    spark: SparkSession,
    query: str = QUERY_NAME,
    trigger_s: float = 5.0,
    max_offsets: int = 250_000,
    starting_offsets: str = "earliest",
    available_now: bool = False,
    topics: tuple[str, ...] = ev.TOPICS,
) -> tuple[StreamingQuery, SilverProcessor]:
    cfg = settings()
    create_all(spark)
    processor = SilverProcessor(spark, query=query, write_bronze=True)
    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", cfg.kafka.bootstrap)
             .option("subscribe", ",".join(topics))
             .option("startingOffsets", starting_offsets)
             .option("maxOffsetsPerTrigger", str(max_offsets))
             .option("failOnDataLoss", "true")
             .load())
    raw = standardize_kafka(kafka)

    def handle(batch_df, batch_id: int) -> None:
        stats = processor.process_raw(batch_df, batch_id)
        m = streaming_metrics()
        for topic, d in stats.by_topic.items():
            m.accepted.labels(topic).inc(d.get("accepted_new", 0))
            m.duplicates.labels(topic).inc(d.get("duplicates_in_batch", 0) + d.get("duplicates_known", 0))
            if d.get("rejected"):
                m.rejected.labels(topic, "any").inc(d["rejected"])
            if d.get("max_ing") is not None:
                m.processing_lag.labels(query).set(max(0.0, time.time() - d["max_ing"].timestamp()))
        log.info("batch %s: %s", batch_id, {k: v for k, v in stats.as_dict().items() if k != "by_topic"})

    writer = (raw.writeStream.queryName(query)
              .foreachBatch(handle)
              .option("checkpointLocation", str(checkpoint_path(query))))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=f"{trigger_s} seconds")
    return writer.start(), processor


def run(
    available_now: bool = False,
    trigger_s: float = 5.0,
    max_offsets: int = 250_000,
    metrics_port: int | None = 9108,
    timeout_s: float | None = None,
) -> SilverProcessor:
    from monitoring.metrics import serve

    spark = get_spark("atlas-silver-stream")
    listener = MetricsListener()
    spark.streams.addListener(listener)
    if metrics_port:
        try:
            serve(metrics_port)
        except OSError:
            log.warning("metrics port %s busy; not serving", metrics_port)
    q, processor = build_query(spark, trigger_s=trigger_s, max_offsets=max_offsets, available_now=available_now)
    q.awaitTermination(timeout_s)
    if q.isActive:
        q.stop()
    if q.exception():
        raise RuntimeError(f"streaming query failed: {q.exception()}")
    return processor
