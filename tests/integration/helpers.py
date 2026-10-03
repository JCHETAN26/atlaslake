from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from spark.common.pipeline import SilverProcessor
from spark.common.tables import fq
from spark.streaming.silver_stream import build_query


def drain(spark: SparkSession, query: str = "silver_ingest") -> SilverProcessor:
    """Run the real streaming query in availableNow mode until Kafka is drained."""
    q, processor = build_query(spark, query=query, available_now=True, max_offsets=100_000)
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(q.exception())
    return processor


def totals(p: SilverProcessor) -> dict[str, int]:
    out: dict[str, int] = {}
    for s in p.history:
        for k in ("received", "rejected", "duplicates_in_batch", "duplicates_known", "accepted_new", "late_events"):
            out[k] = out.get(k, 0) + getattr(s, k)
    return out


def table(spark: SparkSession, name: str, where: str = "true") -> list[dict[str, Any]]:
    return [r.asDict() for r in spark.sql(f"SELECT * FROM {fq(name)} WHERE {where}").collect()]


def send_all(producer, events: list[tuple[str, dict]]) -> None:
    for topic, e in events:
        producer.send(e, topic)
    producer.flush()
