"""Prometheus instrumentation shared by long-running processes and batch jobs.

* Long-running processes (relay, streaming jobs) expose ``/metrics`` via ``serve(port)``.
* Batch jobs push a final snapshot to the Pushgateway via ``push_job_metrics``.
All metric names are prefixed ``atlas_``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from functools import lru_cache

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
)

from core.config import settings

log = logging.getLogger(__name__)
REGISTRY = CollectorRegistry(auto_describe=True)


@dataclass(frozen=True)
class RelayMetrics:
    events: Counter
    last_seq: Gauge


@lru_cache(maxsize=1)
def relay_metrics() -> RelayMetrics:
    return RelayMetrics(
        events=Counter("atlas_relay_events", "Outbox events relayed to Kafka", registry=REGISTRY),
        last_seq=Gauge("atlas_relay_last_seq", "Last outbox seq checkpointed", registry=REGISTRY),
    )


@dataclass(frozen=True)
class StreamingMetrics:
    input_rows: Counter
    rows_per_second: Gauge
    batch_duration: Histogram
    batch_id: Gauge
    failed_batches: Counter
    accepted: Counter
    rejected: Counter
    duplicates: Counter
    late_dropped: Counter
    processing_lag: Gauge
    watermark_lag: Gauge
    last_progress: Gauge


@lru_cache(maxsize=1)
def streaming_metrics() -> StreamingMetrics:
    lbl = ["query"]
    return StreamingMetrics(
        input_rows=Counter("atlas_stream_input_rows", "Rows read per micro-batch", lbl, registry=REGISTRY),
        rows_per_second=Gauge("atlas_stream_processed_rows_per_second", "Processed rows/sec", lbl, registry=REGISTRY),
        batch_duration=Histogram("atlas_stream_batch_duration_seconds", "Micro-batch duration", lbl,
                                 buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300), registry=REGISTRY),
        batch_id=Gauge("atlas_stream_batch_id", "Last completed batch id", lbl, registry=REGISTRY),
        failed_batches=Counter("atlas_stream_failed_batches", "Failed micro-batches", lbl, registry=REGISTRY),
        accepted=Counter("atlas_stream_accepted_events", "Events accepted into Silver", ["topic"], registry=REGISTRY),
        rejected=Counter("atlas_stream_rejected_events", "Events routed to DLQ", ["topic", "reason"], registry=REGISTRY),
        duplicates=Counter("atlas_stream_duplicate_events", "Duplicate deliveries detected", ["topic"], registry=REGISTRY),
        late_dropped=Counter("atlas_stream_late_events", "Events beyond the watermark (excluded from real-time aggregates)",
                             ["topic"], registry=REGISTRY),
        processing_lag=Gauge("atlas_stream_processing_lag_seconds", "now - max(ingestion_timestamp) processed",
                             lbl, registry=REGISTRY),
        watermark_lag=Gauge("atlas_stream_watermark_lag_seconds", "now - current event-time watermark", lbl,
                            registry=REGISTRY),
        last_progress=Gauge("atlas_stream_last_progress_timestamp", "Unix time of last progress", lbl,
                            registry=REGISTRY),
    )


def serve(port: int) -> None:
    start_http_server(port, registry=REGISTRY)
    log.info("metrics on :%s/metrics", port)


def push_job_metrics(job: str, values: dict[str, float], labels: dict[str, str] | None = None,
                     success: bool = True) -> bool:
    """Push a batch job's final metrics to the Pushgateway. Never fails the job if it is down."""
    reg = CollectorRegistry()
    label_names = sorted((labels or {}).keys())
    for name, value in values.items():
        g = Gauge(f"atlas_job_{name}", f"{job} {name}", label_names, registry=reg)
        (g.labels(**labels) if labels else g).set(value)
    last = Gauge("atlas_job_last_run_timestamp", "Unix time of last run", label_names, registry=reg)
    (last.labels(**labels) if labels else last).set(time.time())
    status = Gauge("atlas_job_success", "1 if last run succeeded", label_names, registry=reg)
    (status.labels(**labels) if labels else status).set(1 if success else 0)
    try:
        push_to_gateway(settings().pushgateway, job=job, registry=reg, timeout=3)
        return True
    except Exception as exc:  # monitoring must never break the pipeline
        log.debug("pushgateway unavailable: %s", exc)
        return False
