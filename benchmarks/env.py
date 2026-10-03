"""Reproducible experiment environments: every benchmark starts from a seeded, known state."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

EVENTS_PER_ORDER = 14.2  # measured from the generator at default config (see baseline_pipeline.json)
SEED = 7


@dataclass(frozen=True)
class Scale:
    name: str
    events: int

    @property
    def orders(self) -> int:
        return max(500, int(self.events / EVENTS_PER_ORDER))


SCALES = {
    "100k": Scale("100k", 100_000),
    "1m": Scale("1m", 1_000_000),
    "10m": Scale("10m", 10_000_000),
    "50m": Scale("50m", 50_000_000),
}
LADDERS = {"small": ["100k", "1m"], "medium": ["100k", "1m", "10m"], "large": ["100k", "1m", "10m", "50m"]}
# the scale every non-ladder experiment runs at
WORKING_SCALE = {"small": "1m", "medium": "1m", "large": "10m"}


def prepare_source(scale: Scale, seed: int = SEED, days: float | None = None) -> dict[str, Any]:
    """Truncate + regenerate the operational source for a scale (deterministic)."""
    from datetime import UTC, datetime

    from simulator.generator import CommerceSimulator, SimulationConfig
    from simulator.postgres_sink import apply_stream, connect, init_schema, load_stores, record_run
    from simulator.postgres_sink import reset_source as _reset

    # keep ~2-3k orders per day so time-based layouts/partitions are realistic at every scale
    days = days or max(7.0, min(90.0, round(scale.orders / 2_500, 1)))
    cfg = SimulationConfig(seed=seed, orders=scale.orders, days=days, start=datetime(2026, 1, 1, tzinfo=UTC))
    t0 = time.time()
    with connect() as conn:
        init_schema(conn)
        _reset(conn)
        sim = CommerceSimulator(cfg)
        load_stores(conn, sim.world.stores)
        record_run(conn, f"bench-{scale.name}-{seed}", "historical", cfg.to_json())
        totals = apply_stream(conn, sim.stream())
    with connect(autocommit=True) as conn:  # VACUUM cannot run inside a transaction block
        conn.execute("VACUUM ANALYZE")
    return {
        "scale": scale.name,
        "orders": scale.orders,
        "days": days,
        "events": sum(totals.values()),
        "by_topic": totals,
        "load_s": round(time.time() - t0, 2),
    }


def reset_pipeline(spark=None) -> None:
    """Fresh Kafka topics, relay checkpoint, delivery log, lake tables, checkpoints and Gold."""
    from core.platform import init_all, reset_gold, reset_kafka, reset_lake
    from ingestion.cdc.outbox_relay import set_checkpoint
    from simulator.postgres_sink import connect

    init_all()
    reset_kafka()
    with connect() as c:
        set_checkpoint(c, "outbox-relay", 0)
        c.execute("TRUNCATE ops.delivery_log")
        c.commit()
    reset_lake(spark)
    reset_gold()


def relay_all(**kw) -> dict[str, Any]:
    from ingestion.cdc.outbox_relay import run_relay

    r = run_relay(batch_size=50_000, metrics_port=None, **kw)
    return {
        "events": r.events,
        "elapsed_s": r.elapsed_s,
        "events_per_s": r.events_per_s,
        "producer": r.producer,
    }


def drain(
    spark, max_offsets: int = 500_000, parallel_writes: bool = True, query: str = "silver_ingest"
) -> dict[str, Any]:
    from spark.streaming.silver_stream import build_query

    t0 = time.time()
    q, p = build_query(spark, query=query, available_now=True, max_offsets=max_offsets)
    p.parallel_writes = parallel_writes
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(q.exception())
    elapsed = time.time() - t0
    received = sum(s.received for s in p.history)
    stages: dict[str, float] = {}
    for s in p.history:
        for k, v in s.stage_s.items():
            stages[k] = round(stages.get(k, 0) + v, 3)
    return {
        "batches": len(p.history),
        "received": received,
        "elapsed_s": round(elapsed, 2),
        "events_per_s": round(received / elapsed, 1) if elapsed else None,
        "accepted_new": sum(s.accepted_new for s in p.history),
        "rejected": sum(s.rejected for s in p.history),
        "duplicates_detected": sum(s.duplicates_in_batch + s.duplicates_known for s in p.history),
        "late_events": sum(s.late_events for s in p.history),
        "stage_seconds_total": stages,
        "batch_durations_s": [s.duration_s for s in p.history],
    }


def build_gold(full_refresh: bool = True) -> dict[str, Any]:
    from ingestion.batch.postgres_snapshot import ingest_stores
    from lineage.dbt_runner import dbt_build
    from spark.common.session import get_spark

    ingest_stores(get_spark("bench"), full=True)
    r = dbt_build(full_refresh=full_refresh, emit_lineage=False)
    if not r["success"]:
        raise RuntimeError(f"dbt build failed: {r['exception']}")
    return r


def full_pipeline(spark, scale: Scale, max_offsets: int = 500_000) -> dict[str, Any]:
    src = prepare_source(scale)
    reset_pipeline(spark)
    relay = relay_all()
    stream = drain(spark, max_offsets=max_offsets)
    gold = build_gold()
    return {"source": src, "relay": relay, "stream": stream, "gold_build_s": gold["elapsed_s"]}
