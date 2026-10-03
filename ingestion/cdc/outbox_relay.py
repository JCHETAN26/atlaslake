"""Transactional-outbox relay: Postgres ``commerce.outbox`` → Kafka.

Delivery semantics: **at-least-once.** A batch is read after ``last_seq``, produced, flushed (all acks
received) and only then is the checkpoint advanced. A crash between flush and checkpoint re-sends
that batch — those physical duplicates are absorbed downstream by event_id deduplication.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import psycopg

from contracts import events as ev
from core.config import settings
from monitoring.metrics import relay_metrics
from producers.chaos import ChaosConfig
from producers.kafka_producer import EventProducer
from simulator.postgres_sink import connect

log = logging.getLogger(__name__)

SELECT_BATCH = """
    SELECT seq, event_id::text, event_type, topic, partition_key,
           (extract(epoch from event_timestamp) * 1000000)::bigint AS event_us, payload
    FROM commerce.outbox
    WHERE seq > %s
    ORDER BY seq
    LIMIT %s
"""


@dataclass
class RelayResult:
    events: int
    last_seq: int
    elapsed_s: float
    producer: dict[str, Any]

    @property
    def events_per_s(self) -> float:
        return round(self.events / self.elapsed_s, 1) if self.elapsed_s else 0.0


def get_checkpoint(conn: psycopg.Connection, name: str) -> int:
    row = conn.execute("SELECT last_seq FROM ops.relay_checkpoint WHERE relay_name = %s", (name,)).fetchone()
    return int(row[0]) if row else 0


def set_checkpoint(conn: psycopg.Connection, name: str, seq: int) -> None:
    conn.execute(
        """INSERT INTO ops.relay_checkpoint (relay_name, last_seq) VALUES (%s, %s)
           ON CONFLICT (relay_name) DO UPDATE SET last_seq = excluded.last_seq, updated_at = now()""",
        (name, seq),
    )
    conn.commit()


def row_to_event(row: tuple) -> tuple[str, dict[str, Any]]:
    _seq, event_id, event_type, topic, _key, event_us, payload = row
    event = dict(payload)
    event["event_id"] = event_id
    event["event_type"] = event_type
    event["event_timestamp"] = int(event_us)
    return topic, event


def run_relay(
    name: str = "outbox-relay",
    batch_size: int = 20_000,
    follow: bool = False,
    poll_interval_s: float = 0.5,
    max_events: int | None = None,
    schema_versions: dict[str, int] | None = None,
    chaos: ChaosConfig | None = None,
    crash_after_flush_once: bool = False,
    idle_exit_s: float | None = None,
    metrics_port: int | None = 9109,
) -> RelayResult:
    """Drain the outbox (and keep following it if ``follow``)."""
    metrics = relay_metrics()
    if follow and metrics_port:
        from monitoring.metrics import serve

        try:
            serve(metrics_port)
        except OSError:
            log.warning("relay metrics port %s busy", metrics_port)
    read_conn = connect()
    log_conn = connect()
    producer = EventProducer(schema_versions=schema_versions, chaos=chaos, delivery_sink=log_conn,
                             run_id=f"{name}-{int(time.time())}")
    last = get_checkpoint(read_conn, name)
    started = time.time()
    sent = 0
    idle_since = time.time()
    try:
        while True:
            rows = read_conn.execute(SELECT_BATCH, (last, batch_size)).fetchall()
            read_conn.commit()
            if not rows:
                producer.release_due()
                if not follow or (idle_exit_s is not None and time.time() - idle_since > idle_exit_s):
                    break
                time.sleep(poll_interval_s)
                continue
            idle_since = time.time()
            for row in rows:
                topic, event = row_to_event(row)
                producer.send(event, topic)
            # Late/duplicate deliveries may still be held back; in follow mode we don't block on them.
            producer.flush(wait_for_delayed=not follow)
            if crash_after_flush_once:
                raise SystemExit("simulated relay crash after flush, before checkpoint")
            last = int(rows[-1][0])
            set_checkpoint(read_conn, name, last)
            sent += len(rows)
            metrics.events.inc(len(rows))
            metrics.last_seq.set(last)
            if max_events and sent >= max_events:
                break
        producer.flush(wait_for_delayed=True)
    finally:
        read_conn.close()
        log_conn.close()
    return RelayResult(sent, last, round(time.time() - started, 3), producer.stats.as_dict())


def outbox_backlog(name: str = "outbox-relay") -> int:
    with connect() as conn:
        last = get_checkpoint(conn, name)
        return int(conn.execute("SELECT count(*) FROM commerce.outbox WHERE seq > %s", (last,)).fetchone()[0])


def topics_in_outbox() -> dict[str, int]:
    with connect() as conn:
        rows = conn.execute("SELECT topic, count(*) FROM commerce.outbox GROUP BY 1").fetchall()
    out = {t: 0 for t in ev.TOPICS}
    out.update({r[0]: int(r[1]) for r in rows})
    return out


__all__ = ["RelayResult", "outbox_backlog", "run_relay", "settings"]
