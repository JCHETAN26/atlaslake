"""Avro event producer with optional chaos injection and a durable delivery log."""

from __future__ import annotations

import heapq
import io
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaException, Producer

from contracts import events as ev
from contracts import schemas, serde
from contracts.registry import SchemaRegistry
from core.config import settings
from producers.chaos import Chaos, ChaosConfig


@dataclass
class ProducerStats:
    sent: Counter = field(default_factory=Counter)  # by fault label
    delivered: int = 0
    failed: int = 0
    started: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        elapsed = max(time.time() - self.started, 1e-9)
        return {"deliveries": self.delivered, "failed": self.failed, "by_fault": dict(self.sent),
                "elapsed_s": round(elapsed, 3), "deliveries_per_s": round(self.delivered / elapsed, 1)}


class EventProducer:
    """Serialises canonical events to Confluent-Avro and produces them, injecting faults if asked.

    ``delivery_sink`` (e.g. a Postgres connection) receives one row per physical delivery:
    (topic, partition, offset, event_id, fault, run_id).
    """

    def __init__(
        self,
        schema_versions: dict[str, int] | None = None,
        chaos: ChaosConfig | None = None,
        run_id: str | None = None,
        registry: SchemaRegistry | None = None,
        delivery_sink: Any | None = None,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        cfg = settings()
        self.registry = registry or SchemaRegistry()
        self.versions = {t: schemas.latest_version(t) for t in ev.TOPICS}
        self.versions.update(schema_versions or {})
        self.schema_ids: dict[str, int] = {}
        for topic, version in self.versions.items():
            found = self.registry.lookup(schemas.subject(topic), schemas.schema_text(topic, version))
            if found is None:
                raise RuntimeError(f"schema {topic} v{version} not registered; run `atlas contracts register`")
            self.schema_ids[topic] = int(found["id"])
        self.chaos = Chaos(chaos) if chaos and chaos.enabled else None
        self.chaos_cfg = chaos or ChaosConfig()
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self.stats = ProducerStats()
        self.delivery_sink = delivery_sink
        self._log: list[tuple] = []
        self._delayed: list[tuple[float, int, str, str, bytes, str, str]] = []
        self._reorder: list[tuple[str, str, bytes, str, str]] = []
        self._seq = 0
        conf = {
            "bootstrap.servers": cfg.kafka.bootstrap,
            "acks": "all",
            "enable.idempotence": True,  # no duplicates from producer *retries*; chaos dups are explicit
            "linger.ms": 25,
            "batch.size": 1_048_576,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "message.max.bytes": 2_000_000,
        }
        conf.update(extra_config or {})
        self._producer = Producer(conf)

    # ----------------------------------------------------------------- core
    def _on_delivery(self, event_id: str | None, fault: str):
        def cb(err, msg) -> None:
            if err is not None:
                self.stats.failed += 1
                return
            self.stats.delivered += 1
            self._log.append((msg.topic(), msg.partition(), msg.offset(), event_id, fault, self.run_id))
        return cb

    def _produce(self, topic: str, key: str, value: bytes, event_id: str | None, fault: str) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=value,
                                       on_delivery=self._on_delivery(event_id, fault))
                break
            except BufferError:
                self._producer.poll(0.05)
        self.stats.sent[fault] += 1
        self._producer.poll(0)

    def _emit(self, topic: str, key: str, value: bytes, event_id: str | None, fault: str) -> None:
        if self.chaos and self.chaos_cfg.reorder_window > 1:
            self._reorder.append((topic, key, value, event_id or "", fault))
            if len(self._reorder) >= self.chaos_cfg.reorder_window:
                self._flush_reorder()
            return
        self._produce(topic, key, value, event_id, fault)

    def _flush_reorder(self) -> None:
        assert self.chaos is not None
        self.chaos.rng.shuffle(self._reorder)
        for topic, key, value, event_id, fault in self._reorder:
            self._produce(topic, key, value, event_id or None, fault)
        self._reorder.clear()

    def serialize(self, event: dict[str, Any], topic: str) -> tuple[str, bytes, dict[str, Any]]:
        version = self.versions[topic]
        rec = schemas.to_record(event, topic, version)
        key = str(event.get(ev.PARTITION_KEY[topic]) or rec.get(ev.PARTITION_KEY[topic]) or "")
        return key, serde.serialize(rec, schemas.parsed(topic, version), self.schema_ids[topic]), rec

    def send(self, event: dict[str, Any], topic: str) -> None:
        key, value, rec = self.serialize(event, topic)
        event_id = rec["event_id"]
        chaos = self.chaos if (self.chaos and topic in self.chaos_cfg.topics) else None
        fault = "none"
        if chaos:
            if chaos.roll(self.chaos_cfg.corrupt_probability):
                value, fault = chaos.corrupt(value)
            elif chaos.roll(self.chaos_cfg.invalid_probability):
                bad, fault = chaos.mutate_invalid(rec, topic)
                value = serde.serialize(bad, schemas.parsed(topic, self.versions[topic]), self.schema_ids[topic])
            if chaos.roll(self.chaos_cfg.late_probability):
                self._seq += 1
                release = time.time() + self.chaos_cfg.late_delay_s
                label = "late" if fault == "none" else f"{fault}+late"
                heapq.heappush(self._delayed, (release, self._seq, topic, key, value, event_id, label))
            else:
                self._emit(topic, key, value, event_id, fault)
            if fault == "none" and chaos.roll(self.chaos_cfg.duplicate_probability):
                # the duplicate is released later => it arrives out of order relative to its original
                self._seq += 1
                release = time.time() + chaos.rng.uniform(0.0, max(self.chaos_cfg.late_delay_s, 1.0) / 4)
                heapq.heappush(self._delayed, (release, self._seq, topic, key, value, event_id, "duplicate"))
        else:
            self._emit(topic, key, value, event_id, fault)
        self.release_due()

    def send_raw(self, topic: str, key: str, value: bytes, event_id: str | None, fault: str) -> None:
        """Produce pre-built bytes (used by DLQ replay and explicit test scenarios)."""
        self._produce(topic, key, value, event_id, fault)

    def release_due(self, force: bool = False) -> None:
        now = time.time()
        while self._delayed and (force or self._delayed[0][0] <= now):
            _, _, topic, key, value, event_id, fault = heapq.heappop(self._delayed)
            self._produce(topic, key, value, event_id, fault)

    def pending_delayed(self) -> int:
        return len(self._delayed)

    def flush(self, wait_for_delayed: bool = True, timeout: float = 120.0) -> None:
        if self._reorder:
            self._flush_reorder()
        if wait_for_delayed:
            while self._delayed:
                time.sleep(max(0.0, min(1.0, self._delayed[0][0] - time.time())))
                self.release_due()
        remaining = self._producer.flush(timeout)
        if remaining:
            raise KafkaException(f"{remaining} messages still in queue after flush")
        self.write_delivery_log()

    def write_delivery_log(self) -> int:
        rows, self._log = self._log, []
        if not rows or self.delivery_sink is None:
            return len(rows)
        buf = io.StringIO()
        for r in rows:
            buf.write("\t".join("\\N" if v is None else str(v) for v in r) + "\n")
        with self.delivery_sink.cursor() as cur, cur.copy(
            "COPY ops.delivery_log (topic, kafka_partition, kafka_offset, event_id, fault, run_id) FROM STDIN"
        ) as cp:
            cp.write(buf.getvalue())
        self.delivery_sink.commit()
        return len(rows)

    def close(self) -> None:
        self.flush()
