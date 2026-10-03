"""Kafka topic administration."""

from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from contracts import events as ev
from core.config import settings

TOPIC_CONFIG = {
    # Broker stamps the ingestion time; producers own event_time inside the payload.
    "message.timestamp.type": "LogAppendTime",
    "retention.ms": str(14 * 24 * 3600 * 1000),
    "retention.bytes": str(20 * 1024**3),
    "compression.type": "producer",
    "min.insync.replicas": "1",
}


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": settings().kafka.bootstrap})


def ensure_topics(partitions: int | None = None, topics: tuple[str, ...] = ev.TOPICS) -> list[str]:
    client = admin()
    existing = set(client.list_topics(timeout=10).topics)
    n = partitions or settings().kafka.partitions
    new = [
        NewTopic(t, num_partitions=n, replication_factor=1, config=TOPIC_CONFIG)
        for t in topics
        if t not in existing
    ]
    if new:
        for _, fut in client.create_topics(new).items():
            fut.result()
    return [t.topic for t in new]


def delete_topics(topics: tuple[str, ...] = ev.TOPICS) -> None:
    client = admin()
    existing = set(client.list_topics(timeout=10).topics)
    doomed = [t for t in topics if t in existing]
    if doomed:
        for _, fut in client.delete_topics(doomed).items():
            fut.result()


def end_offsets(topic: str) -> dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": settings().kafka.bootstrap,
            "group.id": "atlas-offset-probe",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic, timeout=10).topics[topic]
        out = {}
        for p in md.partitions:
            _, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            out[p] = hi
        return out
    finally:
        c.close()


def consumer_group_lag(group: str, topic: str) -> dict[int, int]:
    """Lag per partition = log end offset − committed offset (committed=-1 ⇒ full end offset)."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {"bootstrap.servers": settings().kafka.bootstrap, "group.id": group, "enable.auto.commit": False}
    )
    try:
        ends = end_offsets(topic)
        committed = c.committed([TopicPartition(topic, p) for p in ends], timeout=10)
        return {tp.partition: ends[tp.partition] - max(tp.offset, 0) for tp in committed}
    finally:
        c.close()
