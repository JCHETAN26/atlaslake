"""DLQ replay (Phase 13): correct rejected records and send them back through the front door.

Corrected records are re-published to their original topic — they are *not* written into Silver
directly — so they pass the exact same decode/validation/dedup path as any other event. Safety:

* ``event_id`` and ``event_timestamp`` are immutable: corrections may not change them, so a replayed
  record can never create a second logical event (dedup + ledger still apply).
* Publish happens before the DLQ row is marked REPLAYED; a crash in between only causes a duplicate
  delivery, which deduplication absorbs.
* A correction that still fails validation is not published; the row stays PENDING with a note.

Correction sources, in priority order:
1. ``--corrections`` JSONL: ``{"event_id"|"dlq_id": ..., "set": {"field": value}}`` or
   ``{"event_id"|"dlq_id": ..., "action": "discard", "reason": "..."}``
2. ``--from-source``: the authoritative copy of the event in the source outbox (located by event_id,
   or — for undecodable bytes — by the Kafka coordinates recorded in ``ops.delivery_log``).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from contracts import events as ev
from contracts import schemas
from contracts.registry import SchemaRegistry
from contracts.serde import DecodeError, deserialize, schema_id_of
from contracts.validation import validate
from ingestion.cdc.outbox_relay import row_to_event
from producers.kafka_producer import EventProducer
from simulator.postgres_sink import connect

log = logging.getLogger(__name__)
IMMUTABLE = ("event_id", "event_timestamp")


def load_corrections(path: str | None) -> dict[str, dict]:
    if not path:
        return {}
    out = {}
    for line in Path(path).read_text().splitlines():
        if line.strip():
            c = json.loads(line)
            out[c.get("dlq_id") or c["event_id"]] = c
    return out


def _source_event(conn, event_id: str | None, topic: str, partition: int | None, offset: int | None
                  ) -> dict[str, Any] | None:
    if not event_id and partition is not None and offset is not None:
        row = conn.execute("SELECT event_id::text FROM ops.delivery_log WHERE topic=%s AND kafka_partition=%s "
                           "AND kafka_offset=%s", (topic, partition, offset)).fetchone()
        event_id = row[0] if row else None
    if not event_id:
        return None
    row = conn.execute(
        """SELECT seq, event_id::text, event_type, topic, partition_key,
                  (extract(epoch from event_timestamp) * 1000000)::bigint, payload
           FROM commerce.outbox WHERE event_id = %s::uuid""", (event_id,)).fetchone()
    return row_to_event(row)[1] if row else None


def replay(topic: str | None = None, corrections_path: str | None = None, recover_from_source: bool = True,
           dry_run: bool = False, spark=None) -> dict[str, Any]:
    from pyspark.sql import functions as F

    from spark.common.session import get_spark
    from spark.common.tables import fq

    spark = spark or get_spark("atlas-dlq-replay")
    topic_name = ev.DATASETS.get(topic, topic) if topic else None
    pending = spark.table(fq("ops.dlq")).filter("status = 'PENDING'")
    if topic_name:
        pending = pending.filter(F.col("topic") == topic_name)
    rows = pending.collect()
    corrections = load_corrections(corrections_path)
    registry = SchemaRegistry()
    results: dict[str, list] = {"replayed": [], "discarded": [], "unresolved": []}
    producer = None if dry_run else EventProducer(delivery_sink=connect(), run_id="dlq-replay")

    with connect() as conn:
        for r in rows:
            t = r["topic"]
            decoded: dict[str, Any] | None = None
            try:
                sid = schema_id_of(bytes(r["payload"]) if r["payload"] is not None else None)
                decoded = deserialize(bytes(r["payload"]), registry.schema_by_id(sid),
                                      schemas.parsed(t, schemas.latest_version(t)))
            except (DecodeError, Exception) as exc:  # undecodable bytes
                log.debug("dlq %s undecodable: %s", r["dlq_id"], exc)
            event_id = (decoded or {}).get("event_id") or r["event_id"]
            corr = corrections.get(r["dlq_id"]) or (corrections.get(event_id) if event_id else None)
            fixed: dict[str, Any] | None = None
            note = ""
            if corr and corr.get("action") == "discard":
                results["discarded"].append((r["dlq_id"], corr.get("reason", "discarded by operator")))
                continue
            if corr and decoded is not None:
                changes = corr.get("set", {})
                if any(k in changes for k in IMMUTABLE):
                    results["unresolved"].append((r["dlq_id"], "correction may not change event_id/event_timestamp"))
                    continue
                fixed = {**decoded, **changes}
                note = f"corrected fields {sorted(changes)}"
            elif recover_from_source:
                fixed = _source_event(conn, event_id, t, r["kafka_partition"], r["kafka_offset"])
                note = "recovered authoritative copy from source outbox"
            if fixed is None:
                results["unresolved"].append((r["dlq_id"], "no correction available"))
                continue
            rec = schemas.to_record(fixed, t, schemas.latest_version(t))
            errors = validate(rec, t)
            if errors:
                results["unresolved"].append((r["dlq_id"], f"still invalid: {errors}"))
                continue
            if producer:
                producer.send(fixed, t)
            results["replayed"].append((r["dlq_id"], note))
        if producer:
            producer.flush()

    if not dry_run:
        updates = ([(i, "REPLAYED", n) for i, n in results["replayed"]]
                   + [(i, "DISCARDED", n) for i, n in results["discarded"]])
        if updates:
            df = spark.createDataFrame(updates, "dlq_id string, new_status string, note string")
            df.createOrReplaceTempView("dlq_updates")
            spark.sql(f"""MERGE INTO {fq('ops.dlq')} t USING dlq_updates s ON t.dlq_id = s.dlq_id
                          WHEN MATCHED AND t.status = 'PENDING' THEN UPDATE SET
                            status = s.new_status, resolution_note = s.note, resolved_at = current_timestamp()""")
    return {"pending_examined": len(rows), "replayed": len(results["replayed"]),
            "discarded": len(results["discarded"]), "unresolved": len(results["unresolved"]),
            "unresolved_examples": results["unresolved"][:10], "dry_run": dry_run}
