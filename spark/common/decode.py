"""Decode + validate raw Kafka/Bronze records inside Spark (``mapInPandas``).

Shared by the streaming job and every batch job, so a record is judged identically no matter which
path processes it. Decoding uses the **writer** schema fetched from the registry by the id in the
wire header and resolves into the latest local contract (reader schema) — the only correct way to
read an evolving Avro topic.

Failure semantics:
* corrupt bytes / unknown schema id / Avro errors / rule violations → record flagged → DLQ
* registry *unreachable* → the batch fails and is retried (we never DLQ good data because a
  dependency is down)
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from contracts import events as ev
from contracts import schemas
from contracts.registry import RegistryError, SchemaRegistry
from contracts.serde import DecodeError, deserialize, schema_id_of
from contracts.validation import validate

ENTITY_KEY = {
    ev.TOPIC_ORDERS: "order_id",
    ev.TOPIC_PAYMENTS: "payment_id",
    ev.TOPIC_INVENTORY: "inventory_key",
    ev.TOPIC_CUSTOMERS: "customer_id",
    ev.TOPIC_PRODUCTS: "product_id",
}

RAW_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_key", StringType()),
    StructField("payload", BinaryType()),
    StructField("ingestion_timestamp", TimestampType()),
])

DECODED_SCHEMA = StructType(RAW_SCHEMA.fields + [
    StructField("schema_id", IntegerType()),
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", TimestampType()),
    StructField("entity_key", StringType()),
    StructField("record_json", StringType()),
    StructField("error_codes", StringType()),
    StructField("error_message", StringType()),
])

_REGISTRY: SchemaRegistry | None = None


def _registry(url: str) -> SchemaRegistry:
    global _REGISTRY
    if _REGISTRY is None or _REGISTRY.url != url.rstrip("/"):
        _REGISTRY = SchemaRegistry(url)
    return _REGISTRY


def decode_record(payload: bytes | None, topic: str, ingestion_us: int | None, registry: SchemaRegistry
                  ) -> dict[str, Any]:
    """Pure-Python decode+validate of one record. Returns the decoded-columns dict."""
    out: dict[str, Any] = {"schema_id": None, "schema_version": None, "event_id": None, "event_type": None,
                           "event_timestamp": None, "entity_key": None, "record_json": None,
                           "error_codes": None, "error_message": None}
    if topic not in ev.TOPICS:
        out.update(error_codes="UNKNOWN_TOPIC", error_message=f"unexpected topic {topic}")
        return out
    try:
        sid = schema_id_of(payload)
        out["schema_id"] = sid
        try:
            writer = registry.schema_by_id(sid)
        except RegistryError as e:
            if e.status == 404:
                raise DecodeError("UNKNOWN_SCHEMA_ID", f"schema id {sid} not in registry", sid) from e
            raise
        rec = deserialize(payload, writer, schemas.parsed(topic, schemas.latest_version(topic)))  # type: ignore[arg-type]
    except DecodeError as e:
        out.update(error_codes=e.code, error_message=e.message[:1000])
        return out
    out["schema_version"] = rec.get("schema_version")
    out["event_id"] = rec.get("event_id") or None
    out["event_type"] = rec.get("event_type")
    ts = rec.get("event_timestamp")
    out["event_timestamp"] = ts if isinstance(ts, int) else None
    out["entity_key"] = rec.get(ENTITY_KEY[topic]) or None
    out["record_json"] = json.dumps(rec, separators=(",", ":"), ensure_ascii=False)
    errors = validate(rec, topic, ingestion_us)
    if errors:
        out["error_codes"] = ",".join(errors)
        out["error_message"] = f"failed {len(errors)} validation rule(s)"
    return out


def _decode_batches(registry_url: str):
    def fn(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        reg = _registry(registry_url)
        for pdf in batches:
            ingest = pd.to_datetime(pdf["ingestion_timestamp"])
            # unit-safe: Arrow may hand us ns or us resolution depending on versions
            ingest_us = ingest.astype("datetime64[us]").astype("int64").where(ingest.notna(), None)
            rows = [decode_record(p, t, None if pd.isna(i) else int(i), reg)
                    for p, t, i in zip(pdf["payload"], pdf["topic"], ingest_us, strict=True)]
            dec = pd.DataFrame(rows, index=pdf.index)
            dec["event_timestamp"] = pd.to_datetime(dec["event_timestamp"], unit="us")
            for c in ("schema_id", "schema_version"):
                dec[c] = dec[c].astype("Int32")
            yield pd.concat([pdf, dec], axis=1)
    return fn


def standardize_kafka(df: DataFrame) -> DataFrame:
    """Kafka source columns → RAW_SCHEMA."""
    return df.select(
        F.lit("kafka").alias("source"),
        F.col("topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").alias("payload"),
        F.col("timestamp").alias("ingestion_timestamp"),
    )


def decode(raw: DataFrame, registry_url: str) -> DataFrame:
    return raw.select(*RAW_SCHEMA.fieldNames()).mapInPandas(_decode_batches(registry_url), DECODED_SCHEMA)
