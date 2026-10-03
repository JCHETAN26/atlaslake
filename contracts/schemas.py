"""Load the versioned Avro contracts and turn canonical event dicts into schema-conformant records."""

from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import fastavro

from contracts import events as ev

AVRO_DIR = Path(__file__).parent / "avro"

SCHEMA_FILES: dict[str, dict[int, str]] = {
    ev.TOPIC_ORDERS: {1: "order_event.v1.avsc", 2: "order_event.v2.avsc"},
    ev.TOPIC_PAYMENTS: {1: "payment_event.v1.avsc"},
    ev.TOPIC_INVENTORY: {1: "inventory_event.v1.avsc"},
    ev.TOPIC_CUSTOMERS: {1: "customer_event.v1.avsc"},
    ev.TOPIC_PRODUCTS: {1: "product_event.v1.avsc"},
}


def subject(topic: str) -> str:
    """Confluent TopicNameStrategy subject."""
    return f"{topic}-value"


@lru_cache(maxsize=None)
def schema_text(topic: str, version: int) -> str:
    return (AVRO_DIR / SCHEMA_FILES[topic][version]).read_text()


@lru_cache(maxsize=None)
def schema_json(topic: str, version: int) -> dict:
    return json.loads(schema_text(topic, version))


@lru_cache(maxsize=None)
def parsed(topic: str, version: int) -> Any:
    return fastavro.parse_schema(schema_json(topic, version))


def latest_version(topic: str) -> int:
    return max(SCHEMA_FILES[topic])


def field_names(topic: str, version: int) -> list[str]:
    return [f["name"] for f in schema_json(topic, version)["fields"]]


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def to_record(event: dict[str, Any], topic: str, version: int, producer: str = ev.PRODUCER) -> dict[str, Any]:
    """Build an Avro record for ``topic``/``version`` from a canonical event dict.

    Fields not present in that schema version (e.g. ``sales_channel`` for OrderEvent v1) are dropped;
    that is precisely how an old producer behaves.
    """
    rec: dict[str, Any] = {}
    for f in schema_json(topic, version)["fields"]:
        name = f["name"]
        if name == "producer":
            rec[name] = event.get("producer") or producer
        elif name == "schema_version":
            rec[name] = version
        else:
            rec[name] = _clean(event.get(name))
    for int_field in ("amount_minor", "attempt", "on_hand_delta", "reserved_delta", "unit_price_minor",
                      "event_timestamp", "signup_timestamp"):
        if rec.get(int_field) is not None and not isinstance(rec[int_field], int):
            rec[int_field] = int(rec[int_field])
    return rec
