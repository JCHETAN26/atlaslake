"""Semantic validation of decoded events — one implementation shared by batch, streaming and tests.

Avro guarantees *shape*; these rules guarantee *meaning*. A record failing any rule is routed to the
DLQ with every violated rule listed (comma-separated stable codes), never silently dropped.
"""

from __future__ import annotations

import re
from typing import Any

from contracts import events as ev

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
MIN_EVENT_TS = 946_684_800_000_000  # 2000-01-01
MAX_FUTURE_SKEW_US = 24 * 3600 * 1_000_000  # event_time may not be > 1 day after ingestion

REQUIRED: dict[str, dict[str, tuple[str, ...]]] = {
    ev.TOPIC_ORDERS: {
        "*": ("order_id", "status"),
        "ORDER_CREATED": ("customer_id", "store_id", "amount_minor", "currency", "items"),
    },
    ev.TOPIC_PAYMENTS: {"*": ("payment_id", "order_id", "status", "currency")},
    ev.TOPIC_INVENTORY: {"*": ("inventory_key", "store_id", "product_id", "reason")},
    ev.TOPIC_CUSTOMERS: {"*": ("customer_id", "email", "city", "country", "segment")},
    ev.TOPIC_PRODUCTS: {"*": ("product_id", "sku", "name", "category", "currency")},
}

PAYMENT_EVENT_STATUS = {
    "PAYMENT_STARTED": "STARTED",
    "PAYMENT_COMPLETED": "COMPLETED",
    "PAYMENT_FAILED": "FAILED",
}
INVENTORY_REASONS_BY_TYPE = {
    "INVENTORY_RESERVED": {"RESERVATION"},
    "INVENTORY_UPDATED": {"SHIPMENT", "RELEASE", "RESTOCK"},
}


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def validate(rec: dict[str, Any], topic: str, ingestion_us: int | None = None) -> list[str]:
    """Return a list of violated rule codes (empty list == valid)."""
    errors: list[str] = []
    et = rec.get("event_type")
    if _blank(rec.get("event_id")) or not UUID_RE.match(str(rec.get("event_id"))):
        errors.append("INVALID_EVENT_ID")
    if et not in ev.EVENT_TYPES.get(topic, ()):
        errors.append("UNKNOWN_EVENT_TYPE")
    ts = rec.get("event_timestamp")
    if not isinstance(ts, int) or ts < MIN_EVENT_TS:
        errors.append("INVALID_EVENT_TIMESTAMP")
    elif ingestion_us is not None and ts - ingestion_us > MAX_FUTURE_SKEW_US:
        errors.append("EVENT_TIMESTAMP_IN_FUTURE")

    rules = REQUIRED.get(topic, {})
    for f in rules.get("*", ()) + rules.get(str(et), ()):
        if _blank(rec.get(f)):
            errors.append(f"MISSING_{f.upper()}")

    if rec.get("currency") is not None and rec["currency"] not in ev.CURRENCIES:
        errors.append("INVALID_CURRENCY")
    amount = rec.get("amount_minor")
    if amount is not None and amount < 0:
        errors.append("NEGATIVE_AMOUNT")

    if topic == ev.TOPIC_ORDERS:
        status = rec.get("status")
        if status is not None and status not in ev.ORDER_STATUSES:
            errors.append("INVALID_STATUS")
        expected = ev.EVENT_STATUS.get(str(et))
        if expected and status in ev.ORDER_STATUSES and status != expected:
            errors.append("STATUS_EVENT_MISMATCH")
        if rec.get("sales_channel") is not None and rec["sales_channel"] not in ev.SALES_CHANNELS:
            errors.append("INVALID_SALES_CHANNEL")
        items = rec.get("items")
        if et == "ORDER_CREATED" and items:
            if any(
                i.get("quantity", 0) <= 0 or i.get("unit_price_minor", -1) < 0 or _blank(i.get("product_id"))
                for i in items
            ):
                errors.append("INVALID_ORDER_ITEM")
            elif amount is not None and amount != sum(i["quantity"] * i["unit_price_minor"] for i in items):
                errors.append("AMOUNT_MISMATCH")
    elif topic == ev.TOPIC_PAYMENTS:
        if rec.get("status") not in ev.PAYMENT_STATUSES:
            errors.append("INVALID_STATUS")
        elif PAYMENT_EVENT_STATUS.get(str(et)) not in (None, rec.get("status")):
            errors.append("STATUS_EVENT_MISMATCH")
        if not isinstance(rec.get("attempt"), int) or rec["attempt"] < 1:
            errors.append("INVALID_ATTEMPT")
    elif topic == ev.TOPIC_INVENTORY:
        reason = rec.get("reason")
        if reason not in ev.INVENTORY_REASONS:
            errors.append("INVALID_REASON")
        elif reason not in INVENTORY_REASONS_BY_TYPE.get(str(et), {reason}):
            errors.append("REASON_EVENT_MISMATCH")
        key = rec.get("inventory_key")
        if key and f"{rec.get('store_id')}:{rec.get('product_id')}" != key:
            errors.append("INVALID_INVENTORY_KEY")
    elif topic == ev.TOPIC_CUSTOMERS:
        if rec.get("email") and "@" not in rec["email"]:
            errors.append("INVALID_EMAIL")
        if rec.get("segment") is not None and rec["segment"] not in ev.CUSTOMER_SEGMENTS:
            errors.append("INVALID_SEGMENT")
    elif topic == ev.TOPIC_PRODUCTS:
        price = rec.get("unit_price_minor")
        if price is not None and price < 0:
            errors.append("NEGATIVE_AMOUNT")
    return errors
