"""Hand-built canonical events for exact test/experiment scenarios.

The simulator produces realistic *volumes*; these helpers produce exact *sequences* (e.g. A B C B A,
or SHIPPED arriving before CREATED) with stable ids so assertions are precise.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from contracts import events as ev

NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://atlaslake/scenarios")


def eid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


def us(dt: datetime) -> int:
    return int(dt.replace(tzinfo=dt.tzinfo or UTC).timestamp() * 1_000_000)


def order_event(order_id: str, event_type: str, at: datetime, *, status: str | None = None,
                amount_minor: int = 1_000, currency: str = "GBP", customer_id: str = "c_test_1",
                store_id: str = "s_test_1", sales_channel: str | None = "WEB", tag: str = "") -> dict[str, Any]:
    status = status or ev.EVENT_STATUS.get(event_type) or "PAID"
    e: dict[str, Any] = {
        "event_id": eid(f"{order_id}:{event_type}:{tag}"), "event_type": event_type, "event_timestamp": us(at),
        "order_id": order_id, "status": status, "customer_id": customer_id, "store_id": store_id,
        "amount_minor": amount_minor, "currency": currency, "sales_channel": sales_channel,
    }
    if event_type == "ORDER_CREATED":
        e["items"] = [{"line_number": 1, "product_id": "p_test_1", "quantity": 1, "unit_price_minor": amount_minor}]
    if event_type == "ORDER_CANCELLED":
        e["cancel_reason"] = "CUSTOMER_REQUEST"
    if event_type == "ORDER_SHIPPED":
        e["carrier"], e["tracking_number"] = "DHL", f"TRK-{order_id}"
    return e


def payment_event(order_id: str, event_type: str, at: datetime, *, attempt: int = 1, amount_minor: int = 1_000,
                  currency: str = "GBP", tag: str = "") -> dict[str, Any]:
    status = {"PAYMENT_STARTED": "STARTED", "PAYMENT_COMPLETED": "COMPLETED", "PAYMENT_FAILED": "FAILED"}[event_type]
    return {
        "event_id": eid(f"pay:{order_id}:{attempt}:{event_type}:{tag}"), "event_type": event_type,
        "event_timestamp": us(at), "payment_id": f"pay_{order_id}_{attempt}", "order_id": order_id,
        "attempt": attempt, "status": status, "amount_minor": amount_minor, "currency": currency,
        "method": "CARD", "failure_reason": "CARD_DECLINED" if status == "FAILED" else None,
    }


def customer_event(customer_id: str, event_type: str, at: datetime, city: str, *, state: str | None = None,
                   country: str = "United States", tag: str = "") -> dict[str, Any]:
    return {
        "event_id": eid(f"cust:{customer_id}:{event_type}:{city}:{tag}"), "event_type": event_type,
        "event_timestamp": us(at), "customer_id": customer_id, "email": f"{customer_id}@example.com",
        "phone": "+1-555-0000000", "first_name": "Test", "last_name": "Customer", "city": city, "state": state,
        "country": country, "segment": "CONSUMER", "signup_timestamp": us(datetime(2025, 6, 1, tzinfo=UTC)),
    }


def lifecycle(order_id: str, t0: datetime, amount_minor: int = 1_000) -> list[tuple[str, dict[str, Any]]]:
    """CREATED 10:00 → PAYMENT_COMPLETED 10:01 → PAID 10:01:01 → SHIPPED 10:05 (topic, event)."""
    return [
        (ev.TOPIC_ORDERS, order_event(order_id, "ORDER_CREATED", t0, amount_minor=amount_minor)),
        (ev.TOPIC_PAYMENTS, payment_event(order_id, "PAYMENT_COMPLETED", t0 + timedelta(minutes=1),
                                          amount_minor=amount_minor)),
        (ev.TOPIC_ORDERS, order_event(order_id, "ORDER_UPDATED", t0 + timedelta(minutes=1, seconds=1),
                                      status="PAID", amount_minor=amount_minor)),
        (ev.TOPIC_ORDERS, order_event(order_id, "ORDER_SHIPPED", t0 + timedelta(minutes=5), amount_minor=amount_minor)),
    ]


def now_utc() -> datetime:
    return datetime.now(UTC)
