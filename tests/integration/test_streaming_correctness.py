"""Phases 11-13 against the real stack: Kafka → Spark Structured Streaming → Iceberg."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from benchmarks import scenarios as sc
from contracts import events as ev
from tests.integration.helpers import drain, send_all, table, totals

T0 = datetime(2026, 3, 1, 10, 0, tzinfo=UTC)


def test_duplicates_a_b_c_b_a_yield_a_b_c(clean_platform, producer):
    """Phase 11: deliveries A B C B A → exactly A B C in Silver."""
    spark = clean_platform
    a = sc.order_event("o_dup_1", "ORDER_CREATED", T0)
    b = sc.order_event("o_dup_1", "ORDER_UPDATED", T0 + timedelta(minutes=1), status="PAID")
    c = sc.order_event("o_dup_1", "ORDER_SHIPPED", T0 + timedelta(minutes=5))
    send_all(producer, [(ev.TOPIC_ORDERS, x) for x in (a, b, c, b, a)])

    t = totals(drain(spark))
    assert t["received"] == 5
    assert t["duplicates_in_batch"] + t["duplicates_known"] == 2
    assert t["accepted_new"] == 3

    ledger = table(spark, "silver.events", "entity_key = 'o_dup_1'")
    assert sorted(r["event_id"] for r in ledger) == sorted([a["event_id"], b["event_id"], c["event_id"]])
    orders = table(spark, "silver.orders", "order_id = 'o_dup_1'")
    assert len(orders) == 1 and orders[0]["status"] == "SHIPPED"
    assert len(table(spark, "bronze.orders")) == 5  # Bronze keeps every physical delivery


def test_duplicates_across_micro_batches_and_redelivery(clean_platform, producer):
    spark = clean_platform
    events = sc.lifecycle("o_dup_2", T0)
    send_all(producer, events)
    first = totals(drain(spark))
    send_all(producer, events)  # full redelivery later (e.g. relay crash before checkpoint)
    second = totals(drain(spark))
    assert first["accepted_new"] == 4
    assert second["accepted_new"] == 0 and second["duplicates_known"] == 4
    assert len(table(spark, "silver.events")) == 4
    pay = table(spark, "silver.payments")
    assert len(pay) == 1 and pay[0]["status"] == "COMPLETED"


def test_out_of_order_arrival_resolves_by_event_time(clean_platform, producer):
    """Phase 12: arrival SHIPPED, CREATED, PAYMENT_COMPLETED (event times 10:05, 10:00, 10:01)."""
    spark = clean_platform
    created, payment, paid, shipped = (e for _, e in sc.lifecycle("o_ooo_1", T0, amount_minor=2_500))
    # Separate micro-batches so state is merged across batches, not only within one.
    send_all(producer, [(ev.TOPIC_ORDERS, shipped)])
    drain(spark)
    partial = table(spark, "silver.orders", "order_id = 'o_ooo_1'")[0]
    assert partial["status"] == "SHIPPED" and partial["customer_id"] is None  # created not seen yet

    send_all(producer, [(ev.TOPIC_ORDERS, created)])
    drain(spark)
    send_all(producer, [(ev.TOPIC_PAYMENTS, payment), (ev.TOPIC_ORDERS, paid)])
    drain(spark)

    o = table(spark, "silver.orders", "order_id = 'o_ooo_1'")[0]
    assert o["status"] == "SHIPPED"  # a late CREATED/PAID never rolls status back
    assert o["customer_id"] == "c_test_1" and o["amount_minor"] == 2_500
    assert o["order_timestamp"].replace(tzinfo=UTC) == T0
    assert o["paid_at"] is not None and o["shipped_at"] is not None
    assert table(spark, "silver.payments")[0]["status"] == "COMPLETED"


def test_invalid_and_corrupt_records_go_to_dlq_and_replay_safely(clean_platform, producer):
    """Phase 13: invalid → DLQ with reasons; corrected replay re-enters exactly once."""
    from contracts import schemas, serde
    from ingestion.streaming.dlq_replay import replay

    spark = clean_platform
    good = sc.order_event("o_dlq_1", "ORDER_CREATED", T0)
    negative = sc.order_event("o_dlq_2", "ORDER_CREATED", T0, amount_minor=-50)
    negative["items"][0]["unit_price_minor"] = -50
    bad_currency = sc.order_event("o_dlq_3", "ORDER_CREATED", T0, currency="XXX")
    send_all(producer, [(ev.TOPIC_ORDERS, x) for x in (good, negative, bad_currency)])
    key, payload, _ = producer.serialize(sc.order_event("o_dlq_4", "ORDER_CREATED", T0), ev.TOPIC_ORDERS)
    producer.send_raw(ev.TOPIC_ORDERS, key, payload[: len(payload) // 2], None, "corrupt:TRUNCATED_AVRO")
    producer.send_raw(ev.TOPIC_ORDERS, key, b"\x00" + (424242).to_bytes(4, "big") + payload[5:], None,
                      "corrupt:UNKNOWN_SCHEMA_ID")
    producer.flush()

    t = totals(drain(spark))
    assert t["received"] == 5 and t["rejected"] == 4 and t["accepted_new"] == 1
    dlq = {r["dlq_id"]: r for r in table(spark, "ops.dlq")}
    codes = sorted(r["error_codes"] for r in dlq.values())
    assert any("NEGATIVE_AMOUNT" in c for c in codes)
    assert any("INVALID_CURRENCY" in c for c in codes)
    assert "AVRO_DECODE_ERROR" in codes and "UNKNOWN_SCHEMA_ID" in codes
    assert all(r["status"] == "PENDING" and r["payload"] for r in dlq.values())

    # operator corrections: fix the currency, discard the negative order; corrupt ones stay unresolved
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        fh.write(json.dumps({"event_id": bad_currency["event_id"], "set": {"currency": "GBP"}}) + "\n")
        fh.write(json.dumps({"event_id": negative["event_id"], "action": "discard",
                             "reason": "test order, never real"}) + "\n")
    res = replay(topic="orders", corrections_path=fh.name, recover_from_source=False, spark=spark)
    assert res["replayed"] == 1 and res["discarded"] == 1 and res["unresolved"] == 2

    t2 = totals(drain(spark))
    assert t2["accepted_new"] == 1
    fixed = table(spark, "silver.orders", "order_id = 'o_dlq_3'")
    assert fixed and fixed[0]["currency"] == "GBP"
    # replaying the same correction again must not create a second logical event
    res2 = replay(topic="orders", corrections_path=fh.name, recover_from_source=False, spark=spark)
    assert res2["replayed"] == 0
    statuses = sorted(r["status"] for r in table(spark, "ops.dlq"))
    assert statuses == ["DISCARDED", "PENDING", "PENDING", "REPLAYED"]
    assert len(table(spark, "silver.events")) == 2
    _ = schemas, serde
