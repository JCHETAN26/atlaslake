"""Phases 16 & 40: schema evolution end-to-end, and a breaking change rejected safely."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from benchmarks import scenarios as sc
from contracts import events as ev
from contracts import schemas
from contracts.registry import RegistryError, SchemaRegistry
from producers.kafka_producer import EventProducer
from tests.integration.helpers import drain, send_all, table, totals

T0 = datetime(2026, 4, 1, 9, 0, tzinfo=UTC)
BREAKING = (schemas.AVRO_DIR / "incompatible" / "order_event.v3_breaking.avsc").read_text()


def test_v1_and_v2_producers_coexist_through_to_silver(clean_platform, producer):
    spark = clean_platform
    reg = SchemaRegistry()
    subj = schemas.subject(ev.TOPIC_ORDERS)
    assert reg.get_compatibility(subj) == "BACKWARD_TRANSITIVE"
    assert len(reg.versions(subj)) == 2

    old = EventProducer(schema_versions={ev.TOPIC_ORDERS: 1}, run_id="v1-producer")
    v1_event = sc.order_event("o_v1", "ORDER_CREATED", T0, sales_channel="MOBILE")  # v1 cannot carry it
    old.send(v1_event, ev.TOPIC_ORDERS)
    old.flush()
    send_all(producer, [(ev.TOPIC_ORDERS, sc.order_event("o_v2", "ORDER_CREATED", T0, sales_channel="MOBILE"))])

    t = totals(drain(spark))
    assert t["accepted_new"] == 2 and t["rejected"] == 0
    orders = {r["order_id"]: r for r in table(spark, "silver.orders")}
    assert orders["o_v1"]["sales_channel"] is None
    assert orders["o_v2"]["sales_channel"] == "MOBILE"
    versions = {r["entity_key"]: r["schema_version"] for r in table(spark, "silver.events")}
    assert versions == {"o_v1": 1, "o_v2": 2}
    bronze_ids = {r["schema_id"] for r in table(spark, "bronze.orders")}
    assert len(bronze_ids) == 2  # both writer schemas retained with the raw bytes


def test_breaking_change_is_rejected_and_consumers_stay_healthy(clean_platform, producer):
    spark = clean_platform
    reg = SchemaRegistry()
    subj = schemas.subject(ev.TOPIC_ORDERS)
    res = reg.check_compatibility(subj, BREAKING)
    assert res.compatible is False
    assert any("amount_minor" in m for m in res.messages), res.messages
    with pytest.raises(RegistryError) as e:
        reg.register(subj, BREAKING)
    assert e.value.status == 409
    assert len(reg.versions(subj)) == 2  # nothing changed

    # The pipeline keeps flowing on the existing contract.
    send_all(producer, [(ev.TOPIC_ORDERS, sc.order_event("o_after_reject", "ORDER_CREATED", T0))])
    assert totals(drain(spark))["accepted_new"] == 1


def test_correct_migration_procedure_add_optional_field(clean_platform):
    """The documented migration: additive optional field with default → compatible → registered."""
    reg = SchemaRegistry()
    subj = schemas.subject(ev.TOPIC_ORDERS)
    v3 = json.loads(schemas.schema_text(ev.TOPIC_ORDERS, 2))
    v3["fields"].append({"name": "gift_message", "type": ["null", "string"], "default": None})
    text = json.dumps(v3)
    assert reg.check_compatibility(subj, text).compatible
    sid = reg.register(subj, text)
    try:
        assert len(reg.versions(subj)) == 3
    finally:
        # keep the shared dev registry at the contract versions checked into the repo
        reg._req("DELETE", f"/subjects/{subj}/versions/3")
        reg._req("DELETE", f"/subjects/{subj}/versions/3?permanent=true")
    assert sid > 0
    _ = timedelta
