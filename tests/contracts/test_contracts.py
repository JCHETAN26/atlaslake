"""Offline contract tests: schemas parse, agree with the domain model, and evolve compatibly.

Registry-level compatibility enforcement is tested in tests/integration/test_schema_registry.py.
"""

import io
import json

import fastavro
import pytest

from contracts import events as ev
from contracts import schemas, serde
from contracts.validation import validate

ALL_VERSIONS = [(t, v) for t, vs in schemas.SCHEMA_FILES.items() for v in vs]


@pytest.mark.parametrize(("topic", "version"), ALL_VERSIONS)
def test_schema_parses_and_has_envelope(topic, version):
    schemas.parsed(topic, version)
    names = schemas.field_names(topic, version)
    assert names[: len(ev.ENVELOPE_FIELDS)] == list(ev.ENVELOPE_FIELDS)


@pytest.mark.parametrize("topic", ev.TOPICS)
def test_latest_schema_matches_domain_payload_fields(topic):
    latest = schemas.latest_version(topic)
    payload = schemas.field_names(topic, latest)[len(ev.ENVELOPE_FIELDS):]
    assert tuple(payload) == ev.PAYLOAD_FIELDS[topic]
    assert latest == ev.LATEST_SCHEMA_VERSION[topic]


def _order_event(**over):
    base = {
        "event_id": "6700b6ed-7016-5462-ae82-79418ce901b4", "event_type": "ORDER_CREATED",
        "event_timestamp": 1_767_259_606_285_228, "order_id": "o0000000001", "customer_id": "c00000001",
        "store_id": "s0001", "status": "CREATED", "amount_minor": 990, "currency": "GBP",
        "items": [{"line_number": 1, "product_id": "p00001", "quantity": 2, "unit_price_minor": 495}],
        "sales_channel": "WEB",
    }
    return {**base, **over}


def _roundtrip(writer_v: int, reader_v: int, event: dict) -> dict:
    t = ev.TOPIC_ORDERS
    payload = serde.serialize(schemas.to_record(event, t, writer_v), schemas.parsed(t, writer_v), 7)
    assert serde.schema_id_of(payload) == 7
    return serde.deserialize(payload, schemas.parsed(t, writer_v), schemas.parsed(t, reader_v))


def test_v1_data_readable_by_v2_reader_with_null_sales_channel():
    rec = _roundtrip(1, 2, _order_event())
    assert rec["sales_channel"] is None
    assert rec["amount_minor"] == 990
    assert rec["event_timestamp"] == 1_767_259_606_285_228


def test_v2_data_readable_by_v1_reader_forward_compatible():
    rec = _roundtrip(2, 1, _order_event())
    assert "sales_channel" not in rec
    assert rec["order_id"] == "o0000000001"


def test_breaking_schema_cannot_read_existing_data():
    bad = fastavro.parse_schema(json.loads((schemas.AVRO_DIR / "incompatible" / "order_event.v3_breaking.avsc").read_text()))
    payload = serde.serialize(schemas.to_record(_order_event(), ev.TOPIC_ORDERS, 2), schemas.parsed(ev.TOPIC_ORDERS, 2), 1)
    with pytest.raises(serde.DecodeError):
        serde.deserialize(payload, schemas.parsed(ev.TOPIC_ORDERS, 2), bad)


@pytest.mark.parametrize("payload", [b"", b"\x00\x00", b"\x01\x00\x00\x00\x01abc"])
def test_corrupt_headers_raise_corrupt_payload(payload):
    with pytest.raises(serde.DecodeError) as e:
        serde.schema_id_of(payload)
    assert e.value.code == "CORRUPT_PAYLOAD"


def test_truncated_avro_raises_decode_error():
    t = ev.TOPIC_ORDERS
    payload = serde.serialize(schemas.to_record(_order_event(), t, 2), schemas.parsed(t, 2), 3)
    with pytest.raises(serde.DecodeError) as e:
        serde.deserialize(payload[:-7], schemas.parsed(t, 2), schemas.parsed(t, 2))
    assert e.value.code == "AVRO_DECODE_ERROR"


def test_v1_writer_drops_fields_it_does_not_know():
    rec = schemas.to_record(_order_event(), ev.TOPIC_ORDERS, 1)
    assert "sales_channel" not in rec
    assert rec["schema_version"] == 1
    fastavro.schemaless_writer(io.BytesIO(), schemas.parsed(ev.TOPIC_ORDERS, 1), rec)


# ------------------------------------------------------------------ validation rules
def test_valid_event_passes():
    assert validate(_order_event(), ev.TOPIC_ORDERS) == []


@pytest.mark.parametrize(
    ("override", "code"),
    [
        ({"order_id": ""}, "MISSING_ORDER_ID"),
        ({"amount_minor": -5}, "NEGATIVE_AMOUNT"),
        ({"amount_minor": 991}, "AMOUNT_MISMATCH"),
        ({"currency": "XXX"}, "INVALID_CURRENCY"),
        ({"status": "TELEPORTED"}, "INVALID_STATUS"),
        ({"status": "SHIPPED"}, "STATUS_EVENT_MISMATCH"),
        ({"event_type": "ORDER_EXPLODED"}, "UNKNOWN_EVENT_TYPE"),
        ({"event_id": "not-a-uuid"}, "INVALID_EVENT_ID"),
        ({"event_timestamp": 5}, "INVALID_EVENT_TIMESTAMP"),
        ({"sales_channel": "FAX"}, "INVALID_SALES_CHANNEL"),
        ({"customer_id": None}, "MISSING_CUSTOMER_ID"),
    ],
)
def test_invalid_order_events_are_flagged(override, code):
    assert code in validate(_order_event(**override), ev.TOPIC_ORDERS)


def test_future_event_time_is_flagged_relative_to_ingestion():
    rec = _order_event(event_timestamp=1_767_259_606_285_228 + 3 * 86_400 * 1_000_000)
    assert "EVENT_TIMESTAMP_IN_FUTURE" in validate(rec, ev.TOPIC_ORDERS, ingestion_us=1_767_259_606_285_228)


def test_payment_and_inventory_rules():
    pay = {"event_id": "6700b6ed-7016-5462-ae82-79418ce901b4", "event_type": "PAYMENT_COMPLETED",
           "event_timestamp": 1_767_259_606_285_228, "payment_id": "pay1", "order_id": "o1", "attempt": 1,
           "status": "FAILED", "amount_minor": 10, "currency": "GBP"}
    assert "STATUS_EVENT_MISMATCH" in validate(pay, ev.TOPIC_PAYMENTS)
    inv = {"event_id": "6700b6ed-7016-5462-ae82-79418ce901b4", "event_type": "INVENTORY_RESERVED",
           "event_timestamp": 1_767_259_606_285_228, "inventory_key": "s1:p2", "store_id": "s1",
           "product_id": "p1", "reason": "SHIPMENT", "on_hand_delta": 0, "reserved_delta": 1}
    errs = validate(inv, ev.TOPIC_INVENTORY)
    assert "REASON_EVENT_MISMATCH" in errs and "INVALID_INVENTORY_KEY" in errs


def test_every_simulated_event_is_valid_and_serialisable():
    from simulator.generator import CommerceSimulator, SimulationConfig

    batch = next(CommerceSimulator(SimulationConfig(orders=300, seed=2, days=2)).stream())
    for topic, df in batch.items():
        version = schemas.latest_version(topic)
        for event in df.head(200).to_dict("records"):
            rec = schemas.to_record(event, topic, version)
            assert validate(rec, topic) == [], (topic, rec)
            serde.serialize(rec, schemas.parsed(topic, version), 1)
