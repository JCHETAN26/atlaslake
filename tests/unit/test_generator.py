import hashlib

import numpy as np
import pandas as pd
import pytest

from contracts import events as ev
from simulator.generator import CHUNK_ORDERS, CommerceSimulator, SimulationConfig
from simulator.postgres_sink import outbox_rows, reduce_orders


def _collect(cfg: SimulationConfig) -> dict[str, pd.DataFrame]:
    parts: dict[str, list[pd.DataFrame]] = {t: [] for t in ev.TOPICS}
    for batch in CommerceSimulator(cfg).stream():
        for t, df in batch.items():
            parts[t].append(df)
    return {t: pd.concat(v, ignore_index=True) for t, v in parts.items()}


def _fingerprint(events: dict[str, pd.DataFrame]) -> str:
    h = hashlib.sha256()
    for t in ev.TOPICS:
        df = events[t]
        h.update(t.encode())
        h.update("".join(df["event_id"]).encode())
        h.update(df["event_timestamp"].to_numpy().tobytes())
    return h.hexdigest()


@pytest.fixture(scope="module")
def small() -> dict[str, pd.DataFrame]:
    return _collect(SimulationConfig(orders=3_000, seed=11, days=7))


def test_same_seed_is_byte_identical(small):
    again = _collect(SimulationConfig(orders=3_000, seed=11, days=7))
    assert _fingerprint(small) == _fingerprint(again)


def test_different_seed_differs(small):
    other = _collect(SimulationConfig(orders=3_000, seed=12, days=7))
    assert _fingerprint(small) != _fingerprint(other)


def test_chunking_spans_multiple_chunks_deterministically():
    cfg = SimulationConfig(orders=CHUNK_ORDERS + 500, seed=3, days=3)
    a, b = _collect(cfg), _collect(cfg)
    assert _fingerprint(a) == _fingerprint(b)
    assert a[ev.TOPIC_ORDERS]["order_id"].nunique() == cfg.orders


def test_event_ids_unique_and_types_match_topics(small):
    all_ids = pd.concat([df["event_id"] for df in small.values()])
    assert all_ids.is_unique
    for topic, df in small.items():
        assert set(df["event_type"]) <= set(ev.EVENT_TYPES[topic])


def test_batches_are_event_time_ordered():
    for batch in CommerceSimulator(SimulationConfig(orders=2_000, seed=5, days=5)).stream():
        for df in batch.values():
            if not df.empty:
                assert df["event_timestamp"].is_monotonic_increasing


def test_no_events_after_window(small):
    cfg = SimulationConfig(orders=3_000, seed=11, days=7)
    for df in small.values():
        assert (df["event_timestamp"] < cfg.end_us).all()


def test_order_amount_equals_sum_of_items(small):
    created = small[ev.TOPIC_ORDERS].query("event_type == 'ORDER_CREATED'")
    for _, row in created.head(500).iterrows():
        assert row["amount_minor"] == sum(i["quantity"] * i["unit_price_minor"] for i in row["items"])
        assert [i["line_number"] for i in row["items"]] == list(range(1, len(row["items"]) + 1))


def test_order_lifecycles_follow_valid_transitions(small):
    o = small[ev.TOPIC_ORDERS].sort_values(["order_id", "event_timestamp"])
    for _, grp in o.groupby("order_id"):
        statuses = grp["status"].tolist()
        assert statuses[0] == "CREATED"
        for a, b in zip(statuses, statuses[1:], strict=False):
            assert b in ev.VALID_TRANSITIONS[a], (a, b)


def test_paid_orders_have_a_completed_payment(small):
    o, p = small[ev.TOPIC_ORDERS], small[ev.TOPIC_PAYMENTS]
    paid = set(o.query("status == 'PAID'")["order_id"])
    completed = set(p.query("event_type == 'PAYMENT_COMPLETED'")["order_id"])
    assert paid == completed


def test_inventory_available_never_negative(small):
    inv = small[ev.TOPIC_INVENTORY].sort_values(["event_timestamp", "event_id"])
    inv = inv.assign(avail_delta=inv["on_hand_delta"] - inv["reserved_delta"])
    running = inv.groupby("inventory_key")["avail_delta"].cumsum()
    assert running.min() >= 0


def test_customer_moves_change_city(small):
    c = small[ev.TOPIC_CUSTOMERS]
    created = c.query("event_type == 'CUSTOMER_CREATED'").set_index("customer_id")
    moved = c.query("event_type == 'CUSTOMER_UPDATED'")
    assert len(moved) > 0
    assert (moved["city"].to_numpy() != created.loc[moved["customer_id"], "city"].to_numpy()).all()


def test_reduce_orders_is_last_writer_wins_and_order_independent(small):
    o = small[ev.TOPIC_ORDERS]
    a = reduce_orders(o).set_index("order_id").sort_index()
    b = reduce_orders(o.sample(frac=1.0, random_state=1)).set_index("order_id").sort_index()
    pd.testing.assert_frame_equal(a, b)
    delivered = o.query("event_type == 'ORDER_DELIVERED'")["order_id"]
    assert (a.loc[delivered, "status"] == "DELIVERED").all()


def test_outbox_rows_are_time_ordered_with_json_payloads(small):
    batch = {t: df.head(50) for t, df in small.items()}
    ob = outbox_rows(batch)
    assert len(ob) == sum(len(df) for df in batch.values())
    assert ob["event_timestamp"].is_monotonic_increasing
    assert np.all(ob["payload"].str.startswith("{"))
