"""Phase 35: one order, TEST_001, traced through every layer of the platform."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from benchmarks import scenarios as sc
from contracts import events as ev
from tests.integration.helpers import drain, table


def _batch(events: list[tuple[str, dict]]) -> dict[str, pd.DataFrame]:
    by: dict[str, list[dict]] = {t: [] for t in ev.TOPICS}
    for topic, e in events:
        by[topic].append(e)
    return {t: pd.DataFrame(v) for t, v in by.items()}


def test_test_001_flows_from_source_to_gold(clean_platform):
    from confluent_kafka import Consumer, TopicPartition

    from core.config import settings
    from ingestion.batch.postgres_snapshot import ingest_stores
    from ingestion.batch.publish import publish_postgres, publish_snowflake, snowflake_config
    from ingestion.cdc.outbox_relay import run_relay
    from lakehouse.duckdb_plugin import connect_gold
    from lineage.dbt_runner import dbt_build
    from reconciliation.reconcile import run_reconciliation
    from simulator.generator import CommerceSimulator, SimulationConfig
    from simulator.postgres_sink import apply_batch, apply_stream, connect, load_stores

    spark = clean_platform
    sim = CommerceSimulator(SimulationConfig(orders=200, seed=35, days=2))
    store = sim.world.stores.iloc[0]
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    with connect() as conn:
        load_stores(conn, sim.world.stores)
        apply_stream(conn, sim.stream())  # customers/products exist before TEST_001 references them
        customer = conn.execute("SELECT customer_id FROM commerce.customers LIMIT 1").fetchone()[0]
        product = conn.execute(
            "SELECT product_id, unit_price_minor FROM commerce.products LIMIT 1"
        ).fetchone()
        price = int(product[1])
        created = sc.order_event(
            "TEST_001",
            "ORDER_CREATED",
            t0,
            amount_minor=2 * price,
            customer_id=customer,
            store_id=store.store_id,
            currency=store.currency,
        )
        created["items"] = [
            {"line_number": 1, "product_id": product[0], "quantity": 2, "unit_price_minor": price}
        ]
        events = [
            (ev.TOPIC_ORDERS, created),
            (
                ev.TOPIC_INVENTORY,
                {
                    "event_id": sc.eid("TEST_001:inv"),
                    "event_type": "INVENTORY_RESERVED",
                    "event_timestamp": sc.us(t0) + 1,
                    "inventory_key": f"{store.store_id}:{product[0]}",
                    "store_id": store.store_id,
                    "product_id": product[0],
                    "reason": "RESERVATION",
                    "on_hand_delta": 0,
                    "reserved_delta": 2,
                    "order_id": "TEST_001",
                    "line_number": 1,
                },
            ),
            (
                ev.TOPIC_PAYMENTS,
                {
                    **sc.payment_event(
                        "TEST_001",
                        "PAYMENT_STARTED",
                        t0 + timedelta(seconds=30),
                        amount_minor=2 * price,
                        currency=store.currency,
                    ),
                    "payment_id": "pay_TEST_001_1",
                },
            ),
            (
                ev.TOPIC_PAYMENTS,
                {
                    **sc.payment_event(
                        "TEST_001",
                        "PAYMENT_COMPLETED",
                        t0 + timedelta(seconds=40),
                        amount_minor=2 * price,
                        currency=store.currency,
                    ),
                    "payment_id": "pay_TEST_001_1",
                },
            ),
            (
                ev.TOPIC_ORDERS,
                sc.order_event(
                    "TEST_001",
                    "ORDER_UPDATED",
                    t0 + timedelta(seconds=41),
                    status="PAID",
                    amount_minor=2 * price,
                    customer_id=customer,
                    store_id=store.store_id,
                    currency=store.currency,
                ),
            ),
            (
                ev.TOPIC_ORDERS,
                sc.order_event(
                    "TEST_001",
                    "ORDER_SHIPPED",
                    t0 + timedelta(hours=5),
                    amount_minor=2 * price,
                    customer_id=customer,
                    store_id=store.store_id,
                    currency=store.currency,
                ),
            ),
        ]
        apply_batch(conn, _batch(events))
        assert (
            conn.execute("SELECT status FROM commerce.orders WHERE order_id='TEST_001'").fetchone()[0]
            == "SHIPPED"
        )

    run_relay(batch_size=5_000, metrics_port=None)

    # ✓ Kafka: the TEST_001 events are on commerce.orders, keyed by order_id
    c = Consumer(
        {
            "bootstrap.servers": settings().kafka.bootstrap,
            "group.id": "e2e-probe",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    parts = c.list_topics(ev.TOPIC_ORDERS).topics[ev.TOPIC_ORDERS].partitions
    c.assign([TopicPartition(ev.TOPIC_ORDERS, p, 0) for p in parts])
    keys, deadline = [], datetime.now().timestamp() + 20
    while datetime.now().timestamp() < deadline:
        m = c.poll(1.0)
        if m is None:
            if keys.count("TEST_001") >= 3:
                break
            continue
        keys.append((m.key() or b"").decode())
    c.close()
    assert keys.count("TEST_001") == 3

    drain(spark)
    # ✓ Bronze keeps the raw deliveries
    assert len(table(spark, "bronze.orders", "kafka_key = 'TEST_001'")) == 3
    # ✓ Silver holds the reconciled state
    order = table(spark, "silver.orders", "order_id = 'TEST_001'")[0]
    assert (order["status"], order["amount_minor"], order["customer_id"]) == ("SHIPPED", 2 * price, customer)
    assert table(spark, "silver.payments", "order_id = 'TEST_001'")[0]["status"] == "COMPLETED"

    ingest_stores(spark, full=True)
    assert dbt_build()["success"]
    # ✓ Gold star schema
    g = connect_gold()
    fo = g.execute(
        "SELECT status, is_revenue, customer_sk <> '-1', store_sk <> '-1' FROM gold.fact_orders "
        "WHERE order_id = 'TEST_001'"
    ).fetchone()
    assert fo == ("SHIPPED", True, True, True)
    assert (
        g.execute(
            "SELECT count(*) FROM gold.fact_order_items WHERE order_id='TEST_001' AND product_sk <> '-1'"
        ).fetchone()[0]
        == 1
    )
    g.close()

    # ✓ serving copies
    publish_postgres()
    with connect(settings().pg.analytics_db) as conn:
        assert (
            conn.execute("SELECT status FROM gold.fact_orders WHERE order_id='TEST_001'").fetchone()[0]
            == "SHIPPED"
        )
    if snowflake_config():
        assert not publish_snowflake().get("skipped")
    elif os.environ.get("CI_REQUIRE_SNOWFLAKE"):
        pytest.fail("Snowflake credentials required in this environment")

    # ✓ reconciliation: every source event accounted for, money matches to the cent
    rec = run_reconciliation(write=False)
    assert rec["summary"]["passed"], rec["summary"]
    assert rec["summary"]["accepted"] == rec["summary"]["source_events"]
