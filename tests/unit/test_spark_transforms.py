"""Spark unit tests: the Silver MERGEs are commutative and idempotent (no stack needed).

Runs real Spark + Iceberg with a local Hadoop catalog in a temp dir, applies the same order and
payment events as one batch, as shuffled micro-batches, and with replays — the resulting state must be
identical every time. This is the property every correctness guarantee rests on.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime

import pytest

from benchmarks import scenarios as sc
from contracts import events as ev

pytestmark = pytest.mark.spark
T0 = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    import os
    import sys

    from pyspark.sql import SparkSession

    from spark.common.session import ICEBERG_VERSION

    os.environ["PYSPARK_PYTHON"] = sys.executable
    wh = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]").appName("atlas-unit")
         .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.t.type", "hadoop")
         .config("spark.sql.catalog.t.warehouse", str(wh))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def _events() -> list[tuple[str, dict]]:
    out = []
    for i in range(6):
        out += sc.lifecycle(f"o_unit_{i}", T0.replace(minute=i), amount_minor=100 * (i + 1))
    out.append((ev.TOPIC_ORDERS, sc.order_event("o_unit_0", "ORDER_DELIVERED", T0.replace(hour=20))))
    out.append((ev.TOPIC_ORDERS, sc.order_event("o_unit_1", "ORDER_CANCELLED", T0.replace(hour=11))))
    out.append((ev.TOPIC_PAYMENTS, sc.payment_event("o_unit_2", "PAYMENT_STARTED", T0.replace(minute=2, second=30))))
    return out


def _decoded_df(spark, events: list[tuple[str, dict]]):
    from contracts import schemas

    rows = []
    for topic, e in events:
        rec = schemas.to_record(e, topic, schemas.latest_version(topic))
        rows.append((rec["event_id"], rec["event_type"], topic,
                     datetime.fromtimestamp(rec["event_timestamp"] / 1e6, UTC).replace(tzinfo=None),
                     datetime(2026, 1, 2), rec["schema_version"], "kafka", json.dumps(rec)))
    return spark.createDataFrame(rows, "event_id string, event_type string, topic string, event_timestamp timestamp, "
                                       "ingestion_timestamp timestamp, schema_version int, source string, record_json string")


def _apply(spark, table_suffix: str, batches: list[list[tuple[str, dict]]]) -> list[tuple]:
    from spark.common import transforms as T
    from spark.common.tables import TABLES

    for name in ("silver.orders", "silver.payments"):
        cols = TABLES[name][0]
        spark.sql(f"CREATE TABLE IF NOT EXISTS t.db.{name.split('.')[1]}_{table_suffix} ({cols}) USING iceberg")
    for batch in batches:
        df = _decoded_df(spark, batch)
        orders = T.typed(df, ev.TOPIC_ORDERS)
        T.reduce_orders(orders).createOrReplaceTempView("o_patch")
        spark.sql(T.MERGE_ORDERS.format(target=f"t.db.orders_{table_suffix}", source="o_patch"))
        T.reduce_payments(T.typed(df, ev.TOPIC_PAYMENTS)).createOrReplaceTempView("p_patch")
        spark.sql(T.MERGE_PAYMENTS.format(target=f"t.db.payments_{table_suffix}", source="p_patch"))
    o = spark.sql(f"""SELECT order_id, customer_id, status, amount_minor, order_timestamp, paid_at, shipped_at,
                             delivered_at, cancelled_at FROM t.db.orders_{table_suffix} ORDER BY order_id""").collect()
    p = spark.sql(f"SELECT payment_id, status, started_at, completed_at FROM t.db.payments_{table_suffix} ORDER BY 1").collect()
    return [tuple(r) for r in o] + [tuple(r) for r in p]


def test_merge_is_order_independent_and_idempotent(spark):
    events = _events()
    one_batch = _apply(spark, "one", [events])

    rng = random.Random(7)
    shuffled = events[:]
    rng.shuffle(shuffled)
    micro = [shuffled[i:i + 3] for i in range(0, len(shuffled), 3)]
    assert _apply(spark, "micro", micro) == one_batch

    reversed_with_replays = list(reversed(events))
    replays = [reversed_with_replays[:5], reversed_with_replays, reversed_with_replays[5:], events[:4]]
    assert _apply(spark, "replay", replays) == one_batch


def test_merge_semantics(spark):
    state = {r[0]: r for r in _apply(spark, "sem", [_events()]) if r[0].startswith("o_")}
    assert state["o_unit_0"][2] == "DELIVERED"
    # latest event time wins; lifecycle validity (cancel after ship) is a quality rule, not a merge rule
    assert state["o_unit_1"][2] == "CANCELLED"
    assert state["o_unit_3"][2] == "SHIPPED" and state["o_unit_3"][3] == 400
    assert all(r[1] == "c_test_1" for r in state.values())
