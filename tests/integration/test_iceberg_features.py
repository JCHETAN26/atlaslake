"""Phase 14: Iceberg inserts, upserts, snapshots, time travel, schema evolution, partition pruning."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from benchmarks import scenarios as sc
from contracts import events as ev
from spark.common.tables import fq
from tests.integration.helpers import drain, send_all, table

T0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def _snapshots(spark, name):
    return spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {fq(name)}.snapshots ORDER BY committed_at"
    ).collect()


def test_upserts_snapshots_and_time_travel(clean_platform, producer):
    spark = clean_platform
    created, _, paid, shipped = (e for _, e in sc.lifecycle("o_tt_1", T0))
    send_all(producer, [(ev.TOPIC_ORDERS, created)])
    drain(spark)
    snap_created = _snapshots(spark, "silver.orders")[-1]["snapshot_id"]
    send_all(producer, [(ev.TOPIC_ORDERS, paid), (ev.TOPIC_ORDERS, shipped)])
    drain(spark)

    snaps = _snapshots(spark, "silver.orders")
    assert len(snaps) >= 2 and snaps[-1]["operation"] in ("overwrite", "append")
    now_row = table(spark, "silver.orders")[0]
    then_row = spark.sql(f"SELECT status FROM {fq('silver.orders')} VERSION AS OF {snap_created}").collect()[
        0
    ]
    assert (now_row["status"], then_row["status"]) == ("SHIPPED", "CREATED")  # upsert + time travel
    assert spark.table(fq("silver.orders")).count() == 1  # updated in place, not appended


def test_schema_evolution_add_column_keeps_old_data_readable(clean_platform, producer):
    spark = clean_platform
    send_all(producer, [(ev.TOPIC_ORDERS, sc.order_event("o_se_1", "ORDER_CREATED", T0))])
    drain(spark)
    before = _snapshots(spark, "silver.orders")[-1]["snapshot_id"]
    spark.sql(f"ALTER TABLE {fq('silver.orders')} ADD COLUMN gift_message string COMMENT 'Phase 14 demo'")
    try:
        spark.sql(f"UPDATE {fq('silver.orders')} SET gift_message = 'hi' WHERE order_id = 'o_se_1'")
        assert table(spark, "silver.orders")[0]["gift_message"] == "hi"
        old = spark.sql(f"SELECT * FROM {fq('silver.orders')} VERSION AS OF {before}").collect()[0].asDict()
        assert "gift_message" not in old  # the old snapshot is read with its own schema
    finally:
        spark.sql(f"ALTER TABLE {fq('silver.orders')} DROP COLUMN gift_message")


def test_partition_pruning_on_day_partitioned_table(clean_platform, producer):
    """silver.inventory_movements is partitioned by days(event_timestamp): a 1-day filter prunes."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    from lakehouse.catalog import load_table

    spark = clean_platform
    events = []
    for d in range(10):
        at = T0 + timedelta(days=d)
        events.append(
            (
                ev.TOPIC_INVENTORY,
                {
                    "event_id": sc.eid(f"inv-prune-{d}"),
                    "event_type": "INVENTORY_UPDATED",
                    "event_timestamp": sc.us(at),
                    "inventory_key": "s1:p1",
                    "store_id": "s1",
                    "product_id": "p1",
                    "reason": "RESTOCK",
                    "on_hand_delta": 10,
                    "reserved_delta": 0,
                    "order_id": None,
                },
            )
        )
        send_all(producer, events[-1:])
        drain(spark)  # one commit per day → one file per day partition
    tbl = load_table("silver.inventory_movements")
    all_files = list(tbl.scan().plan_files())
    day = (T0 + timedelta(days=3)).replace(hour=0)
    pruned = list(
        tbl.scan(
            row_filter=And(
                GreaterThanOrEqual("event_timestamp", day.isoformat()),
                LessThan("event_timestamp", (day + timedelta(days=1)).isoformat()),
            )
        ).plan_files()
    )
    assert len(all_files) == 10
    assert len(pruned) == 1
    assert spark.table(fq("silver.inventory")).collect()[0]["quantity_on_hand"] == 100
