"""Phase 12: event-time processing and the watermark policy at its boundaries."""

from __future__ import annotations

import json
from datetime import timedelta

from benchmarks import scenarios as sc
from contracts import events as ev
from spark.streaming import realtime_aggregates as rt
from tests.integration.helpers import drain, send_all, table, totals


def _run_rt(spark) -> list[dict]:
    q = rt.build_query(spark, available_now=True)
    q.awaitTermination()
    assert q.exception() is None, q.exception()
    return [p if isinstance(p, dict) else json.loads(p.json) for p in q.recentProgress]


def test_watermark_policy_on_time_slightly_late_very_late(clean_platform, producer):
    spark = clean_platform
    now = sc.now_utc().replace(second=0, microsecond=0)

    def created(oid: str, at, amount: int):
        return (ev.TOPIC_ORDERS, sc.order_event(oid, "ORDER_CREATED", at, amount_minor=amount))

    # 1) on-time event advances max event time to `now` → watermark = now - 10 min
    send_all(producer, [created("o_late_on_time", now, 100)])
    progress = _run_rt(spark)
    # 2) arrivals after the watermark moved: 5 min late (inside), 1 h late and 1 day late (beyond)
    send_all(producer, [created("o_late_5m", now - timedelta(minutes=5), 200),
                        created("o_late_1h", now - timedelta(hours=1), 400),
                        created("o_late_1d", now - timedelta(days=1), 800)])
    progress += _run_rt(spark)
    # 3) a later event pushes the watermark past those windows so append-mode emits them
    send_all(producer, [created("o_late_flush", now + timedelta(minutes=30), 1)])
    progress += _run_rt(spark)

    windows = {(r["window_start"], r["currency"]): r for r in table(spark, "silver.realtime_sales_1m")}
    starts = {k[0].replace(tzinfo=None) for k in windows}
    assert now.replace(tzinfo=None) in starts                              # on time
    assert (now - timedelta(minutes=5)).replace(tzinfo=None) in starts     # slightly late: counted
    assert (now - timedelta(hours=1)).replace(tzinfo=None) not in starts   # very late: excluded
    assert (now - timedelta(days=1)).replace(tzinfo=None) not in starts
    assert rt.dropped_by_watermark(progress) == 2

    # ... but Silver (and therefore Gold) keeps every event: nothing is lost
    t = totals(drain(spark))
    assert t["accepted_new"] == 5
    assert t["late_events"] == 2  # lateness > allowed (10 min): the 1 h and 1 day arrivals
    ids = {r["order_id"] for r in table(spark, "silver.orders")}
    assert {"o_late_1h", "o_late_1d", "o_late_5m", "o_late_on_time"} <= ids
