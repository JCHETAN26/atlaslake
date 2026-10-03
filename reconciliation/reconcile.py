"""Source → Bronze → Silver → Gold reconciliation (Phase 23).

Event conservation is checked at the level of individual event ids, per topic and per event date:

    source events (outbox) = accepted (silver ledger)
                           + rejected (DLQ: PENDING or DISCARDED; corrupt bytes attributed via delivery_log)
                           + in_flight (not yet relayed from the outbox)
                           + pending_processing (in Kafka, not yet consumed into Bronze)
                           + UNEXPLAINED            ← must be 0
    accepted − source      = UNKNOWN ORIGIN          ← must be 0

plus physical-delivery accounting (Kafka/Bronze), logical-duplicate checks in Silver and Gold, and a
to-the-cent financial reconciliation per currency (Source → Silver → Gold) in which every excluded
order is explained by a rejected/in-flight event or an incomplete (held-back) order.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import duckdb

from contracts import events as ev
from core.config import settings
from lakehouse.duckdb_plugin import configure


def _connect() -> duckdb.DuckDBPyConnection:
    cfg = settings()
    con = duckdb.connect()
    configure(con)
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{cfg.pg.dsn(cfg.pg.source_db)}' AS pg (TYPE postgres, READ_ONLY)")
    if cfg.gold_duckdb.exists():
        con.execute(f"ATTACH '{cfg.gold_duckdb}' AS gold_db (READ_ONLY)")
    return con


def _has(con: duckdb.DuckDBPyConnection, sql: str) -> bool:
    try:
        con.execute(sql)
        return True
    except duckdb.Error:
        return False


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def run_reconciliation(run_date: date | None = None, write: bool = True) -> dict[str, Any]:
    t0 = time.time()
    con = _connect()
    date_filter = f"AND cast(event_timestamp AS date) = DATE '{run_date}'" if run_date else ""
    has_gold = _has(con, "SELECT 1 FROM gold_db.gold.fact_orders LIMIT 1")
    bronze_union = " UNION ALL ".join(
        f"SELECT topic, kafka_partition, kafka_offset FROM silver_bronze_{d}" for d in ev.DATASETS)

    from lakehouse.catalog import catalog

    for d in ev.DATASETS:
        loc = catalog().load_table(f"bronze.{d}").metadata_location
        con.execute(f"CREATE OR REPLACE VIEW silver_bronze_{d} AS SELECT * FROM iceberg_scan('{loc}')")

    con.execute(f"""
        CREATE TEMP TABLE src AS
        SELECT event_id::varchar AS event_id, topic, partition_key, seq,
               cast(event_timestamp AS date) AS d
        FROM pg.commerce.outbox WHERE true {date_filter}""")
    con.execute(f"""
        CREATE TEMP TABLE acc AS
        SELECT DISTINCT event_id, topic, cast(event_timestamp AS date) AS d FROM silver.events WHERE true {date_filter}""")
    con.execute("""
        CREATE TEMP TABLE dl AS
        SELECT event_id::varchar AS event_id, topic, kafka_partition, kafka_offset, fault FROM pg.ops.delivery_log""")
    con.execute(f"CREATE TEMP TABLE bronze_coords AS {bronze_union}")
    con.execute("""
        CREATE TEMP TABLE rej AS
        SELECT DISTINCT coalesce(q.event_id, dl.event_id) AS event_id, q.topic, q.status
        FROM ops.dlq q
        LEFT JOIN dl ON q.topic = dl.topic AND q.kafka_partition = dl.kafka_partition
                    AND q.kafka_offset = dl.kafka_offset
        WHERE q.status IN ('PENDING', 'DISCARDED')""")
    last_seq = con.execute("SELECT coalesce(max(last_seq), 0) FROM pg.ops.relay_checkpoint").fetchone()[0]
    con.execute(f"""
        CREATE TEMP TABLE pending_proc AS
        SELECT DISTINCT dl.event_id FROM dl
        ANTI JOIN bronze_coords b ON b.topic = dl.topic AND b.kafka_partition = dl.kafka_partition
                                 AND b.kafka_offset = dl.kafka_offset
        WHERE dl.event_id IS NOT NULL""")
    con.execute(f"""
        CREATE TEMP TABLE classified AS
        SELECT s.*,
          CASE WHEN a.event_id IS NOT NULL THEN 'accepted'
               WHEN r.event_id IS NOT NULL THEN 'rejected'
               WHEN s.seq > {last_seq} THEN 'in_flight'
               WHEN p.event_id IS NOT NULL THEN 'pending_processing'
               ELSE 'unexplained' END AS outcome
        FROM src s
        LEFT JOIN acc a USING (event_id)
        LEFT JOIN (SELECT DISTINCT event_id FROM rej) r USING (event_id)
        LEFT JOIN pending_proc p USING (event_id)""")

    per_topic = _rows(con, """
        SELECT topic, count(*) AS source_events,
               count(*) FILTER (WHERE outcome = 'accepted') AS accepted,
               count(*) FILTER (WHERE outcome = 'rejected') AS rejected,
               count(*) FILTER (WHERE outcome = 'in_flight') AS in_flight,
               count(*) FILTER (WHERE outcome = 'pending_processing') AS pending_processing,
               count(*) FILTER (WHERE outcome = 'unexplained') AS unexplained
        FROM classified GROUP BY topic ORDER BY topic""")
    per_date = _rows(con, """
        SELECT d AS event_date, count(*) AS source_events,
               count(*) FILTER (WHERE outcome = 'accepted') AS accepted,
               count(*) FILTER (WHERE outcome = 'rejected') AS rejected,
               count(*) FILTER (WHERE outcome NOT IN ('accepted', 'rejected')) AS not_yet_accepted,
               count(*) FILTER (WHERE outcome = 'unexplained') AS unexplained
        FROM classified GROUP BY d ORDER BY d""")
    unknown_origin = con.execute("SELECT count(*) FROM acc ANTI JOIN src USING (event_id)").fetchone()[0]
    unexplained_examples = _rows(con, "SELECT event_id, topic, d FROM classified WHERE outcome='unexplained' LIMIT 10")

    # ---------------------------------------------------------------- physical deliveries
    physical = _rows(con, """
        SELECT b.topic, b.bronze_rows, coalesce(l.deliveries, 0) AS deliveries_logged,
               coalesce(l.duplicates_injected, 0) AS duplicates_injected,
               coalesce(l.corrupt_injected, 0) AS corrupt_injected,
               coalesce(l.invalid_injected, 0) AS invalid_injected
        FROM (SELECT topic, count(*) AS bronze_rows FROM bronze_coords GROUP BY 1) b
        LEFT JOIN (SELECT topic, count(*) AS deliveries,
                          count(*) FILTER (WHERE fault = 'duplicate') AS duplicates_injected,
                          count(*) FILTER (WHERE fault LIKE 'corrupt%') AS corrupt_injected,
                          count(*) FILTER (WHERE fault LIKE 'invalid%') AS invalid_injected
                   FROM dl GROUP BY 1) l USING (topic)
        ORDER BY 1""")

    # ---------------------------------------------------------------- logical duplicates
    dupes = {
        "silver.events": con.execute("SELECT count(*) - count(DISTINCT event_id) FROM silver.events").fetchone()[0],
        "silver.orders": con.execute("SELECT count(*) - count(DISTINCT order_id) FROM silver.orders").fetchone()[0],
        "silver.payments": con.execute("SELECT count(*) - count(DISTINCT payment_id) FROM silver.payments").fetchone()[0],
        "silver.order_items": con.execute(
            "SELECT count(*) - count(DISTINCT (order_id, line_number)) FROM silver.order_items").fetchone()[0],
    }
    if has_gold:
        dupes["gold.fact_orders"] = con.execute(
            "SELECT count(*) - count(DISTINCT order_id) FROM gold_db.gold.fact_orders").fetchone()[0]
        dupes["gold.fact_order_items"] = con.execute(
            "SELECT count(*) - count(DISTINCT (order_id, line_number)) FROM gold_db.gold.fact_order_items").fetchone()[0]

    # ---------------------------------------------------------------- financials (orders)
    con.execute("""
        CREATE TEMP TABLE affected_orders AS
        SELECT DISTINCT partition_key AS order_id FROM classified
        WHERE outcome <> 'accepted' AND topic IN ('commerce.orders', 'commerce.payments')""")
    order_date_filter = f"AND cast(created_at AS date) = DATE '{run_date}'" if run_date else ""
    silver_date_filter = f"AND cast(order_timestamp AS date) = DATE '{run_date}'" if run_date else ""
    fin = _rows(con, f"""
        WITH s AS (
            SELECT currency::varchar AS currency, count(*) AS orders, sum(amount_minor) AS revenue_minor
            FROM pg.commerce.orders
            WHERE paid_at IS NOT NULL AND status::varchar <> 'CANCELLED' {order_date_filter}
              AND order_id NOT IN (SELECT order_id FROM affected_orders)
            GROUP BY 1),
        sv AS (
            SELECT currency, count(*) AS orders, sum(amount_minor) AS revenue_minor
            FROM silver.orders
            WHERE paid_at IS NOT NULL AND status <> 'CANCELLED' AND order_timestamp IS NOT NULL {silver_date_filter}
              AND order_id NOT IN (SELECT order_id FROM affected_orders)
            GROUP BY 1)
        SELECT coalesce(s.currency, sv.currency) AS currency,
               s.orders AS source_orders, sv.orders AS silver_orders,
               s.revenue_minor AS source_revenue_minor, sv.revenue_minor AS silver_revenue_minor
        FROM s FULL OUTER JOIN sv ON s.currency = sv.currency ORDER BY 1""")
    excluded_orders = con.execute("SELECT count(*) FROM affected_orders").fetchone()[0]
    incomplete = con.execute("SELECT count(*) FROM silver.orders WHERE order_timestamp IS NULL").fetchone()[0]
    gold_vs_silver: list[dict[str, Any]] = []
    if has_gold:
        gold_date = f"WHERE order_date = DATE '{run_date}'" if run_date else ""
        gold_vs_silver = _rows(con, f"""
            WITH g AS (SELECT currency, sum(orders_paid) AS orders, sum(gross_revenue_minor) AS revenue_minor
                       FROM gold_db.gold.mart_daily_sales {gold_date} GROUP BY 1),
                 sv AS (SELECT currency, count(*) AS orders, sum(amount_minor) AS revenue_minor
                        FROM silver.orders
                        WHERE paid_at IS NOT NULL AND status <> 'CANCELLED' AND order_timestamp IS NOT NULL
                        {silver_date_filter} GROUP BY 1)
            SELECT coalesce(g.currency, sv.currency) AS currency, sv.orders AS silver_orders, g.orders AS gold_orders,
                   sv.revenue_minor AS silver_revenue_minor, g.revenue_minor AS gold_revenue_minor
            FROM g FULL OUTER JOIN sv ON g.currency = sv.currency ORDER BY 1""")

    fin_mismatch = [r for r in fin if (r["source_orders"], r["source_revenue_minor"]) != (r["silver_orders"], r["silver_revenue_minor"])]
    gold_mismatch = [r for r in gold_vs_silver if (r["silver_orders"], r["silver_revenue_minor"]) != (r["gold_orders"], r["gold_revenue_minor"])]
    unexplained = sum(r["unexplained"] for r in per_topic)
    totals = {k: sum(r[k] for r in per_topic) for k in
              ("source_events", "accepted", "rejected", "in_flight", "pending_processing", "unexplained")}
    logical_dupes = sum(int(v) for v in dupes.values())
    passed = (unexplained == 0 and unknown_origin == 0 and logical_dupes == 0 and not fin_mismatch
              and not gold_mismatch)
    report = {
        "scope": {"event_date": str(run_date) if run_date else "all", "gold_checked": has_gold,
                  "relay_checkpoint_seq": last_seq},
        "summary": {
            "passed": passed,
            **totals,
            "unexplained": unexplained,
            "unknown_origin": int(unknown_origin),
            "logical_duplicates": logical_dupes,
            "financial_mismatches": len(fin_mismatch),
            "gold_vs_silver_mismatches": len(gold_mismatch),
            "orders_excluded_from_financials": int(excluded_orders),
            "incomplete_orders_held_from_gold": int(incomplete),
            "invariant": "source = accepted + rejected + in_flight + pending_processing (+ unexplained == 0)",
            "runtime_s": round(time.time() - t0, 3),
        },
        "events_by_topic": per_topic,
        "events_by_date": per_date,
        "physical_deliveries": physical,
        "logical_duplicates": dupes,
        "financials_source_vs_silver": fin,
        "financials_silver_vs_gold": gold_vs_silver,
        "unexplained_examples": unexplained_examples,
    }
    if write:
        from benchmarks.common import write_result

        write_result("reconciliation", report)
        try:
            from monitoring.metrics import push_job_metrics

            push_job_metrics("reconcile", {"reconciliation_passed": 1 if passed else 0,
                                           "reconciliation_unexplained": unexplained + unknown_origin,
                                           "runtime_seconds": report["summary"]["runtime_s"]}, success=True)
        except Exception:
            pass
    con.close()
    return report
