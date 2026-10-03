"""Benchmark & failure-experiment suites (Phases 4, 20, 26, 27, 36-42, 44).

Each function sets up a seeded state, measures, and writes ``evaluation/<name>.json``. Nothing here
hard-codes an expected number: assertions are on *correctness invariants* (zero loss, zero leaked
duplicates, equal fingerprints); performance is recorded as measured.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from benchmarks import env as E
from benchmarks.common import percentiles, write_result
from core.config import REPO_ROOT, settings

log = logging.getLogger(__name__)
ATLAS = [sys.executable, "-m", "cli.main"]


def _penv() -> dict[str, str]:
    e = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    e["PYTHONUNBUFFERED"] = "1"
    return e


# =========================================================================== Phase 4 / 42: baseline
def baseline_pipeline(ladder: list[str]) -> dict[str, Any]:
    from baseline.pipeline import run as baseline_run
    from benchmarks.queries import run_postgres

    runs = []
    for name in ladder:
        scale = E.SCALES[name]
        src = E.prepare_source(scale)
        res = baseline_run()
        res["query_latency"] = {k: v["warm"] for k, v in run_postgres(repeats=10).items()}
        runs.append({"scale": name, "source_load": src, **res})
        log.info("baseline %s: %.1fs total", name, res["total_runtime_s"])
    payload = {
        "description": "Naive full-refresh pipeline: Postgres → CSV/Parquet → pandas → Postgres",
        "runs": runs,
    }
    write_result("baseline_pipeline", payload)
    return payload


# =========================================================================== Spark performance
def spark_performance(spark, scale: E.Scale) -> dict[str, Any]:
    E.prepare_source(scale)
    E.reset_pipeline(spark)
    relay = E.relay_all()
    from core.platform import reset_lake

    sweeps = []
    for max_offsets, parallel in ((100_000, True), (250_000, True), (500_000, True), (500_000, False)):
        reset_lake(spark)
        r = E.drain(spark, max_offsets=max_offsets, parallel_writes=parallel)
        sweeps.append({"max_offsets_per_trigger": max_offsets, "parallel_table_writes": parallel, **r})
        log.info("drain max_offsets=%s parallel=%s: %s ev/s", max_offsets, parallel, r["events_per_s"])
    payload = {
        "scale": scale.name,
        "relay": relay,
        "drains": sweeps,
        "note": "Same Kafka data each run; lake reset between runs. events_per_s includes query startup.",
    }
    write_result("spark_performance", payload)
    return payload


# =========================================================================== Phase 20: full vs incremental
def full_vs_incremental(spark) -> dict[str, Any]:
    from lakehouse.fingerprint import gold_fingerprint
    from lineage.dbt_runner import dbt_build

    full_1 = E.build_gold(full_refresh=True)
    # a realistic increment: a short burst of live activity
    inc_src = subprocess.run(
        [*ATLAS, "workload", "--rate", "40", "--duration", "30"],
        env=_penv(),
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    relay = E.relay_all()
    stream = E.drain(spark)
    inc = dbt_build(full_refresh=False, emit_lineage=False)
    fp_inc = gold_fingerprint()
    full = dbt_build(full_refresh=True, emit_lineage=False)
    fp_full = gold_fingerprint()

    def model_times(r):
        return {n["node"]: n["execution_time_s"] for n in r["nodes"] if n["resource_type"] == "model"}

    import duckdb

    with duckdb.connect(str(settings().gold_duckdb), read_only=True) as con:
        facts = {
            t: con.execute(f"SELECT count(*) FROM gold.gold.{t}").fetchone()[0]
            for t in ("fact_orders", "fact_order_items", "fact_payments", "fact_inventory")
        }
    incremental_models = [
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_inventory",
        "mart_daily_sales",
        "mart_order_funnel",
        "mart_payment_metrics",
    ]
    t_inc, t_full = model_times(inc), model_times(full)
    payload = {
        "increment": {
            "workload_rc": inc_src.returncode,
            "relayed_events": relay["events"],
            "accepted_new_events": stream["accepted_new"],
        },
        "full_refresh": {
            "elapsed_s": full["elapsed_s"],
            "incremental_models_s": round(sum(t_full.get(m, 0) for m in incremental_models), 3),
            "model_s": t_full,
            "rows_in_fact_tables": facts,
        },
        "incremental": {
            "elapsed_s": inc["elapsed_s"],
            "incremental_models_s": round(sum(t_inc.get(m, 0) for m in incremental_models), 3),
            "model_s": t_inc,
            "rows_reprocessed_estimate": stream["accepted_new"],
        },
        "initial_full_build_s": full_1["elapsed_s"],
        "results_identical": fp_inc == fp_full,
        "fingerprint_diff_tables": sorted(k for k in fp_full if fp_full.get(k) != fp_inc.get(k)),
    }
    payload["speedup_incremental_models"] = (
        round(
            payload["full_refresh"]["incremental_models_s"] / payload["incremental"]["incremental_models_s"],
            2,
        )
        if payload["incremental"]["incremental_models_s"]
        else None
    )
    write_result("full_vs_incremental", payload)
    return payload


# =========================================================================== query performance
def query_performance(scale: E.Scale) -> dict[str, Any]:
    import duckdb

    from baseline.pipeline import run as baseline_run
    from benchmarks.queries import run_duckdb, run_postgres

    baseline_run()  # same source data as the lake at this point
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(f"ATTACH '{settings().gold_duckdb}' AS gold_db (READ_ONLY)")
    gold = run_duckdb(con, repeats=25)
    base = run_postgres(repeats=25)
    out = {}
    for q in gold:
        out[q] = {
            "baseline_postgres": base[q],
            "atlaslake_gold_duckdb": gold[q],
            "same_result": base[q]["result_digest"] == gold[q]["result_digest"],
        }
    from ingestion.batch.publish import snowflake_config

    payload = {
        "scale": scale.name,
        "queries": out,
        "snowflake": "not run: no credentials configured"
        if snowflake_config() is None
        else "see snowflake section",
    }
    write_result("query_performance", payload)
    return payload


# =========================================================================== Phase 25/41: backfill
def backfill_benchmark(spark) -> dict[str, Any]:
    from ingestion.batch.backfill import run_backfill
    from lakehouse.fingerprint import diff, silver_fingerprint
    from spark.common.tables import fq

    fp_stream = silver_fingerprint()
    silver_tables = [
        "silver.events",
        "silver.orders",
        "silver.order_items",
        "silver.payments",
        "silver.customers",
        "silver.customer_changes",
        "silver.products",
        "silver.product_changes",
        "silver.inventory",
        "silver.inventory_movements",
    ]
    for t in silver_tables:  # simulate losing Silver entirely; Bronze is the system of record
        spark.sql(f"DELETE FROM {fq(t)} WHERE true")
    lo, hi = spark.sql(
        f"SELECT min(event_timestamp), max(event_timestamp) FROM {fq('bronze.orders')}"
    ).collect()[0]
    start, end = lo.date(), hi.date()
    chunks, cur = [], start - timedelta(days=400)  # entity snapshots (products/customers) predate orders
    edges = [cur, start - timedelta(days=1)]
    d = start
    while d <= end:
        edges.append(min(d + timedelta(days=6), end))
        d += timedelta(days=7)
    t0 = time.time()
    prev = edges[0]
    for e in edges[1:]:
        r = run_backfill(
            list(__import__("contracts.events", fromlist=["DATASETS"]).DATASETS),
            prev,
            e,
            rebuild_gold=False,
            spark=spark,
        )
        r.pop("silver_fingerprint")
        chunks.append(r)
        prev = e + timedelta(days=1)
    total_s = time.time() - t0
    fp_backfill = silver_fingerprint()
    # process the first full week again: must change nothing
    week = (start, start + timedelta(days=6))
    rerun = run_backfill(
        list(__import__("contracts.events", fromlist=["DATASETS"]).DATASETS),
        *week,
        rebuild_gold=True,
        spark=spark,
    )
    fp_rerun = rerun.pop("silver_fingerprint")
    records = sum(c["records_read"] for c in chunks)
    payload = {
        "range": [str(edges[0]), str(end)],
        "chunks": len(chunks),
        "records": records,
        "runtime_s": round(total_s, 2),
        "throughput_records_per_s": round(records / total_s, 1),
        "chunk_runtimes_s": [c["silver_runtime_s"] for c in chunks],
        "duplicates_written": sum(c["already_present"] for c in chunks),
        "matches_streaming_state": fp_backfill == fp_stream,
        "tables_differing_from_streaming": diff(fp_stream, fp_backfill),
        "rerun_week": {
            "range": [str(week[0]), str(week[1])],
            "records_read": rerun["records_read"],
            "accepted_new": rerun["accepted_new"],
            "state_unchanged": fp_rerun == fp_backfill,
            "gold_rebuilt": rerun["gold"],
        },
    }
    write_result("backfill_benchmark", payload)
    return payload


# =========================================================================== Phase 26: partitioning
PARTITION_QUERIES = {
    "orders_one_day": (
        "topic = 'commerce.orders' AND event_timestamp >= TIMESTAMP '{d}' AND event_timestamp < TIMESTAMP '{d}' + INTERVAL 1 DAY",
        [
            ("topic", "==", "commerce.orders"),
            ("event_timestamp", ">=", "{d}"),
            ("event_timestamp", "<", "{d+1}"),
        ],
    ),
    "payments_week": (
        "topic = 'commerce.payments' AND event_timestamp >= TIMESTAMP '{d}' AND event_timestamp < TIMESTAMP '{d}' + INTERVAL 7 DAY",
        [
            ("topic", "==", "commerce.payments"),
            ("event_timestamp", ">=", "{d}"),
            ("event_timestamp", "<", "{d+7}"),
        ],
    ),
    "all_topics_one_day": (
        "event_timestamp >= TIMESTAMP '{d}' AND event_timestamp < TIMESTAMP '{d}' + INTERVAL 1 DAY",
        [("event_timestamp", ">=", "{d}"), ("event_timestamp", "<", "{d+1}")],
    ),
    "entity_lookup": ("entity_key = '{k}'", [("entity_key", "==", "{k}")]),
}


def _pyiceberg_filter(conds, d: date, k: str):
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    ops = {"==": EqualTo, ">=": GreaterThanOrEqual, "<": LessThan}
    exprs = []
    for col, op, val in conds:
        val = (
            val.replace("{d+1}", str(d + timedelta(days=1)))
            .replace("{d+7}", str(d + timedelta(days=7)))
            .replace("{d}", str(d))
            .replace("{k}", k)
        )
        if col == "event_timestamp":
            val = f"{val}T00:00:00+00:00"
        exprs.append(ops[op](col, val))
    out = exprs[0]
    for e in exprs[1:]:
        out = And(out, e)
    return out


def _measure_layout(spark, d: date, key: str, repeats: int = 9) -> dict[str, Any]:
    from lakehouse.catalog import scan_stats
    from spark.common.tables import fq

    out = {}
    for name, (sql_where, conds) in PARTITION_QUERIES.items():
        where = sql_where.replace("{d}", str(d)).replace("{k}", key)
        stats = scan_stats("silver.events", _pyiceberg_filter(conds, d, key))
        lat = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            spark.sql(
                f"SELECT count(*), count(DISTINCT entity_key) FROM {fq('silver.events')} WHERE {where}"
            ).collect()
            lat.append(time.perf_counter() - t0)
        out[name] = {**stats, "latency_s": percentiles(lat[1:]), "cold_s": round(lat[0], 4)}
    return out


def iceberg_partitioning(spark) -> dict[str, Any]:
    from spark.batch.maintenance import file_stats
    from spark.common.tables import fq

    d = spark.sql(
        f"SELECT cast(min(event_timestamp) AS date) + 10 FROM {fq('silver.events')} WHERE topic='commerce.orders'"
    ).collect()[0][0]
    key = spark.sql(
        f"SELECT entity_key FROM {fq('silver.events')} WHERE topic='commerce.orders' LIMIT 1"
    ).collect()[0][0]
    spec_before = spark.sql(f"SELECT * FROM {fq('silver.events')}.partitions LIMIT 0").columns
    # baseline layout: as written by streaming (identity(topic)), compacted so file count isn't the variable
    spark.sql(
        f"CALL {settings().catalog_name}.system.rewrite_data_files(table => 'silver.events', "
        f"options => map('rewrite-all', 'true', 'target-file-size-bytes', '{64 * 1024 * 1024}'))"
    ).collect()
    before = _measure_layout(spark, d, key)
    files_before = file_stats("silver.events")
    t0 = time.time()
    spark.sql(f"ALTER TABLE {fq('silver.events')} ADD PARTITION FIELD days(event_timestamp)")
    spark.sql(f"ALTER TABLE {fq('silver.events')} WRITE ORDERED BY topic, event_timestamp, entity_key")
    spark.sql(
        f"CALL {settings().catalog_name}.system.rewrite_data_files(table => 'silver.events', "
        f"strategy => 'sort', sort_order => 'entity_key', "
        f"options => map('rewrite-all', 'true', 'target-file-size-bytes', '{64 * 1024 * 1024}'))"
    ).collect()
    rewrite_s = time.time() - t0
    after = _measure_layout(spark, d, key)
    files_after = file_stats("silver.events")
    payload = {
        "table": "silver.events",
        "query_date": str(d),
        "layout_before": "PARTITIONED BY (topic) — initial streaming layout",
        "layout_after": "PARTITIONED BY (topic, days(event_timestamp)), files sorted by entity_key",
        "partition_evolution_rewrite_s": round(rewrite_s, 2),
        "files_before": files_before,
        "files_after": files_after,
        "before": before,
        "after": after,
        "improvement": {
            q: {
                "files_scanned": f"{before[q]['files_scanned']} -> {after[q]['files_scanned']}",
                "bytes_scanned_reduction_pct": round(
                    100 * (1 - after[q]["bytes_scanned"] / before[q]["bytes_scanned"]), 1
                )
                if before[q]["bytes_scanned"]
                else None,
                "p50_latency_change_pct": round(
                    100 * (after[q]["latency_s"]["p50"] / before[q]["latency_s"]["p50"] - 1), 1
                ),
            }
            for q in before
        },
        "partition_columns_before": spec_before,
    }
    write_result("iceberg_partitioning", payload)
    return payload


# =========================================================================== Phase 27: small files
def _compaction_snapshot(spark, tables: list[str]) -> dict[str, Any]:
    import duckdb

    from lakehouse.duckdb_plugin import configure
    from spark.batch.maintenance import file_stats
    from spark.common.tables import fq

    out = {}
    con = duckdb.connect()
    configure(con)
    for t in tables:
        ns, name = t.split(".")
        lat_spark, lat_duck = [], []
        for _ in range(7):
            t0 = time.perf_counter()
            spark.sql(f"SELECT count(*) FROM {fq(t)}").collect()
            lat_spark.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            con.execute(f"SELECT count(*) FROM {ns}.{name}").fetchall()
            lat_duck.append(time.perf_counter() - t0)
        meta_t0 = time.perf_counter()
        from lakehouse.catalog import load_table

        list(load_table(t).scan().plan_files())
        planning_s = time.perf_counter() - meta_t0
        out[t] = {
            **file_stats(t),
            "spark_count_latency_s": percentiles(lat_spark[1:]),
            "duckdb_count_latency_s": percentiles(lat_duck[1:]),
            "planning_s": round(planning_s, 4),
        }
    return out


def iceberg_compaction(spark, scale: E.Scale) -> dict[str, Any]:
    from spark.batch.maintenance import compact_table, expire_snapshots

    E.prepare_source(scale)
    E.reset_pipeline(spark)
    E.relay_all()
    # sustained streaming with tiny micro-batches: the classic small-file generator
    stream = E.drain(spark, max_offsets=2_000)
    tables = ["bronze.orders", "silver.events", "silver.inventory_movements", "silver.order_items"]
    before = _compaction_snapshot(spark, tables)
    write_result("before_compaction", {"micro_batches": stream["batches"], "tables": before})
    t0 = time.time()
    results = {t: compact_table(spark, t, target_file_mb=128)["result"] for t in tables}
    now = datetime.now(UTC).replace(tzinfo=None)
    expired = {t: expire_snapshots(spark, t, now, retain_last=5) for t in tables}
    maint_s = time.time() - t0
    after = _compaction_snapshot(spark, tables)
    write_result("after_compaction", {"tables": after})
    summary = {
        t: {
            "files": f"{before[t]['data_files']} -> {after[t]['data_files']}",
            "avg_file_kb": f"{before[t]['avg_file_bytes'] // 1024} -> {after[t]['avg_file_bytes'] // 1024}",
            "snapshots": f"{before[t]['snapshots']} -> {after[t]['snapshots']}",
            "spark_count_p50_s": f"{before[t]['spark_count_latency_s']['p50']} -> {after[t]['spark_count_latency_s']['p50']}",
            "duckdb_count_p50_s": f"{before[t]['duckdb_count_latency_s']['p50']} -> {after[t]['duckdb_count_latency_s']['p50']}",
            "planning_s": f"{before[t]['planning_s']} -> {after[t]['planning_s']}",
        }
        for t in tables
    }
    payload = {
        "scale": scale.name,
        "micro_batches": stream["batches"],
        "max_offsets_per_trigger": 2_000,
        "maintenance_runtime_s": round(maint_s, 2),
        "rewrite_results": results,
        "expire_results": expired,
        "summary": summary,
        "before": before,
        "after": after,
    }
    write_result("iceberg_compaction", payload)
    return payload


# =========================================================================== Phases 11/37: duplicates
def duplicate_test(spark, scale: E.Scale, duplicate_p: float = 0.10) -> dict[str, Any]:
    from producers.chaos import ChaosConfig
    from reconciliation.reconcile import run_reconciliation

    E.prepare_source(scale)
    E.reset_pipeline(spark)
    relay = E.relay_all(chaos=ChaosConfig(duplicate_probability=duplicate_p, late_delay_s=5.0, seed=37))
    stream = E.drain(spark)
    E.build_gold()
    rec = run_reconciliation(write=False)
    injected = sum(r["duplicates_injected"] for r in rec["physical_deliveries"])
    payload = {
        "scale": scale.name,
        "duplicate_probability": duplicate_p,
        "original_events": rec["summary"]["source_events"],
        "deliveries": relay["producer"]["deliveries"],
        "duplicates_injected": injected,
        "duplicates_detected": stream["duplicates_detected"],
        "accepted_unique": stream["accepted_new"],
        "duplicates_leaked_silver": sum(
            v for k, v in rec["logical_duplicates"].items() if k.startswith("silver")
        ),
        "duplicates_leaked_gold": sum(
            v for k, v in rec["logical_duplicates"].items() if k.startswith("gold")
        ),
        "reconciliation_passed": rec["summary"]["passed"],
        "unexplained": rec["summary"]["unexplained"],
        "financial_mismatches": rec["summary"]["financial_mismatches"],
        "stream": stream,
    }
    payload["all_duplicates_detected"] = payload["duplicates_detected"] == injected
    write_result("duplicate_test", payload)
    return payload


# =========================================================================== Phases 12/38: late data
def late_event_test(spark, orders_per_tier: int = 50) -> dict[str, Any]:
    from benchmarks import scenarios as sc
    from contracts import events as ev
    from lakehouse.duckdb_plugin import connect_gold
    from producers.kafka_producer import EventProducer
    from simulator.postgres_sink import connect
    from spark.common.tables import fq
    from spark.streaming import realtime_aggregates as rt

    E.reset_pipeline(spark)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    tiers = {
        "on_time": timedelta(0),
        "late_5m": timedelta(minutes=5),
        "late_1h": timedelta(hours=1),
        "late_1d": timedelta(days=1),
    }
    expected_rt = {"on_time": True, "late_5m": True, "late_1h": False, "late_1d": False}
    conn = connect()
    prod = EventProducer(delivery_sink=conn, run_id="late-test")
    amounts = {}

    def run_rt():
        q = rt.build_query(spark, available_now=True)
        q.awaitTermination()
        return [p if isinstance(p, dict) else json.loads(p.json) for p in q.recentProgress]

    # tier 0 first (advances the watermark), then the late tiers, then a flush event
    progress = []
    for i, (tier, lag) in enumerate(tiers.items()):
        for j in range(orders_per_tier):
            amount = 1000 + i
            prod.send(
                sc.order_event(
                    f"o_late_{tier}_{j}", "ORDER_CREATED", now - lag, amount_minor=amount, currency="USD"
                ),
                ev.TOPIC_ORDERS,
            )
            amounts[tier] = amount
        prod.flush()
        if i == 0:
            progress += run_rt()
    progress += run_rt()
    prod.send(
        sc.order_event(
            "o_late_flush", "ORDER_CREATED", now + timedelta(minutes=30), amount_minor=1, currency="EUR"
        ),
        ev.TOPIC_ORDERS,
    )
    prod.flush()
    progress += run_rt()
    stream = E.drain(spark)
    E.build_gold()
    rows = spark.sql(
        f"SELECT window_start, orders, revenue_minor FROM {fq('silver.realtime_sales_1m')} WHERE currency='USD'"
    ).collect()
    rt_windows = {r["window_start"].replace(tzinfo=None): r["orders"] for r in rows}
    silver = {
        r["k"]: r["n"]
        for r in spark.sql(
            f"SELECT regexp_extract(order_id, 'o_late_(.*)_[0-9]+$', 1) AS k, count(*) AS n FROM {fq('silver.orders')} "
            "WHERE order_id LIKE 'o_late_%' GROUP BY 1"
        ).collect()
    }
    g = connect_gold()
    gold = {
        r[0]: r[1]
        for r in g.execute(
            "SELECT regexp_extract(order_id, 'o_late_(.*)_[0-9]+$', 1), count(*) FROM gold.fact_orders "
            "WHERE order_id LIKE 'o_late_%' GROUP BY 1"
        ).fetchall()
    }
    results = {}
    for tier, lag in tiers.items():
        win = (now - lag).replace(tzinfo=None)
        in_rt = rt_windows.get(win, 0)
        results[tier] = {
            "event_time_lag_s": int(lag.total_seconds()),
            "sent": orders_per_tier,
            "in_realtime_aggregate": in_rt,
            "in_silver": silver.get(tier, 0),
            "in_gold": gold.get(tier, 0),
            "policy_expected_in_realtime": expected_rt[tier],
            "policy_ok": (in_rt == orders_per_tier) == expected_rt[tier]
            and silver.get(tier, 0) == orders_per_tier
            and gold.get(tier, 0) == orders_per_tier,
        }
    from spark.streaming.realtime_aggregates import dropped_by_watermark

    payload = {
        "watermark": rt.WATERMARK,
        "window": rt.WINDOW,
        "allowed_lateness_s": 600,
        "tiers": results,
        "rows_dropped_by_watermark": dropped_by_watermark(progress),
        "silver_late_events_counted": stream["late_events"],
        "all_policies_hold": all(r["policy_ok"] for r in results.values()),
        "policy": "≤ watermark: counted in real-time windows; beyond: excluded from real-time aggregate "
        "only — always accepted into Silver and Gold (exact).",
    }
    write_result("late_event_test", payload)
    conn.close()
    return payload


# =========================================================================== Phases 16/40: schema
def schema_evolution(spark) -> dict[str, Any]:
    from benchmarks import scenarios as sc
    from contracts import events as ev
    from contracts import schemas
    from contracts.registry import RegistryError, SchemaRegistry
    from producers.kafka_producer import EventProducer
    from spark.common.tables import fq

    E.reset_pipeline(spark)
    reg = SchemaRegistry()
    subj = schemas.subject(ev.TOPIC_ORDERS)
    t0 = datetime(2026, 2, 1, tzinfo=UTC)
    v1 = EventProducer(schema_versions={ev.TOPIC_ORDERS: 1}, run_id="schema-v1")
    v2 = EventProducer(run_id="schema-v2")
    for i in range(500):
        v1.send(sc.order_event(f"o_v1_{i}", "ORDER_CREATED", t0 + timedelta(seconds=i)), ev.TOPIC_ORDERS)
        v2.send(
            sc.order_event(f"o_v2_{i}", "ORDER_CREATED", t0 + timedelta(seconds=i), sales_channel="MOBILE"),
            ev.TOPIC_ORDERS,
        )
    v1.flush()
    v2.flush()
    drain1 = E.drain(spark)
    breaking = (schemas.AVRO_DIR / "incompatible" / "order_event.v3_breaking.avsc").read_text()
    check = reg.check_compatibility(subj, breaking)
    try:
        reg.register(subj, breaking)
        registered = True
    except RegistryError as e:
        registered = e.status != 409
    for i in range(100):  # consumers keep working after the rejected change
        v2.send(
            sc.order_event(f"o_after_{i}", "ORDER_CREATED", t0 + timedelta(hours=1, seconds=i)),
            ev.TOPIC_ORDERS,
        )
    v2.flush()
    drain2 = E.drain(spark)
    stats = spark.sql(f"""SELECT substring(order_id, 1, 4) AS p, count(*) AS n,
                          count(sales_channel) AS with_channel FROM {fq("silver.orders")} GROUP BY 1""").collect()
    by = {r["p"]: (r["n"], r["with_channel"]) for r in stats}
    versions = {
        r["schema_version"]: r["n"]
        for r in spark.sql(
            f"SELECT schema_version, count(*) AS n FROM {fq('silver.events')} GROUP BY 1"
        ).collect()
    }
    payload = {
        "compatibility_level": reg.get_compatibility(subj),
        "registered_versions": reg.versions(subj),
        "v1_events_readable": by.get("o_v1", (0, 0))[0] == 500,
        "v1_sales_channel_null": by.get("o_v1", (0, 1))[1] == 0,
        "v2_events_readable": by.get("o_v2", (0, 0))[0] == 500,
        "v2_sales_channel_present": by.get("o_v2", (0, 0))[1] == 500,
        "events_by_schema_version": versions,
        "breaking_change": {
            "compatible": check.compatible,
            "messages": check.messages[:3],
            "registered": registered,
            "rejected_safely": not registered,
        },
        "consumers_healthy_after_rejection": drain2["accepted_new"] == 100 and drain2["rejected"] == 0,
        "drains": {"mixed_versions": drain1, "after_rejection": drain2},
    }
    write_result("schema_evolution", payload)
    return payload


# =========================================================================== live tests helpers
def _checkpoint_lag(query: str = "silver_ingest") -> int | None:
    """Kafka end offsets minus the offsets Spark has *committed* in its checkpoint."""
    from producers.topics import end_offsets

    cp = settings().checkpoint_dir / query
    commits = (
        sorted(int(p.name) for p in (cp / "commits").glob("[0-9]*")) if (cp / "commits").exists() else []
    )
    if not commits:
        return None
    lines = (cp / "offsets" / str(commits[-1])).read_text().splitlines()
    offsets = json.loads(lines[-1])
    lag = 0
    for topic, parts in offsets.items():
        ends = end_offsets(topic)
        lag += sum(max(0, ends[int(p)] - int(o)) for p, o in parts.items())
    return lag


def _start(args: list[str], log_name: str) -> subprocess.Popen:
    logs = settings().data_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    fh = open(logs / f"{log_name}.log", "w")
    return subprocess.Popen(
        [*ATLAS, *args],
        env=_penv(),
        cwd=REPO_ROOT,
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _wait_stream_ready(timeout: float = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if (settings().checkpoint_dir / "silver_ingest" / "commits").exists():
            return
        time.sleep(1)
    raise TimeoutError("streaming query did not start")


def _stop(p: subprocess.Popen, sig=signal.SIGTERM, timeout: float = 60) -> None:
    if p.poll() is None:
        os.killpg(p.pid, sig)
        try:
            p.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)


def _latencies_since(spark, since: datetime) -> dict[str, Any]:
    from spark.common.tables import fq

    rows = spark.sql(f"""
        SELECT (unix_micros(silver_loaded_at) - unix_micros(event_timestamp)) / 1e6 AS e2e_s,
               (unix_micros(silver_loaded_at) - unix_micros(ingestion_timestamp)) / 1e6 AS kafka_to_silver_s
        FROM {fq("silver.events")} WHERE event_timestamp >= TIMESTAMP '{since.strftime("%Y-%m-%d %H:%M:%S")}'
          AND event_timestamp <= silver_loaded_at""").collect()
    return {
        "events": len(rows),
        "end_to_end_s": percentiles([r[0] for r in rows]),
        "kafka_to_silver_s": percentiles([r[1] for r in rows]),
    }


# =========================================================================== streaming throughput
def streaming_throughput(spark, rates: list[int], seconds: int = 90) -> dict[str, Any]:
    """Sustained live load: workload → Postgres → relay → Kafka → Spark (2 s trigger) → Silver."""
    results = []
    prior = (
        json.loads((settings().evaluation_dir / "spark_performance.json").read_text())
        if (settings().evaluation_dir / "spark_performance.json").exists()
        else None
    )
    for rate in rates:  # orders/second
        stream = _start(
            ["stream", "--trigger-s", "2", "--max-offsets", "400000", "--metrics-port", "9108"],
            f"stream_{rate}",
        )
        _wait_stream_ready()
        relay = _start(["relay", "--follow"], f"relay_{rate}")
        t_start = datetime.now(UTC).replace(tzinfo=None)
        work = subprocess.run(
            [*ATLAS, "workload", "--rate", str(rate), "--duration", str(seconds)],
            env=_penv(),
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        stats = json.loads(work.stdout[work.stdout.index("{") :]) if "{" in work.stdout else {}
        lag_samples = []
        for _ in range(30):  # let the pipeline catch up, sampling lag
            lag_samples.append(_checkpoint_lag())
            if lag_samples[-1] == 0:
                break
            time.sleep(4)
        _stop(relay)
        _stop(stream)
        lat = _latencies_since(spark, t_start)
        results.append(
            {
                "target_orders_per_s": rate,
                "workload": {
                    k: stats.get(k) for k in ("events", "events_per_s", "elapsed_s", "commit_p95_s")
                },
                "latency": lat,
                "lag_after_load": lag_samples,
                "caught_up": bool(lag_samples) and lag_samples[-1] == 0,
            }
        )
        log.info(
            "rate %s orders/s: %s events/s, e2e p95 %s",
            rate,
            stats.get("events_per_s"),
            lat["end_to_end_s"].get("p95"),
        )
    payload = {
        "duration_s_per_level": seconds,
        "levels": results,
        "batch_drain_reference": prior["drains"] if prior else None,
        "sustained_events_per_s": max((r["workload"]["events_per_s"] or 0) for r in results if r["caught_up"])
        if any(r["caught_up"] for r in results)
        else None,
    }
    write_result("streaming_throughput", payload)
    return payload


# =========================================================================== Phase 36: consumer crash
def consumer_recovery(spark, rate: int = 150, warmup_s: int = 45, outage_s: int = 45) -> dict[str, Any]:
    from reconciliation.reconcile import run_reconciliation

    stream = _start(["stream", "--trigger-s", "2", "--metrics-port", "9108"], "recovery_stream_1")
    _wait_stream_ready()
    relay = _start(["relay", "--follow"], "recovery_relay")
    work = _start(
        ["workload", "--rate", str(rate), "--duration", str(warmup_s + outage_s + 120)], "recovery_workload"
    )
    time.sleep(warmup_s)
    pre = [_checkpoint_lag() for _ in range(3)]
    steady_lag = max(x or 0 for x in pre)
    kill_at = time.time()
    _stop(stream, signal.SIGKILL)  # hard crash mid-flow, no graceful shutdown
    lag_timeline = []
    while time.time() - kill_at < outage_s:
        lag_timeline.append({"t": round(time.time() - kill_at, 1), "lag": _checkpoint_lag(), "phase": "down"})
        time.sleep(5)
    restart_at = time.time()
    stream2 = _start(["stream", "--trigger-s", "2", "--metrics-port", "9108"], "recovery_stream_2")
    recovered_at = None
    peak = max(x["lag"] or 0 for x in lag_timeline) if lag_timeline else 0
    while time.time() - restart_at < 600:
        lag = _checkpoint_lag()
        lag_timeline.append({"t": round(time.time() - kill_at, 1), "lag": lag, "phase": "recovering"})
        if lag is not None and lag <= max(steady_lag, 1_000) and time.time() - restart_at > 5:
            recovered_at = time.time()
            break
        time.sleep(3)
    work.wait()
    for _ in range(60):  # drain fully before reconciling
        if _checkpoint_lag() == 0:
            from ingestion.cdc.outbox_relay import outbox_backlog

            if outbox_backlog() == 0:
                break
        time.sleep(3)
    _stop(relay)
    _stop(stream2)
    E.build_gold(full_refresh=False)
    rec = run_reconciliation(write=False)
    payload = {
        "workload_orders_per_s": rate,
        "outage_s": outage_s,
        "steady_state_lag": steady_lag,
        "peak_backlog": peak,
        "recovery_time_s": round(recovered_at - restart_at, 1) if recovered_at else None,
        "time_from_crash_to_recovered_s": round(recovered_at - kill_at, 1) if recovered_at else None,
        "records_lost": rec["summary"]["unexplained"],
        "unknown_origin": rec["summary"]["unknown_origin"],
        "unexpected_logical_duplicates": rec["summary"]["logical_duplicates"],
        "reconciliation_passed": rec["summary"]["passed"],
        "source_events": rec["summary"]["source_events"],
        "lag_timeline": lag_timeline,
    }
    write_result("consumer_recovery", payload)
    return payload


def data_quality() -> dict[str, Any]:
    from quality.runner import run_suite

    return run_suite(write=True)["summary"]


def reconciliation() -> dict[str, Any]:
    from reconciliation.reconcile import run_reconciliation

    return run_reconciliation(write=True)["summary"]


__all__ = [n for n in dir() if not n.startswith("_")]
_ = Path
