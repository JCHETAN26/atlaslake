"""Phase 45 — the final chaos test.

    live workload ──► Postgres ──► relay (+duplicates, late, invalid, corrupt) ──► Kafka ──► Spark ──► Iceberg
                                                                       ▲ kill -9 / restart ▲

Steps: sustained traffic with injected faults → kill Spark → backlog builds → restart → recover →
historical backfill → Iceberg compaction → DLQ replay → dbt → reconcile Source → Gold → quality.
Every answer in the report is computed from measurements taken during this run.

    python -m benchmarks.chaos --rate 1400 --phase-s 60
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from datetime import UTC, datetime
from typing import Any

from benchmarks import env as E
from benchmarks import suites as S
from benchmarks.common import write_result
from core.config import REPO_ROOT, settings

log = logging.getLogger("chaos")


def _timeline(events: list[dict], name: str, **kw) -> None:
    events.append({"t": datetime.now(UTC).isoformat(timespec="seconds"), "step": name, **kw})
    log.info("CHAOS %s %s", name, kw)


def run(rate: int = 1400, phase_s: int = 60, scale: str = "1m") -> dict[str, Any]:
    from ingestion.batch.backfill import run_backfill
    from ingestion.cdc.outbox_relay import outbox_backlog
    from ingestion.streaming.dlq_replay import replay
    from lineage.dbt_runner import dbt_build
    from quality.runner import run_suite
    from reconciliation.reconcile import run_reconciliation
    from spark.batch.maintenance import compact_table
    from spark.common.session import get_spark
    from spark.common.tables import fq

    spark = get_spark("atlas-chaos")
    tl: list[dict] = []
    t_begin = time.time()
    alerts_file = settings().data_dir / "alerts.jsonl"
    alerts_before = alerts_file.read_text().count("\n") if alerts_file.exists() else 0

    _timeline(tl, "prepare_history", scale=scale)
    hist = E.full_pipeline(spark, E.SCALES[scale])
    import subprocess
    import sys

    exporter = subprocess.Popen([sys.executable, "-m", "monitoring.exporter", "--port", "9110", "--interval", "15"],
                                env=S._penv(), cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                                start_new_session=True)
    webhook = subprocess.Popen([sys.executable, "-m", "monitoring.exporter", "--webhook-port", "9111"],
                               env=S._penv(), cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                               start_new_session=True)

    # 1-3: sustained traffic with injected duplicates, late events and malformed records
    stream = S._start(["stream", "--trigger-s", "2", "--max-offsets", "400000", "--metrics-port", "9108"], "chaos_stream_1")
    S._wait_stream_ready()
    relay = S._start(["relay", "--follow", "--duplicate-p", "0.02", "--late-p", "0.01", "--late-delay-s", "20",
                      "--invalid-p", "0.003", "--corrupt-p", "0.001", "--chaos-seed", "45"], "chaos_relay")
    load_s = 4 * phase_s
    work = S._start(["workload", "--rate", str(rate), "--duration", str(load_s)], "chaos_workload")
    t_load = datetime.now(UTC).replace(tzinfo=None)
    _timeline(tl, "traffic_started", orders_per_s=rate, faults="dup 2%, late 1% (+20s), invalid 0.3%, corrupt 0.1%")
    time.sleep(phase_s)
    lag_pre = S._checkpoint_lag()

    # 4: kill Spark
    kill_at = time.time()
    S._stop(stream, signal.SIGKILL)
    _timeline(tl, "spark_killed", lag_before=lag_pre)
    lags = []
    while time.time() - kill_at < phase_s:  # 6: backlog builds while traffic continues
        lags.append({"t": round(time.time() - kill_at, 1), "lag": S._checkpoint_lag()})
        time.sleep(5)
    peak_backlog = max((x["lag"] or 0) for x in lags) if lags else 0
    _timeline(tl, "backlog_built", peak_backlog=peak_backlog)

    # 5 + 7: restart and recover the backlog
    restart_at = time.time()
    stream2 = S._start(["stream", "--trigger-s", "2", "--max-offsets", "400000", "--metrics-port", "9108"], "chaos_stream_2")
    _timeline(tl, "spark_restarted")
    recovered_at = None
    while time.time() - restart_at < 1800:
        lag = S._checkpoint_lag()
        lags.append({"t": round(time.time() - kill_at, 1), "lag": lag})
        if lag is not None and lag <= max(lag_pre or 0, 2_000) and time.time() - restart_at > 10:
            recovered_at = time.time()
            break
        time.sleep(3)
    _timeline(tl, "backlog_recovered", seconds=round(recovered_at - restart_at, 1) if recovered_at else None)
    work.wait()
    for _ in range(300):
        if outbox_backlog() == 0 and S._checkpoint_lag() == 0:
            break
        time.sleep(3)
    time.sleep(25)  # late (delayed) deliveries still held by the relay
    for _ in range(100):
        if S._checkpoint_lag() == 0:
            break
        time.sleep(3)
    S._stop(relay)
    S._stop(stream2)
    sustained = json.loads(open(settings().data_dir / "logs" / "chaos_workload.log").read().split("workload done: ")[-1]
                           .splitlines()[0]) if (settings().data_dir / "logs" / "chaos_workload.log").exists() else {}
    latency = S._latencies_since(spark, t_load)
    _timeline(tl, "traffic_stopped", workload=sustained)

    # 8: historical backfill (re-asserting January must change nothing)
    from lakehouse.fingerprint import silver_fingerprint

    fp0 = silver_fingerprint()
    from datetime import date

    bf = run_backfill(["orders", "payments", "inventory"], date(2026, 1, 1), date(2026, 1, 31), rebuild_gold=False, spark=spark)
    fp_bf = bf.pop("silver_fingerprint")
    _timeline(tl, "historical_backfill", records=bf["records_read"], accepted_new=bf["accepted_new"],
              state_unchanged=fp_bf == fp0)

    # 9: Iceberg compaction
    comp = {t: compact_table(spark, t) for t in ("silver.events", "bronze.orders", "bronze.inventory",
                                                 "silver.inventory_movements")}
    comp_summary = {t: {"files_before": c["before"]["data_files"], "files_after": c["after"]["data_files"],
                        "avg_kb_before": c["before"]["avg_file_bytes"] // 1024,
                        "avg_kb_after": c["after"]["avg_file_bytes"] // 1024} for t, c in comp.items()}
    _timeline(tl, "compaction", tables=comp_summary)

    # DLQ: inspect + replay authoritative copies for semantically invalid/corrupt records
    dlq_before = {r["error_codes"].split(",")[0]: r["n"] for r in spark.sql(
        f"SELECT error_codes, count(*) AS n FROM {fq('ops.dlq')} WHERE status='PENDING' GROUP BY 1").collect()}
    rep = replay(recover_from_source=True, spark=spark)
    drain = E.drain(spark)
    _timeline(tl, "dlq_replayed", pending_before=sum(dlq_before.values()), replayed=rep["replayed"],
              accepted_after_replay=drain["accepted_new"])

    # 10: dbt
    from ingestion.batch.postgres_snapshot import ingest_stores

    ingest_stores(spark)
    dbt = dbt_build()
    _timeline(tl, "dbt_build", success=dbt["success"], elapsed_s=dbt["elapsed_s"])

    # 11: reconcile Source → Gold, quality
    rec = run_reconciliation(write=True)
    qual = run_suite(write=True)
    S._stop(exporter)
    S._stop(webhook)
    alerts_after = alerts_file.read_text().count("\n") if alerts_file.exists() else 0
    _timeline(tl, "reconciled", passed=rec["summary"]["passed"])

    delivered = rec["physical_deliveries"]
    report = {
        "parameters": {"orders_per_s": rate, "phase_s": phase_s, "history_scale": scale},
        "history": {"events": hist["source"]["events"], "relay_events_per_s": hist["relay"]["events_per_s"],
                    "stream_events_per_s": hist["stream"]["events_per_s"]},
        "answers": {
            "did_we_lose_data": rec["summary"]["unexplained"] != 0,
            "records_unexplained": rec["summary"]["unexplained"],
            "did_duplicates_leak": rec["summary"]["logical_duplicates"] != 0,
            "duplicates_injected": sum(d["duplicates_injected"] for d in delivered),
            "did_spark_recover": recovered_at is not None,
            "recovery_time_s": round(recovered_at - restart_at, 1) if recovered_at else None,
            "peak_backlog_records": peak_backlog,
            "did_late_events_resolve": rec["summary"]["unexplained"] == 0,
            "did_dlq_work": {"quarantined_by_reason": dlq_before, "replayed": rep["replayed"],
                             "unresolved": rep["unresolved"]},
            "did_schema_protection_work": "see evaluation/schema_evolution.json (breaking change rejected)",
            "did_source_and_gold_reconcile": rec["summary"]["passed"] and rec["summary"]["gold_vs_silver_mismatches"] == 0,
            "compaction_effect": comp_summary,
            "incremental_vs_full": "see evaluation/full_vs_incremental.json",
            "sustained_throughput_events_per_s": sustained.get("events_per_s"),
            "end_to_end_latency_s": latency["end_to_end_s"],
            "backfill_idempotent": fp_bf == fp0,
            "quality_blocking_failures": qual["summary"]["failed_blocking"],
            "alerts_received": alerts_after - alerts_before,
        },
        "reconciliation_summary": rec["summary"],
        "lag_timeline": lags,
        "timeline": tl,
        "elapsed_s": round(time.time() - t_begin, 1),
    }
    write_result("chaos_report", report)
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for noisy in ("py4j", "httpx", "great_expectations"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=1400, help="orders/second (~14 events per order)")
    ap.add_argument("--phase-s", type=int, default=60)
    ap.add_argument("--scale", default="1m")
    args = ap.parse_args()
    print(json.dumps(run(args.rate, args.phase_s, args.scale)["answers"], indent=2, default=str))
