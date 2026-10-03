"""Render docs/RESULTS.md from evaluation/*.json (Phase 44: no number is typed by hand).

python -m benchmarks.report
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.config import REPO_ROOT, settings

OUT = REPO_ROOT / "docs" / "RESULTS.md"


def _load(name: str) -> dict[str, Any] | None:
    p = settings().evaluation_dir / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def _n(x: Any, nd: int = 1) -> str:
    if x is None:
        return "—"
    if isinstance(x, bool):
        return "✅ yes" if x else "❌ no"
    if isinstance(x, int):
        return f"{x:,}"
    if isinstance(x, float):
        return f"{x:,.{nd}f}"
    return str(x)


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(_n(c) if not isinstance(c, str) else c for c in r) + " |" for r in rows]
    return "\n".join(lines)


def _ctx(d: dict | None) -> str:
    if not d:
        return ""
    c = d.get("context", {})
    h = c.get("host", {})
    return f"_Source: `evaluation/{d.get('benchmark')}.json` · git `{c.get('git_sha')}` · {c.get('generated_at')} · {h.get('cpus')} CPU / {h.get('memory_gb')} GB_"


def section_comparison(sec: list[str]) -> None:
    base, sp, st, q, bf, dup, rec = (
        _load(n)
        for n in (
            "baseline_pipeline",
            "spark_performance",
            "streaming_throughput",
            "query_performance",
            "backfill_benchmark",
            "duplicate_test",
            "consumer_recovery",
        )
    )
    if not base:
        return
    working = sp["scale"] if sp else None
    brun = next((r for r in base["runs"] if r["scale"] == working), base["runs"][-1])
    best_drain = max(sp["drains"], key=lambda d: d["events_per_s"] or 0) if sp else None
    sustained = st.get("sustained_events_per_s") if st else None
    caught = [lv for lv in (st or {}).get("levels", []) if lv["caught_up"]]
    lat = caught[-1]["latency"]["end_to_end_s"] if caught else {}
    rows = [
        [
            "Data freshness (event → queryable)",
            f"≥ {_n(brun['staleness_at_completion_s'])} s after each run *plus* the schedule interval (full refresh)",
            f"p50 {_n(lat.get('p50'))} s / p95 {_n(lat.get('p95'))} s (continuous)" if lat else "—",
        ],
        [
            "Throughput",
            f"{_n(round(brun['rows_processed'] / brun['total_runtime_s']))} source rows/s — recomputes from *current state*; consumes no events, keeps no history",
            f"{_n(best_drain['events_per_s'])} events/s historical drain (validate, dedup, Bronze + DLQ + Silver) · {_n(sustained)} events/s sustained live (bounded by the single-process source workload)"
            if best_drain
            else "—",
        ],
        [
            "Processing runtime (whole history)",
            f"{_n(brun['total_runtime_s'])} s",
            f"{_n(best_drain['elapsed_s'])} s stream drain" if best_drain else "—",
        ],
        [
            "Backfill runtime",
            f"{_n(brun['total_runtime_s'])} s (only option: full refresh)",
            f"{_n(bf['runtime_s'])} s for {_n(bf['records'])} records, {bf['chunks']} weekly chunks; rerun changes nothing: {_n(bf['rerun_week']['state_unchanged'])}"
            if bf
            else "—",
        ],
        [
            "Duplicate correctness",
            "not applicable: reads source *state*, cannot consume an event stream",
            f"{_n(dup['duplicates_injected'])} injected → {_n(dup['duplicates_leaked_silver'] + dup['duplicates_leaked_gold'])} leaked"
            if dup
            else "—",
        ],
        [
            "Recovery from crash",
            f"re-run from scratch ({_n(brun['total_runtime_s'])} s at this scale)",
            f"{_n(rec['recovery_time_s'])} s after restart; lost {_n(rec['records_lost'])}, duplicates {_n(rec['unexpected_logical_duplicates'])}"
            if rec
            else "—",
        ],
    ]
    if q:
        bp50 = sorted(v["baseline_postgres"]["warm"]["p50"] for v in q["queries"].values())
        gp50 = sorted(v["atlaslake_gold_duckdb"]["warm"]["p50"] for v in q["queries"].values())
        bp95 = max(v["baseline_postgres"]["warm"]["p95"] for v in q["queries"].values())
        gp95 = max(v["atlaslake_gold_duckdb"]["warm"]["p95"] for v in q["queries"].values())
        rows += [
            [
                "Query p50 (median across the 6 queries)",
                f"{bp50[len(bp50) // 2] * 1000:.2f} ms",
                f"{gp50[len(gp50) // 2] * 1000:.2f} ms",
            ],
            ["Query p95 (worst query)", f"{bp95 * 1000:.2f} ms", f"{gp95 * 1000:.2f} ms"],
            ["Same answers on every query", "", _n(all(v["same_result"] for v in q["queries"].values()))],
        ]
    sec += [
        "## Baseline vs AtlasLake (Phase 42)",
        "",
        f"Working scale: **{working}** events. Every cell is copied from the JSON files below by `benchmarks/report.py`.",
        "",
        _table(["Metric", "Baseline (Phase 4)", "AtlasLake"], rows),
        "",
        "**How to read this.** The baseline is a full refresh of *current source state*. At small scale it is "
        "cheap, but it has no event history (no SCD2, no replay), its freshness is bounded by its schedule, "
        "it cannot absorb duplicate, late or invalid *events*, and its cost grows with total history on every "
        "run (see the scale ladder below). AtlasLake processes each event once, incrementally, through validation, "
        "dedup and an immutable Bronze record; its per-event work is higher, and that buys the correctness "
        "rows of this table.",
        "",
    ]


def section_scaling(sec: list[str]) -> None:
    """Same suites at different working scales (evaluation/small/ archive vs evaluation/)."""
    rows = []
    for label, directory in (
        ("archive: evaluation/small", settings().evaluation_dir / "small"),
        ("current: evaluation", settings().evaluation_dir),
    ):
        sp = directory / "spark_performance.json"
        bf = directory / "backfill_benchmark.json"
        if not sp.exists():
            continue
        spd = json.loads(sp.read_text())
        best = max(spd["drains"], key=lambda d: d["events_per_s"] or 0)
        bfd = json.loads(bf.read_text()) if bf.exists() else {}
        rows.append(
            [
                spd["scale"],
                best["received"],
                best["events_per_s"],
                best["max_offsets_per_trigger"],
                best["elapsed_s"],
                bfd.get("throughput_records_per_s"),
                bfd.get("matches_streaming_state"),
                label,
            ]
        )
    if len(rows) > 1:
        sec += [
            "## Scaling the AtlasLake pipeline",
            "",
            _table(
                [
                    "working scale",
                    "events drained",
                    "best events/s",
                    "at maxOffsets",
                    "drain s",
                    "backfill records/s",
                    "backfill == streaming",
                    "source",
                ],
                rows,
            ),
            "",
        ]


def section_baseline(sec: list[str]) -> None:
    d = _load("baseline_pipeline")
    if not d:
        return
    rows = [
        [
            r["scale"],
            r["source_events"],
            r["rows_processed"],
            r["stage_runtime_s"]["extract_s"],
            r["stage_runtime_s"]["transform_s"],
            r["stage_runtime_s"]["load_s"],
            r["total_runtime_s"],
            round(r["storage_bytes"]["parquet"] / 1e6, 1),
            r["peak_rss_mb"],
        ]
        for r in d["runs"]
    ]
    sec += [
        "## Baseline scale ladder (Phase 4)",
        "",
        _ctx(d),
        "",
        _table(
            [
                "scale",
                "source events",
                "rows",
                "extract s",
                "transform s",
                "load s",
                "total s",
                "parquet MB",
                "peak RSS MB",
            ],
            rows,
        ),
        "",
    ]


def section_spark(sec: list[str]) -> None:
    d = _load("spark_performance")
    if not d:
        return
    rows = [
        [
            x["max_offsets_per_trigger"],
            x["parallel_table_writes"],
            x["batches"],
            x["received"],
            x["elapsed_s"],
            x["events_per_s"],
            x["stage_seconds_total"].get("entity_merges"),
            x["stage_seconds_total"].get("decode_validate"),
        ]
        for x in d["drains"]
    ]
    sec += [
        "## Spark processing (Kafka → Bronze/DLQ/Silver)",
        "",
        _ctx(d),
        "",
        f"Relay (Postgres outbox → Kafka): **{_n(d['relay']['events_per_s'])} events/s** for {_n(d['relay']['events'])} events.",
        "",
        _table(
            [
                "maxOffsetsPerTrigger",
                "parallel writes",
                "batches",
                "events",
                "elapsed s",
                "events/s",
                "MERGE s",
                "decode s",
            ],
            rows,
        ),
        "",
    ]


def section_streaming(sec: list[str]) -> None:
    d = _load("streaming_throughput")
    if not d:
        return
    rows = [
        [
            lv["target_orders_per_s"],
            lv["workload"]["events_per_s"],
            lv["latency"]["end_to_end_s"].get("p50"),
            lv["latency"]["end_to_end_s"].get("p95"),
            lv["latency"]["kafka_to_silver_s"].get("p95"),
            lv["caught_up"],
        ]
        for lv in d["levels"]
    ]
    sec += [
        "## Sustained live load (workload → Postgres → relay → Kafka → Spark → Silver)",
        "",
        _ctx(d),
        "",
        _table(["orders/s", "events/s", "e2e p50 s", "e2e p95 s", "Kafka→Silver p95 s", "kept up"], rows),
        "",
        f"Highest load the pipeline kept up with: **{_n(d.get('sustained_events_per_s'))} events/s**.",
        "",
    ]


def section_correctness(sec: list[str]) -> None:
    dup, late, rec, sch, recon = (
        _load(n)
        for n in (
            "duplicate_test",
            "late_event_test",
            "consumer_recovery",
            "schema_evolution",
            "reconciliation",
        )
    )
    if dup:
        sec += [
            "## Duplicates (Phases 11 / 37)",
            "",
            _ctx(dup),
            "",
            _table(
                [
                    "original events",
                    "deliveries",
                    "duplicates injected",
                    "detected",
                    "leaked to Silver",
                    "leaked to Gold",
                    "reconciled",
                ],
                [
                    [
                        dup["original_events"],
                        dup["deliveries"],
                        dup["duplicates_injected"],
                        dup["duplicates_detected"],
                        dup["duplicates_leaked_silver"],
                        dup["duplicates_leaked_gold"],
                        dup["reconciliation_passed"],
                    ]
                ],
            ),
            "",
        ]
    if late:
        rows = [
            [
                t,
                v["event_time_lag_s"],
                v["sent"],
                v["in_realtime_aggregate"],
                v["in_silver"],
                v["in_gold"],
                v["policy_ok"],
            ]
            for t, v in late["tiers"].items()
        ]
        sec += [
            "## Late events vs the watermark (Phases 12 / 38)",
            "",
            _ctx(late),
            "",
            f"Watermark {late['watermark']}, window {late['window']}. Rows dropped by the watermark (Spark): **{late['rows_dropped_by_watermark']}**.",
            "",
            _table(
                ["tier", "lateness s", "sent", "in real-time agg", "in Silver", "in Gold", "policy holds"],
                rows,
            ),
            "",
        ]
    if rec:
        sec += [
            "## Consumer crash (Phase 36)",
            "",
            _ctx(rec),
            "",
            _table(
                [
                    "outage s",
                    "peak backlog",
                    "recovery s (after restart)",
                    "records lost",
                    "unexpected duplicates",
                    "reconciled",
                ],
                [
                    [
                        rec["outage_s"],
                        rec["peak_backlog"],
                        rec["recovery_time_s"],
                        rec["records_lost"],
                        rec["unexpected_logical_duplicates"],
                        rec["reconciliation_passed"],
                    ]
                ],
            ),
            "",
        ]
    if sch:
        b = sch["breaking_change"]
        sec += [
            "## Schema evolution (Phases 16 / 40)",
            "",
            _ctx(sch),
            "",
            _table(
                ["check", "result"],
                [
                    ["v1 events readable after v2", sch["v1_events_readable"]],
                    ["v1 rows have NULL sales_channel", sch["v1_sales_channel_null"]],
                    ["v2 rows carry sales_channel", sch["v2_sales_channel_present"]],
                    ["breaking change judged compatible", b["compatible"]],
                    ["breaking change rejected by registry", b["rejected_safely"]],
                    ["consumers healthy after rejection", sch["consumers_healthy_after_rejection"]],
                ],
            ),
            "",
        ]
    if recon:
        s = recon["summary"]
        sec += [
            "## Reconciliation Source → Gold (Phase 23)",
            "",
            _ctx(recon),
            "",
            f"Invariant: `{s['invariant']}`",
            "",
            _table(
                [
                    "source events",
                    "accepted",
                    "rejected",
                    "in flight",
                    "pending",
                    "unexplained",
                    "unknown origin",
                    "logical dupes",
                    "financial mismatches",
                    "passed",
                ],
                [
                    [
                        s["source_events"],
                        s["accepted"],
                        s["rejected"],
                        s["in_flight"],
                        s["pending_processing"],
                        s["unexplained"],
                        s["unknown_origin"],
                        s["logical_duplicates"],
                        s["financial_mismatches"],
                        s["passed"],
                    ]
                ],
            ),
            "",
        ]


def section_lakehouse(sec: list[str]) -> None:
    part, comp, fvi, bf = (
        _load(n)
        for n in ("iceberg_partitioning", "iceberg_compaction", "full_vs_incremental", "backfill_benchmark")
    )
    if part:
        rows = [
            [
                q,
                part["before"][q]["files_scanned"],
                part["after"][q]["files_scanned"],
                round(part["before"][q]["bytes_scanned"] / 1e6, 2),
                round(part["after"][q]["bytes_scanned"] / 1e6, 2),
                part["before"][q]["latency_s"]["p50"],
                part["after"][q]["latency_s"]["p50"],
                part["before"][q]["latency_s"]["p95"],
                part["after"][q]["latency_s"]["p95"],
            ]
            for q in part["before"]
        ]
        sec += [
            "## Iceberg partition optimisation (Phase 26)",
            "",
            _ctx(part),
            "",
            f"`{part['table']}`: {part['layout_before']} → {part['layout_after']}.",
            "",
            _table(
                [
                    "query",
                    "files before",
                    "files after",
                    "MB before",
                    "MB after",
                    "p50 s before",
                    "p50 s after",
                    "p95 s before",
                    "p95 s after",
                ],
                rows,
            ),
            "",
        ]
    if comp:
        rows = [[t, *v.values()] for t, v in comp["summary"].items()]
        sec += [
            "## Small files and compaction (Phase 27)",
            "",
            _ctx(comp),
            "",
            f"{comp['micro_batches']} micro-batches at maxOffsetsPerTrigger={comp['max_offsets_per_trigger']}. "
            "Full details: `evaluation/before_compaction.json`, `evaluation/after_compaction.json`.",
            "",
            _table(
                [
                    "table",
                    "files",
                    "avg file KB",
                    "snapshots",
                    "Spark count p50 s",
                    "DuckDB count p50 s",
                    "planning s",
                ],
                rows,
            ),
            "",
        ]
    if fvi:
        sec += [
            "## dbt full refresh vs incremental (Phase 20)",
            "",
            _ctx(fvi),
            "",
            _table(
                ["", "full refresh", "incremental"],
                [
                    [
                        "whole `dbt build` s",
                        fvi["full_refresh"]["elapsed_s"],
                        fvi["incremental"]["elapsed_s"],
                    ],
                    [
                        "incremental models s",
                        fvi["full_refresh"]["incremental_models_s"],
                        fvi["incremental"]["incremental_models_s"],
                    ],
                ],
            ),
            "",
            f"New events in the increment: {_n(fvi['increment']['accepted_new_events'])}. "
            f"Results identical (Gold fingerprints): **{_n(fvi['results_identical'])}**. "
            f"Speed-up on incremental models: **{_n(fvi.get('speedup_incremental_models'), 2)}×**.",
            "",
        ]
    if bf:
        sec += [
            "## Historical backfill (Phases 25 / 41)",
            "",
            _ctx(bf),
            "",
            _table(
                [
                    "records",
                    "chunks",
                    "runtime s",
                    "records/s",
                    "matches streaming state",
                    "rerun week unchanged",
                    "rerun accepted new",
                ],
                [
                    [
                        bf["records"],
                        bf["chunks"],
                        bf["runtime_s"],
                        bf["throughput_records_per_s"],
                        bf["matches_streaming_state"],
                        bf["rerun_week"]["state_unchanged"],
                        bf["rerun_week"]["accepted_new"],
                    ]
                ],
            ),
            "",
        ]


def section_quality_queries(sec: list[str]) -> None:
    q, dq = _load("query_performance"), _load("data_quality")
    if q:
        rows = [
            [
                name,
                v["baseline_postgres"]["warm"]["p50"] * 1000,
                v["baseline_postgres"]["warm"]["p95"] * 1000,
                v["atlaslake_gold_duckdb"]["warm"]["p50"] * 1000,
                v["atlaslake_gold_duckdb"]["warm"]["p95"] * 1000,
                v["baseline_postgres"]["bytes_touched"],
                v["atlaslake_gold_duckdb"]["rows_scanned"],
                v["same_result"],
            ]
            for name, v in q["queries"].items()
        ]
        sec += [
            "## Query performance (same queries, same data)",
            "",
            _ctx(q),
            "",
            _table(
                [
                    "query",
                    "baseline p50 ms",
                    "baseline p95 ms",
                    "Gold p50 ms",
                    "Gold p95 ms",
                    "PG bytes touched",
                    "DuckDB rows scanned",
                    "same result",
                ],
                rows,
            ),
            "",
            f"Snowflake: {q.get('snowflake')}.",
            "",
        ]
    if dq:
        s = dq["summary"]
        sec += [
            "## Data quality (Phase 22)",
            "",
            _ctx(dq),
            "",
            _table(
                [
                    "checks",
                    "passed",
                    "failed (blocking)",
                    "failed (warning)",
                    "technical",
                    "business",
                    "engine",
                ],
                [
                    [
                        s["checks"],
                        s["passed"],
                        s["failed_blocking"],
                        s["failed_warning"],
                        s["technical_checks"],
                        s["business_checks"],
                        s["engine"],
                    ]
                ],
            ),
            "",
        ]


def section_chaos(sec: list[str]) -> None:
    c = _load("chaos_report")
    if not c:
        return
    a = c["answers"]
    rows = [
        [
            "Did we lose data?",
            f"{'yes' if a['did_we_lose_data'] else 'no'} ({_n(a['records_unexplained'])} unexplained)",
        ],
        [
            "Did duplicates leak?",
            f"{'yes' if a['did_duplicates_leak'] else 'no'} ({_n(a['duplicates_injected'])} injected)",
        ],
        ["Did Spark recover?", _n(a["did_spark_recover"])],
        [
            "How long did recovery take?",
            f"{_n(a['recovery_time_s'])} s (peak backlog {_n(a['peak_backlog_records'])} records)",
        ],
        ["Did late events resolve correctly?", _n(a["did_late_events_resolve"])],
        [
            "Did the DLQ work?",
            f"quarantined {_n(sum(a['did_dlq_work']['quarantined_by_reason'].values()))}, replayed {_n(a['did_dlq_work']['replayed'])}, unresolved {_n(a['did_dlq_work']['unresolved'])}",
        ],
        ["Did schema protection work?", a["did_schema_protection_work"]],
        ["Did Source and Gold reconcile?", _n(a["did_source_and_gold_reconcile"])],
        ["Backfill during chaos idempotent?", _n(a["backfill_idempotent"])],
        ["Sustained throughput", f"{_n(a['sustained_throughput_events_per_s'])} events/s"],
        [
            "End-to-end latency",
            f"p50 {_n(a['end_to_end_latency_s'].get('p50'))} s / p95 {_n(a['end_to_end_latency_s'].get('p95'))} s",
        ],
        ["Alerts received by webhook", _n(a["alerts_received"])],
    ]
    sec += [
        "## Final chaos test (Phase 45)",
        "",
        _ctx(c),
        "",
        _table(["Question", "Measured answer"], rows),
        "",
    ]


README = REPO_ROOT / "README.md"
START, END = "<!-- results:start -->", "<!-- results:end -->"


def headline_rows() -> list[list[Any]]:
    rows: list[list[Any]] = []
    dup, rec, late, bf, sp, fvi, rcn, sch, q, chaos = (
        _load(n)
        for n in (
            "duplicate_test",
            "consumer_recovery",
            "late_event_test",
            "backfill_benchmark",
            "spark_performance",
            "full_vs_incremental",
            "reconciliation",
            "schema_evolution",
            "query_performance",
            "chaos_report",
        )
    )
    if sp:
        best = max(sp["drains"], key=lambda d: d["events_per_s"] or 0)
        rows.append(
            [
                "Kafka → Bronze/DLQ/Silver throughput",
                f"{_n(best['events_per_s'])} events/s ({_n(best['received'])} events, maxOffsets {_n(best['max_offsets_per_trigger'])})",
            ]
        )
    if dup:
        rows.append(
            [
                "Duplicates injected → leaked",
                f"{_n(dup['duplicates_injected'])} → {_n(dup['duplicates_leaked_silver'] + dup['duplicates_leaked_gold'])}",
            ]
        )
    if rec:
        rows.append(
            [
                "Spark `kill -9` mid-stream",
                f"recovered {_n(rec['recovery_time_s'])} s after restart; lost {_n(rec['records_lost'])}, duplicates {_n(rec['unexpected_logical_duplicates'])}",
            ]
        )
    if late:
        rows.append(
            [
                "Late-data policy (on time / 5 min / 1 h / 1 day)",
                "all tiers hold" if late["all_policies_hold"] else "VIOLATED",
            ]
        )
    if bf:
        rows.append(
            [
                "Rebuild Silver from Bronze (backfill)",
                f"{_n(bf['records'])} records in {_n(bf['runtime_s'])} s; identical to streaming: {_n(bf['matches_streaming_state'])}; rerun changes nothing: {_n(bf['rerun_week']['state_unchanged'])}",
            ]
        )
    if fvi:
        rows.append(
            [
                "dbt incremental vs full refresh",
                f"{_n(fvi.get('speedup_incremental_models'), 2)}× faster, identical results: {_n(fvi['results_identical'])}",
            ]
        )
    if sch:
        rows.append(
            [
                "Breaking schema change",
                "rejected by registry, consumers unaffected"
                if sch["breaking_change"]["rejected_safely"] and sch["consumers_healthy_after_rejection"]
                else "NOT rejected",
            ]
        )
    if q:
        rows.append(
            [
                "Gold vs baseline answers (6 queries)",
                "identical" if all(v["same_result"] for v in q["queries"].values()) else "DIFFER",
            ]
        )
    if rcn:
        s_ = rcn["summary"]
        rows.append(
            [
                "Source → Gold reconciliation",
                f"{_n(s_['source_events'])} events, unexplained {_n(s_['unexplained'])}, passed {_n(s_['passed'])}",
            ]
        )
    if chaos:
        a = chaos["answers"]
        rows.append(
            [
                "Final chaos test",
                f"lost data: {'yes' if a['did_we_lose_data'] else 'no'}; duplicates leaked: {'yes' if a['did_duplicates_leak'] else 'no'}; reconciled: {_n(a['did_source_and_gold_reconcile'])}",
            ]
        )
    return rows


def update_readme() -> None:
    if not README.exists():
        return
    text = README.read_text()
    if START not in text or END not in text:
        return
    block = "\n".join(
        [
            START,
            "",
            _table(["Experiment", "Measured (auto-generated from evaluation/)"], headline_rows()),
            "",
            END,
        ]
    )
    pre, rest = text.split(START, 1)
    post = rest.split(END, 1)[1]
    README.write_text(pre + block + post)


def render() -> Path:
    sec = [
        "# AtlasLake — measured results",
        "",
        "> Generated by `python -m benchmarks.report` from `evaluation/*.json`. Do not edit by hand; "
        "re-run `make benchmark` to refresh.",
        "",
    ]
    for fn in (
        section_comparison,
        section_correctness,
        section_spark,
        section_streaming,
        section_lakehouse,
        section_quality_queries,
        section_scaling,
        section_baseline,
        section_chaos,
    ):
        fn(sec)
    OUT.write_text("\n".join(sec) + "\n")
    update_readme()
    return OUT


if __name__ == "__main__":
    print(render())
