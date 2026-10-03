"""Execute the quality suites with Great Expectations and write evaluation/data_quality.json."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import duckdb

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
logging.getLogger("great_expectations").setLevel(logging.WARNING)

import great_expectations as gx
import great_expectations.expectations as gxe

from contracts import events as ev
from core.config import settings
from lakehouse.duckdb_plugin import configure
from quality.suites import SUITES, Suite

log = logging.getLogger(__name__)


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    configure(con)
    from lakehouse.catalog import catalog

    parts = []
    for d in ev.DATASETS:
        try:
            loc = catalog().load_table(f"bronze.{d}").metadata_location
            parts.append(f"SELECT * FROM iceberg_scan('{loc}')")
        except Exception:
            continue
    if parts:
        con.execute(f"CREATE OR REPLACE VIEW bronze_all AS {' UNION ALL '.join(parts)}")
    gold = settings().gold_duckdb
    if gold.exists():
        con.execute(f"ATTACH '{gold}' AS gold_db (READ_ONLY)")
    return con


def _run_suite(ctx, con: duckdb.DuckDBPyConnection, suite: Suite) -> list[dict[str, Any]]:
    df = con.execute(suite.sql).df()
    ds = ctx.data_sources.add_or_update_pandas(f"ds_{suite.name}")
    asset = ds.add_dataframe_asset(suite.name)
    batch = asset.add_batch_definition_whole_dataframe("all").get_batch(batch_parameters={"dataframe": df})
    out = []
    for check in suite.checks:
        expectation = getattr(gxe, check.expectation)(**check.kwargs)
        res = batch.validate(expectation)
        r = res.result or {}
        out.append({
            "suite": suite.name, "layer": suite.layer, "expectation": check.expectation,
            "kwargs": {k: v for k, v in check.kwargs.items() if k != "value_set"},
            "severity": check.severity, "category": check.category, "description": check.description,
            "success": bool(res.success), "rows_checked": len(df),
            "unexpected_count": r.get("unexpected_count"), "unexpected_percent": r.get("unexpected_percent"),
            "observed_value": r.get("observed_value"),
            "partial_unexpected": [str(x) for x in (r.get("partial_unexpected_list") or [])[:5]],
        })
    return out


def run_suite(layers: list[str] | None = None, write: bool = True) -> dict[str, Any]:
    t0 = time.time()
    logging.getLogger("great_expectations").setLevel(logging.WARNING)
    ctx = gx.get_context(mode="ephemeral")
    con = _connect()
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for suite in SUITES:
        if layers and suite.layer not in layers:
            continue
        try:
            results.extend(_run_suite(ctx, con, suite))
        except Exception as exc:  # a suite that cannot run is itself a blocking failure
            log.exception("suite %s failed to run", suite.name)
            errors.append({"suite": suite.name, "error": f"{type(exc).__name__}: {exc}"[:500]})
    failed = [r for r in results if not r["success"]]
    summary = {
        "checks": len(results), "passed": len(results) - len(failed), "failed": len(failed),
        "failed_blocking": sum(r["severity"] == "blocking" for r in failed) + len(errors),
        "failed_warning": sum(r["severity"] == "warning" for r in failed),
        "suites_errored": len(errors),
        "technical_checks": sum(r["category"] == "technical" for r in results),
        "business_checks": sum(r["category"] == "business" for r in results),
        "engine": f"great_expectations {gx.__version__}",
        "runtime_s": round(time.time() - t0, 3),
    }
    report = {"summary": summary, "failures": failed, "suite_errors": errors, "results": results}
    if write:
        from benchmarks.common import write_result
        from monitoring.metrics import push_job_metrics

        write_result("data_quality", report)
        push_job_metrics("quality", {"quality_failed": summary["failed"],
                                     "quality_failed_blocking": summary["failed_blocking"],
                                     "runtime_seconds": summary["runtime_s"]},
                         success=summary["failed_blocking"] == 0)
    con.close()
    return report
