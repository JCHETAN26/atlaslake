"""Run dbt programmatically and return structured timings (used by CLI, Airflow, benchmarks)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from core.config import REPO_ROOT, settings

DBT_DIR = REPO_ROOT / "dbt"


def _env() -> None:
    os.environ["ATLAS_GOLD_DUCKDB"] = str(settings().gold_duckdb)
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    settings().gold_duckdb.parent.mkdir(parents=True, exist_ok=True)


def dbt(args: list[str]) -> dict[str, Any]:
    from dbt.cli.main import dbtRunner

    _env()
    t0 = time.time()
    res = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    elapsed = time.time() - t0
    nodes = []
    if res.result is not None and hasattr(res.result, "results"):
        for r in res.result.results:
            resp = getattr(r, "adapter_response", {}) or {}
            nodes.append({
                "node": r.node.name, "resource_type": str(r.node.resource_type), "status": str(r.status),
                "execution_time_s": round(r.execution_time or 0, 3),
                "rows_affected": resp.get("rows_affected"),
                "failures": getattr(r, "failures", None),
                "message": (r.message or "")[:300] if hasattr(r, "message") else None,
            })
    return {"args": args, "success": bool(res.success), "returncode": 0 if res.success else 1,
            "elapsed_s": round(elapsed, 3), "nodes": nodes,
            "exception": str(res.exception) if res.exception else None}


def dbt_build(full_refresh: bool = False, select: str | None = None, emit_lineage: bool = True) -> dict[str, Any]:
    args = ["build"]
    if full_refresh:
        args.append("--full-refresh")
    if select:
        args += ["--select", select]
    out = dbt(args)
    if emit_lineage:
        try:
            from lineage.openlineage_emitter import emit_dbt_run

            emit_dbt_run(out)
        except Exception:  # lineage must never fail the build
            pass
    return out


def manifest() -> dict[str, Any]:
    path = DBT_DIR / "target" / "manifest.json"
    if not path.exists():
        dbt(["parse"])
    return json.loads(Path(path).read_text())
