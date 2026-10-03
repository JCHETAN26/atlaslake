"""Run dbt programmatically and return structured timings (used by CLI, Airflow, benchmarks)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
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
    """Run dbt in a subprocess (isolates the DuckDB file lock/memory, exactly like Airflow does)."""
    _env()
    exe = Path(sys.executable).parent / "dbt"
    cmd = [str(exe), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    t0 = time.time()
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    proc = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    elapsed = time.time() - t0
    nodes = []
    results_path = DBT_DIR / "target" / "run_results.json"
    if results_path.exists() and results_path.stat().st_mtime >= t0 - 1:
        for r in json.loads(results_path.read_text()).get("results", []):
            resp = r.get("adapter_response") or {}
            name = (
                r["unique_id"].split(".")[-1]
                if not r["unique_id"].startswith("test.")
                else r["unique_id"].split(".")[2]
            )
            nodes.append(
                {
                    "node": name,
                    "resource_type": r["unique_id"].split(".")[0],
                    "status": r["status"],
                    "execution_time_s": round(r.get("execution_time") or 0, 3),
                    "rows_affected": resp.get("rows_affected"),
                    "failures": r.get("failures"),
                    "message": (r.get("message") or "")[:300],
                }
            )
    ok = proc.returncode == 0
    return {
        "args": args,
        "success": ok,
        "returncode": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "nodes": nodes,
        "exception": None if ok else proc.stdout[-3000:] + proc.stderr[-2000:],
    }


def dbt_build(
    full_refresh: bool = False, select: str | None = None, emit_lineage: bool = True
) -> dict[str, Any]:
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
