"""Shared helpers for writing reproducible evaluation artefacts (Phase 44).

Every ``evaluation/*.json`` carries the run context (git SHA, host, versions, timestamp) so numbers in
the docs can always be traced to the exact code and hardware that produced them.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import time
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from core.config import REPO_ROOT, settings


def git_sha() -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, text=True
        ).strip()
        dirty = subprocess.call(["git", "diff", "--quiet"], cwd=REPO_ROOT) != 0
        return f"{sha}{'-dirty' if dirty else ''}"
    except Exception:
        return "unknown"


def _mem_gb() -> float | None:
    try:
        with open("/proc/meminfo") as fh:
            kb = int(fh.readline().split()[1])
        return round(kb / 1024 / 1024, 1)
    except Exception:
        return None


def run_context() -> dict[str, Any]:
    versions = {}
    for pkg in (
        "pyspark",
        "pyiceberg",
        "duckdb",
        "dbt-core",
        "dbt-duckdb",
        "confluent-kafka",
        "pandas",
        "pyarrow",
    ):
        try:
            versions[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            pass
    from spark.common.session import ICEBERG_VERSION

    versions["iceberg-spark-runtime"] = ICEBERG_VERSION
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_sha": git_sha(),
        "host": {
            "cpus": os.cpu_count(),
            "memory_gb": _mem_gb(),
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
        "versions": versions,
    }


def write_result(name: str, payload: dict[str, Any], directory: Path | None = None) -> Path:
    out_dir = directory or settings().evaluation_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    doc = {"benchmark": name, "context": run_context(), **payload}
    path.write_text(json.dumps(doc, indent=2, default=str) + "\n")
    return path


def percentiles(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {}
    s = sorted(samples)

    def pct(p: float) -> float:
        k = (len(s) - 1) * p
        lo, hi = int(k), min(int(k) + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)

    return {
        "n": len(s),
        "p50": round(pct(0.5), 4),
        "p95": round(pct(0.95), 4),
        "p99": round(pct(0.99), 4),
        "min": round(s[0], 4),
        "max": round(s[-1], 4),
        "mean": round(statistics.fmean(s), 4),
    }


class Stopwatch:
    def __init__(self) -> None:
        self.t0 = time.perf_counter()

    def lap(self) -> float:
        now = time.perf_counter()
        dt, self.t0 = now - self.t0, now
        return round(dt, 3)
