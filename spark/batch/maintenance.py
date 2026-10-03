"""Iceberg table maintenance (Phases 27-28): compaction, snapshot expiry, orphan cleanup, manifests.

Retention policy (docs/RETENTION.md is generated from RETENTION below):
* snapshots are kept for ``snapshot_days`` AND at least ``retain_last`` are always kept, so time
  travel is guaranteed for that window — it must exceed the longest supported replay/backfill check.
* orphan files (written by failed/aborted commits) are removed only when older than ``orphan_days``,
  so in-flight writers are never affected.
* compaction runs when a table has many small files (avg < ``small_file_mb`` and > ``min_files``).
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from core.config import settings
from spark.common.tables import TABLES

log = logging.getLogger(__name__)
MB = 1024 * 1024


@dataclass(frozen=True)
class Retention:
    snapshot_days: int
    retain_last: int
    orphan_days: int = 3
    small_file_mb: int = 32
    min_files: int = 8
    target_file_mb: int = 128


RETENTION: dict[str, Retention] = {
    "bronze": Retention(snapshot_days=30, retain_last=50),   # raw history: generous replay window
    "silver": Retention(snapshot_days=7, retain_last=20),    # time travel for a week of audits
    "ops": Retention(snapshot_days=14, retain_last=20),
}


def file_stats(table: str) -> dict[str, Any]:
    """Current data/delete files, sizes and metadata footprint (from the Iceberg metadata via PyIceberg)."""
    from lakehouse.catalog import load_table

    tbl = load_table(table)
    files = [t.file for t in tbl.scan().plan_files()]
    sizes = [f.file_size_in_bytes for f in files]
    snaps = list(tbl.metadata.snapshots)
    manifests = tbl.current_snapshot().manifests(tbl.io) if tbl.current_snapshot() else []
    return {
        "data_files": len(files),
        "total_bytes": sum(sizes),
        "avg_file_bytes": int(sum(sizes) / len(sizes)) if sizes else 0,
        "small_files_lt_8mb": sum(s < 8 * MB for s in sizes),
        "records": sum(f.record_count for f in files),
        "snapshots": len(snaps),
        "manifests": len(manifests),
        "metadata_log_entries": len(tbl.metadata.metadata_log),
    }


def _cat() -> str:
    return settings().catalog_name


def compact_table(spark: SparkSession, table: str, target_file_mb: int = 128, strategy: str = "binpack",
                  sort_order: str | None = None) -> dict[str, Any]:
    before = file_stats(table)
    t0 = time.time()
    opts = f"map('target-file-size-bytes', '{target_file_mb * MB}', 'min-input-files', '2', 'partial-progress.enabled', 'true')"
    sort = f", sort_order => '{sort_order}'" if sort_order else ""
    row = spark.sql(
        f"CALL {_cat()}.system.rewrite_data_files(table => '{table}', strategy => '{strategy}'{sort}, options => {opts})"
    ).collect()[0].asDict()
    spark.sql(f"CALL {_cat()}.system.rewrite_manifests('{table}')").collect()
    after = file_stats(table)
    return {"table": table, "runtime_s": round(time.time() - t0, 3), "result": row, "before": before, "after": after}


def expire_snapshots(spark: SparkSession, table: str, older_than: datetime, retain_last: int) -> dict[str, Any]:
    row = spark.sql(
        f"CALL {_cat()}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}', retain_last => {retain_last})"
    ).collect()[0].asDict()
    return {k: int(v) for k, v in row.items()}


def remove_orphans(spark: SparkSession, table: str, older_than: datetime, dry_run: bool = False) -> dict[str, Any]:
    try:
        rows = spark.sql(
            f"CALL {_cat()}.system.remove_orphan_files(table => '{table}', "
            f"older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}', dry_run => {str(dry_run).lower()}, "
            f"prefix_listing => true)"
        ).collect()
        return {"orphan_files": len(rows), "examples": [r[0] for r in rows[:3]]}
    except Exception as exc:  # listing support depends on FileIO; never fail maintenance for it
        return {"orphan_files": None, "error": str(exc).splitlines()[0][:200]}


def needs_compaction(stats: dict[str, Any], policy: Retention) -> bool:
    return stats["data_files"] > policy.min_files and stats["avg_file_bytes"] < policy.small_file_mb * MB


def run_maintenance(spark: SparkSession, tables: list[str] | None = None, dry_run: bool = False,
                    now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC).replace(tzinfo=None)
    report: dict[str, Any] = {"started_at": now.isoformat(), "policy": {k: asdict(v) for k, v in RETENTION.items()},
                              "tables": {}}
    for table in tables or list(TABLES):
        layer = table.split(".")[0]
        policy = RETENTION.get(layer)
        if policy is None:
            continue
        entry: dict[str, Any] = {"before": file_stats(table)}
        if needs_compaction(entry["before"], policy) and not dry_run:
            entry["compaction"] = compact_table(spark, table, policy.target_file_mb)["result"]
        if not dry_run:
            entry["expire"] = expire_snapshots(spark, table, now - timedelta(days=policy.snapshot_days), policy.retain_last)
        entry["orphans"] = remove_orphans(spark, table, now - timedelta(days=policy.orphan_days), dry_run=dry_run)
        entry["after"] = file_stats(table)
        report["tables"][table] = entry
        log.info("maintained %s: files %s -> %s, snapshots %s -> %s", table, entry["before"]["data_files"],
                 entry["after"]["data_files"], entry["before"]["snapshots"], entry["after"]["snapshots"])
    try:
        from monitoring.metrics import push_job_metrics

        push_job_metrics("iceberg_maintenance", {
            "tables": len(report["tables"]),
            "files_after": sum(t["after"]["data_files"] for t in report["tables"].values()),
        })
    except Exception:
        pass
    return report
