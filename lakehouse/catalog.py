"""PyIceberg access to the same JDBC catalog Spark uses (planning stats, light reads, DuckDB handoff)."""

from __future__ import annotations

from functools import lru_cache

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from core.config import settings


@lru_cache(maxsize=1)
def catalog() -> SqlCatalog:
    cfg = settings()
    return SqlCatalog(
        cfg.catalog_name,
        uri=cfg.pg.sqlalchemy_url(cfg.pg.catalog_db),
        warehouse=cfg.s3.warehouse,
        **{
            "s3.endpoint": cfg.s3.endpoint,
            "s3.access-key-id": cfg.s3.access_key,
            "s3.secret-access-key": cfg.s3.secret_key,
            "s3.region": cfg.s3.region,
            "s3.path-style-access": "true",
        },
    )


def load_table(name: str) -> Table:
    """Always fresh metadata (no caching of table state)."""
    return catalog().load_table(name)


def metadata_location(name: str) -> str:
    return load_table(name).metadata_location


def scan_stats(name: str, row_filter=None) -> dict:
    """Files/bytes an engine would read for a filter, from Iceberg planning (partition + stats pruning)."""
    tbl = load_table(name)
    tasks = list(tbl.scan(row_filter=row_filter).plan_files() if row_filter is not None else tbl.scan().plan_files())
    total = list(tbl.scan().plan_files())
    return {
        "files_scanned": len(tasks),
        "bytes_scanned": sum(t.file.file_size_in_bytes for t in tasks),
        "rows_in_scanned_files": sum(t.file.record_count for t in tasks),
        "files_total": len(total),
        "bytes_total": sum(t.file.file_size_in_bytes for t in total),
    }
