"""Order-independent fingerprints of logical table state.

fingerprint(table) = (row count, sum of 64-bit row hashes) over every column EXCEPT processing-time
bookkeeping (silver_updated_at, silver_loaded_at, dbt_loaded_at, ...). Two states with equal
fingerprints contain the same rows, regardless of how/when/in what order they were produced — which
is exactly what "same final logical state" means for idempotency and backfill tests.
"""

from __future__ import annotations

from typing import Any

import duckdb

from lakehouse.duckdb_plugin import configure

PROCESSING_COLUMNS = {
    "silver_updated_at", "silver_loaded_at", "bronze_loaded_at", "dbt_loaded_at", "max_dbt_loaded_at",
    "computed_at", "extracted_at", "processed_at", "dlq_timestamp", "resolved_at",
    # which physical delivery won the in-batch dedup race is not logical state:
    "kafka_partition", "kafka_offset", "ingestion_timestamp", "source",
}

SILVER_TABLES = ["events", "orders", "order_items", "payments", "customers", "customer_changes", "products",
                 "product_changes", "inventory", "inventory_movements"]
GOLD_TABLES = ["dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_inventory",
               "mart_daily_sales", "mart_order_funnel", "mart_payment_metrics", "mart_inventory_health"]


def _fp(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, Any]:
    cols = [r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall() if r[0] not in PROCESSING_COLUMNS]
    expr = ", ".join(f'"{c}"' for c in cols)
    n, h = con.execute(f"SELECT count(*), coalesce(sum(hash({expr})::hugeint), 0) FROM {relation}").fetchone()
    return {"rows": int(n), "hash": str(h)}


def silver_fingerprint(con: duckdb.DuckDBPyConnection | None = None) -> dict[str, dict[str, Any]]:
    own = con is None
    con = con or duckdb.connect()
    if own:
        configure(con)
    out = {t: _fp(con, f"silver.{t}") for t in SILVER_TABLES}
    if own:
        con.close()
    return out


def gold_fingerprint() -> dict[str, dict[str, Any]]:
    from core.config import settings

    path = settings().gold_duckdb
    if not path.exists():
        return {}
    with duckdb.connect(str(path), read_only=True) as con:
        return {t: _fp(con, f"gold.{t}") for t in GOLD_TABLES}


def diff(a: dict[str, dict], b: dict[str, dict]) -> list[str]:
    return sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
