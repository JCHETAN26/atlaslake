"""Publish Gold to serving targets (Phase 21).

Targets
* ``postgres``  — analytics.gold.* serving copy (full replace per table, one transaction per table)
* ``iceberg``   — iceberg gold.* (overwrite) so any engine (Spark/Trino/Snowflake) can read Gold in place
* ``snowflake`` — ATLASLAKE.GOLD.*: dimensions full-replace, facts/marts **incremental** via
  STAGING + MERGE on each table's grain key, driven by a high-water mark on ``dbt_loaded_at``
  persisted in ATLASLAKE.STAGING.PUBLISH_WATERMARKS. Skipped (with a reason) when no credentials.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import duckdb

from core.config import settings

log = logging.getLogger(__name__)

# table -> merge key columns (None = full replace)
GOLD_TABLES: dict[str, list[str] | None] = {
    "dim_customer": None, "dim_product": None, "dim_store": None, "dim_date": None,
    "fact_orders": ["order_id"], "fact_order_items": ["order_id", "line_number"],
    "fact_payments": ["payment_id"], "fact_inventory": ["event_id"],
    "mart_daily_sales": ["order_date", "store_id", "currency"], "mart_order_funnel": ["order_date"],
    "mart_payment_metrics": ["payment_date", "method", "currency"], "mart_inventory_health": None,
}
WATERMARK_COL = {"fact_orders": "dbt_loaded_at", "fact_order_items": "dbt_loaded_at",
                 "fact_payments": "dbt_loaded_at", "fact_inventory": "dbt_loaded_at",
                 "mart_daily_sales": "max_dbt_loaded_at", "mart_order_funnel": "max_dbt_loaded_at",
                 "mart_payment_metrics": "max_dbt_loaded_at"}


def _gold() -> duckdb.DuckDBPyConnection:
    """In-memory session with the Gold file attached read-only as ``gold_db`` (always fully qualified)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{settings().gold_duckdb}' AS gold_db (READ_ONLY)")
    return con


def publish_postgres() -> dict[str, Any]:
    cfg = settings()
    t0 = time.time()
    con = _gold()  # Gold attached read-only; Postgres attached read-write
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{cfg.pg.dsn(cfg.pg.analytics_db)}' AS pg (TYPE postgres)")
    con.execute("CREATE SCHEMA IF NOT EXISTS pg.gold")
    rows = {}
    for table in GOLD_TABLES:
        con.execute("BEGIN")
        con.execute(f"DROP TABLE IF EXISTS pg.gold.{table}")
        con.execute(f"CREATE TABLE pg.gold.{table} AS SELECT * FROM gold_db.gold.{table}")
        con.execute("COMMIT")
        rows[table] = con.execute(f"SELECT count(*) FROM gold_db.gold.{table}").fetchone()[0]
    con.close()
    return {"target": "postgres", "tables": rows, "runtime_s": round(time.time() - t0, 3)}


def publish_iceberg() -> dict[str, Any]:
    from pyiceberg.exceptions import NoSuchTableError

    from lakehouse.catalog import catalog

    t0 = time.time()
    cat = catalog()
    try:
        cat.create_namespace("gold")
    except Exception:
        pass
    con = _gold()
    rows = {}
    for table in GOLD_TABLES:
        arrow = con.execute(f"SELECT * FROM gold_db.gold.{table}").fetch_arrow_table()
        try:
            tbl = cat.load_table(f"gold.{table}")
            if tbl.schema().as_arrow() != arrow.schema:
                cat.drop_table(f"gold.{table}")
                raise NoSuchTableError(table)
        except NoSuchTableError:
            tbl = cat.create_table(f"gold.{table}", schema=arrow.schema,
                                   properties={"format-version": "2", "write.parquet.compression-codec": "zstd"})
        tbl.overwrite(arrow)
        rows[table] = arrow.num_rows
    con.close()
    return {"target": "iceberg", "tables": rows, "runtime_s": round(time.time() - t0, 3)}


def snowflake_config() -> dict[str, str] | None:
    keys = ("ACCOUNT", "USER", "PASSWORD")
    if not all(os.environ.get(f"ATLAS_SNOWFLAKE_{k}") for k in keys):
        return None
    return {
        "account": os.environ["ATLAS_SNOWFLAKE_ACCOUNT"], "user": os.environ["ATLAS_SNOWFLAKE_USER"],
        "password": os.environ["ATLAS_SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("ATLAS_SNOWFLAKE_ROLE", "ATLAS_LOADER"),
        "warehouse": os.environ.get("ATLAS_SNOWFLAKE_WAREHOUSE", "ATLAS_WH"),
        "database": os.environ.get("ATLAS_SNOWFLAKE_DATABASE", "ATLASLAKE"),
    }


def merge_sql(table: str, keys: list[str], columns: list[str]) -> str:
    t, s = f"GOLD.{table.upper()}", f"STAGING.{table.upper()}_LOAD"
    on = " AND ".join(f't."{k.upper()}" = s."{k.upper()}"' for k in keys)
    upd = ", ".join(f't."{c.upper()}" = s."{c.upper()}"' for c in columns if c not in keys)
    cols = ", ".join(f'"{c.upper()}"' for c in columns)
    vals = ", ".join(f's."{c.upper()}"' for c in columns)
    return (f"MERGE INTO {t} t USING {s} s ON {on} WHEN MATCHED THEN UPDATE SET {upd} "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})")


def publish_snowflake(connector=None, write_pandas=None) -> dict[str, Any]:
    cfg = snowflake_config()
    if cfg is None and connector is None:
        return {"target": "snowflake", "skipped": True,
                "reason": "ATLAS_SNOWFLAKE_ACCOUNT/USER/PASSWORD not set (see snowflake/*.sql and docs)"}
    if connector is None:
        import snowflake.connector as connector  # type: ignore[no-redef]
    if write_pandas is None:
        from snowflake.connector.pandas_tools import write_pandas

    t0 = time.time()
    sf = connector.connect(**(cfg or {}))
    cur = sf.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS STAGING.PUBLISH_WATERMARKS (TABLE_NAME STRING, WATERMARK TIMESTAMP_NTZ)")
    marks = dict(cur.execute("SELECT TABLE_NAME, WATERMARK FROM STAGING.PUBLISH_WATERMARKS").fetchall())
    con = _gold()
    out: dict[str, Any] = {}
    for table, keys in GOLD_TABLES.items():
        wm_col = WATERMARK_COL.get(table)
        where = f"WHERE {wm_col} > TIMESTAMP '{marks[table.upper()]}'" if (keys and wm_col and table.upper() in marks) else ""
        df = con.execute(f"SELECT * FROM gold_db.gold.{table} {where}").df()
        df.columns = [c.upper() for c in df.columns]
        if keys is None:
            cur.execute("BEGIN")
            cur.execute(f"TRUNCATE TABLE GOLD.{table.upper()}")
            write_pandas(sf, df, table.upper(), schema="GOLD", quote_identifiers=True)
            cur.execute("COMMIT")
            out[table] = {"mode": "full_replace", "rows": len(df)}
            continue
        if df.empty:
            out[table] = {"mode": "incremental", "rows": 0}
            continue
        cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE STAGING.{table.upper()}_LOAD LIKE GOLD.{table.upper()}")
        write_pandas(sf, df, f"{table.upper()}_LOAD", schema="STAGING", quote_identifiers=True)
        cur.execute("BEGIN")
        cur.execute(merge_sql(table, keys, [c.lower() for c in df.columns]))
        new_mark = df[wm_col.upper()].max()
        cur.execute("DELETE FROM STAGING.PUBLISH_WATERMARKS WHERE TABLE_NAME = %s", (table.upper(),))
        cur.execute("INSERT INTO STAGING.PUBLISH_WATERMARKS VALUES (%s, %s)", (table.upper(), str(new_mark)))
        cur.execute("COMMIT")
        out[table] = {"mode": "incremental_merge", "rows": len(df), "watermark": str(new_mark)}
    con.close()
    sf.close()
    return {"target": "snowflake", "tables": out, "runtime_s": round(time.time() - t0, 3)}


def publish(target: str = "all") -> dict[str, Any]:
    from lineage.openlineage_emitter import emit_publish

    targets = ["postgres", "iceberg", "snowflake"] if target == "all" else [target]
    results = {}
    for t in targets:
        fn = {"postgres": publish_postgres, "iceberg": publish_iceberg, "snowflake": publish_snowflake}[t]
        results[t] = fn()
    done = [t for t, r in results.items() if not r.get("skipped")]
    emit_publish(done)
    return results
