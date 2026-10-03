"""dbt-duckdb plugin + helpers: expose Silver/ops Iceberg tables to DuckDB as views.

Every connection resolves each table's *current* metadata file from the shared JDBC catalog and
creates ``silver.<table>`` / ``ops.<table>`` views over ``iceberg_scan(...)`` — so dbt always reads a
consistent, committed Iceberg snapshot, and nothing is copied out of the lake.
"""

from __future__ import annotations

import os
from typing import Any

import duckdb
from dbt.adapters.duckdb.plugins import BasePlugin

from core.config import settings

LAKE_TABLES = {
    "silver": [
        "orders",
        "order_items",
        "payments",
        "customers",
        "customer_changes",
        "products",
        "product_changes",
        "inventory",
        "inventory_movements",
        "stores",
        "events",
        "realtime_sales_1m",
    ],
    "ops": ["dlq", "stream_batches"],
    "bronze": ["orders", "payments", "inventory", "customers", "products"],
}


def session_settings(conn: duckdb.DuckDBPyConnection) -> None:
    """Settings every AtlasLake DuckDB session needs (UTC; offline extension directory if provided)."""
    conn.execute("SET TimeZone = 'UTC'")
    if os.environ.get("ATLAS_DUCKDB_EXTENSION_DIR"):
        conn.execute(f"SET extension_directory = '{os.environ['ATLAS_DUCKDB_EXTENSION_DIR']}'")


def configure(conn: duckdb.DuckDBPyConnection, tables: dict[str, list[str]] | None = None) -> dict[str, str]:
    """Load extensions, register the S3 secret, (re)create lake views. Returns {view: metadata_location}."""
    from lakehouse.catalog import catalog

    cfg = settings().s3
    session_settings(conn)  # UTC date bucketing; offline extension dir in containers
    conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    if cfg.access_key:
        conn.execute(f"""CREATE OR REPLACE SECRET atlas_s3 (TYPE s3, KEY_ID '{cfg.access_key}',
                         SECRET '{cfg.secret_key}', ENDPOINT '{cfg.endpoint_host}', URL_STYLE 'path',
                         USE_SSL {str(cfg.use_ssl).lower()}, REGION '{cfg.region}')""")
    else:  # AWS: IAM role credentials via the default chain
        conn.execute("INSTALL aws; LOAD aws;")
        conn.execute(
            f"CREATE OR REPLACE SECRET atlas_s3 (TYPE s3, PROVIDER credential_chain, REGION '{cfg.region}')"
        )
    cat = catalog()
    out = {}
    for ns, names in (tables or LAKE_TABLES).items():
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        for name in names:
            try:
                loc = cat.load_table(f"{ns}.{name}").metadata_location
            except Exception:  # table not created yet: skip (dbt sources will fail loudly if needed)
                continue
            conn.execute(f"CREATE OR REPLACE VIEW {ns}.{name} AS SELECT * FROM iceberg_scan('{loc}')")
            out[f"{ns}.{name}"] = loc
    return out


class Plugin(BasePlugin):
    def initialize(self, plugin_config: dict[str, Any]) -> None:
        self.tables = plugin_config.get("tables") or LAKE_TABLES

    def configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        try:
            configure(conn, self.tables)
        except Exception:
            # CI `dbt parse/compile` runs without a lake: ATLAS_LAKE_OPTIONAL=1 makes that non-fatal.
            if os.environ.get("ATLAS_LAKE_OPTIONAL") != "1":
                raise


def connect_gold(read_only: bool = True, with_lake: bool = True) -> duckdb.DuckDBPyConnection:
    """Connection to the Gold DuckDB file, optionally with lake views attached (in-memory catalog)."""
    path = settings().gold_duckdb
    if with_lake:
        conn = duckdb.connect()
        conn.execute(f"ATTACH '{path}' AS gold_db {'(READ_ONLY)' if read_only else ''}")
        configure(conn)
        conn.execute("USE gold_db")
        return conn
    return duckdb.connect(str(path), read_only=read_only)
