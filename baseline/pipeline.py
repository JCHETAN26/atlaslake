"""Phase 4 — the deliberately naive baseline pipeline.

    PostgreSQL ──COPY──► CSV ──► Parquet ──pandas──► marts ──COPY──► PostgreSQL (analytics.baseline)

Full refresh every run, single process, everything in memory. It is kept forever as the yardstick
AtlasLake is measured against (Phase 42), so it computes exactly the same business definitions as
the Gold layer (see docs/METRICS.md) and writes tables with the same names and columns.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from core.config import settings
from simulator.postgres_sink import connect

SOURCE_TABLES = ("customers", "products", "stores", "orders", "order_items", "payments", "inventory")
LOW_STOCK_THRESHOLD = 25


@dataclass
class StageTimer:
    timings: dict[str, float] = field(default_factory=dict)

    def run(self, name: str, fn, *args, **kwargs):
        t0 = time.perf_counter()
        out = fn(*args, **kwargs)
        self.timings[name] = round(time.perf_counter() - t0, 3)
        return out


def extract(out_dir: Path) -> dict[str, dict]:
    """Full extract of every source table to CSV, then CSV → Parquet."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stats: dict[str, dict] = {}
    with connect() as conn, conn.cursor() as cur:
        for table in SOURCE_TABLES:
            csv_path = out_dir / f"{table}.csv"
            with csv_path.open("wb") as fh, cur.copy(
                f"COPY (SELECT * FROM commerce.{table}) TO STDOUT WITH (FORMAT csv, HEADER)"
            ) as cp:
                for chunk in cp:
                    fh.write(chunk)
            tbl = pacsv.read_csv(csv_path)
            pq.write_table(tbl, out_dir / f"{table}.parquet")
            stats[table] = {"rows": tbl.num_rows, "csv_bytes": csv_path.stat().st_size,
                            "parquet_bytes": (out_dir / f"{table}.parquet").stat().st_size}
    return stats


def _date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True).dt.date


def transform(src_dir: Path) -> dict[str, pd.DataFrame]:
    r = {t: pd.read_parquet(src_dir / f"{t}.parquet") for t in SOURCE_TABLES}
    orders, items, payments = r["orders"], r["order_items"], r["payments"]

    fact_orders = pd.DataFrame({
        "order_id": orders["order_id"],
        "customer_id": orders["customer_id"],
        "store_id": orders["store_id"],
        "order_ts": pd.to_datetime(orders["created_at"], utc=True),
        "order_date": _date(orders["created_at"]),
        "status": orders["status"],
        "sales_channel": orders["sales_channel"],
        "amount_minor": orders["amount_minor"].astype("int64"),
        "currency": orders["currency"],
        "is_paid": orders["paid_at"].notna(),
        "is_cancelled": orders["status"].eq("CANCELLED"),
        "is_shipped": orders["shipped_at"].notna(),
        "is_delivered": orders["delivered_at"].notna(),
    })
    item_counts = items.groupby("order_id")["quantity"].sum().rename("item_count")
    fact_orders = fact_orders.merge(item_counts, on="order_id", how="left")
    fact_orders["item_count"] = fact_orders["item_count"].fillna(0).astype("int64")
    fact_orders["is_revenue"] = fact_orders["is_paid"] & ~fact_orders["is_cancelled"]

    fi = items.merge(fact_orders[["order_id", "order_date", "currency", "is_revenue"]], on="order_id")
    fact_order_items = pd.DataFrame({
        "order_id": fi["order_id"], "line_number": fi["line_number"], "product_id": fi["product_id"],
        "order_date": fi["order_date"], "quantity": fi["quantity"],
        "unit_price_minor": fi["unit_price_minor"],
        "line_amount_minor": fi["quantity"].astype("int64") * fi["unit_price_minor"].astype("int64"),
        "currency": fi["currency"], "is_revenue": fi["is_revenue"],
    })

    rev = fact_orders[fact_orders["is_revenue"]]
    daily = fact_orders.groupby(["order_date", "store_id", "currency"], as_index=False).agg(
        orders_placed=("order_id", "count"))
    daily_rev = rev.groupby(["order_date", "store_id", "currency"], as_index=False).agg(
        orders_paid=("order_id", "count"), gross_revenue_minor=("amount_minor", "sum"),
        items_sold=("item_count", "sum"))
    mart_daily_sales = daily.merge(daily_rev, on=["order_date", "store_id", "currency"], how="left").fillna(
        {"orders_paid": 0, "gross_revenue_minor": 0, "items_sold": 0})
    for c in ("orders_paid", "gross_revenue_minor", "items_sold"):
        mart_daily_sales[c] = mart_daily_sales[c].astype("int64")
    mart_daily_sales["avg_order_value_minor"] = (
        mart_daily_sales["gross_revenue_minor"] / mart_daily_sales["orders_paid"].where(mart_daily_sales["orders_paid"] > 0)
    ).round(2)

    mart_order_funnel = fact_orders.groupby("order_date", as_index=False).agg(
        orders_created=("order_id", "count"), orders_paid=("is_paid", "sum"),
        orders_shipped=("is_shipped", "sum"), orders_delivered=("is_delivered", "sum"),
        orders_cancelled=("is_cancelled", "sum"))

    pay = payments.assign(payment_date=_date(payments["created_at"]))
    mart_payment_metrics = pay.groupby(["payment_date", "method", "currency"], as_index=False).agg(
        attempts=("payment_id", "count"),
        completed=("status", lambda s: int((s == "COMPLETED").sum())),
        failed=("status", lambda s: int((s == "FAILED").sum())),
        completed_amount_minor=("amount_minor", lambda s: int(s[pay.loc[s.index, "status"] == "COMPLETED"].sum())),
    )
    mart_payment_metrics["success_rate"] = (mart_payment_metrics["completed"] / mart_payment_metrics["attempts"]).round(4)

    inv = r["inventory"]
    mart_inventory_health = pd.DataFrame({
        "store_id": inv["store_id"], "product_id": inv["product_id"],
        "quantity_on_hand": inv["quantity_on_hand"], "quantity_reserved": inv["quantity_reserved"],
        "quantity_available": inv["quantity_on_hand"] - inv["quantity_reserved"],
    })
    mart_inventory_health["is_low_stock"] = mart_inventory_health["quantity_available"] < LOW_STOCK_THRESHOLD

    cust = r["customers"]
    dim_customer = cust.drop(columns=["email", "phone", "first_name", "last_name"])  # no PII in analytics
    return {
        "dim_customer": dim_customer,
        "dim_product": r["products"],
        "dim_store": r["stores"],
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "mart_daily_sales": mart_daily_sales,
        "mart_order_funnel": mart_order_funnel,
        "mart_payment_metrics": mart_payment_metrics,
        "mart_inventory_health": mart_inventory_health,
    }


_PG_TYPES = {"int64": "bigint", "Int64": "bigint", "int32": "int", "float64": "double precision",
             "bool": "boolean", "boolean": "boolean"}


def _pg_type(series: pd.Series) -> str:
    dt = str(series.dtype)
    if dt in _PG_TYPES:
        return _PG_TYPES[dt]
    if dt.startswith("datetime64"):
        return "timestamptz"
    if series.name and str(series.name).endswith("_date"):
        return "date"
    return "text"


def load(tables: dict[str, pd.DataFrame], schema: str = "baseline") -> dict[str, int]:
    """Full refresh: drop + recreate + COPY every table, in one transaction."""
    rows = {}
    with connect(settings().pg.analytics_db) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for name, df in tables.items():
            cols = ", ".join(f'"{c}" {_pg_type(df[c])}' for c in df.columns)
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{name}")
            cur.execute(f"CREATE TABLE {schema}.{name} ({cols})")
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, na_rep="\\N")
            with cur.copy(f"COPY {schema}.{name} FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
                cp.write(buf.getvalue())
            rows[name] = len(df)
        for idx in ("fact_orders(order_date)", "fact_order_items(order_date)", "mart_daily_sales(order_date)",
                    "fact_orders(customer_id)"):
            cur.execute(f"CREATE INDEX ON {schema}.{idx}")
        cur.execute(f"ANALYZE {schema}.fact_orders")
        cur.execute(f"ANALYZE {schema}.fact_order_items")
        conn.commit()
    return rows


def run(work_dir: Path | None = None) -> dict:
    """Run the full baseline pipeline once and return measurements."""
    work_dir = work_dir or settings().data_dir / "baseline"
    timer = StageTimer()
    started = time.time()
    with connect() as conn:
        source_rows = {t: conn.execute(f"SELECT count(*) FROM commerce.{t}").fetchone()[0] for t in SOURCE_TABLES}
        outbox_events = conn.execute("SELECT count(*) FROM commerce.outbox").fetchone()[0]
    extract_stats = timer.run("extract_s", extract, work_dir / "extract")
    tables = timer.run("transform_s", transform, work_dir / "extract")
    loaded = timer.run("load_s", load, tables)
    total = round(sum(timer.timings.values()), 3)
    return {
        "source_events": outbox_events,
        "source_rows": source_rows,
        "rows_processed": sum(source_rows.values()),
        "stage_runtime_s": timer.timings,
        "total_runtime_s": total,
        # Full refresh: the newest source row is as old as the extract start by the time it lands.
        "staleness_at_completion_s": round(time.time() - started, 3),
        "storage_bytes": {
            "csv": sum(s["csv_bytes"] for s in extract_stats.values()),
            "parquet": sum(s["parquet_bytes"] for s in extract_stats.values()),
        },
        "rows_loaded": loaded,
        "peak_rss_mb": _peak_rss_mb(),
    }


def _peak_rss_mb() -> float:
    import resource

    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def arrow_bytes(tables: dict[str, pd.DataFrame]) -> int:
    return sum(pa.Table.from_pandas(df).nbytes for df in tables.values())
