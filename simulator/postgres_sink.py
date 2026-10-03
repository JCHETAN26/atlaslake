"""Apply simulator event batches to the operational Postgres source (entity tables + outbox).

Each batch is applied in ONE transaction: entity state and the outbox rows describing that change
commit atomically (transactional-outbox pattern), so Kafka can never see an event the database
does not have, and vice versa.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

from contracts import events as ev
from core.config import REPO_ROOT, settings

SQL_DIR = REPO_ROOT / "sql"


def connect(db: str | None = None, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(settings().pg.dsn(db), autocommit=autocommit)


def init_schema(conn: psycopg.Connection, path: Path = SQL_DIR / "source_schema.sql") -> None:
    conn.execute(path.read_text())
    conn.commit()


def reset_source(conn: psycopg.Connection) -> None:
    conn.execute(
        "TRUNCATE commerce.outbox, commerce.order_items, commerce.payments, commerce.inventory, "
        "commerce.orders, commerce.customers, commerce.products, commerce.stores, "
        "ops.delivery_log, ops.relay_checkpoint, ops.simulator_runs, ops.batch_watermarks RESTART IDENTITY CASCADE"
    )
    conn.commit()


def _ts(us: pd.Series | np.ndarray) -> pd.Series:
    return pd.to_datetime(pd.Series(us), unit="us", utc=True)


def _copy_df(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    cols = ", ".join(df.columns)
    with cur.copy(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        cp.write(buf.getvalue())


def _last_per_key(df: pd.DataFrame, key: str) -> pd.DataFrame:
    return df.sort_values(["event_timestamp", "event_id"], kind="stable").drop_duplicates(key, keep="last")


def outbox_rows(batch: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Flatten a batch into outbox rows (event-time order => commit order)."""
    parts = []
    for topic, df in batch.items():
        if df.empty:
            continue
        fields = [f for f in ev.PAYLOAD_FIELDS[topic] if f in df.columns]
        payload = df[fields].to_json(orient="records", lines=True, force_ascii=False).splitlines()
        parts.append(
            pd.DataFrame(
                {
                    "event_id": df["event_id"].to_numpy(),
                    "event_type": df["event_type"].to_numpy(),
                    "topic": topic,
                    "partition_key": df[ev.PARTITION_KEY[topic]].astype(str).to_numpy(),
                    "event_timestamp": df["event_timestamp"].to_numpy(),
                    "payload": payload,
                }
            )
        )
    if not parts:
        return pd.DataFrame(
            columns=["event_id", "event_type", "topic", "partition_key", "event_timestamp", "payload"]
        )
    out = pd.concat(parts, ignore_index=True).sort_values(["event_timestamp", "event_id"], kind="stable")
    out["event_timestamp"] = _ts(out["event_timestamp"].to_numpy()).to_numpy()
    return out


def reduce_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Batch-local reduction of order events to one state row per order (last-writer-wins)."""
    if orders.empty:
        return pd.DataFrame()
    o = orders.copy()
    o["rank"] = o["status"].map(ev.ORDER_STATUS_RANK)
    o = o.sort_values(["event_timestamp", "rank", "event_id"], kind="stable")
    latest = o.drop_duplicates("order_id", keep="last").set_index("order_id")
    ts = o.pivot_table(index="order_id", columns="event_type", values="event_timestamp", aggfunc="min")
    created = o[o["event_type"] == "ORDER_CREATED"].set_index("order_id")
    cancel = o[o["event_type"] == "ORDER_CANCELLED"].drop_duplicates("order_id").set_index("order_id")
    out = pd.DataFrame(index=latest.index)
    out["status"] = latest["status"]
    out["updated_at"] = latest["event_timestamp"]
    for col in ("customer_id", "store_id", "sales_channel", "amount_minor", "currency"):
        out[col] = created[col].reindex(out.index)
    out["created_at"] = created["event_timestamp"].reindex(out.index)
    for et, col in (
        ("ORDER_UPDATED", "paid_at"),
        ("ORDER_CANCELLED", "cancelled_at"),
        ("ORDER_SHIPPED", "shipped_at"),
        ("ORDER_DELIVERED", "delivered_at"),
    ):
        out[col] = ts[et].reindex(out.index) if et in ts.columns else np.nan
    out["cancel_reason"] = cancel["cancel_reason"].reindex(out.index) if not cancel.empty else None
    return out.reset_index()


def apply_batch(conn: psycopg.Connection, batch: dict[str, pd.DataFrame]) -> int:
    """Apply one time-ordered batch atomically. Returns number of events written to the outbox."""
    with conn.cursor() as cur:
        cust = batch.get(ev.TOPIC_CUSTOMERS, pd.DataFrame())
        if not cust.empty:
            c = _last_per_key(cust, "customer_id").copy()
            c["created_at"] = _ts(c["signup_timestamp"].to_numpy()).to_numpy()
            c["updated_at"] = _ts(c["event_timestamp"].to_numpy()).to_numpy()
            cols = [
                "customer_id",
                "email",
                "phone",
                "first_name",
                "last_name",
                "city",
                "state",
                "country",
                "segment",
                "created_at",
                "updated_at",
            ]
            cur.execute("CREATE TEMP TABLE s_customers (LIKE commerce.customers) ON COMMIT DROP")
            _copy_df(cur, "s_customers", c[cols])
            cur.execute(f"""
                INSERT INTO commerce.customers SELECT * FROM s_customers
                ON CONFLICT (customer_id) DO UPDATE SET
                  {", ".join(f"{k} = excluded.{k}" for k in cols if k not in ("customer_id", "created_at"))}
                WHERE commerce.customers.updated_at <= excluded.updated_at""")

        prod = batch.get(ev.TOPIC_PRODUCTS, pd.DataFrame())
        if not prod.empty:
            p = _last_per_key(prod, "product_id").copy()
            p["created_at"] = _ts(p["event_timestamp"].to_numpy()).to_numpy()
            p["updated_at"] = p["created_at"]
            cols = [
                "product_id",
                "sku",
                "name",
                "category",
                "unit_price_minor",
                "currency",
                "active",
                "created_at",
                "updated_at",
            ]
            cur.execute("CREATE TEMP TABLE s_products (LIKE commerce.products) ON COMMIT DROP")
            _copy_df(cur, "s_products", p[cols])
            cur.execute("""
                INSERT INTO commerce.products SELECT * FROM s_products
                ON CONFLICT (product_id) DO UPDATE SET
                  name = excluded.name, category = excluded.category,
                  unit_price_minor = excluded.unit_price_minor, active = excluded.active,
                  updated_at = excluded.updated_at
                WHERE commerce.products.updated_at <= excluded.updated_at""")

        orders = batch.get(ev.TOPIC_ORDERS, pd.DataFrame())
        if not orders.empty:
            red = reduce_orders(orders)
            for col in ("updated_at", "created_at", "paid_at", "cancelled_at", "shipped_at", "delivered_at"):
                red[col] = pd.to_datetime(red[col], unit="us", utc=True)
            red["amount_minor"] = red["amount_minor"].astype("Int64")
            cols = [
                "order_id",
                "customer_id",
                "store_id",
                "status",
                "sales_channel",
                "amount_minor",
                "currency",
                "created_at",
                "updated_at",
                "paid_at",
                "cancelled_at",
                "cancel_reason",
                "shipped_at",
                "delivered_at",
            ]
            cur.execute("""CREATE TEMP TABLE s_orders (
                order_id text, customer_id text, store_id text, status text, sales_channel text,
                amount_minor bigint, currency text, created_at timestamptz, updated_at timestamptz,
                paid_at timestamptz, cancelled_at timestamptz, cancel_reason text,
                shipped_at timestamptz, delivered_at timestamptz) ON COMMIT DROP""")
            _copy_df(cur, "s_orders", red[cols])
            cur.execute("""
                INSERT INTO commerce.orders
                SELECT order_id, customer_id, store_id, status::commerce.order_status, sales_channel,
                       amount_minor, currency, created_at, updated_at, paid_at, cancelled_at,
                       cancel_reason, shipped_at, delivered_at
                FROM s_orders WHERE created_at IS NOT NULL""")
            cur.execute("""
                UPDATE commerce.orders o SET
                  status = CASE WHEN s.updated_at >= o.updated_at THEN s.status::commerce.order_status ELSE o.status END,
                  updated_at = greatest(o.updated_at, s.updated_at),
                  paid_at = coalesce(o.paid_at, s.paid_at),
                  cancelled_at = coalesce(o.cancelled_at, s.cancelled_at),
                  cancel_reason = coalesce(o.cancel_reason, s.cancel_reason),
                  shipped_at = coalesce(o.shipped_at, s.shipped_at),
                  delivered_at = coalesce(o.delivered_at, s.delivered_at)
                FROM s_orders s WHERE o.order_id = s.order_id AND s.created_at IS NULL""")
            created = orders[orders["event_type"] == "ORDER_CREATED"]
            if not created.empty:
                items = created[["order_id", "items"]].explode("items", ignore_index=True)
                items_df = pd.DataFrame(items["items"].tolist())
                items_df.insert(0, "order_id", items["order_id"].to_numpy())
                _copy_df(
                    cur,
                    "commerce.order_items",
                    items_df[["order_id", "line_number", "product_id", "quantity", "unit_price_minor"]],
                )

        pays = batch.get(ev.TOPIC_PAYMENTS, pd.DataFrame())
        if not pays.empty:
            started = pays[pays["event_type"] == "PAYMENT_STARTED"].drop_duplicates("payment_id")
            last = _last_per_key(pays, "payment_id").copy()
            last["created_at"] = last["payment_id"].map(started.set_index("payment_id")["event_timestamp"])
            last["created_at"] = last["created_at"].fillna(last["event_timestamp"])
            last["created_at"] = _ts(last["created_at"].astype("int64").to_numpy()).to_numpy()
            last["updated_at"] = _ts(last["event_timestamp"].to_numpy()).to_numpy()
            cols = [
                "payment_id",
                "order_id",
                "attempt",
                "status",
                "method",
                "amount_minor",
                "currency",
                "failure_reason",
                "created_at",
                "updated_at",
            ]
            cur.execute("""CREATE TEMP TABLE s_payments (payment_id text, order_id text, attempt int,
                status text, method text, amount_minor bigint, currency text, failure_reason text,
                created_at timestamptz, updated_at timestamptz) ON COMMIT DROP""")
            _copy_df(cur, "s_payments", last[cols])
            cur.execute("""
                INSERT INTO commerce.payments
                SELECT payment_id, order_id, attempt, status::commerce.payment_status, method, amount_minor,
                       currency, failure_reason, created_at, updated_at FROM s_payments
                ON CONFLICT (payment_id) DO UPDATE SET
                  status = excluded.status, failure_reason = excluded.failure_reason,
                  updated_at = excluded.updated_at
                WHERE commerce.payments.updated_at <= excluded.updated_at""")

        inv = batch.get(ev.TOPIC_INVENTORY, pd.DataFrame())
        if not inv.empty:
            agg = inv.groupby(["store_id", "product_id"], as_index=False).agg(
                quantity_on_hand=("on_hand_delta", "sum"),
                quantity_reserved=("reserved_delta", "sum"),
                updated_at=("event_timestamp", "max"),
            )
            agg["updated_at"] = _ts(agg["updated_at"].to_numpy()).to_numpy()
            cur.execute("CREATE TEMP TABLE s_inventory (LIKE commerce.inventory) ON COMMIT DROP")
            _copy_df(cur, "s_inventory", agg)
            cur.execute("""
                INSERT INTO commerce.inventory SELECT * FROM s_inventory
                ON CONFLICT (store_id, product_id) DO UPDATE SET
                  quantity_on_hand = commerce.inventory.quantity_on_hand + excluded.quantity_on_hand,
                  quantity_reserved = commerce.inventory.quantity_reserved + excluded.quantity_reserved,
                  updated_at = greatest(commerce.inventory.updated_at, excluded.updated_at)""")

        ob = outbox_rows(batch)
        _copy_df(
            cur,
            "commerce.outbox",
            ob[["event_id", "event_type", "topic", "partition_key", "event_timestamp", "payload"]],
        )
    conn.commit()
    return len(ob)


def load_stores(conn: psycopg.Connection, stores: pd.DataFrame) -> None:
    s = stores.copy()
    s["opened_at"] = _ts(s["opened_at_us"].to_numpy()).to_numpy()
    s["updated_at"] = s["opened_at"]
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE s_stores (LIKE commerce.stores) ON COMMIT DROP")
        _copy_df(
            cur, "s_stores", s[["store_id", "name", "city", "country", "currency", "opened_at", "updated_at"]]
        )
        cur.execute("INSERT INTO commerce.stores SELECT * FROM s_stores ON CONFLICT (store_id) DO NOTHING")
    conn.commit()


def record_run(
    conn: psycopg.Connection,
    run_id: str,
    mode: str,
    config: dict,
    stats: dict | None = None,
    finished: bool = False,
) -> None:
    conn.execute(
        """INSERT INTO ops.simulator_runs (run_id, mode, config, stats, finished_at)
           VALUES (%s, %s, %s, %s, CASE WHEN %s THEN now() END)
           ON CONFLICT (run_id) DO UPDATE SET stats = excluded.stats, finished_at = excluded.finished_at""",
        (run_id, mode, json.dumps(config), json.dumps(stats) if stats else None, finished),
    )
    conn.commit()


def apply_stream(conn: psycopg.Connection, batches: Iterable[dict[str, pd.DataFrame]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for batch in batches:
        apply_batch(conn, batch)
        for t, df in batch.items():
            totals[t] = totals.get(t, 0) + len(df)
    return totals
