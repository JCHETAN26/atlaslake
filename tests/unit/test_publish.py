"""Snowflake publish logic against a fake connector (no account needed)."""

from __future__ import annotations

import duckdb
import pytest

from ingestion.batch import publish as pub


def test_merge_sql_uses_grain_keys_and_updates_non_keys():
    sql = pub.merge_sql(
        "mart_daily_sales",
        ["order_date", "store_id", "currency"],
        ["order_date", "store_id", "currency", "orders_paid", "gross_revenue_minor"],
    )
    assert sql.startswith("MERGE INTO GOLD.MART_DAILY_SALES t USING STAGING.MART_DAILY_SALES_LOAD s")
    assert 't."ORDER_DATE" = s."ORDER_DATE" AND t."STORE_ID" = s."STORE_ID"' in sql
    assert 't."ORDERS_PAID" = s."ORDERS_PAID"' in sql
    assert 't."ORDER_DATE" = s."ORDER_DATE",' not in sql.split("UPDATE SET")[1]  # keys never updated


def test_every_gold_table_has_a_publish_mode():
    assert set(pub.GOLD_TABLES) >= {"fact_orders", "mart_daily_sales", "dim_customer"}
    for table, keys in pub.GOLD_TABLES.items():
        if keys:
            assert table in pub.WATERMARK_COL, table


def test_skips_without_credentials(monkeypatch):
    for k in ("ACCOUNT", "USER", "PASSWORD"):
        monkeypatch.delenv(f"ATLAS_SNOWFLAKE_{k}", raising=False)
    out = pub.publish_snowflake()
    assert out["skipped"] is True


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((sql, params))
        return self

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self):
        self.log: list = []

    def cursor(self):
        return FakeCursor(self.log)

    def close(self):
        pass


class FakeConnector:
    def __init__(self):
        self.conn = FakeConn()

    def connect(self, **_):
        return self.conn


@pytest.fixture
def tiny_gold(tmp_path, monkeypatch):
    path = tmp_path / "goldtest.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA gold")
    for t, keys in pub.GOLD_TABLES.items():
        wm = pub.WATERMARK_COL.get(t)
        cols = list(dict.fromkeys([*(keys or []), "k", *([wm] if wm else [])]))
        con.execute(
            f"CREATE TABLE gold.{t} ({', '.join(f'{c} timestamp' if c == wm else f'{c} varchar' for c in cols)})"
        )
        vals = ", ".join("TIMESTAMP '2026-01-01'" if c == wm else "'x'" for c in cols)
        con.execute(f"INSERT INTO gold.{t} VALUES ({vals})")
    con.close()
    from core import config

    monkeypatch.setattr(config, "settings", lambda: type("S", (), {"gold_duckdb": path})())
    monkeypatch.setattr(pub, "settings", lambda: type("S", (), {"gold_duckdb": path})())
    return path


def test_incremental_merge_flow_with_fake_connector(tiny_gold):
    fake, written = FakeConnector(), []
    out = pub.publish_snowflake(
        connector=fake,
        write_pandas=lambda conn, df, name, **kw: written.append((name, kw["schema"], len(df))),
    )
    modes = {t: v["mode"] for t, v in out["tables"].items()}
    assert modes["dim_customer"] == "full_replace"
    assert modes["fact_orders"] == "incremental_merge"
    sql = [s for s, _ in fake.conn.log]
    assert any(s.startswith("MERGE INTO GOLD.FACT_ORDERS") for s in sql)
    assert any(s.startswith("TRUNCATE TABLE GOLD.DIM_CUSTOMER") for s in sql)
    assert ("FACT_ORDERS_LOAD", "STAGING", 1) in written
    # watermark advanced inside the same transaction as the MERGE
    i_merge = next(i for i, s in enumerate(sql) if s.startswith("MERGE INTO GOLD.FACT_ORDERS"))
    assert (
        sql[i_merge + 2].startswith("INSERT INTO STAGING.PUBLISH_WATERMARKS") and sql[i_merge + 3] == "COMMIT"
    )
