"""Declarative data-quality suites (Phase 22), executed with Great Expectations.

Each suite = (layer, name, SQL that produces the batch, expectations). Batches are pulled with DuckDB
straight from Iceberg (Silver/Bronze/ops) or the Gold DuckDB file, so row-level checks see exactly
what downstream consumers read. Aggregate/business checks are expressed as SQL that returns one
metrics row, validated with range expectations.

Severity: ``blocking`` failures fail the pipeline (Airflow stops before publish); ``warning`` failures
are reported and alerted but do not block.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from contracts import events as ev


@dataclass(frozen=True)
class Check:
    expectation: str  # great_expectations.expectations class name
    kwargs: dict[str, Any]
    severity: str = "blocking"
    category: str = "technical"  # technical | business
    description: str = ""


@dataclass(frozen=True)
class Suite:
    layer: str
    name: str
    sql: str
    checks: list[Check] = field(default_factory=list)


def _c(exp: str, severity: str = "blocking", category: str = "technical", description: str = "", **kw) -> Check:
    return Check(exp, kw, severity, category, description)


SUITES: list[Suite] = [
    # ------------------------------------------------------------------ bronze
    Suite("bronze", "bronze_ingestion", """
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE decode_error IS NOT NULL) * 1.0 / greatest(count(*), 1) AS corrupt_rate,
               count(*) FILTER (WHERE payload IS NULL) AS null_payloads,
               count(*) - count(DISTINCT (topic, kafka_partition, kafka_offset)) AS duplicate_offsets
        FROM (SELECT * FROM bronze_all)""", [
        _c("ExpectColumnValuesToBeBetween", column="null_payloads", min_value=0, max_value=0,
           description="raw bytes are always retained"),
        _c("ExpectColumnValuesToBeBetween", column="duplicate_offsets", min_value=0, max_value=0,
           description="each Kafka offset lands in Bronze exactly once (exactly-once sink)"),
        _c("ExpectColumnValuesToBeBetween", column="corrupt_rate", min_value=0, max_value=0.01,
           severity="warning", description="undecodable deliveries stay below 1%"),
    ]),
    # ------------------------------------------------------------------ silver (technical)
    Suite("silver", "silver_orders", """
        SELECT order_id, customer_id, store_id, status, amount_minor, currency, sales_channel, order_timestamp
        FROM silver.orders WHERE order_timestamp IS NOT NULL""", [
        _c("ExpectColumnValuesToBeUnique", column="order_id"),
        _c("ExpectColumnValuesToNotBeNull", column="order_id"),
        _c("ExpectColumnValuesToNotBeNull", column="customer_id"),
        _c("ExpectColumnValuesToNotBeNull", column="store_id"),
        _c("ExpectColumnValuesToBeBetween", column="amount_minor", min_value=0),
        _c("ExpectColumnValuesToBeInSet", column="status", value_set=list(ev.ORDER_STATUSES)),
        _c("ExpectColumnValuesToBeInSet", column="currency", value_set=list(ev.CURRENCIES)),
        _c("ExpectColumnValuesToNotBeNull", column="sales_channel", mostly=0.95, severity="warning",
           description="null threshold: v1 producers (no sales_channel) should be < 5% of orders"),
    ]),
    Suite("silver", "silver_order_items", """
        SELECT order_id, line_number, product_id, quantity, unit_price_minor, line_amount_minor FROM silver.order_items""", [
        _c("ExpectCompoundColumnsToBeUnique", column_list=["order_id", "line_number"]),
        _c("ExpectColumnValuesToBeBetween", column="quantity", min_value=1),
        _c("ExpectColumnValuesToBeBetween", column="unit_price_minor", min_value=0),
        _c("ExpectColumnValuesToNotBeNull", column="product_id"),
    ]),
    Suite("silver", "silver_payments", """
        SELECT payment_id, order_id, status, amount_minor, currency, attempt FROM silver.payments""", [
        _c("ExpectColumnValuesToBeUnique", column="payment_id"),
        _c("ExpectColumnValuesToNotBeNull", column="order_id"),
        _c("ExpectColumnValuesToBeInSet", column="status", value_set=list(ev.PAYMENT_STATUSES)),
        _c("ExpectColumnValuesToBeBetween", column="amount_minor", min_value=0),
        _c("ExpectColumnValuesToBeInSet", column="currency", value_set=list(ev.CURRENCIES)),
        _c("ExpectColumnValuesToBeBetween", column="attempt", min_value=1, max_value=5),
    ]),
    Suite("silver", "silver_customers", """
        SELECT customer_id, email, country, segment FROM silver.customers""", [
        _c("ExpectColumnValuesToBeUnique", column="customer_id"),
        _c("ExpectColumnValuesToNotBeNull", column="email"),
        _c("ExpectColumnValuesToMatchRegex", column="email", regex=r"^[^@\s]+@[^@\s]+$"),
        _c("ExpectColumnValuesToNotBeNull", column="country"),
        _c("ExpectColumnValuesToBeInSet", column="segment", value_set=list(ev.CUSTOMER_SEGMENTS)),
    ]),
    Suite("silver", "silver_referential_integrity", """
        SELECT
          (SELECT count(*) FROM silver.order_items i ANTI JOIN silver.orders o USING (order_id)) AS orphan_items,
          (SELECT count(*) FROM silver.payments p ANTI JOIN silver.orders o USING (order_id)) AS orphan_payments,
          (SELECT count(*) FROM silver.orders o ANTI JOIN silver.customers c USING (customer_id)
            WHERE o.customer_id IS NOT NULL) AS orders_unknown_customer,
          (SELECT count(*) FROM silver.order_items i ANTI JOIN silver.products p USING (product_id)) AS items_unknown_product,
          (SELECT count(*) FROM silver.orders o ANTI JOIN silver.stores s USING (store_id)
            WHERE o.store_id IS NOT NULL) AS orders_unknown_store""", [
        _c("ExpectColumnValuesToBeBetween", column="orphan_items", min_value=0, max_value=0),
        _c("ExpectColumnValuesToBeBetween", column="orphan_payments", min_value=0, max_value=0),
        _c("ExpectColumnValuesToBeBetween", column="orders_unknown_customer", min_value=0, max_value=0),
        _c("ExpectColumnValuesToBeBetween", column="items_unknown_product", min_value=0, max_value=0),
        _c("ExpectColumnValuesToBeBetween", column="orders_unknown_store", min_value=0, max_value=0,
           severity="warning", description="stores are batch-extracted; a just-opened store may lag a run"),
    ]),
    Suite("silver", "silver_inventory", """
        SELECT inventory_key, quantity_on_hand, quantity_reserved,
               quantity_on_hand - quantity_reserved AS available FROM silver.inventory""", [
        _c("ExpectColumnValuesToBeUnique", column="inventory_key"),
        _c("ExpectColumnValuesToBeBetween", column="quantity_reserved", min_value=0,
           description="reservations can never go negative (dup/lost deltas would show here)"),
        _c("ExpectColumnValuesToBeBetween", column="available", min_value=0, category="business",
           description="never oversold"),
    ]),
    # ------------------------------------------------------------------ silver (business)
    Suite("silver", "business_kpis", """
        SELECT
          count(*) FILTER (WHERE status = 'COMPLETED') * 1.0
            / nullif(count(*) FILTER (WHERE status IN ('COMPLETED', 'FAILED')), 0) AS payment_success_rate,
          (SELECT count(*) FILTER (WHERE status = 'CANCELLED') * 1.0 / nullif(count(*), 0)
             FROM silver.orders WHERE order_timestamp IS NOT NULL) AS cancellation_rate,
          (SELECT avg(amount_minor) FROM silver.orders WHERE order_timestamp IS NOT NULL) AS avg_order_value_minor
        FROM silver.payments""", [
        _c("ExpectColumnValuesToBeBetween", column="payment_success_rate", min_value=0.80, max_value=0.995,
           category="business", description="payment success rate within its historical band"),
        _c("ExpectColumnValuesToBeBetween", column="cancellation_rate", min_value=0.0, max_value=0.20,
           category="business"),
        _c("ExpectColumnValuesToBeBetween", column="avg_order_value_minor", min_value=100, max_value=100_000,
           category="business", severity="warning"),
    ]),
    Suite("silver", "daily_volume_anomalies", """
        WITH d AS (SELECT cast(order_timestamp AS date) AS day, count(*) AS n FROM silver.orders
                   WHERE order_timestamp IS NOT NULL GROUP BY 1),
             s AS (SELECT avg(n) AS mu, stddev_pop(n) AS sd FROM d)
        SELECT day, n, abs(n - mu) / nullif(sd, 0) AS zscore FROM d, s""", [
        _c("ExpectColumnValuesToBeBetween", column="zscore", max_value=4.0, mostly=0.99, severity="warning",
           category="business", description="expected volume: no day > 4σ from the mean"),
        _c("ExpectColumnValuesToBeBetween", column="n", min_value=1, category="business",
           description="every day in range has orders"),
    ]),
    Suite("silver", "freshness_and_dlq", """
        SELECT
          date_diff('second', (SELECT max(silver_updated_at) FROM silver.orders), now()::timestamp) AS silver_orders_age_s,
          (SELECT coalesce(sum(rejected), 0) * 1.0 / greatest(coalesce(sum(received), 0), 1) FROM ops.stream_batches) AS dlq_rate""", [
        _c("ExpectColumnValuesToBeBetween", column="silver_orders_age_s", max_value=24 * 3600, severity="warning",
           description="Silver processed within the last day"),
        _c("ExpectColumnValuesToBeBetween", column="dlq_rate", max_value=0.02, severity="warning",
           description="DLQ rate below 2% of received deliveries"),
    ]),
    # ------------------------------------------------------------------ gold
    Suite("gold", "gold_fact_orders", """
        SELECT order_id, customer_sk, store_sk, date_key, amount_minor, is_revenue FROM gold_db.gold.fact_orders""", [
        _c("ExpectColumnValuesToBeUnique", column="order_id"),
        _c("ExpectColumnValuesToNotBeInSet", column="customer_sk", value_set=["-1"], mostly=0.999,
           description="SCD2 point-in-time join resolves ≥ 99.9% of orders"),
        _c("ExpectColumnValuesToNotBeInSet", column="store_sk", value_set=["-1"], mostly=0.999),
        _c("ExpectColumnValuesToBeBetween", column="amount_minor", min_value=0),
    ]),
    Suite("gold", "gold_marts", """
        SELECT order_date, store_id, currency, orders_placed, orders_paid, gross_revenue_minor
        FROM gold_db.gold.mart_daily_sales""", [
        _c("ExpectCompoundColumnsToBeUnique", column_list=["order_date", "store_id", "currency"]),
        _c("ExpectColumnValuesToBeBetween", column="gross_revenue_minor", min_value=0),
        _c("ExpectColumnPairValuesAToBeGreaterThanB", column_A="orders_placed", column_B="orders_paid", or_equal=True,
           category="business", description="cannot pay more orders than were placed"),
    ]),
]
