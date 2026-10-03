# Business metric definitions

One definition per metric, implemented identically in the baseline (`baseline/pipeline.py`), the Gold
layer (`dbt/models/marts`), and the benchmark queries (`benchmarks/queries.py`). The query-performance
benchmark asserts that both engines return identical results for every query (`same_result`).

| Metric | Definition | Grain | Gold model |
|---|---|---|---|
| `orders_placed` | Orders whose `ORDER_CREATED` event time falls on the date (UTC) | order_date × store × currency | `mart_daily_sales` |
| `orders_paid` | Orders placed that day that were paid (`paid_at` set) **and not cancelled** | " | `mart_daily_sales` |
| `gross_revenue_minor` | Σ `amount_minor` of `orders_paid` orders, in minor currency units, never summed across currencies | " | `mart_daily_sales` |
| `items_sold` | Σ line quantities of `orders_paid` orders | " | `mart_daily_sales` |
| `avg_order_value_minor` | `gross_revenue_minor / orders_paid` | " | `mart_daily_sales` |
| Funnel counts | Orders created that day that reached each stage (paid, shipped, delivered) or were cancelled | order_date | `mart_order_funnel` |
| `success_rate` | `COMPLETED / attempts` of payment attempts started that day | payment_date × method × currency | `mart_payment_metrics` |
| `quantity_available` | `on_hand − reserved` (sum of all movements) | store × product | `mart_inventory_health` |
| `is_low_stock` | `quantity_available < 25` (`var('low_stock_threshold')`) | store × product | `mart_inventory_health` |

Rules that make these trustworthy:

* **Money** is integer minor units end to end (Avro `long`, Iceberg `bigint`, DuckDB `bigint`). No floats.
* **Dates** are UTC. Every process pins `TZ=UTC` (`core/__init__.py`) and every DuckDB session runs
  `SET TimeZone='UTC'`. Without this, `timestamptz → date` silently depends on the host's zone. That
  bug happened during development and was caught by `assert_gold_revenue_reconciles_to_silver`.
* **Late events change history.** A late payment updates the order date it belongs to (event time),
  not the date it arrived. The incremental marts recompute every date touched since the last run.

## Platform metrics (Phase 0 §6)

These are measured by `make benchmark` into `evaluation/`. Definitions are in `PROJECT_SPEC.md §6`.
