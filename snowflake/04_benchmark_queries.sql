-- Benchmark query set — identical business questions to the Phase 4 baseline (Postgres) and the
-- DuckDB Gold layer, so p50/p95 are comparable across engines. Parameters: :d1, :d2 (dates), :d (date).
-- Disable the result cache when benchmarking, or every repetition after the first is free:
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
USE WAREHOUSE ATLAS_WH;
USE SCHEMA ATLASLAKE.GOLD;

-- Q1 revenue_by_day: revenue per day and currency over a date range
SELECT order_date, currency, SUM(gross_revenue_minor) AS revenue_minor
FROM mart_daily_sales
WHERE order_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Q2 top_products: top 20 products by line revenue in a date range
SELECT product_id, SUM(line_amount_minor) AS revenue_minor
FROM fact_order_items
WHERE order_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- Q3 customer_ltv: top 100 customers by lifetime revenue
SELECT customer_id, COUNT(*) AS orders, SUM(amount_minor) AS revenue_minor
FROM fact_orders
WHERE is_paid AND NOT is_cancelled
GROUP BY 1
ORDER BY 3 DESC
LIMIT 100;

-- Q4 single_day_orders: selective point lookup on one day
SELECT COUNT(*) AS orders, SUM(amount_minor) AS amount_minor
FROM fact_orders
WHERE order_date = '2026-01-15';

-- Q5 funnel_range: lifecycle funnel over a week
SELECT *
FROM mart_order_funnel
WHERE order_date BETWEEN '2026-01-08' AND '2026-01-14'
ORDER BY order_date;

-- Q6 store_channel_mix: orders per store and sales channel
SELECT store_id, sales_channel, COUNT(*) AS orders
FROM fact_orders
GROUP BY 1, 2
ORDER BY 1, 2;

-- Bytes / partitions scanned per query (Phase 42 metrics):
-- SELECT query_id, query_text, total_elapsed_time, bytes_scanned, partitions_scanned, partitions_total
-- FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) ORDER BY start_time DESC LIMIT 10;
