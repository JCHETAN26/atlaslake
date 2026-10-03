-- Grain: order_date × store × currency. Revenue = orders paid and not cancelled (docs/METRICS.md).
-- Incremental: only the order dates touched since the last run are recomputed (delete+insert).
{{ config(materialized='incremental', unique_key=['order_date', 'store_id', 'currency'],
          incremental_strategy='delete+insert') }}

with affected_dates as (
    select distinct order_date from {{ ref('fact_orders') }}
    {% if is_incremental() %}
    where dbt_loaded_at > (select coalesce(max(max_dbt_loaded_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)
select
    f.order_date, f.store_id, f.currency,
    count(*)                                                      as orders_placed,
    count(*) filter (where f.is_revenue)                          as orders_paid,
    coalesce(sum(f.amount_minor) filter (where f.is_revenue), 0)  as gross_revenue_minor,
    coalesce(sum(f.item_count) filter (where f.is_revenue), 0)    as items_sold,
    round(sum(f.amount_minor) filter (where f.is_revenue)
          / nullif(count(*) filter (where f.is_revenue), 0), 2)   as avg_order_value_minor,
    max(f.dbt_loaded_at)                                          as max_dbt_loaded_at
from {{ ref('fact_orders') }} f
where f.order_date in (select order_date from affected_dates)
group by 1, 2, 3
