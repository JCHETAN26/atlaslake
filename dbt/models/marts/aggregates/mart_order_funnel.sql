-- Grain: order_date. Lifecycle stage counts for orders created that day.
{{ config(materialized='incremental', unique_key='order_date', incremental_strategy='delete+insert') }}

with affected_dates as (
    select distinct order_date from {{ ref('fact_orders') }}
    {% if is_incremental() %}
    where dbt_loaded_at > (select coalesce(max(max_dbt_loaded_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)
select
    order_date,
    count(*)                                   as orders_created,
    count(*) filter (where is_paid)            as orders_paid,
    count(*) filter (where is_shipped)         as orders_shipped,
    count(*) filter (where is_delivered)       as orders_delivered,
    count(*) filter (where is_cancelled)       as orders_cancelled,
    round(count(*) filter (where is_paid) / count(*), 4)      as paid_rate,
    round(count(*) filter (where is_delivered) / count(*), 4) as delivered_rate,
    max(dbt_loaded_at)                         as max_dbt_loaded_at
from {{ ref('fact_orders') }}
where order_date in (select order_date from affected_dates)
group by 1
