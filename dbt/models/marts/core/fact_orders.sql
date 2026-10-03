-- depends_on: {{ ref('stg_customer_changes') }}
-- Accumulating-snapshot fact. Grain: ONE ROW PER ORDER (current lifecycle state).
-- Incremental on silver processing time. Orders whose customer history changed since the last run
-- are also re-pointed, so late-arriving SCD2 changes never leave a stale customer_sk behind.
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert') }}

with changed as (
    select * from {{ ref('stg_orders') }} o
    where {{ incremental_since('o.silver_updated_at') }}
    {% if is_incremental() %}
       or o.customer_id in (
            select customer_id from {{ ref('stg_customer_changes') }}
            where silver_loaded_at > (select max(dbt_loaded_at) - interval {{ var('incremental_lookback_minutes') }} minute
                                     from {{ this }}))
    {% endif %}
)
select
    o.order_id,
    coalesce(c.customer_sk, '-1')                                   as customer_sk,
    coalesce(s.store_sk, '-1')                                      as store_sk,
    cast(strftime(o.order_date, '%Y%m%d') as integer)               as date_key,
    o.customer_id, o.store_id, o.order_date, o.order_timestamp,
    o.status, o.sales_channel, o.amount_minor, o.currency, o.item_count, o.line_count,
    o.paid_at is not null                                           as is_paid,
    o.status = 'CANCELLED'                                          as is_cancelled,
    o.shipped_at is not null                                        as is_shipped,
    o.delivered_at is not null                                      as is_delivered,
    o.paid_at is not null and o.status <> 'CANCELLED'               as is_revenue,
    o.paid_at, o.cancelled_at, o.cancel_reason, o.shipped_at, o.delivered_at,
    date_diff('second', o.order_timestamp, o.delivered_at) / 3600.0 as hours_to_deliver,
    o.silver_updated_at,
    cast('{{ run_started_at }}' as timestamp)                       as dbt_loaded_at
from changed o
left join {{ ref('dim_customer') }} c
       on c.customer_id = o.customer_id
      and o.order_timestamp >= c.valid_from
      and (o.order_timestamp < c.valid_to or c.valid_to is null)
left join {{ ref('dim_store') }} s on s.store_id = o.store_id
