-- depends_on: {{ ref('stg_customer_changes') }}
-- Accumulating-snapshot fact. Grain: ONE ROW PER ORDER (current lifecycle state).
-- Incremental on silver processing time. Orders whose customer history changed since the last run
-- are also re-pointed, so late-arriving SCD2 changes never leave a stale customer_sk behind.
-- The changed set is computed as a small key set first and semi-joined (an OR over a subquery in the
-- WHERE clause defeated DuckDB's filter pushdown and made incremental slower than a full refresh).
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert') }}

{% if is_incremental() %}
with since as (
    select max(silver_updated_at) - interval {{ var('incremental_lookback_minutes') }} minute as silver_mark,
           max(dbt_loaded_at) - interval {{ var('incremental_lookback_minutes') }} minute as dbt_mark
    from {{ this }}
),
moved_customers as (
    select distinct customer_id from {{ ref('stg_customer_changes') }}, since
    where silver_loaded_at > since.dbt_mark
),
changed_keys as (
    select order_id from {{ ref('stg_orders') }}, since where silver_updated_at > since.silver_mark
    union
    select o.order_id from {{ ref('stg_orders') }} o semi join moved_customers m on o.customer_id = m.customer_id
),
changed as (
    select o.* from {{ ref('stg_orders') }} o semi join changed_keys k on o.order_id = k.order_id
)
{% else %}
with changed as (select * from {{ ref('stg_orders') }})
{% endif %}
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
      and {{ as_of('c', 'o.order_timestamp') }}
left join {{ ref('dim_store') }} s on s.store_id = o.store_id
