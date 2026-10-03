-- Transaction fact. Grain: ONE ROW PER ORDER LINE (immutable once created).
-- product_sk is the product version valid at order time (SCD2 point-in-time join).
{{ config(materialized='incremental', unique_key=['order_id', 'line_number'], incremental_strategy='delete+insert') }}

with items as (
    select * from {{ ref('stg_order_items') }} i
    where {{ incremental_since('i.silver_loaded_at') }}
)
select
    i.order_id, i.line_number,
    coalesce(p.product_sk, '-1')                                  as product_sk,
    coalesce(fo.customer_sk, '-1')                                as customer_sk,
    coalesce(fo.store_sk, '-1')                                   as store_sk,
    cast(strftime(i.order_date, '%Y%m%d') as integer)             as date_key,
    i.product_id, i.order_date, i.quantity, i.unit_price_minor, i.line_amount_minor, i.currency,
    i.silver_loaded_at,
    cast('{{ run_started_at }}' as timestamp)                     as dbt_loaded_at
from items i
left join {{ ref('dim_product') }} p
       on p.product_id = i.product_id
      and {{ as_of('p', 'i.order_timestamp') }}
left join {{ ref('fact_orders') }} fo on fo.order_id = i.order_id
