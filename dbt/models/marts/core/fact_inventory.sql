-- Transaction fact. Grain: ONE ROW PER INVENTORY MOVEMENT (immutable).
{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='delete+insert') }}

select
    m.event_id, m.inventory_key,
    coalesce(s.store_sk, '-1')                                    as store_sk,
    coalesce(p.product_sk, '-1')                                  as product_sk,
    cast(strftime(m.movement_date, '%Y%m%d') as integer)          as date_key,
    m.store_id, m.product_id, m.reason, m.on_hand_delta, m.reserved_delta, m.order_id,
    m.event_timestamp, m.movement_date, m.silver_loaded_at,
    cast('{{ run_started_at }}' as timestamp)                     as dbt_loaded_at
from {{ ref('stg_inventory_movements') }} m
left join {{ ref('dim_store') }} s on s.store_id = m.store_id
left join {{ ref('dim_product') }} p
       on p.product_id = m.product_id
      and {{ as_of('p', 'm.event_timestamp') }}
where {{ incremental_since('m.silver_loaded_at') }}
