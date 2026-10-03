-- Grain: store × product (current stock position). Small: rebuilt each run.
select
    i.store_id, i.product_id,
    coalesce(s.store_sk, '-1') as store_sk,
    coalesce(p.product_sk, '-1') as product_sk,
    p.product_name, p.category,
    i.quantity_on_hand, i.quantity_reserved, i.quantity_available,
    i.quantity_available < {{ var('low_stock_threshold') }} as is_low_stock,
    i.movement_count, i.updated_at as last_movement_at
from {{ ref('stg_inventory') }} i
left join {{ ref('dim_store') }} s on s.store_id = i.store_id
left join {{ ref('dim_product') }} p on p.product_id = i.product_id and p.is_current
