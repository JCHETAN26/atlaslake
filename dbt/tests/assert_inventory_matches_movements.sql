-- Stock position == sum of every movement (no lost or double-counted deltas).
with m as (
    select store_id, product_id, sum(on_hand_delta) as on_hand, sum(reserved_delta) as reserved
    from {{ ref('fact_inventory') }} group by 1, 2
)
select h.store_id, h.product_id, h.quantity_on_hand, m.on_hand
from {{ ref('mart_inventory_health') }} h
join m using (store_id, product_id)
where h.quantity_on_hand <> m.on_hand or h.quantity_reserved <> m.reserved
