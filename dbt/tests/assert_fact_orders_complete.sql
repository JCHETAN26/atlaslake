-- Every complete Silver order is in fact_orders exactly once, and vice versa.
select 'missing_in_gold' as problem, o.order_id from {{ ref('stg_orders') }} o
left join {{ ref('fact_orders') }} f using (order_id) where f.order_id is null
union all
select 'extra_in_gold', f.order_id from {{ ref('fact_orders') }} f
left join {{ ref('stg_orders') }} o using (order_id) where o.order_id is null
