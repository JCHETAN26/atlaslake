-- Order total must equal the sum of its lines (catches partial item loads).
select f.order_id, f.amount_minor, sum(i.line_amount_minor) as lines
from {{ ref('fact_orders') }} f
join {{ ref('fact_order_items') }} i using (order_id)
group by 1, 2
having f.amount_minor <> sum(i.line_amount_minor)
