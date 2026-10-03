-- Orders whose ORDER_CREATED has not arrived yet (out-of-order delivery) are incomplete and are held
-- back from Gold until it does; reconciliation reports them as `pending_incomplete`.
select
    order_id, customer_id, store_id, order_timestamp,
    cast(order_timestamp as date)              as order_date,
    status, amount_minor, currency, sales_channel, item_count, line_count,
    paid_at, cancelled_at, cancel_reason, shipped_at, carrier, delivered_at,
    updated_at, silver_updated_at
from {{ source('silver', 'orders') }}
where order_timestamp is not null
