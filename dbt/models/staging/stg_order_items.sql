select order_id, line_number, product_id, quantity, unit_price_minor, line_amount_minor, currency,
       order_timestamp, cast(order_timestamp as date) as order_date, silver_loaded_at
from {{ source('silver', 'order_items') }}
