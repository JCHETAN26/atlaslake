select event_id, inventory_key, store_id, product_id, reason, on_hand_delta, reserved_delta, order_id,
       event_timestamp, cast(event_timestamp as date) as movement_date, silver_loaded_at
from {{ source('silver', 'inventory_movements') }}
