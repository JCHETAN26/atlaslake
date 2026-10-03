select inventory_key, store_id, product_id, quantity_on_hand, quantity_reserved,
       quantity_on_hand - quantity_reserved as quantity_available, movement_count, updated_at, silver_updated_at
from {{ source('silver', 'inventory') }}
