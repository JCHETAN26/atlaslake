select event_id, product_id, event_type, sku, name, category, unit_price_minor, currency, active,
       event_timestamp, silver_loaded_at
from {{ source('silver', 'product_changes') }}
