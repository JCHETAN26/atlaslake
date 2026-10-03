select store_id, name as store_name, city, country, currency, opened_at, updated_at, extracted_at
from {{ source('silver', 'stores') }}
