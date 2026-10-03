-- SCD Type 2 product dimension. Grain: one row per product per distinct (name, category, price, active).
select
    {{ sk(['product_id', 'valid_from']) }} as product_sk,
    product_id, sku, name as product_name, category, unit_price_minor as list_price_minor, currency,
    active, valid_from, valid_to, valid_to is null as is_current, version_number
from {{ ref('int_product_versions') }}
union all
select '-1', '-1', null, 'Unknown', 'Unknown', null, null, null, timestamp '1900-01-01', null, true, 0
