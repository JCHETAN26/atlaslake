with changes as (
    select *, row_number() over (partition by product_id, event_timestamp order by event_id desc) as rn
    from {{ ref('stg_product_changes') }}
),
hashed as (
    select *, md5(concat_ws('|', name, category, cast(unit_price_minor as varchar), cast(active as varchar))) as attr_hash
    from changes where rn = 1
),
marked as (
    select *, lag(attr_hash) over (partition by product_id order by event_timestamp) as prev_hash from hashed
)
select
    product_id, sku, name, category, unit_price_minor, currency, active,
    event_timestamp as valid_from,
    lead(event_timestamp) over (partition by product_id order by event_timestamp) as valid_to,
    row_number() over (partition by product_id order by event_timestamp)         as version_number,
    event_id as source_event_id
from marked
where prev_hash is null or prev_hash <> attr_hash
