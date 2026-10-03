-- Type 1 store dimension. Grain: one row per store.
select {{ sk(['store_id']) }} as store_sk, store_id, store_name, city, country, currency, opened_at
from {{ ref('stg_stores') }}
union all
select '-1', '-1', 'Unknown', 'Unknown', 'Unknown', null, null
