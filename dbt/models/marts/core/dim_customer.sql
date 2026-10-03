-- SCD Type 2 customer dimension. Grain: one row per customer per distinct attribute version.
-- Rebuilt in full each run: it is small, and a full rebuild makes late-arriving customer changes
-- (which can re-split history) always correct.
select
    {{ sk(['customer_id', 'valid_from']) }} as customer_sk,
    customer_id, email_masked, city, state, country, segment, signup_timestamp,
    valid_from, valid_to, valid_to is null as is_current, version_number
from {{ ref('int_customer_versions') }}
union all
select '-1', '-1', null, 'Unknown', null, 'Unknown', 'Unknown', null,
       timestamp '1900-01-01', null, true, 0
