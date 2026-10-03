-- SCD2 versions from the full customer change history (event time = business validity).
-- Snapshot events that do not change a tracked attribute are collapsed into the previous version.
with changes as (
    select *, row_number() over (partition by customer_id, event_timestamp order by event_id desc) as rn
    from {{ ref('stg_customer_changes') }}
),
hashed as (
    select *,
        md5(concat_ws('|', city, coalesce(state, ''), country, segment, email_masked)) as attr_hash
    from changes where rn = 1
),
marked as (
    select *, lag(attr_hash) over (partition by customer_id order by event_timestamp) as prev_hash
    from hashed
)
select
    customer_id, email_masked, city, state, country, segment, signup_timestamp,
    event_timestamp as valid_from,
    lead(event_timestamp) over (partition by customer_id order by event_timestamp) as valid_to,
    row_number() over (partition by customer_id order by event_timestamp)         as version_number,
    event_id as source_event_id
from marked
where prev_hash is null or prev_hash <> attr_hash
