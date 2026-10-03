-- PII is dropped here: only a masked email leaves the staging layer.
select event_id, customer_id, event_type, {{ mask_email('email') }} as email_masked,
       city, state, country, segment, signup_timestamp, event_timestamp, silver_loaded_at
from {{ source('silver', 'customer_changes') }}
