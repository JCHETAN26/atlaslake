select payment_id, order_id, attempt, status, method, amount_minor, currency, failure_reason,
       started_at, completed_at, failed_at,
       coalesce(started_at, completed_at, failed_at)            as payment_timestamp,
       cast(coalesce(started_at, completed_at, failed_at) as date) as payment_date,
       updated_at, silver_updated_at
from {{ source('silver', 'payments') }}
