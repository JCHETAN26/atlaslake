-- Accumulating fact. Grain: ONE ROW PER PAYMENT ATTEMPT.
{{ config(materialized='incremental', unique_key='payment_id', incremental_strategy='delete+insert') }}

select
    p.payment_id, p.order_id, p.attempt, p.status, p.method, p.amount_minor, p.currency, p.failure_reason,
    cast(strftime(p.payment_date, '%Y%m%d') as integer) as date_key,
    p.payment_date, p.payment_timestamp, p.started_at, p.completed_at, p.failed_at,
    date_diff('millisecond', p.started_at, coalesce(p.completed_at, p.failed_at)) / 1000.0 as seconds_to_outcome,
    p.silver_updated_at,
    cast('{{ run_started_at }}' as timestamp) as dbt_loaded_at
from {{ ref('stg_payments') }} p
where {{ incremental_since('p.silver_updated_at') }}
