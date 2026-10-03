-- Grain: payment_date × method × currency.
{{ config(materialized='incremental', unique_key=['payment_date', 'method', 'currency'],
          incremental_strategy='delete+insert') }}

with affected_dates as (
    select distinct payment_date from {{ ref('fact_payments') }}
    {% if is_incremental() %}
    where dbt_loaded_at > (select coalesce(max(max_dbt_loaded_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)
select
    payment_date, method, currency,
    count(*)                                                        as attempts,
    count(*) filter (where status = 'COMPLETED')                    as completed,
    count(*) filter (where status = 'FAILED')                       as failed,
    coalesce(sum(amount_minor) filter (where status = 'COMPLETED'), 0) as completed_amount_minor,
    round(count(*) filter (where status = 'COMPLETED') / count(*), 4) as success_rate,
    round(median(seconds_to_outcome), 3)                            as median_seconds_to_outcome,
    max(dbt_loaded_at)                                              as max_dbt_loaded_at
from {{ ref('fact_payments') }}
where payment_date in (select payment_date from affected_dates)
group by 1, 2, 3
