-- Business invariant: total Gold revenue == revenue computed directly from Silver order state.
with gold as (
    select currency, sum(gross_revenue_minor) as revenue from {{ ref('mart_daily_sales') }} group by 1
), silver as (
    select currency, sum(amount_minor) as revenue
    from {{ ref('stg_orders') }}
    where paid_at is not null and status <> 'CANCELLED'
    group by 1
)
select coalesce(g.currency, s.currency) as currency, g.revenue as gold, s.revenue as silver
from gold g full outer join silver s on g.currency = s.currency
where coalesce(g.revenue, -1) <> coalesce(s.revenue, -1)
