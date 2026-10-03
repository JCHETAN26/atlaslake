-- Conformed calendar. Grain: one row per day spanning all fact dates (+ a year of padding).
with bounds as (
    select least(coalesce(min(cast(order_timestamp as date)), current_date), date '2025-01-01') as lo,
           greatest(coalesce(max(cast(order_timestamp as date)), current_date), current_date) + 365 as hi
    from {{ ref('stg_orders') }}
)
select
    cast(strftime(d, '%Y%m%d') as integer)  as date_key,
    cast(d as date)                         as date_day,
    year(d) as year, quarter(d) as quarter, month(d) as month, monthname(d) as month_name,
    day(d) as day_of_month, isodow(d) as iso_day_of_week, dayname(d) as day_name,
    isodow(d) in (6, 7) as is_weekend,
    weekofyear(d) as iso_week
from bounds, generate_series(bounds.lo, bounds.hi, interval 1 day) as t(d)
