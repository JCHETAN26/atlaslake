{% test non_negative(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test scd2_valid(model, key, valid_from='valid_from', valid_to='valid_to', current_flag='is_current') %}
-- violations: overlapping versions, zero/negative-length versions, or != 1 current version per key
with v as (
    select {{ key }} as k, {{ valid_from }} as vf, {{ valid_to }} as vt, {{ current_flag }} as cur,
           lead({{ valid_from }}) over (partition by {{ key }} order by {{ valid_from }}) as next_vf
    from {{ model }}
    where {{ key }} <> '-1'
)
select k, 'overlap_or_gap' as problem from v where vt is not null and (next_vf is null or vt <> next_vf)
union all
select k, 'non_positive_interval' from v where vt is not null and vt <= vf
union all
select k, 'current_count' from v group by k having sum(case when cur then 1 else 0 end) <> 1
{% endtest %}

{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
