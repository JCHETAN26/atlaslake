{# Deterministic surrogate key: md5 of the natural key parts. #}
{% macro sk(cols) -%}
    md5(concat_ws('||', {% for c in cols %}coalesce(cast({{ c }} as varchar), '~null~'){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}

{# Incremental predicate on a *processing-time* column (never event time: late events would be missed). #}
{% macro incremental_since(column, target_column=none) -%}
    {%- if is_incremental() -%}
    {{ column }} > (
        select coalesce(max({{ target_column or column.split('.')[-1] }}), timestamp '1900-01-01')
               - interval {{ var('incremental_lookback_minutes') }} minute
        from {{ this }}
    )
    {%- else -%}
    true
    {%- endif -%}
{%- endmacro %}

{% macro mask_email(col) -%}
    case when {{ col }} is null then null
         else substr({{ col }}, 1, 1) || '***@' || split_part({{ col }}, '@', 2) end
{%- endmacro %}

{# SCD2 point-in-time predicate. Written with an open-ended upper bound instead of
   `(ts < valid_to OR valid_to IS NULL)`: the OR form forces DuckDB off the hash join and took 112 s
   on 705K orders; this form takes 0.09 s with an identical result (benchmarked, see PROJECT_SPEC §8). #}
{% macro as_of(dim, ts) -%}
    {{ ts }} >= {{ dim }}.valid_from and {{ ts }} < coalesce({{ dim }}.valid_to, timestamp '9999-12-31')
{%- endmacro %}
