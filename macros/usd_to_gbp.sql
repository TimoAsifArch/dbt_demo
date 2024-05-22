{% macro usd_to_gbp(usd_value) -%}
    CAST({{ usd_value }} * 0.80 AS numeric(18,2))
{%- endmacro %}