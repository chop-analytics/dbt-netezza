{% macro netezza__bool_or(condition) %}
    max(case when {{ condition }} then 1 else 0 end)::char(1)::bool
{% endmacro %}
