{% macro netezza__concat(fields) %}
    {{ fields|join(' || ') }}
{% endmacro %}
