{% macro netezza__power(base, exponent) %}
    pow(
        {{ base }},
        {{ exponent }}
        )
{% endmacro %}
