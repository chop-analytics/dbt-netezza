{% macro netezza__cast_bool_to_text(field) %}
    lower(cast({{ field }} as varchar(5)))
{% endmacro %}