{% macro netezza__hash(field) -%}
    lower(
        rawtohex(
            hash(
                coalesce(
                    cast({{ field }} as {{ type_string() }}),
                    ''
                )
            )
        )
    )
{%- endmacro %}
