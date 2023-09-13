{% macro netezza__right(string_text, length_expression) %}

    strright(
        {{ string_text }},
        {{ length_expression }}
    )

{%- endmacro -%}