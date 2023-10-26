{% macro netezza__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}
    {% if order_by_clause -%}
        {{ exceptions.raise_compiler_error("Netezza does not support 'order_by_clause' argument to group_concat") }}
    {% endif %}
    {% if limit_num -%}
        {{ exceptions.raise_compiler_error("Netezza does not support 'limit_num' argument to group_concat") }}
    {% endif %}
    {% if delimiter_text and delimiter_text|length != 3 -%}
        {{ exceptions.raise_compiler_error("Netezza does not support multiple characters for 'delimiter_text' and value must be quoted (" ~ delimiter_text ~ ")") }}
    {% endif %}
    group_concat({{ measure }}, {{ delimiter_text }})
{%- endmacro -%}