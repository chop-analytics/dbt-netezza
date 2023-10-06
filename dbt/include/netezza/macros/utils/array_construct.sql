{% macro netezza__array_instantiate(data_type) -%}
    {% set type_int_mapping = {
    "INT1": 1,
    "INT2": 2,
    "INT": 3,
    "BIGINT": 4,
    "DATE": 5,
    "TIME": 6,
    "TIMESTAMP": 7,
    "VARCHAR": 8,
    "NVARCHAR": 9,
    "FLOAT": 10,
    "DOUBLE": 11,
    "TIMETZ": 15
    } %}
    array({{ type_int_mapping[data_type] }})
{%- endmacro %}

{% macro netezza__array_construct(inputs, data_type) -%}
    {% if inputs|length == 0 %}
        {{netezza__array_instantiate(data_type)}}
    {% else %}
        {{array_append(array_construct(inputs[:-1]), inputs[-1])}}
    {% endif %}
{%- endmacro %}