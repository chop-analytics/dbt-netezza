{% macro netezza__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}
