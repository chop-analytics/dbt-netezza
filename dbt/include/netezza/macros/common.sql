{% macro netezza__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where catalog_name ilike '{{ database.strip("\"") }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro netezza__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if relation.type == 'view' %}
        drop {{ relation.type }} {{ relation }}
    {% else %}
        drop {{ relation.type }} {{ relation }} if exists
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro netezza__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}
