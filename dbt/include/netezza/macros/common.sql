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
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}