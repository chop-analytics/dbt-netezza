{% macro dist(dist) %}
  {%- if dist is not none -%}
      {%- if dist is string -%}
        {%- if dist in ['random'] -%}
          distribute on {{ dist }}
        {%- else -%}
          distribute on ({{ dist }})
        {%- endif -%}
      {%- else -%}
        distribute on (
          {%- for item in dist -%}
            {{ item }}
            {%- if not loop.last -%},{%- endif -%}
          {%- endfor -%}
        )  
      {%- endif -%}
  {%- endif -%}
{%- endmacro -%}


{% macro netezza__create_table_as(temporary, relation, sql) -%}
  {%- set _dist = config.get('dist') -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation }}
  as (
    {{ sql }}
  )
  {{ dist(_dist) }}
  ;
{%- endmacro %}

{% macro netezza__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where catalog_name ilike '{{ database.strip("\"") }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro netezza__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True, auto_begin=False) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schema as schema,
      'table' as type
    from {{ schema_relation.database }}.._v_table
    where schema ilike '{{ schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schema as schema,
      'view' as type
    from {{ schema_relation.database }}.._v_view
    where schema ilike '{{ schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro netezza__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    {{ exceptions.raise_compiler_error("dbt-netezza does not support drop_schema") }}
  {% endcall %}
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

{% macro netezza__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from {{ relation.information_schema('columns') }}
      where table_name ilike '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema ilike '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro netezza__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = netezza_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{% macro netezza__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name if column_dict[column_name]['quote'] else column_name | upper in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = netezza_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro netezza_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  '{{ comment | replace("'", "''")}}'
{%- endmacro %}
