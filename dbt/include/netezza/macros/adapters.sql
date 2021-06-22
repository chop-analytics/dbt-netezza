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
