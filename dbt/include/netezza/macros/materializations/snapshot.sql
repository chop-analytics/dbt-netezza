{% macro netezza__snapshot_hash_arguments(args) -%}
    lower(
      rawtohex(
        hash(
          coalesce(
            cast(
              {%- for arg in args -%}
                coalesce(cast({{ arg }} as varchar(255)), '')
                {% if not loop.last %} || '|' || {% endif %}
              {%- endfor -%}
            as {{ type_string() }}),
          '')
        )
      )
    )
{%- endmacro %}

{% macro netezza__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    update {{ target }}
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::{{ type_string() }} = {{ target }}.dbt_scd_id::{{ type_string() }}
      and DBT_INTERNAL_SOURCE.dbt_change_type::{{ type_string() }} in ('update'::{{ type_string() }}, 'delete'::{{ type_string() }})
      and {{ target }}.dbt_valid_to is null;

    insert into {{ target }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::{{ type_string() }} = 'insert'::{{ type_string() }};
{% endmacro %}
