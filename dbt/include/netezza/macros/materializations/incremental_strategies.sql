-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) %}
  {{ default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) }};
{% endmacro %}

-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}

{% macro netezza__get_incremental_default_sql(arg_dict) %}
  {% set unique_key = arg_dict.get("unique_key") %}
  {% if unique_key %}
    {% do return(get_incremental_merge_sql(arg_dict)) %}
  {% else %}
    {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
  {% endif %}
{% endmacro %}