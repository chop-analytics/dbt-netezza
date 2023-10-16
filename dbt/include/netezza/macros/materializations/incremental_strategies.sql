-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {{ default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) }};
{% endmacro %}

-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}

{% macro netezza__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}