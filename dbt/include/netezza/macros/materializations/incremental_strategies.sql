{% macro netezza__get_incremental_default_sql(arg_dict) %}
  {% set unique_key = arg_dict.get("unique_key") %}
  {% if unique_key %}
    {% do return(get_incremental_merge_sql(arg_dict)) %}
  {% else %}
    {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
  {% endif %}
{% endmacro %}