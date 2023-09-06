
{% macro dbt_netezza_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy") or "delete+insert" -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) %}
  {{ default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) }};
{% endmacro %}

-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}

{% macro dbt_netezza_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {% if strategy == 'merge' %}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% materialization incremental, adapter='netezza' -%}
   
  {%- set unique_key = config.get('unique_key') -%}

  {#-- load grants config, if it exists --#}
  {% set grant_config = config.get('grants') %}
  
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(target_relation) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_netezza_validate_get_incremental_strategy(config) -%}
  
  {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
  {% set backup_identifier = model['name'] + "__dbt_backup" %}
  -- the intermediate_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {% set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier,
                                                                  schema=schema,
                                                                  database=database) %}
  {% set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                            schema=schema,
                                                            database=database) %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  
  {% elif trigger_full_refresh %}
    {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
    {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
    {% set backup_identifier = model['name'] + '__dbt_backup' %}
    {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
    {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

    {% set build_sql = create_table_as(False, intermediate_relation, sql) %}
    {% set need_swap = true %}
    {% do to_drop.append(backup_relation) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% set build_sql = dbt_netezza_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if grant_config %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% endif %}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
