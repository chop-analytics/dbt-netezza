
{% macro netezza__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    select
        _v_relation_column.database as table_database,
        _v_objects.schema as table_schema,
        _v_objects.objname as table_name,
        _v_objects.objtype as table_type,
        coalesce(_v_table.description, _v_view.description) as table_comment,
        _v_relation_column.attname as column_name,
        _v_relation_column.attnum as column_index,
        format_type as column_type,
        _v_relation_column.description as column_comment,
        _v_relation_column.owner as table_owner
    from 
        _v_objects
        left join _v_table on _v_table.objid = _v_objects.objid
        left join _v_view on _v_view.objid = _v_objects.objid
        inner join _v_relation_column on _v_relation_column.objid = _v_objects.objid
    where 
        _v_objects.objtype in ('TABLE', 'VIEW')
    order by
        _v_objects.schema,
        _v_objects.objname,
        _v_relation_column.attnum

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
