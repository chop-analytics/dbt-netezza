{% macro netezza__get_catalog(information_schema, schemas) -%}
    {%- set query -%}
        {{ netezza__get_catalog_sql() }}
        {{ netezza__get_catalog_schemas_where_clause_sql(schemas) }}
        {{ netezza__get_catalog_order_by_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}
{%- endmacro %}


{% macro netezza__get_catalog_relations(information_schema, relations) -%}
    {%- set query -%}
        {{ netezza__get_catalog_sql() }}
        {{ netezza__get_catalog_relations_where_clause_sql(relations) }}
        {{ netezza__get_catalog_order_by_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}
{%- endmacro %}


{% macro netezza__get_catalog_sql() -%}
    select
        coalesce(_v_table.database, _v_view.database) as table_database,
        _v_objects.schema as table_schema,
        _v_objects.objname as table_name,
        _v_objects.objtype as table_type,
        coalesce(_v_table.description, _v_view.description) as table_comment,
        _v_objects.owner as table_owner,
        _v_relation_column.attname as column_name,
        _v_relation_column.attnum as column_index,
        _v_relation_column.format_type as column_type,
        _v_relation_column.description as column_comment
    from
        _v_objects
    left join _v_table on _v_table.objid = _v_objects.objid
    left join _v_view on _v_view.objid = _v_objects.objid
    inner join _v_relation_column on _v_relation_column.objid = _v_objects.objid
{%- endmacro %}


{% macro netezza__catalog_equals(field, value) -%}
    upper({{ field }}) = upper('{{ value }}')
{%- endmacro %}


{% macro netezza__get_catalog_schemas_where_clause_sql(schemas) -%}
    where _v_objects.objtype in ('TABLE', 'VIEW') and 
    ({%- for schema in schemas -%}
        ({{ netezza__catalog_equals('table_schema', schema) }}){%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro netezza__get_catalog_relations_where_clause_sql(relations) -%}
    where _v_objects.objtype in ('TABLE', 'VIEW') and (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    {{ netezza__catalog_equals('table_schema', relation.schema) }}
                    and {{ netezza__catalog_equals('table_name', relation.identifier) }}
                )
            {% elif relation.schema %}
                (
                    {{ netezza__catalog_equals('table_schema', relation.schema) }}
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}


{% macro netezza__get_catalog_order_by_sql() -%}
    order by column_index
{%- endmacro %}
