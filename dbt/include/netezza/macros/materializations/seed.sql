{% macro netezza__load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}
    {% set filepath = model['root_path'] + model['root_path'][0] + model['original_file_path'] %}

    {% set sql %}
        insert into {{ this.render() }} ({{ cols_sql }})
        select * from external '{{ filepath }}'
        using (
            REMOTESOURCE 'ODBC'
            NULLVALUE ''
            DELIMITER ','
            SKIPROWS 1
            MAXERRORS 1
            DATESTYLE YMD
            DATEDELIM '-'
            TIMESTYLE '24HOUR'
            TIMEDELIM ':'
            QUOTEDVALUE Double
            CTRLCHARS True
            DATETIMEDELIM 'T'
        )
    {% endset %} 

    {{ adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) }}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(sql) }}
{% endmacro %}

