{% macro netezza__load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set temp = '/tmp/'~ this.render() ~ '.csv' %}
    {{ agate_table.to_csv(temp) }}

    {% set sql %}
        insert into {{ this.render() }} ({{ cols_sql }})
        select * from external '{{ temp }}'
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
        )
    {% endset %} 

    {{ adapter.add_query(sql, bindings=[], abridge_sql_log=True) }}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(sql) }}
{% endmacro %}

