{% macro netezza__load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}
    {% set seed_file_path = adapter.get_seed_file_path(model) %}

    {% set sql %}
        insert into {{ this.render() }} ({{ cols_sql }})
        select * from external '{{ seed_file_path }}'
        using (
            REMOTESOURCE 'ODBC'
            MAXERRORS 1
            SKIPROWS {{ config.get("skiprows", default="1") }}
            CTRLCHARS {{ config.get("ctrlchars", default="true") }}
            QUOTEDVALUE {{ config.get("quotedvalue", default="Double") }}
            NULLVALUE '{{ config.get("nullvalue", default="") }}'
            DELIMITER '{{ config.get("delimiter", default=",") }}'
            DATEDELIM '{{ config.get("datedelim", default="-") }}'
            TIMEDELIM '{{ config.get("timedelim", default=":") }}'
            DATETIMEDELIM '{{ config.get("datetimedelim", default="T") }}'
            BOOLSTYLE {{ config.get("boolstyle", default="1_0") }}
            DATESTYLE {{ config.get("datestyle", default="YMD") }}
            TIMESTYLE {{ config.get("timestyle", default="24HOUR") }}
        )
    {% endset %} 

    {{ adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) }}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(sql) }}
{% endmacro %}

