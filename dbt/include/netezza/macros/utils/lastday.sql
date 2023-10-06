{% macro netezza__last_day(date, datepart) -%}
    {% if datepart == 'year' %}
        add_months({{date}} - extract(doy from {{date}}), 12)
    {% elif datepart == 'quarter' %}
        next_quarter({{date}}) - interval '1 days'
    {% elif datepart == 'month' %}
        last_day({{date}})
    {% endif %}
{%- endmacro %}