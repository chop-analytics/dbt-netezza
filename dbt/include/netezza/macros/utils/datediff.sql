{% macro netezza__datediff(first_date, second_date, datepart) %}

    {% if datepart == 'year' %} days_between({{second_date}}::date - {{second_date}}::date) / 365.25
    -- {% elif datepart == 'month' %}
    {% elif datepart == 'day' %} days_between(({{second_date}}::timestamp) - ({{first_date}}::timestamp))
    {% elif datepart == 'week' %} weeks_between({{second_date}}::timestamp, {{first_date}}::timestamp)
    {% elif datepart == 'hour' %} hours_between({{second_date}}::timestamp, {{first_date}}::timestamp)
    {% elif datepart == 'minute' %} minutes_between({{second_date}}::timestamp, {{first_date}}::timestamp)
    {% elif datepart == 'second' %} seconds_between({{second_date}}::timestamp, {{first_date}}::timestamp)
    -- {% elif datepart == 'millisecond' %}
    -- {% elif datepart == 'microsecond' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported datepart for macro datediff in postgres: {!r}".format(datepart)) }}
    {% endif %}

{% endmacro %}
