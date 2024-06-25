{% macro netezza__date_spine(datepart, start_date, end_date) -%}
    with rawdata as (
        {{generate_series(
            get_intervals_between(start_date, end_date, datepart)
        )}}
    ),

    all_periods as (
        select (
            {{
                dateadd(
                    datepart,
                    "generated_number - 1",
                    start_date
                )
            }}
        ) as date_{{datepart}}
        from rawdata
    ),

    filtered as (
        select *
        from all_periods
        where date_{{datepart}} < {{ end_date }}
    )

    select * from filtered
{%- endmacro %}