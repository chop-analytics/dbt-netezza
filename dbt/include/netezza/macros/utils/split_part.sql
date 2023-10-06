{% macro netezza__split_part(string_text, delimiter_text, part_number) %}
    get_value_varchar(array_split({{string_text}}, {{delimiter_text}}), {{part_number}})
{% endmacro %}