{% macro netezza__snapshot_hash_arguments(args) -%}
    hash8({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar(255) ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}

{% macro netezza__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}