{% macro netezza__get_show_grant_sql(relation) %}
    {% set privilege_lookup = [
        ('privilege_list', 0, 'list'), 
        ('privilege_select', 1, 'select'),  
        ('privilege_insert', 2, 'insert'), 
        ('privilege_update', 3, 'update'), 
        ('privilege_delete', 4, 'delete'), 
        ('privilege_truncate', 5, 'truncate'),  
        ('privilege_lock', 6, 'lock'), 
        ('privilege_alter', 7, 'alter'), 
        ('privilege_drop', 8, 'drop'), 
        ('privilege_abort', 9, 'abort'), 
        ('privilege_load', 10, 'load'), 
        ('privilege_genstats', 11, 'genstats'),  
        ('privilege_groom', 12, 'groom'),  
        ('privilege_execute', 13, 'execute'), 
        ('privilege_label_access', 14, 'label access'),  
        ('privilege_label_restrict', 15, 'label restrict'), 
        ('privilege_label_expand', 16, 'label expand'), 
        ('privilege_execute_as', 17, 'execute as'), 
    ] %}

    with parse_bitwise as (
        select
            username as grantee,
            {% for wide_col_name, shift_bits, _ in privilege_lookup %}
            case when int8and(uopobjpriv, 1<<{{ shift_bits }}) = 0 then 0 else 1 end as {{ wide_col_name }}
            {%- if not loop.last %},{% endif -%}
            {% endfor %}
        from
            qmr_dev.admin._v_sys_user_priv
        where
            lower(schema) = '{{ relation.schema|lower }}'
            and lower(objectname) = '{{ relation.identifier|lower }}'
            and username != session_user
    ),

    privileges_pivot as (
        {% for wide_col_name, _, privilege_type in privilege_lookup %}
        select grantee, case when {{ wide_col_name }} = 1 then '{{ privilege_type }}' end as privilege_type from parse_bitwise
        {%- if not loop.last %}
        union all
        {% endif -%}
        {% endfor %}
    )

    select
        lower(privilege_type) as privilege_type,
        lower(grantee) as grantee
    from
        privileges_pivot
    where
        privilege_type is not null
{% endmacro %}

{% macro netezza__get_grant_sql(relation, privilege, grantee) %}
    grant {{ privilege }} on {{ relation }} to {{ grantee|join(', ') }}
{% endmacro %}

{% macro netezza__get_revoke_sql(relation, privilege, grantee) %}
    revoke {{ privilege }} on {{ relation }} from {{ grantee|join(', ') }}
{% endmacro %}