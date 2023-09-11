{% macro netezza__get_show_grant_sql(relation) %}
    {% set privilege_lookup = [
        {'wide_col_name': 'privilege_list',           'shift_bits': 0,  'privilege_type': 'list'},
        {'wide_col_name': 'privilege_select',         'shift_bits': 1,  'privilege_type': 'select'},
        {'wide_col_name': 'privilege_insert',         'shift_bits': 2,  'privilege_type': 'insert'},
        {'wide_col_name': 'privilege_update',         'shift_bits': 3,  'privilege_type': 'update'},
        {'wide_col_name': 'privilege_delete',         'shift_bits': 4,  'privilege_type': 'delete'},
        {'wide_col_name': 'privilege_truncate',       'shift_bits': 5,  'privilege_type': 'truncate'},
        {'wide_col_name': 'privilege_lock',           'shift_bits': 6,  'privilege_type': 'lock'},
        {'wide_col_name': 'privilege_alter',          'shift_bits': 7,  'privilege_type': 'alter'},
        {'wide_col_name': 'privilege_drop',           'shift_bits': 8,  'privilege_type': 'drop'},
        {'wide_col_name': 'privilege_abort',          'shift_bits': 9,  'privilege_type': 'abort'},
        {'wide_col_name': 'privilege_load',           'shift_bits': 10, 'privilege_type': 'load'},
        {'wide_col_name': 'privilege_genstats',       'shift_bits': 11, 'privilege_type': 'genstats'},
        {'wide_col_name': 'privilege_groom',          'shift_bits': 12, 'privilege_type': 'groom'},
        {'wide_col_name': 'privilege_execute',        'shift_bits': 13, 'privilege_type': 'execute'},
        {'wide_col_name': 'privilege_label_access',   'shift_bits': 14, 'privilege_type': 'label access'},
        {'wide_col_name': 'privilege_label_restrict', 'shift_bits': 15, 'privilege_type': 'label restrict'},
        {'wide_col_name': 'privilege_label_expand',   'shift_bits': 16, 'privilege_type': 'label expand'},
        {'wide_col_name': 'privilege_execute_as',     'shift_bits': 17, 'privilege_type': 'execute as'},
    ] %}

    with parse_bitwise as (
        select
            username as grantee,
            {% for record in privilege_lookup %}
            case when int8and(uopobjpriv, 1<<{{ record['shift_bits'] }}) = 0 then 0 else 1 end as {{ record['wide_col_name'] }}
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
        {% for record in privilege_lookup %}
        select grantee, case when {{ record['wide_col_name'] }} = 1 then '{{ record["privilege_type"] }}' end as privilege_type from parse_bitwise
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