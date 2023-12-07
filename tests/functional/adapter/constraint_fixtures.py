model_quoted_column_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
      materialized: table
    constraints:
      - type: check
        # this one is the on the user
        expression: ("from" = 'blue')
        columns: [ '"from"' ]
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: from  # reserved word
        quote: true
        data_type: varchar(100)
        constraints:
          - type: not_null
      - name: date_day
        data_type: varchar(100)
"""

test_constraint_quoted_column_netezza__expected_sql = """
create table <model_identifier> (
    id integer not null,
    "from" varchar(100) not null,
    date_day varchar(100),
    check (("from" = 'blue'))
) ;
insert into <model_identifier> 
    select id, "from", date_day
    from (
        select
        'blue' as "from",
        1 as id,
        '2019-01-01' as date_day
    ) as model_subq;
    """

test_base_model_constraints_runtime_enforcement__expected_sql = """
create table <model_identifier> (
    id integer not null,
    color varchar(2000),
    date_day varchar(2000),
    check ((id > 0)),
    check (id >= 1),
    primary key (id),
    constraint strange_uniqueness_requirement unique (color, date_day),
    foreign key (id) references <foreign_key_model_identifier> (id)
) ;
insert into <model_identifier> 
    select
       id,
       color,
       date_day
       from
    (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq;
"""

test_base_constraints_runtime_ddl_enforcement__expected_sql = """
create table <model_identifier> (
    id integer not null primary key check ((id > 0)) check (id >= 1) references <foreign_key_model_identifier> (id) unique,
    color varchar(2000),
    date_day varchar(2000)
) ;
insert into <model_identifier>
    select
       id,
       color,
       date_day
       from
    (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq;
"""


my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}

{% call set_sql_header(config) %}
set time zone 'Asia/Kolkata';
{%- endcall %}
select current_timestamp::timetz as current_time_w_tz
"""


model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(2000)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
"""

model_fk_constraint_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
          - type: foreign_key
            expression: {schema}.foreign_key_model (id)
          - type: unique
        tests:
          - unique
      - name: color
        data_type: varchar(2000)
      - name: date_day
        data_type: varchar(2000)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(2000)
      - name: date_day
        data_type: varchar(2000)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(2000)
      - name: date_day
        data_type: varchar(2000)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(2000)
      - name: date_day
        data_type: varchar(2000)
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: unique
          - type: primary_key
"""

model_contract_header_schema_yml = """
version: 2
models:
  - name: my_model_contract_sql_header
    config:
      contract:
        enforced: true
    columns:
      - name: column_name
        data_type: varchar(2000)
"""

constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: check
        expression: id >= 1
      - type: primary_key
        columns: [ id ]
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement
      - type: foreign_key
        columns: [ id ]
        expression: {schema}.foreign_key_model (id)
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: varchar(2000)
      - name: date_day
        data_type: varchar(2000)
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: unique
          - type: primary_key
"""