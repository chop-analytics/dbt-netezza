from dbt.tests.adapter.constraints import fixtures


def fix_text_data_type(sql):
    return sql.replace("text", "varchar(2000)")


def fix_sql_header_timezone(sql):
    return sql.replace("session", "").replace(
        "current_setting('timezone')", "'Asia/Kolkata'"
    )


model_schema_yml = fix_text_data_type(fixtures.model_schema_yml)
model_fk_constraint_schema_yml = fix_text_data_type(
    fixtures.model_fk_constraint_schema_yml
)
my_model_contract_sql_header_sql = fix_sql_header_timezone(
    fixtures.my_model_contract_sql_header_sql
)
my_model_incremental_contract_sql_header_sql = fix_sql_header_timezone(
    fixtures.my_model_incremental_contract_sql_header_sql
)
model_contract_header_schema_yml = fix_text_data_type(
    fixtures.model_contract_header_schema_yml
)
constrained_model_schema_yml = fix_text_data_type(fixtures.constrained_model_schema_yml)
model_quoted_column_schema_yml = fix_text_data_type(
    fixtures.model_quoted_column_schema_yml
)

test_constraint_quoted_column_netezza__expected_sql = """
create table <model_identifier> (
    id integer not null,
    "from" varchar(2000) not null,
    date_day varchar(2000),
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
