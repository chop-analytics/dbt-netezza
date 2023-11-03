from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
    BaseIncrementalContractSqlHeader,
    BaseTableContractSqlHeader,
    TestIncrementalForeignKeyConstraint as BaseIncrementalForeignKeyConstraint,
)

from dbt.tests.adapter.constraints.fixtures import my_model_with_quoted_column_name_sql
import pytest

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


class TestConstraintQuotedColumnNetezza(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
            return """
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


class TestIncrementalConstraintsColumnsEqualNetezza(
    BaseIncrementalConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsRollbackNetezza(BaseIncrementalConstraintsRollback):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcementNetezza(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


class TestIncrementalContractSqlHeaderNetezza(BaseIncrementalContractSqlHeader):
    pass


class TestIncrementalForeignKeyConstraintNetezza(BaseIncrementalForeignKeyConstraint):
    pass


class TestModelConstraintsRuntimeEnforcementNetezza(
    BaseModelConstraintsRuntimeEnforcement
):
    pass


class TestTableConstraintsColumnsEqualNetezza(BaseTableConstraintsColumnsEqual):
    pass


class TestTableConstraintsRollbackNetezza(BaseConstraintsRollback):
    pass


class TestTableConstraintsRuntimeDdlEnforcementNetezza(
    BaseConstraintsRuntimeDdlEnforcement
):
    pass


class TestTableContractSqlHeaderNetezza(BaseTableContractSqlHeader):
    pass


class TestViewConstraintsColumnsEqualNetezza(BaseViewConstraintsColumnsEqual):
    pass
