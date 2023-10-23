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


class TestConstraintQuotedColumnNetezza(BaseConstraintQuotedColumn):
    pass


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
