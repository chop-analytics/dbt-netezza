from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
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

from dbt.tests.adapter.constraints.fixtures import (
    my_model_with_quoted_column_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_incremental_wrong_name_sql,
    my_model_wrong_order_depends_on_fk_sql,
    foreign_key_model_sql,
    my_incremental_model_sql,
)
from tests.functional.adapter.constraint_fixtures import (
    model_quoted_column_schema_yml,
    test_constraint_quoted_column_netezza__expected_sql,
    model_schema_yml,
    model_fk_constraint_schema_yml,
    test_base__constraints_runtime_ddl_enforcement__expected_sql,
)
import pytest


class BaseConstraintsColumnsEqualNetezza(BaseConstraintsColumnsEqual):
    # overwrite to use valid Netezza string type
    @pytest.fixture
    def string_type(self):
        return "varchar(2000)"

    # overwrite to use all valid netezza types
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            # ["'1'", string_type, string_type],
            # ["true", "boolean", "BOOL"],
            # ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "DATETIMETZ"],
            # ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "DATETIME"],
            # ["ARRAY['a','b','c']", "text[]", "STRINGARRAY"],
            # ["ARRAY[1,2,3]", "int[]", "INTEGERARRAY"],
            # ["'1'::numeric", "numeric", "DECIMAL"],
        ]

class BaseIncrementalConstraintsColumnsEqualNetezza(BaseConstraintsColumnsEqualNetezza):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestConstraintQuotedColumnNetezza(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
            return test_constraint_quoted_column_netezza__expected_sql


class TestIncrementalConstraintsColumnsEqualNetezza(
    BaseIncrementalConstraintsColumnsEqualNetezza
):
    pass


class BaseIncrementalConstraintsRollbackNetezza(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['Field cannot contain null values']

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestIncrementalConstraintsRollbackNetezza(BaseIncrementalConstraintsRollbackNetezza):
    pass


class BaseConstraintsRuntimeDdlEnforcementNetezza(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return test_base__constraints_runtime_ddl_enforcement__expected_sql

class TestIncrementalConstraintsRuntimeDdlEnforcementNetezza(
    BaseConstraintsRuntimeDdlEnforcementNetezza
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


class TestTableConstraintsColumnsEqualNetezza(BaseConstraintsColumnsEqualNetezza):
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
