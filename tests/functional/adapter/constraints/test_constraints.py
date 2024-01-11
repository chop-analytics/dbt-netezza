import pytest
from dbt.tests.util import (
    get_manifest,
    run_dbt_and_capture,
    write_file,
)
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
    BaseIncrementalContractSqlHeader,
    BaseTableContractSqlHeader,
    BaseConstraintQuotedColumn,
    TestIncrementalForeignKeyConstraint as BaseIncrementalForeignKeyConstraint,
)
from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_incremental_model_sql,
    foreign_key_model_sql,
    my_model_data_type_sql,
    model_data_type_schema_yml,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_incremental_wrong_name_sql,
    my_model_wrong_order_depends_on_fk_sql,
    my_model_incremental_wrong_order_depends_on_fk_sql,
    my_model_with_quoted_column_name_sql,
)
from tests.functional.adapter.constraints import fixtures as custom_fixtures


class NetezzaBaseConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    # Override to use valid Netezza string type
    @pytest.fixture
    def string_type(self):
        return "varchar(2000)"

    # Override to use all valid netezza types
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "boolean", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "DATETIME"],
        ]

    # Override because test creates a series of models parameterized with a data type;
    # if these are not dropped in between iterations, Netezza will complain about
    # writing different data types into an existing table
    def test__constraints_wrong_column_data_types(
        self,
        project,
        string_type,
        int_type,
        schema_string_type,
        schema_int_type,
        data_types,
    ):
        for sql_column_value, schema_data_type, error_data_type in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )

            # Write wrong data_type to corresponding schema file
            # Write integer type for all schema yaml values except when testing integer type itself
            wrong_schema_data_type = (
                schema_int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else schema_string_type
            )
            wrong_schema_error_data_type = (
                int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else string_type
            )
            write_file(
                model_data_type_schema_yml.format(data_type=wrong_schema_data_type),
                "models",
                "constraints_schema.yml",
            )

            results, log_output = run_dbt_and_capture(
                ["run", "-s", "my_model_data_type"], expect_pass=False
            )
            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config.enforced is True
            expected = [
                "wrong_data_type_column_name",
                error_data_type,
                wrong_schema_error_data_type,
                "data type mismatch",
            ]
            assert all(
                [(exp in log_output or exp.upper() in log_output) for exp in expected]
            )


class TestTableConstraintsColumnsEqualNetezza(
    NetezzaBaseConstraintsColumnsEqual, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": custom_fixtures.model_schema_yml,
        }


class TestViewConstraintsColumnsEqualNetezza(
    NetezzaBaseConstraintsColumnsEqual, BaseViewConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def project_cleanup_manifest_relations(project_root) -> list[tuple[str, str]]:
        return [("view", "my_model_wrong_order")]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": custom_fixtures.model_schema_yml,
        }


class TestIncrementalConstraintsColumnsEqualNetezza(
    NetezzaBaseConstraintsColumnsEqual, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": custom_fixtures.model_schema_yml,
        }


class TestTableConstraintsRuntimeDdlEnforcementNetezza(
    BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": custom_fixtures.model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return (
            custom_fixtures.test_base_constraints_runtime_ddl_enforcement__expected_sql
        )


class TestTableConstraintsRollbackNetezza(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Field cannot contain null values"]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": custom_fixtures.model_schema_yml,
        }


class TestIncrementalConstraintsRuntimeDdlEnforcementNetezza(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": custom_fixtures.model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return (
            custom_fixtures.test_base_constraints_runtime_ddl_enforcement__expected_sql
        )


class TestIncrementalConstraintsRollbackNetezza(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Field cannot contain null values"]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": custom_fixtures.model_schema_yml,
        }


class TestTableContractSqlHeaderNetezza(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": custom_fixtures.my_model_contract_sql_header_sql,
            "constraints_schema.yml": custom_fixtures.model_contract_header_schema_yml,
        }


class TestIncrementalContractSqlHeaderNetezza(BaseIncrementalContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": custom_fixtures.my_model_incremental_contract_sql_header_sql,
            "constraints_schema.yml": custom_fixtures.model_contract_header_schema_yml,
        }


class TestModelConstraintsRuntimeEnforcementNetezza(
    BaseModelConstraintsRuntimeEnforcement
):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return custom_fixtures.test_base_model_constraints_runtime_enforcement__expected_sql

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": custom_fixtures.constrained_model_schema_yml,
        }


class TestConstraintQuotedColumnNetezza(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": custom_fixtures.model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return custom_fixtures.test_constraint_quoted_column_netezza__expected_sql


class TestIncrementalForeignKeyConstraintNetezza(BaseIncrementalForeignKeyConstraint):
    pass
