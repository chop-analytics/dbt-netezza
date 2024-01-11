import pytest
from dbt.tests.adapter.column_types.test_column_types import (
    TestPostgresColumnTypes as BasePostgresColumnTypes,
    macro_test_is_type_sql,
)


class TestColumnTypesNetezza(BasePostgresColumnTypes):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_is_type.sql": macro_test_is_type_sql.replace(" if not loop.last", "")
        }
