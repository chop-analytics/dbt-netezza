import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import (
    BaseTypeTimestamp,
    seeds__expected_csv,
    seeds__expected_yml,
)
from dbt.tests.util import get_connection


@pytest.fixture(autouse=True)
def run_around_tests(unique_schema, adapter):
    # Remove the "expected" table or view if it exists, which can cause the test to fail
    # due to the lack of schema isolation
    with get_connection(adapter):
        relations = adapter.list_relations(
            adapter.config.credentials.database, unique_schema
        )
        expected_relations = [
            rel for rel in relations if rel.identifier.lower() == "expected"
        ]
        if expected_relations:
            adapter.drop_relation(expected_relations[0])

    yield


class TestTypeBigIntNetezza(BaseTypeBigInt):
    pass


class TestTypeBooleanNetezza(BaseTypeBoolean):
    pass


class TestTypeFloatNetezza(BaseTypeFloat):
    pass


class TestTypeIntNetezza(BaseTypeInt):
    pass


class TestTypeNumericNetezza(BaseTypeNumeric):
    pass


@pytest.mark.skip(
    reason="Netezza requires `varchar` length to be specified and `text` type results in CLOB error."
)
class TestTypeStringNetezza(BaseTypeString):
    pass


class TestTypeTimestampNetezza(BaseTypeTimestamp):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__expected_csv.replace(" ", "T"),
            "expected.yml": seeds__expected_yml,
        }

    pass
