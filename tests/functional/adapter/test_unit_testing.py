import pytest
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestUnitTestCaseInsensivityNetezza(BaseUnitTestCaseInsensivity):
    pass


class TestUnitTestInvalidInputNetezza(BaseUnitTestInvalidInput):
    pass


class TestUnitTestingTypesNetezza(BaseUnitTestingTypes):
    # Override to remove JSON, NUMERIC, TIMESTAMPTZ, TIMESTAMP, and change DATE to DATETIME
    # the latter two should be possible, but there are issue with the ODBC casting
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATETIME '2020-01-02'", "2020-01-02"],
        ]
