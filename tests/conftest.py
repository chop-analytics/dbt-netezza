# Source: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
import os

import pytest

# Import the dbt fixtures and custom dbt schema fixture
pytest_plugins = ["dbt.tests.fixtures.project", "tests.dbt_schema_fixture"]


def dbt_profile_target_values():
    return {
        "type": "netezza",
        "threads": 4,
        "host": os.getenv("DBT_TEST_NZ_HOST"),
        "database": os.getenv("DBT_TEST_NZ_DB"),
        "user": os.getenv("DBT_TEST_NZ_USER"),
        "pass": os.getenv("DBT_TEST_NZ_PASS"),
    }


@pytest.fixture(scope="class")
def dbt_profile_target():
    return dbt_profile_target_values()
