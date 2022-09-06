# Source: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
import pytest
import os

# Import the dbt fixtures and custom dbt schema fixture
pytest_plugins = ["dbt.tests.fixtures.project", "tests.dbt_schema_fixture"]


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "netezza",
        "threads": 1,
        "host": os.getenv("DBT_TEST_NZ_HOST"),
        "database": os.getenv("DBT_TEST_NZ_DB"),
        "user": os.getenv("DBT_TEST_NZ_USER"),
        "pass": os.getenv("DBT_TEST_NZ_PASS"),
    }
