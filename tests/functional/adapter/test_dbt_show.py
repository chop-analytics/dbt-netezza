import pytest
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowLimit, BaseShowSqlHeader


class TestShowSqlHeaderNetezza(BaseShowSqlHeader):
    pass


class TestShowLimitNetezza(BaseShowLimit):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE"}}
