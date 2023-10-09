import pytest

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)


class TestIncrementalOnSchemaChangeNetezza(BaseIncrementalOnSchemaChange):
    pass


class TestIncrementalPredicatesDeleteInsertNetezza(BaseIncrementalPredicates):
    pass


class TestIncrementalPredicatesMergeNetezza(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["id != 2"],
                "+incremental_strategy": "merge",
            }
        }


class TestPredicatesDeleteInsertNetezza(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }


class TestPredicatesMergeNetezza(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["id != 2"],
                "+incremental_strategy": "merge",
            }
        }


class TestUniqueKeyNetezza(BaseIncrementalUniqueKey):
    pass
