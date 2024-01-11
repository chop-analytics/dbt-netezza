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

# Overwrite to explicitly cast to string types in model
models__delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello'::{{type_string()}} as msg, 'blue'::{{type_string()}} as color
union all
select 2 as id, 'goodbye'::{{type_string()}} as msg, 'red'::{{type_string()}} as color

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey'::{{type_string()}} as msg, 'blue'::{{type_string()}} as color
union all
select 2 as id, 'yo'::{{type_string()}} as msg, 'green'::{{type_string()}} as color
union all
select 3 as id, 'anyway'::{{type_string()}} as msg, 'purple'::{{type_string()}} as color

{% endif %}
"""


class TestIncrementalOnSchemaChangeNetezza(BaseIncrementalOnSchemaChange):
    pass


# override to drop test models when tests complete, use new models__delete_insert_incremental_predicates_sql
class BaseIncrementalPredicatesNetezza(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    def test__incremental_predicates(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=4
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


class TestIncrementalPredicatesDeleteInsertNetezza(BaseIncrementalPredicatesNetezza):
    pass


class TestIncrementalPredicatesMergeNetezza(BaseIncrementalPredicatesNetezza):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["DBT_INTERNAL_SOURCE.id != 2"],
                "+incremental_strategy": "merge",
            }
        }


class TestPredicatesDeleteInsertNetezza(BaseIncrementalPredicatesNetezza):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }


class TestPredicatesMergeNetezza(BaseIncrementalPredicatesNetezza):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["DBT_INTERNAL_SOURCE.id != 2"],
                "+incremental_strategy": "merge",
            }
        }


class TestIncrementalUniqueKeyNetezza(BaseIncrementalUniqueKey):
    pass
