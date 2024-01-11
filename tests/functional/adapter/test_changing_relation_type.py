import pytest

from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)


# FIXME dbt incorrectly classifies the model as a table rather than a view and fails the rename
@pytest.mark.skip("dbt fails to rematerialize view as an incremental model")
class TestChangeRelationTypesNetezza(BaseChangeRelationTypeValidator):
    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check_materialization("view")
        self._run_and_check_materialization("table")
        self._run_and_check_materialization("view")
        # Test fails here when dbt thinks 'model_mc_modelface' is a table instead of a view
        self._run_and_check_materialization("incremental")
        self._run_and_check_materialization("table", extra_args=["--full-refresh"])
