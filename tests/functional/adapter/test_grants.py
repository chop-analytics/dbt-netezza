"""
Tests for GRANT/REVOKE functionality

Requires the additions of three environment vars to test.env (username can be
any valid username in target db):

DBT_TEST_USER_1=<username>
DBT_TEST_USER_2=<username>
DBT_TEST_USER_3=<username>

NOTE: None of these test usernames should be the same as session_user when the
tests run, or the tests will find no privileges and fail
"""

import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants

from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
    relation_from_name,
    get_connection,
)

my_incremental_model_sql = """
  select 1 as fun
"""

incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class TestInvalidGrantsNetezza(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "ProcessObjectPrivileges: group/user"

    def privilege_does_not_exist_error(self):
        return r"expecting `ALL\' or `ALTER\' or `CREATE\' or `DELETE\' or `DROP\'"


class TestModelGrantsNetezza(BaseModelGrants):
    pass


class TestIncrementalGrantsNetezza(BaseIncrementalGrants):
    def test_incremental_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        print(log_output)
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert (
            "grant " not in log_output
        )  # with space to disambiguate from 'show grants'
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(
            user2_incremental_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            with pytest.raises(RuntimeError):
                adapter.drop_schema(relation)

        # NOTE: The following component of the test as assumes that state has
        # changed due to the above DROP SCHEMA statement; since Netezza does not
        # support that statement and it therefore never runs, state is identical
        # and subsequent test will not actually test anything

        # Incremental materialization, same config, rebuild now that table is missing
        # (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        # assert len(results) == 1
        # assert "grant " in log_output
        # assert "revoke " not in log_output
        # self.assert_expected_grants_match_actual(
        #     project, "my_incremental_model", expected
        # )


class TestSeedGrantsNetezza(BaseSeedGrants):
    pass


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
