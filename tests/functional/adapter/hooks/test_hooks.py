import os
from pathlib import Path

import pytest
from dbt.tests.adapter.hooks.test_model_hooks import (
    MODEL_PRE_HOOK,
    MODEL_POST_HOOK,
    BaseTestPrePost,
    TestDuplicateHooksInConfigs as BaseDuplicateHooksInConfigs,
    TestHookRefs as BaseHookRefs,
    TestHooksRefsOnSeeds as BaseHooksRefsOnSeeds,
    TestPrePostModelHooks as BasePrePostModelHooks,
    TestPrePostModelHooksInConfig as BasePrePostModelHooksInConfig,
    TestPrePostModelHooksInConfigKwargs as BasePrePostModelHooksInConfigKwargs,
    TestPrePostModelHooksInConfigWithCount as BasePrePostModelHooksInConfigWithCount,
    TestPrePostModelHooksOnSeeds as BasePrePostModelHooksOnSeeds,
    TestPrePostModelHooksOnSeedsPlusPrefixed as BasePrePostModelHooksOnSeedsPlusPrefixed,
    TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace as BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    TestPrePostModelHooksOnSnapshots as BasePrePostModelHooksOnSnapshots,
    TestPrePostModelHooksUnderscores as BasePrePostModelHooksUnderscores,
    TestPrePostSnapshotHooksInConfigKwargs as BasePrePostSnapshotHooksInConfigKwargs,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    TestAfterRunHooks as BaseAfterRunHooks,
    TestPrePostRunHooks as BasePrePostRunHooks,
)


class NetezzaBaseTestPrePost(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_cleanup_extra_relations(self):
        return [("table", "on_model_hook")]

    # Override to uppercaase field names
    def get_ctx_vars(self, state, count, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
        ]
        field_list = ", ".join(['"{}"'.format(f.upper()) for f in fields])
        query = f"select {field_list} from {project.test_schema}.on_model_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    # Override to update expected values
    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            assert ctx["target_dbname"] == project.database
            assert ctx["target_host"] == host
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_threads"] == 4
            assert ctx["target_type"] == project.adapter_type
            # assert ctx["target_user"] == "root"
            assert ctx["target_pass"] == ""

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"

    pass


class TestDuplicateHooksInConfigsNetezza(BaseDuplicateHooksInConfigs):
    pass


class TestHookRefsNetezza(NetezzaBaseTestPrePost, BaseHookRefs):
    pass


class TestAfterRunHooksNetezza(BaseAfterRunHooks):
    pass


class TestPrePostRunHooksNetezza(BasePrePostRunHooks):
    @pytest.fixture(scope="class")
    def project_cleanup_extra_relations(self):
        return [("table", "schemas"), ("table", "db_schemas")]

    # Override to change position of 'if exists'
    @pytest.fixture(scope="function")
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_run.sql"))
        project.run_sql(f"drop table { project.test_schema }.schemas if exists")
        project.run_sql(f"drop table { project.test_schema }.db_schemas if exists")
        os.environ["TERM_TEST"] = "TESTING"

    # Override to uppercaase field names
    def get_ctx_vars(self, state, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
        ]
        field_list = ", ".join(['"{}"'.format(f.upper()) for f in fields])
        query = f"select {field_list} from {project.test_schema}.on_run_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into on_run_hook table"
        assert len(vals) == 1, "too many rows in hooks table"
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])

        return ctx

    # Override to expected values
    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] == project.database
        assert ctx["target_host"] == host
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == project.test_schema
        assert ctx["target_threads"] == 4
        assert ctx["target_type"] == project.adapter_type
        # assert ctx["target_user"] == "root"
        assert ctx["target_pass"] == ""

        assert (
            ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
        ), "run_started_at was not set"
        assert (
            ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
        ), "invocation_id was not set"

    # Override to change text to varchar
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id int )",
                "drop table {{ target.schema }}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id int )",
                "drop table {{ target.schema }}.end_hook_order_test",
                "create table {{ target.schema }}.schemas ( schema varchar(2000) )",
                "insert into {{ target.schema }}.schemas (schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
                "create table {{ target.schema }}.db_schemas ( db varchar(2000), schema varchar(2000) )",
                "insert into {{ target.schema }}.db_schemas (db, schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
            ],
            "seeds": {
                "quote_columns": False,
            },
        }


class TestHooksRefsOnSeedsNetezza(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksNetezza(NetezzaBaseTestPrePost, BasePrePostModelHooks):
    # Override to change 'vacuum' to 'generate statistics on'
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


class TestPrePostModelHooksInConfigNetezza(
    NetezzaBaseTestPrePost, BasePrePostModelHooksInConfig
):
    pass


class TestPrePostModelHooksInConfigKwargsNetezza(
    NetezzaBaseTestPrePost, BasePrePostModelHooksInConfigKwargs
):
    pass


class TestPrePostModelHooksInConfigWithCountNetezza(
    NetezzaBaseTestPrePost, BasePrePostModelHooksInConfigWithCount
):
    # Override to change 'vacuum' to 'generate statistics on'
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


# FIXME Refactor test to not add column or investigate transaction
@pytest.mark.skip("Netezza does not support adding a column in a transaction block")
class TestPrePostModelHooksOnSeedsNetezza(BasePrePostModelHooksOnSeeds):
    pass


# FIXME Refactor test to not add column or investigate transaction
@pytest.mark.skip("Netezza does not support adding a column in a transaction block")
class TestPrePostModelHooksOnSeedsPlusPrefixedNetezza(
    BasePrePostModelHooksOnSeedsPlusPrefixed
):
    pass


# FIXME Refactor test to not add column or investigate transaction
@pytest.mark.skip("Netezza does not support adding a column in a transaction block")
class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceNetezza(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    pass


# FIXME Refactor test to not add column or investigate transaction
@pytest.mark.skip("Netezza does not support adding a column in a transaction block")
class TestPrePostModelHooksOnSnapshotsNetezza(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksUnderscoresNetezza(
    NetezzaBaseTestPrePost, BasePrePostModelHooksUnderscores
):
    # Override to change 'vacuum' to 'generate statistics on'
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post_hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "generate statistics on {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


# FIXME Refactor test to not add column or investigate transaction
@pytest.mark.skip("Netezza does not support adding a column in a transaction block")
class TestPrePostSnapshotHooksInConfigKwargsNetezza(
    BasePrePostSnapshotHooksInConfigKwargs
):
    pass
