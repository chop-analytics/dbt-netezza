import os
import re

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralErrorHandling as EphemeralErrorHandling,
)
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralMulti as EphemeralMulti,
)
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralNested as EphemeralNested,
)
from dbt.tests.util import check_relations_equal, run_dbt


class TestEphemeralErrorHandlingNetezza(EphemeralErrorHandling):
    pass


class TestEphemeralMultiNetezza(EphemeralMulti):
    # Override to change db/schema names and remove CTE '__' prefix
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        database = project.adapter.config.credentials.database
        schema = project.test_schema
        expected_sql = (
            f"create view {database}.{schema}.double_dependent__dbt_tmp as ("
            "with dbt__cte__base as ("
            f"select * from {schema}.seed"
            "),  dbt__cte__base_copy as ("
            "select * from dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralNestedNetezza(EphemeralNested):
    # Override to change db/schema names and remove CTE '__' prefix
    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        database = project.adapter.config.credentials.database
        schema = project.test_schema
        expected_sql = (
            f"create view {database}.{schema}.root_view__dbt_tmp as ("
            "with dbt__cte__ephemeral_level_two as ("
            f"select * from {database}.{schema}.source_table"
            "),  dbt__cte__ephemeral as ("
            "select * from dbt__cte__ephemeral_level_two"
            ")select * from dbt__cte__ephemeral"
            ");"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql
