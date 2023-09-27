import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name
from dbt.contracts.results import RunStatus
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    seeds_added_csv,
    schema_base_yml,
    config_materialized_incremental,
)

incremental_sql = """
{{ config(materialized="incremental", unique_key="id") }}
select 
    id, 
    name::varchar(255) as name, 
    some_date 
from 
    {{ source('raw', 'seed') }}
""".strip()


class TestSimpleMaterializationsNetezza(BaseSimpleMaterializations):
    pass


class TestSingularTestsNetezza(BaseSingularTests):
    pass


class TestSingularTestsEphemeralNetezza(BaseSingularTestsEphemeral):
    pass


class TestEmptyNetezza(BaseEmpty):
    pass


class TestEphemeralNetezza(BaseEphemeral):
    pass


class TestIncrementalNetezza:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["-d", "run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])


class TestIncrementalNotSchemaChangeNetezza(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsNetezza(BaseGenericTests):
    pass


class TestSnapshotCheckColsNetezza(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampNetezza(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodNetezza(BaseAdapterMethod):
    def test_adapter_methods(self, project, equal_tables):
        with pytest.raises(RuntimeError, match="does not support"):
            super().test_adapter_methods(project, equal_tables)


class TestDocsGenerateNetezza(BaseDocsGenerate):
    pass


class TestValidateConnectionNetezza(BaseValidateConnection):
    pass
