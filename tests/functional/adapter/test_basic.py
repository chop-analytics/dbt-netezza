import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


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


class TestIncrementalNetezza(BaseIncremental):
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
