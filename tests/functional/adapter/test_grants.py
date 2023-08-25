import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class BaseCopyGrantsNetezza:
    # Try every test case without copy_grants enabled (default),
    # and with copy_grants enabled (this base class)
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+copy_grants": True,
            },
            "seeds": {
                "+copy_grants": True,
            },
            "snapshots": {
                "+copy_grants": True,
            }
        }


class TestInvalidGrantsNetezza(BaseInvalidGrants):
    pass

class TestModelGrantsNetezza(BaseModelGrants):
    pass

class TestModelGrantsCopyGrantsNetezza(BaseCopyGrantsNetezza, BaseModelGrants):
    pass

class TestIncrementalGrantsNetezza(BaseIncrementalGrants):
    pass

class TestIncrementalCopyGrantsNetezza(BaseCopyGrantsNetezza, BaseIncrementalGrants):
    pass

class TestSeedGrantsNetezza(BaseSeedGrants):
    pass

class TestSeedCopyGrantsNetezza(BaseCopyGrantsNetezza, BaseSeedGrants):
    pass

class TestSnapshotGrants(BaseSnapshotGrants):
    pass

class TestSnapshotCopyGrantsNetezza(BaseCopyGrantsNetezza, BaseSnapshotGrants):
    pass