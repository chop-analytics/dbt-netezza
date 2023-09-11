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


class TestInvalidGrantsNetezza(BaseInvalidGrants):
    pass


class TestModelGrantsNetezza(BaseModelGrants):
    pass


class TestIncrementalGrantsNetezza(BaseIncrementalGrants):
    pass


class TestSeedGrantsNetezza(BaseSeedGrants):
    pass


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
