import pytest
from dbt.tests.adapter.simple_copy.test_copy_uppercase import (
    TestSimpleCopyUppercase as BaseSimpleCopyUppercase,
)
from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRunBase,
    SimpleCopyBase,
)
from tests.conftest import dbt_profile_target_values


class TestSimpleCopyNetezza(SimpleCopyBase):
    @pytest.mark.skip("Adapter does not support materialized views.")
    def test_simple_copy_with_materialized_views(self, project):
        pass


class TestEmptyModelsArentRunNetezza(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyUppercaseNetezza(BaseSimpleCopyUppercase):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return dbt_profile_target_values()
