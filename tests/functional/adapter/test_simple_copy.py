from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)
from dbt.tests.adapter.simple_copy.test_copy_uppercase import (
    TestSimpleCopyUppercase as BaseSimpleCopyUppercase,
)


class TestSimpleCopyNetezza(SimpleCopyBase):
    pass


class TestEmptyModelsArentRunNetezza(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyUppercaseNetezza(BaseSimpleCopyUppercase):
    pass
