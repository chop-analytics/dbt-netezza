from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    BaseDebugProfileVariable,
)
from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    TestDebugInvalidProjectPostgres as BaseDebugInvalidProject,
)
from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    TestDebugPostgres as BaseDebug,
)


class TestDebugNetezza(BaseDebug):
    pass


class TestDebugProfileVariableNetezza(BaseDebugProfileVariable):
    pass


class TestDebugInvalidProjectNetezza(BaseDebugInvalidProject):
    pass
