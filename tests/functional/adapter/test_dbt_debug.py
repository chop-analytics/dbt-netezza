from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    TestDebugPostgres as BaseDebug,
    TestDebugProfileVariablePostgres as BaseDebugProfileVariable,
    TestDebugInvalidProjectPostgres as BaseDebugInvalidProject,
)


class TestDebugNetezza(BaseDebug):
    pass


class TestDebugProfileVariableNetezza(BaseDebugProfileVariable):
    pass


class TestDebugInvalidProjectNetezza(BaseDebugInvalidProject):
    pass
