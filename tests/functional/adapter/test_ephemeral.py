from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralErrorHandling as EphemeralErrorHandling,
    TestEphemeralMulti as EphemeralMulti,
    TestEphemeralNested as EphemeralNested,
)


class TestEphemeralErrorHandlingNetezza(EphemeralErrorHandling):
    pass


class TestEphemeralMultiNetezza(EphemeralMulti):
    pass


class TestEphemeralNestedNetezza(EphemeralNested):
    pass
