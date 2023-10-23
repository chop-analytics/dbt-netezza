from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)


class TestAliasesNetezza(BaseAliases):
    pass


class TestAliasErrorsNetezza(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemasNetezza(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabasesNetezza(BaseSameAliasDifferentDatabases):
    pass
