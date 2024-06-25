from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingTest,
    BaseCachingUppercaseModel,
)
from dbt.tests.adapter.caching.test_caching import (
    TestNoPopulateCache as BaseNoPopulateCache,
)
from dbt.tests.util import run_dbt


class BaseCachingTestNetezza(BaseCachingTest):
    # Override to remove cache count check due to tests running with a shared schema
    # and schema casing
    def run_and_inspect_cache(self, project, run_args=None):
        run_dbt(run_args)

        # the cache was empty at the start of the run.
        # the model materialization returned an unquoted relation and added to the cache.
        adapter = project.adapter
        relation = list(adapter.cache.relations).pop()
        relation_count = len(adapter.cache.relations)
        assert relation.schema == project.test_schema.lower()

        # on the second run, dbt will find a relation in the database during cache population.
        # this relation will be quoted, because list_relations_without_caching (by default) uses
        # quote_policy = {"database": True, "schema": True, "identifier": True}
        # when adding relations to the cache.
        run_dbt(run_args)
        adapter = project.adapter
        second_relation = list(adapter.cache.relations).pop()

        assert relation_count == len(adapter.cache.relations)

        # perform a case-insensitive + quote-insensitive comparison
        for key in ["database", "schema", "identifier"]:
            assert (
                getattr(relation, key).lower() == getattr(second_relation, key).lower()
            )


class TestNoPopulateCacheNetezza(BaseCachingTestNetezza, BaseNoPopulateCache):
    pass


class TestCachingLowerCaseModelNetezza(
    BaseCachingTestNetezza, BaseCachingLowercaseModel
):
    pass


class TestCachingUppercaseModelNetezza(
    BaseCachingTestNetezza, BaseCachingUppercaseModel
):
    pass


class TestCachingSelectedSchemaOnlyNetezza(
    BaseCachingTestNetezza, BaseCachingSelectedSchemaOnly
):
    pass
