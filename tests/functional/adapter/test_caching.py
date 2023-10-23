from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
    TestNoPopulateCache as BaseNoPopulateCache,
)


class TestNoPopulateCacheNetezza(BaseNoPopulateCache):
    pass


class TestCachingLowerCaseModelNetezza(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModelNetezza(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnlyNetezza(BaseCachingSelectedSchemaOnly):
    pass
