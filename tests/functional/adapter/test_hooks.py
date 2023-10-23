from dbt.tests.adapter.hooks.test_model_hooks import (
    TestDuplicateHooksInConfigs as BaseDuplicateHooksInConfigs,
    TestHookRefs as BaseHookRefs,
    TestHooksRefsOnSeeds as BaseHooksRefsOnSeeds,
    TestPrePostModelHooks as BasePrePostModelHooks,
    TestPrePostModelHooksInConfig as BasePrePostModelHooksInConfig,
    TestPrePostModelHooksInConfigKwargs as BasePrePostModelHooksInConfigKwargs,
    TestPrePostModelHooksInConfigWithCount as BasePrePostModelHooksInConfigWithCount,
    TestPrePostModelHooksOnSeeds as BasePrePostModelHooksOnSeeds,
    TestPrePostModelHooksOnSeedsPlusPrefixed as BasePrePostModelHooksOnSeedsPlusPrefixed,
    TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace as BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    TestPrePostModelHooksOnSnapshots as BasePrePostModelHooksOnSnapshots,
    TestPrePostModelHooksUnderscores as BasePrePostModelHooksUnderscores,
    TestPrePostSnapshotHooksInConfigKwargs as BasePrePostSnapshotHooksInConfigKwargs,
)

from dbt.tests.adapter.hooks.test_run_hooks import (
    TestAfterRunHooks as BaseAfterRunHooks,
    TestPrePostRunHooks as BasePrePostRunHooks,
)


class TestDuplicateHooksInConfigsNetezza(BaseDuplicateHooksInConfigs):
    pass


class TestHookRefsNetezza(BaseHookRefs):
    pass


class TestAfterRunHooksNetezza(BaseAfterRunHooks):
    pass


class TestPrePostRunHooksNetezza(BasePrePostRunHooks):
    pass


class TestHooksRefsOnSeedsNetezza(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksNetezza(BasePrePostModelHooks):
    pass


class TestPrePostModelHooksInConfigNetezza(BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargsNetezza(BasePrePostModelHooksInConfigKwargs):
    pass


class TestPrePostModelHooksInConfigWithCountNetezza(
    BasePrePostModelHooksInConfigWithCount
):
    pass


class TestPrePostModelHooksOnSeedsNetezza(BasePrePostModelHooksOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedNetezza(
    BasePrePostModelHooksOnSeedsPlusPrefixed
):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceNetezza(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    pass


class TestPrePostModelHooksOnSnapshotsNetezza(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksUnderscoresNetezza(BasePrePostModelHooksUnderscores):
    pass


class TestPrePostSnapshotHooksInConfigKwargsNetezza(
    BasePrePostSnapshotHooksInConfigKwargs
):
    pass
