from dbt.tests.adapter.simple_seed.test_seed import (
    TestBasicSeedTests as BaseBasicSeedTests,
    TestSeedConfigFullRefreshOff as BaseSeedConfigFullRefreshOff,
    TestSeedConfigFullRefreshOn as BaseSeedConfigFullRefreshOn,
    TestSeedCustomSchema as BaseSeedCustomSchema,
    TestSeedParsing as BaseSeedParsing,
    TestSeedSpecificFormats as BaseSeedSpecificFormats,
    TestSimpleSeedEnabledViaConfig as BaseSimpleSeedEnabledViaConfig,
    TestSimpleSeedWithBOM as BaseSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
)


class TestBasicSeedTestsNetezza(BaseBasicSeedTests):
    pass


class TestSeedConfigFullRefreshOffNetezza(BaseSeedConfigFullRefreshOff):
    pass


class TestSeedConfigFullRefreshOnNetezza(BaseSeedConfigFullRefreshOn):
    pass


class TestSeedCustomSchemaNetezza(BaseSeedCustomSchema):
    pass


class TestSeedParsingNetezza(BaseSeedParsing):
    pass


class TestSeedSpecificFormatsNetezza(BaseSeedSpecificFormats):
    pass


class TestSimpleSeedEnabledViaConfigNetezza(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSimpleSeedWithBOMNetezza(BaseSimpleSeedWithBOM):
    pass


class TestSimpleSeedColumnOverrideNetezza(BaseSimpleSeedColumnOverride):
    pass
