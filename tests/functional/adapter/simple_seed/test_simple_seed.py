from pathlib import Path

import pytest
from dbt.tests.util import copy_file
from dbt.tests.adapter.simple_seed.test_seed import (
    SeedTestBase,
    SeedConfigBase,
    TestBasicSeedTests as BaseBasicSeedTests,
    TestSeedConfigFullRefreshOff as BaseSeedConfigFullRefreshOff,
    TestSeedConfigFullRefreshOn as BaseSeedConfigFullRefreshOn,
    TestSeedCustomSchema as BaseSeedCustomSchema,
    TestSeedParsing as BaseSeedParsing,
    TestSeedSpecificFormats as BaseSeedSpecificFormats,
    TestSimpleSeedEnabledViaConfig as BaseSimpleSeedEnabledViaConfig,
    TestSimpleSeedWithBOM as BaseSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.seeds import seeds__expected_sql
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
)


def _get_expected_sql():
    # Replace TEXT and TIMESTAMP WITHOUT TIME ZONE types
    expected_sql = seeds__expected_sql.replace("TEXT", "VARCHAR(2000)")
    expected_sql = expected_sql.replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")

    # Replace a single insert with multiple values with multiple insert statements
    lines = expected_sql.split("\n")
    insert_index = [lines.index(line) for line in lines if line.startswith("INSERT")][0]
    insert_end = insert_index + 3
    insert_into_lines = lines[insert_index:insert_end]
    insert_into_lines[1] = insert_into_lines[1].replace('"', "")
    new_lines = []
    for line in lines[insert_end:]:
        if line.strip():
            new_lines.extend([*insert_into_lines, line[:-1] + ";"])
    expected_sql = "\n".join(lines[:insert_index] + new_lines)

    return expected_sql


class NetezzaSeedConfigBase(SeedConfigBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"quote_columns": False, "datetimedelim": " "}}

    @pytest.fixture(scope="class")
    def project_cleanup_extra_relations(self):
        return [("table", "seed_expected")]

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(_get_expected_sql())


class NetezzaSeedTestBase(NetezzaSeedConfigBase, SeedTestBase):
    pass


class TestBasicSeedTestsNetezza(NetezzaSeedTestBase, BaseBasicSeedTests):
    pass


class TestSeedConfigFullRefreshOffNetezza(
    NetezzaSeedTestBase, BaseSeedConfigFullRefreshOff
):
    pass


class TestSeedConfigFullRefreshOnNetezza(
    NetezzaSeedTestBase, BaseSeedConfigFullRefreshOn
):
    pass


class TestSeedCustomSchemaNetezza(NetezzaSeedTestBase, BaseSeedCustomSchema):
    pass


class TestSeedParsingNetezza(NetezzaSeedConfigBase, BaseSeedParsing):
    pass


class TestSeedSpecificFormatsNetezza(NetezzaSeedConfigBase, BaseSeedSpecificFormats):
    pass


class TestSimpleSeedEnabledViaConfigNetezza(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE", "datetimedelim": " "}}


class TestSimpleSeedWithBOMNetezza(NetezzaSeedConfigBase, BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(_get_expected_sql())
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )

    pass


class TestSimpleSeedColumnOverrideNetezza(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE", "datetimedelim": " "}}
