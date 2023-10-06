import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware

from dbt.tests.util import run_dbt, check_relations_equal


@pytest.mark.skip("any_value not supported by this adapter")
class TestAnyValueNetezza(BaseAnyValue):
    pass


class TestArrayAppendNetezza(BaseArrayAppend):
    pass


class TestArrayConcatNetezza(BaseArrayConcat):
    # Override to skip data type check
    def test_expected_actual(self, project):
        run_dbt(["build"])

        # check contents equal
        check_relations_equal(project.adapter, ["expected", "actual"])


class TestArrayConstructNetezza(BaseArrayConstruct):
    pass


class TestBoolOrNetezza(BaseBoolOr):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE"}}


class TestCastBoolToTextNetezza(BaseCastBoolToText):
    pass


class TestConcatNetezza(BaseConcat):
    pass


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
class TestCurrentTimestampNetezza(BaseCurrentTimestampAware):
    pass


class TestDateTruncNetezza(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestDateAddNetezza(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestDateDiffNetezza(BaseDateDiff):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestEscapeSingleQuotesNetezza(BaseEscapeSingleQuotesQuote):
    pass


class TestExceptNetezza(BaseExcept):
    pass


class TestHashNetezza(BaseHash):
    pass


class TestIntersectNetezza(BaseIntersect):
    pass


class TestLastDayNetezza(BaseLastDay):
    pass


class TestLengthNetezza(BaseLength):
    pass


class TestListaggNetezza(BaseListagg):
    pass


class TestPositionNetezza(BasePosition):
    pass


class TestReplaceNetezza(BaseReplace):
    pass


class TestRightNetezza(BaseRight):
    pass


class TestSafeCastNetezza(BaseSafeCast):
    pass


class TestSplitPartNetezza(BaseSplitPart):
    pass


class TestStringLiteralNetezza(BaseStringLiteral):
    pass
