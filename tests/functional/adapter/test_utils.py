import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
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


seeds__data_dateadd_csv = """from_time,interval_length,datepart,result
2018-01-01T01:00:00,1,day,2018-01-02T01:00:00
2018-01-01T01:00:00,1,month,2018-02-01T01:00:00
2018-01-01T01:00:00,1,year,2019-01-01T01:00:00
2018-01-01T01:00:00,1,hour,2018-01-01T02:00:00
,1,day,
"""


@pytest.mark.skip("any_value not supported by this adapter")
class TestAnyValueNetezza(BaseAnyValue):
    pass


class TestBoolOrNetezza(BaseBoolOr):
    pass


class TestCastBoolToTextNetezza(BaseCastBoolToText):
    pass


class TestConcatNetezza(BaseConcat):
    pass


class TestDateAddNetezza(BaseDateAdd):
    pass


class TestDateDiffNetezza(BaseDateDiff):
    pass


class TestDateTruncNetezza(BaseDateTrunc):
    pass


class TestEscapeSingleQuotesNetezza(BaseEscapeSingleQuotesQuote):
    pass


# class TestEscapeSingleQuotesNetezza(BaseEscapeSingleQuotesBackslash):
#    pass


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
