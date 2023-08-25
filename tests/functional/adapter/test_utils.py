import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd, seeds__data_dateadd_csv
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff, seeds__data_datediff_csv
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc, seeds__data_date_trunc_csv
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


@pytest.mark.skip("any_value not supported by this adapter")
class TestAnyValueNetezza(BaseAnyValue):
    pass


class TestBoolOrNetezza(BaseBoolOr):
    pass


class TestCastBoolToTextNetezza(BaseCastBoolToText):
    pass


class TestConcatNetezza(BaseConcat):
    pass


class TestDateTruncNetezza(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv.replace(" ", "T")}
    pass


class TestDateAddNetezza(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv.replace(" ", "T")}
    pass


class TestDateDiffNetezza(BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv.replace(" ", "T")}
    pass


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
