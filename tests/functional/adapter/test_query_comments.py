from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsNetezza(BaseQueryComments):
    pass


class TestMacroQueryCommentsNetezza(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsNetezza(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsNetezza(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsNetezza(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsNetezza(BaseEmptyQueryComments):
    pass
