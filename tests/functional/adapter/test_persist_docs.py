from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocsNetezza(BasePersistDocs):
    pass


class TestPersistDocsColumnMissingNetezza(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnNetezza(BasePersistDocsCommentOnQuotedColumn):
    pass
