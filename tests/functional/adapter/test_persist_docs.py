import json

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.tests.util import run_dbt


class NetezzaPersistDocsBase(BasePersistDocsBase):
    # Override to change the column names to uppercase
    def _assert_has_table_comments(self, table_node):
        table_node["columns"]["id"] = table_node["columns"]["ID"]
        table_node["columns"]["name"] = table_node["columns"]["NAME"]
        super()._assert_has_table_comments(table_node)

    # Override to change the column names to uppercase
    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_node["columns"]["id"] = view_node["columns"]["ID"]
        view_node["columns"]["name"] = view_node["columns"]["NAME"]
        super()._assert_has_view_comments(
            view_node, has_node_comments, has_column_comments
        )


class TestPersistDocsNetezza(NetezzaPersistDocsBase, BasePersistDocs):
    pass


class TestPersistDocsColumnMissingNetezza(BasePersistDocsColumnMissing):
    # Override to change the column name to uppercase
    def test_missing_column(self, project):
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data

        table_node = catalog_data["nodes"]["model.test.missing_column"]
        table_id_comment = table_node["columns"]["ID"]["comment"]
        assert table_id_comment.startswith("test id column description")


class TestPersistDocsCommentOnQuotedColumnNetezza(BasePersistDocsCommentOnQuotedColumn):
    pass
