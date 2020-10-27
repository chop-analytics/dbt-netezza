import agate

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.netezza import NetezzaConnectionManager
from dbt.adapters.netezza.relation import NetezzaRelation
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import get_relation_returned_multiple_results
from dbt.utils import filter_null_values


class NetezzaAdapter(SQLAdapter):
    ConnectionManager = NetezzaConnectionManager
    Relation = NetezzaRelation

    @classmethod
    def date_function(cls):
        return 'now()'

    # overriding method because Netezza uppercases by default
    # and we want to avoid quoting of columns
    # follows https://github.com/fishtown-analytics/dbt/blob/566f78a95c03f899d740d1df4a32c4462f7e0fca/plugins/snowflake/dbt/adapters/snowflake/impl.py#L32-L56
    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
    ) -> agate.Table:
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )

        return super()._catalog_filter_table(lowered, manifest)

    # set schema, database, and identifier to upper to match Netezza behavior
    def _make_match_kwargs(self, database: str, schema: str, identifier: str):
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {"identifier": identifier, "schema": schema, "database": database}
        )

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode('utf-8')) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return f'varchar({max_len})'

    def drop_relation(self, relation):
        if relation.type == 'view':
            # Netezza does not support `drop view if exists`, so it is necessary
            # to check if the view exists before dropping
            identifier = relation.identifier.upper()
            relations = self.list_relations_without_caching(
                relation, relation.schema)
            no_relation_exists = next(
                rel for rel in relations
                if rel.type == 'view' and rel.identifier == identifier
            ) is None

            if no_relation_exists:
                return

        super().drop_relation(relation)
