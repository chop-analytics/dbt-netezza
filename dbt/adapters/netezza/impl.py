import agate

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.netezza import NetezzaConnectionManager
from dbt.adapters.netezza.relation import NetezzaRelation
from dbt.exceptions import get_relation_returned_multiple_results
from dbt.utils import filter_null_values
from dbt.contracts.graph.manifest import Manifest

class NetezzaAdapter(SQLAdapter):
    ConnectionManager = NetezzaConnectionManager
    Relation = NetezzaRelation

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
        ) -> agate.Table:
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        
        return super()._catalog_filter_table(lowered, manifest)

    def _make_match_kwargs(self, database, schema, identifier):
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
 
    # def _make_match_kwargs(self, database, schema, identifier):
    #     quoting = self.config.quoting

    #     return filter_null_values({
    #         'database': database,
    #         'identifier': identifier,
    #         'schema': schema,
    #     })

    # def rename_relation(self, from_relation, to_relation):
        
    #     from_relation_upper = from_relation
    #     to_relation_upper = to_relation
        
    #     super().rename_relation(from_relation_upper, to_relation_upper)


    # def get_relation(self, database, schema, identifier):
        
    #     database_upper = database.upper()
    #     schema_upper = schema.upper()
    #     identifier_upper = identifier.upper()
    #     print(f'{database} {database_upper} {schema} {schema_upper} {identifier} {identifier_upper}')

    #     super().get_relation(database_upper, schema_upper, identifier_upper)


    def drop_relation(self, relation):
        if relation.type == 'view':
            # Get all relations and check if view exists
            relations = self.list_relations_without_caching(
                relation, relation.schema)

            valid_views = filter(lambda db_relation:
                                 db_relation.type == 'view' and
                                 db_relation.name.lower() == relation.name.lower(),
                                 relations)
            if not list(valid_views):
                return

        super().drop_relation(relation)
