from dbt.adapters.sql import SQLAdapter
from dbt.adapters.netezza import NetezzaConnectionManager


class NetezzaAdapter(SQLAdapter):
    ConnectionManager = NetezzaConnectionManager

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode('utf-8')) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return f'varchar({max_len})'

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
