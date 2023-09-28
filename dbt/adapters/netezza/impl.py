import agate
from dataclasses import dataclass
import os
from typing import Optional, List, Dict

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME
from dbt.adapters.netezza import NetezzaConnectionManager
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.netezza.relation import NetezzaRelation
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import CompilationError, DbtDatabaseError
from dbt.utils import filter_null_values


@dataclass
class NetezzaConfig(AdapterConfig):
    dist: Optional[str] = None


class NetezzaAdapter(SQLAdapter):
    ConnectionManager = NetezzaConnectionManager
    Relation = NetezzaRelation
    AdapterSpecificConfigs = NetezzaConfig

    @classmethod
    def date_function(cls):
        return "now()"

    # Overriding methods because Netezza uppercases by default
    # and we want to avoid quoting of columns
    # Source: https://github.com/dbt-labs/dbt-snowflake/blob/fda11c2e822519996101d2c456a51570f4ed1c04/dbt/adapters/snowflake/impl.py#L45-L54
    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
    ) -> agate.Table:
        lowered = table.rename(column_names=[c.lower() for c in table.column_names])
        return super()._catalog_filter_table(lowered, manifest)

    # Source: https://github.com/dbt-labs/dbt-snowflake/blob/fda11c2e822519996101d2c456a51570f4ed1c04/dbt/adapters/snowflake/impl.py#L56-L69
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

    # Source: https://github.com/dbt-labs/dbt-snowflake/blob/fda11c2e822519996101d2c456a51570f4ed1c04/dbt/adapters/snowflake/impl.py#L128-L166
    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> List[BaseRelation]:
        kwargs = {"schema_relation": schema_relation}
        try:
            results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except DbtDatabaseError as exc:
            # if the schema doesn't exist, we just want to return.
            # Alternatively, we could query the list of schemas before we start
            # and skip listing the missing ones, which sounds expensive.
            if "Object does not exist" in str(exc):
                return []
            raise

        relations: List[BaseRelation] = []
        quote_policy = {"database": True, "schema": True, "identifier": True}

        columns = ["DATABASE", "SCHEMA", "NAME", "TYPE"]
        for _database, _schema, _identifier, _type in results.select(columns):
            try:
                _type = self.Relation.get_relation_type(_type.lower())
            except ValueError:
                _type = self.Relation.External
            relations.append(
                self.Relation.create(
                    database=_database,
                    schema=_schema,
                    identifier=_identifier,
                    quote_policy=quote_policy,
                    type=_type,
                )
            )

        return relations

    # Override with Redshift implementation because Netezza does not support `text`
    # Source: https://github.com/dbt-labs/dbt-redshift/blob/64f6f7ba4f8fbe11d9c547f7c07faeb9b14deb83/dbt/adapters/redshift/impl.py#L54-L61
    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # `lens` must be a list, so this can't be a generator expression,
        # because max() raises an exception if its argument has no members.
        # source: https://github.com/fishtown-analytics/dbt/pull/2255/files#diff-39545f1198b754f67de59957630a527b6d1df026aff22cc90de923f5653d5ad8
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        return f"varchar({max_len})"

    # Override to remove `without time zone` because Netezza does not support this
    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    # Override to check if view exists before dropping because Netezza does not support
    # `drop view if exists`
    def drop_relation(self, relation):
        if relation.type == "view":
            identifier = relation.identifier.upper()
            relations = self.list_relations_without_caching(relation)
            no_relation_exists = (
                next(
                    rel
                    for rel in relations
                    if rel.type == "view" and rel.identifier == identifier
                )
                is None
            )

            if no_relation_exists:
                return

        super().drop_relation(relation)

    # Override to skip the cursor.commit() which causes a ODBC HY010
    # "function sequence error"
    # Source: https://github.com/dbt-labs/dbt-core/blob/c270a77552ae9fc66fdfab359d65a8db1307c3f3/core/dbt/adapters/sql/impl.py#L223-L243
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if hasattr(conn.handle, "commit"):
                # Skip cursor.commit()
                pass
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    @available
    def get_seed_file_path(self, model) -> str:
        return os.path.join(model["root_path"], model["original_file_path"])

    # Override to change the default value of quote_columns to False
    # Source: https://github.com/dbt-labs/dbt-core/blob/7f8d9a7af976f640e376900773a0d793acf3a3ce/core/dbt/adapters/base/impl.py#L812-L828
    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        # Change default value to False
        quote_columns: bool = False
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            raise CompilationError(
                f'The seed configuration value of "quote_columns" has an '
                f"invalid type {type(quote_config)}"
            )

        if quote_columns:
            return self.quote(column)
        else:
            return column

    # Override to search for uppercase keys in grants_table because Netezza always returns
    # uppercase keys and agate.Table.__get_item__ is case-sensitive
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["GRANTEE"]
            privilege = row["PRIVILEGE_TYPE"]
            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["merge", "delete+insert"]
