from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple, Any
import time
import inspect

import agate
from dbt.clients import agate_helper
from dbt.exceptions import DbtRuntimeError, DbtDatabaseError
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager as connection_cls
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.helper_types import Port
import pyodbc

logger = AdapterLogger("Netezza")


@dataclass
class NetezzaCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    dsn: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    host: Optional[str] = None
    port: Port = Port(5480)
    retries: int = 1

    _ALIASES = {"dbname": "database", "user": "username", "pass": "password"}

    @property
    def type(self):
        """Return name of adapter."""
        return "netezza"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return (
            ("dsn", "username")
            if self.dsn
            else ("host", "port", "database", "schema", "username")
        )


class NetezzaConnectionManager(connection_cls):
    TYPE = "netezza"

    @contextmanager
    def exception_handler(self, sql):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug("Netezza error: {}", str(e))
            try:
                self.rollback_if_open()
            except pyodbc.DatabaseError:
                logger.error("Failed to release connection!")

            _, error_message = e.args
            raise DbtDatabaseError(error_message) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        connection_args = {}
        if credentials.dsn:
            connection_args = {"DSN": credentials.dsn}
        else:
            connection_args = {
                "DRIVER": "NetezzaSQL",
                "SERVER": credentials.host,
                "PORT": credentials.port,
                "DATABASE": credentials.database,
                "SCHEMA": credentials.schema,
            }

        def connect():
            handle = pyodbc.connect(
                UID=credentials.username,
                PWD=credentials.password,
                autocommit=True,
                **connection_args,
            )
            return handle

        retryable_exceptions = [
            pyodbc.OperationalError,
        ]

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.handle.close()

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse object
        that has items such as code, rows_affected, etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        if not len(cursor.messages):
            return AdapterResponse("OK", rows_affected=cursor.rowcount)
        last_code, last_message = cursor.messages[-1]
        return AdapterResponse(last_message, last_code, cursor.rowcount)

    # Override to prevent error when calling execute without bindings
    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # pyodbc cursor will fail if bindings are passed to execute and not needed
            if bindings:
                cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            # Get the result of the first non-empty result set (if any)
            while cursor.description is None:
                if not cursor.nextset():
                    break
            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre), 2),
                )
            )

            return connection, cursor

    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        name_map = {
            "int": "INTEGER",
            "str": "STRING",
            "date": "DATE",
            "datetime": "DATETIME",
            "bool": "BOOLEAN",
            "float": "FLOAT",
        }
        assert inspect.isclass(type_code)
        return name_map[type_code.__name__]

    # Override to support multiple queries
    # Source: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/adapters/sqlserver/sql_server_connection_manager.py
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            # Get the result of the first non-empty result set (if any)
            while cursor.description is None:
                if not cursor.nextset():
                    break
            table = self.get_result_from_cursor(cursor)
        else:
            table = agate_helper.empty_table()
        # Step through all result sets so we process all errors
        while cursor.nextset():
            pass
        return response, table
