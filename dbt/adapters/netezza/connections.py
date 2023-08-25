from contextlib import contextmanager
from dataclasses import dataclass
from re import L
from typing import Optional, Tuple, Any
import time
from dataclasses import dataclass
from typing import Optional

from dbt.exceptions import RuntimeException, FailedToConnectException
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

    host: str
    username: str
    password: str
    port: Port = Port(5480)
    dsn: Optional[str] = None

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
            logger.error(f"Netezza error: {e}")
            try:
                self.rollback_if_open()
            except pyodbc.DatabaseError:
                logger.error("Failed to release connection!")

        except Exception as e:
            logger.error(f"Error running SQL: {sql}")
            logger.error("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise RuntimeException(str(e)) from e

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
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

            handle = pyodbc.connect(
                UID=credentials.username,
                PWD=credentials.password,
                autocommit=True,
                **connection_args,
            )

            connection.state = "open"
            connection.handle = handle
        except Exception as e:
            logger.error(
                f"Got an error when attempting to open a netezza connection '{e}'"
            )
            connection.state = "fail"
            connection.handle = None
            raise FailedToConnectException() from e
        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse object
        that has items such as code, rows_affected, etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        if not len(cursor.messages):
            return AdapterResponse("OK")
        last_code, last_message = cursor.messages[-1]
        return AdapterResponse(last_message, last_code, cursor.rowcount)

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.handle.close()

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

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre), 2),
                )
            )

            return connection, cursor
