from contextlib import contextmanager
from dataclasses import dataclass
from typing import List, Optional, Tuple, Any, Iterable, Dict
import pyodbc
import time

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import Connection


NETEZZA_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'dsn': {
            'type': 'string',
        },
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'username': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
    },
    'required': ('username', 'password')
}


class NetezzaCredentials(Credentials):
    SCHEMA = NETEZZA_CREDENTIALS_CONTRACT

    _ALIASES = {
        'user': 'username',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'netezza'

    def _connection_keys(self):
        return ('dsn', 'host', 'port', 'database', 'schema', 'username')


class NetezzaConnectionManager(SQLConnectionManager):
    TYPE = 'netezza'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except pyodbc.DatabaseError as e:
            logger.error(f'netezza error: {str(e)}')
            try:
                self.release()
            except pyodbc.DatabaseError:
                logger.error('Failed to release connection!')

        except Exception as e:
            logger.error("Error running SQL: {}".format(sql))
            logger.error("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise dbt.exceptions.RuntimeException(str(e))

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            connection_args = {}
            if hasattr(credentials, 'dsn'):
                connection_args = {
                    'DSN': credentials.dsn
                }
            else:
                connection_args = {
                    'DRIVER': 'NetezzaSQL',
                    'SERVER': credentials.host,
                    'PORT': credentials.port,
                    'DATABASE': credentials.database,
                    'SCHEMA': credentials.schema,
                }

            handle = pyodbc.connect(
                UID=credentials.username,
                PWD=credentials.password,
                autocommit=True,
                **connection_args
            )

            connection.state = 'open'
            connection.handle = handle
        except Exception as e:
            logger.error(f"Got an error when attempting to open a netezza "
                         "connection '{e}'")
            connection.handle = None
            connection.state = 'fail'
            raise dbt.exceptions.FailedToConnectException(str(e))
        return connection

    @classmethod
    def get_status(cls, cursor):
        # return cursor.statusmessage
        return 'ok'

    def cancel(self, connection):
        # To implement
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(f'On {connection.name}: {sql}')
            pre = time.time()

            cursor = connection.handle.cursor()

            # Driver will fail if bindings are passed to function and not needed
            if bindings:
                cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            logger.debug(
                f"SQL status: {self.get_status(cursor)} in {time.time() - pre:0.2f} seconds")

            return connection, cursor
