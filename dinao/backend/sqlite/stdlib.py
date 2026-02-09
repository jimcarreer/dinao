"""Implementation of SQLite backend using sqlite3."""

import sqlite3

from dinao.backend.base import Connection, ConnectionPool
from dinao.backend.sqlite.base import ConnectionPoolSQLiteMixin
from dinao.mung import StaticMungSymbolProvider


class ConnectionSQLite3(Connection):
    """Implementation of Connection for Sqlite3."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        """Execute SQL on the given cursor with optional parameters."""
        if params:
            cursor.execute(sql, params)
            return
        cursor.execute(sql)


class ConnectionPoolSQLite3(ConnectionPoolSQLiteMixin, ConnectionPool):
    """Implementation of ConnectionPool for SQLite3."""

    _mung_symbol = StaticMungSymbolProvider("?")

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "sqlite3://{filename}"

        Obviously, this is not a real "pool", all connections given out by lease will be new connections.

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        self._cnx_kwargs = self._url_to_cnx_kwargs()
        self._raise_for_unexpected_args()

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = sqlite3.connect(**self._cnx_kwargs)
        return ConnectionSQLite3(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        cnx._cnx.close()

    def dispose(self):  # noqa: D102
        return

    @property
    def mung_symbol(self) -> StaticMungSymbolProvider:  # noqa: D102
        return self._mung_symbol
