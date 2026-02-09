"""Implementation of SQLite backend using aiosqlite."""

from contextlib import asynccontextmanager

from dinao.backend.base import AsyncConnection, AsyncConnectionPool, AsyncResultSet
from dinao.backend.errors import BackendNotInstalledError
from dinao.backend.sqlite.base import ConnectionPoolSQLiteMixin
from dinao.mung import StaticMungSymbolProvider


class ConnectionAiosqlite(AsyncConnection):
    """Async Connection implementation wrapping an aiosqlite connection."""

    async def _execute(self, cursor, sql: str, params: tuple = None):
        """Execute SQL on the given cursor with optional parameters."""
        if params:
            await cursor.execute(sql, params)
            return
        await cursor.execute(sql)

    @asynccontextmanager
    async def query(self, sql: str, params: tuple = None):
        """Execute the given SQL as a statement with the given parameters. Provide the results as context.

        Overrides the base class because the aiosqlite ``connection.cursor()`` is a coroutine.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :returns: an async result set representing the query's results
        """
        cursor = await self._cnx.cursor()
        await self._execute(cursor, sql, params)
        try:
            yield AsyncResultSet(cursor)
        finally:
            await cursor.close()

    async def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:
        """Execute the given SQL and return the affected row count.

        Overrides the base class because the aiosqlite ``connection.cursor()`` is a coroutine.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :param commit: commit the changes after execution, defaults to value given in constructor
        """
        commit = commit if commit is not None else self._auto_commit
        cursor = await self._cnx.cursor()
        await self._execute(cursor, sql, params)
        affected = cursor.rowcount
        if commit:
            await self.commit()
        await cursor.close()
        return affected


class AsyncConnectionPoolAiosqlite(ConnectionPoolSQLiteMixin, AsyncConnectionPool):
    """Async ConnectionPool implementation for aiosqlite."""

    _mung_symbol = StaticMungSymbolProvider("?")

    def __init__(self, db_url: str):
        """Construct an async connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "sqlite3+aiosqlite://{filename}"

        This is not a real pool; each call to ``lease()`` creates a new connection.

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import aiosqlite  # noqa: F401
        except ModuleNotFoundError:  # pragma: no cover
            raise BackendNotInstalledError("Module aiosqlite not installed, cannot create async connection pool")
        self._cnx_kwargs = self._url_to_cnx_kwargs()
        self._raise_for_unexpected_args()

    async def lease(self):  # noqa: D102
        import aiosqlite

        inner_cnx = await aiosqlite.connect(**self._cnx_kwargs)
        return ConnectionAiosqlite(inner_cnx)

    async def release(self, cnx):  # noqa: D102
        await cnx._cnx.close()

    async def dispose(self):  # noqa: D102
        return

    @property
    def mung_symbol(self) -> StaticMungSymbolProvider:  # noqa: D102
        return self._mung_symbol
