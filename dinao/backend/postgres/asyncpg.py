"""Implementation of PostgreSQL backend using asyncpg."""

import asyncio
import re
import ssl as ssl_module
from contextlib import asynccontextmanager
from typing import List, Tuple

from dinao.backend.base import AsyncConnection, ColumnDescriptor
from dinao.backend.errors import BackendNotInstalledError
from dinao.backend.postgres.base import AsyncConnectionPoolPSQL
from dinao.mung import NumberedMungSymbolProvider


class AsyncpgResultSet:
    """Wraps asyncpg query results to conform to the result set interface."""

    def __init__(self, records: list, attributes: tuple):
        """Construct an asyncpg result set.

        :param records: a list of asyncpg Record objects returned from a query
        :param attributes: the attributes from a prepared statement describing result columns
        """
        self._records = list(records)
        self._attributes = attributes
        self._description = None
        self._index = 0

    async def fetchone(self) -> Tuple:
        """Fetch one result tuple from the result set.

        If no results are left, None is returned.

        :returns: a tuple representing a result row or None
        """
        if self._index >= len(self._records):
            return None
        record = self._records[self._index]
        self._index += 1
        return tuple(record)

    async def fetchall(self) -> List[Tuple]:
        """Fetch the remaining result tuples from the result set.

        If no results are left, an empty list is returned.

        :returns: a list of tuples that are the remaining results
        """
        remaining = self._records[self._index :]  # noqa: E203
        self._index = len(self._records)
        return [tuple(r) for r in remaining]

    @property
    def description(self) -> Tuple[ColumnDescriptor]:
        """Return a sequence of column descriptions representing the result set.

        :returns: a tuple of ColumnDescriptors
        """
        if not self._description:
            self._description = tuple(
                ColumnDescriptor(name=attr.name, type_code=attr.type.oid) for attr in self._attributes
            )
        return self._description

    @property
    def rowcount(self) -> int:
        """Return the row count of the result set.

        :returns: the integer count of the rows in the result set
        """
        return len(self._records)


class ConnectionAsyncpg(AsyncConnection):
    """Async Connection implementation wrapping an asyncpg connection."""

    def __init__(self, cnx, auto_commit: bool = True):
        """Construct an asyncpg connection wrapper.

        :param cnx: the underlying asyncpg connection
        :param auto_commit: should calls to execute() be automatically committed, defaults to True
        """
        super().__init__(cnx, auto_commit)
        self._transaction = None

    async def _ensure_transaction(self):
        """Start an explicit transaction when autocommit is disabled and no transaction is active."""
        if not self._auto_commit and self._transaction is None:
            self._transaction = self._cnx.transaction()
            await self._transaction.start()

    async def commit(self):
        """Commit the current explicit transaction if one is active."""
        if self._transaction is not None:
            await self._transaction.commit()
            self._transaction = None

    async def rollback(self):
        """Rollback the current explicit transaction if one is active."""
        if self._transaction is not None:
            await self._transaction.rollback()
            self._transaction = None

    async def _execute(self, cursor, sql: str, params: tuple = None):
        pass  # pragma: no cover

    @asynccontextmanager
    async def query(self, sql: str, params: tuple = None):
        """Execute the given SQL as a query and provide the results as context.

        :param sql: the SQL statement to execute
        :param params: the values to bind to the execution of the given SQL
        :returns: an async result set representing the query's results
        """
        await self._ensure_transaction()
        stmt = await self._cnx.prepare(sql)
        args = params if params else ()
        records = await stmt.fetch(*args)
        attributes = stmt.get_attributes()
        yield AsyncpgResultSet(records, attributes)

    async def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:
        """Execute the given SQL and return the affected row count.

        :param sql: the SQL statement to execute
        :param params: the values to bind to the execution of the given SQL
        :param commit: commit the changes after execution, defaults to value given in constructor
        """
        commit = commit if commit is not None else self._auto_commit
        await self._ensure_transaction()
        args = params if params else ()
        status = await self._cnx.execute(sql, *args)
        affected = self._parse_status(status)
        if commit:
            await self.commit()
        return affected

    @staticmethod
    def _parse_status(status: str) -> int:
        """Parse an asyncpg status string to extract the affected row count.

        :param status: the status string returned by asyncpg (e.g. "INSERT 0 1", "CREATE TABLE")
        :returns: the affected row count, or 0 if not applicable
        """
        match = re.search(r"(\d+)$", status)
        if match:
            return int(match.group(1))
        return 0


class AsyncConnectionPoolPSQLAsyncpg(AsyncConnectionPoolPSQL):
    """Async ConnectionPool implementation for asyncpg."""

    def __init__(self, db_url: str):
        """Construct an async connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgresql+asyncpg+async://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supports the common PostgreSQL optional_args (schema, pool_min_conn,
        pool_max_conn, sslmode, sslrootcert).

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import asyncpg  # noqa: F401  pylint: disable=import-outside-toplevel
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module asyncpg not installed, cannot create async connection pool"
            raise BackendNotInstalledError(issue)
        self._raise_for_unexpected_args()
        self._pool = None
        self._pool_lock = asyncio.Lock()
        self._pool_kwargs = self._make_pool_kwargs()

    def _make_pool_kwargs(self) -> dict:
        """Transform connection kwargs to asyncpg pool creation kwargs.

        :returns: a dictionary of keyword arguments for asyncpg.create_pool
        """
        kwargs = {
            "database": self._cnx_kwargs["dbname"],
            "user": self._cnx_kwargs["user"],
            "password": self._cnx_kwargs["password"],
            "host": self._cnx_kwargs["host"],
            "port": self._cnx_kwargs["port"],
            "min_size": self._pool_min_conn,
            "max_size": self._pool_max_conn,
        }
        options = self._cnx_kwargs.get("options", "")
        if options:
            kwargs["server_settings"] = {"search_path": options.replace("-c search_path=", "")}
        ssl_mode = self._cnx_kwargs.get("sslmode")
        ssl_root_cert = self._cnx_kwargs.get("sslrootcert")
        if ssl_mode or ssl_root_cert:
            ctx = ssl_module.create_default_context()
            if ssl_root_cert:
                ctx.load_verify_locations(ssl_root_cert)
            if ssl_mode == "prefer":
                ctx.check_hostname = False
                ctx.verify_mode = ssl_module.CERT_NONE
            kwargs["ssl"] = ctx
        return kwargs

    async def _ensure_pool(self):
        """Create the asyncpg pool on first use.

        Uses a lock to prevent concurrent coroutines from creating
        multiple pools, which would cause release errors when
        connections from an earlier pool are returned to a later one.
        """
        if self._pool is not None:
            return
        async with self._pool_lock:
            if self._pool is not None:  # pragma: no cover
                return
            import asyncpg  # pylint: disable=import-outside-toplevel

            self._pool = await asyncpg.create_pool(**self._pool_kwargs)

    @property
    def mung_symbol(self) -> NumberedMungSymbolProvider:  # noqa: D102
        return NumberedMungSymbolProvider(1, "$")

    async def lease(self):  # noqa: D102
        await self._ensure_pool()
        inner_cnx = await self._pool.acquire()
        return ConnectionAsyncpg(inner_cnx)

    async def release(self, cnx):  # noqa: D102
        await self._pool.release(cnx._cnx)

    async def dispose(self):  # noqa: D102
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
