"""Implementation of PostgreSQL backend using psycopg (v3)."""

from dinao.backend.base import AsyncConnection, Connection
from dinao.backend.errors import BackendNotInstalledError
from dinao.backend.postgres.base import (
    AsyncConnectionPSQL,
    AsyncConnectionPoolPSQL,
    ConnectionPSQL,
    ConnectionPoolPSQL,
)


class ConnectionPoolPSQLPsycopg3(ConnectionPoolPSQL):
    """Implementation of ConnectionPool for psycopg (v3)."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgresql+psycopg://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supports the common PostgreSQL optional_args (schema, pool_min_conn,
        pool_max_conn, sslmode, sslrootcert).

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import psycopg_pool  # pylint: disable=import-outside-toplevel
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module psycopg-pool not installed, cannot create connection pool"
            raise BackendNotInstalledError(issue)
        self._raise_for_unexpected_args()
        self._pool = psycopg_pool.ConnectionPool(
            min_size=self._pool_min_conn,
            max_size=self._pool_max_conn,
            kwargs=self._cnx_kwargs,
            open=True,
        )

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = self._pool.getconn()
        return ConnectionPSQL(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        self._pool.putconn(cnx._cnx)

    def dispose(self):  # noqa: D102
        if not self._pool._closed:
            self._pool.close()


class AsyncConnectionPoolPSQLPsycopg3(AsyncConnectionPoolPSQL):
    """Async implementation of ConnectionPool for psycopg (v3)."""

    def __init__(self, db_url: str):
        """Construct an async connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgresql+psycopg+async://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supports the common PostgreSQL optional_args (schema, pool_min_conn,
        pool_max_conn, sslmode, sslrootcert).

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import psycopg_pool  # pylint: disable=import-outside-toplevel
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module psycopg-pool not installed, cannot create async connection pool"
            raise BackendNotInstalledError(issue)
        self._raise_for_unexpected_args()
        self._pool = psycopg_pool.AsyncConnectionPool(
            min_size=self._pool_min_conn,
            max_size=self._pool_max_conn,
            kwargs=self._cnx_kwargs,
            open=False,
        )

    async def _open_pool(self):
        """Open the async connection pool if not already open."""
        if self._pool._closed:
            await self._pool.open()

    async def lease(self) -> AsyncConnection:  # noqa: D102
        await self._open_pool()
        inner_cnx = await self._pool.getconn()
        return AsyncConnectionPSQL(inner_cnx)

    async def release(self, cnx: AsyncConnection):  # noqa: D102
        await self._pool.putconn(cnx._cnx)

    async def dispose(self):  # noqa: D102
        if not self._pool._closed:
            await self._pool.close()
