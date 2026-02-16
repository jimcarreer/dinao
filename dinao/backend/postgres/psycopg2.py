"""Implementation of PostgreSQL backend using psycopg2."""

from dinao.backend.base import Connection
from dinao.backend.errors import BackendNotInstalledError
from dinao.backend.postgres.base import ConnectionPSQL, ConnectionPoolPSQL


class ConnectionPoolPSQLPsycopg2(ConnectionPoolPSQL):
    """Implementation of ConnectionPool for psycopg2."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        In addition to common PostgreSQL optional_args, psycopg2 supports:

            * pool_threaded, a boolean specifying a threaded pool should be used, defaults to False

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import psycopg2.pool  # pylint: disable=import-outside-toplevel
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module psycopg2 not installed, cannot create connection pool"
            raise BackendNotInstalledError(issue)
        self._pool_class = psycopg2.pool.SimpleConnectionPool
        if self._get_arg("pool_threaded", bool, False):
            self._pool_class = psycopg2.pool.ThreadedConnectionPool
        self._raise_for_unexpected_args()
        self._pool = self._pool_class(
            minconn=self._pool_min_conn,
            maxconn=self._pool_max_conn,
            **self._cnx_kwargs,
        )

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = self._pool.getconn()
        return ConnectionPSQL(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        self._pool.putconn(cnx._cnx)

    def dispose(self):  # noqa: D102
        if not self._pool.closed:
            self._pool.closeall()
