"""Implementation of mariadb backends."""
import uuid

from dinao.backend.base import Connection, ConnectionPool
from dinao.backend.errors import BackendNotInstalledError, ConfigurationError


class ConnectionMariaDB(Connection):
    """Implementation of Connection for MariaDB Connector."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        cursor.execute(sql, params)


class ConnectionPoolMariaDB(ConnectionPool):
    """Implementation of ConnectionPool for MariaDB Connector."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "mariadb://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supported `optional_args` include:

            * pool_name, a string specifying a unique name for the pool, defaults to a random string
            * pool_size, an integer specifying the size of the pool, defaults to 5

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            from mariadb import ConnectionPool
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module mariadb not installed, cannot create connection pool"
            raise BackendNotInstalledError(issue)
        self._pool_kwargs = self._make_cnx_kwargs()
        self._raise_for_unexpected_args()
        self._pool = ConnectionPool(**self._pool_kwargs)
        self._closed = False

    def _make_cnx_kwargs(self) -> dict:
        dbname = self._db_url.path.strip("/")
        if not dbname:
            raise ConfigurationError("Database name is required but missing")
        pool_name = self._get_arg("pool_name", str, f"dinao-pool-{uuid.uuid4()}")
        if not pool_name:
            raise ConfigurationError("The value for pool_name cannot be an empty string")
        pool_size = self._get_arg("pool_size", int, 5)
        if pool_size <= 0:
            raise ConfigurationError("The value for pool_size must be greater than 0")
        return {
            "database": dbname,
            "pool_name": pool_name,
            "pool_size": pool_size,
            "user": self._db_url.username,
            "password": self._db_url.password,
            "host": self._db_url.hostname,
            "port": self._db_url.port,
        }

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = self._pool.get_connection()
        return ConnectionMariaDB(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        cnx._cnx.close()

    def dispose(self):  # noqa: D102
        if not self._closed:
            self._closed = True
            self._pool.close()

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "?"
