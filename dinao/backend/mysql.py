"""Implementation of mysql backends."""
import uuid
from contextlib import contextmanager

from dinao.backend.base import Connection, ConnectionPool, ResultSet
from dinao.backend.errors import BackendNotInstalledError, ConfigurationError, ConnectionPoolClosed


class ConnectionMySQL(Connection):
    """Implementation of Connection for MySQL Connector."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        cursor.execute(sql, params)

    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSet:
        """Execute the given SQL as a statement with the given parameters. Provide the results as context.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :returns: a result set representing the query's results
        """
        cursor = self._cnx.cursor()
        self._execute(cursor, sql, params)
        try:
            yield ResultSet(cursor)
        finally:
            cursor.reset()
            cursor.close()


class ConnectionPoolMySQL(ConnectionPool):
    """Implementation of ConnectionPool for MySQL Connector."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "mysql://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supported `optional_args` include:

            * pool_name, a string specifying a unique name for the pool, defaults to a random string
            * pool_size, an integer specifying the size of the pool, defaults to 5
            * ssl_ca, an absolute path to a certificate to verify the server with, defaults to None
            * ssl_verify_cert, a boolean specifying that the server's cert must be verified, defaults to None

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            from mysql.connector.pooling import MySQLConnectionPool
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module mariadb not installed, cannot create connection pool"
            raise BackendNotInstalledError(issue)
        self._pool_kwargs = self._make_cnx_kwargs()
        self._raise_for_unexpected_args()
        self._pool = MySQLConnectionPool(**self._pool_kwargs)
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
        kwargs = {
            "database": dbname,
            "pool_name": pool_name,
            "pool_size": pool_size,
            "user": self._db_url.username,
            "password": self._db_url.password,
            "host": self._db_url.hostname,
            "port": self._db_url.port,
        }
        ssl_ca = self._get_arg("ssl_ca", str, None)
        ssl_verify_cert = self._get_arg("ssl_verify_cert", bool, None)
        if ssl_ca is not None:
            kwargs["ssl_ca"] = ssl_ca
        if ssl_verify_cert is not None:
            kwargs["ssl_verify_cert"] = ssl_verify_cert
        return kwargs

    def lease(self) -> Connection:  # noqa: D102
        if self._closed:
            raise ConnectionPoolClosed("Pool is closed")
        inner_cnx = self._pool.get_connection()
        return ConnectionMySQL(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        cnx._cnx.close()

    def dispose(self):  # noqa: D102
        if not self._closed:
            self._closed = True

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"
