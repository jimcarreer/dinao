"""Implementation of postgres backends."""

from dinao.backend.base import Connection, ConnectionPool
from dinao.backend.errors import BackendNotInstalledError, ConfigurationError


class ConnectionPSQLPsycopg2(Connection):
    """Implementation of Connection for Psycopg2."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        cursor.execute(query=sql, vars=params)


class ConnectionPoolPSQLPsycopg2(ConnectionPool):
    """Implementation of ConnectionPool for Psycopg2."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgres+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supported `optional_args` include:

            * schema, a list of strings that sets the search path of the connections, defaults to "public"
            * pool_min_conn, an integer specifying the minimum connections to keep in the pool, defaults to 1
            * pool_max_conn, an integer specifying the maximum connections to keep in the pool, defaults to 1
            * pool_threaded, a boolean specifying a threaded pool should be used, defaults to False

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendNotInstalledError
        """
        super().__init__(db_url)
        try:
            import psycopg2.pool
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module psycopg2 not installed, cannot create connection pool"
            raise BackendNotInstalledError(issue)
        self._cnx_kwargs = self._make_cnx_kwargs()
        self._pool_class = psycopg2.pool.SimpleConnectionPool
        if self._get_arg("pool_threaded", bool, False):
            self._pool_class = psycopg2.pool.ThreadedConnectionPool
        self._pool_impl = None
        self._raise_for_unexpected_args()
        self._pool = self._pool_class(**self._cnx_kwargs)

    def _make_cnx_kwargs(self):
        dbname = self._db_url.path.strip("/")
        if not dbname:
            raise ConfigurationError("Database name is required but missing")
        schema = ",".join(self._get_arg("schema", list, ["public"]))
        max_c = self._get_arg("pool_max_conn", int, 1)
        min_c = self._get_arg("pool_min_conn", int, 1)
        if max_c < min_c:
            raise ConfigurationError("The argument pool_max_conn must be greater or equal to pool_min_conn")
        return {
            "maxconn": max_c,
            "minconn": min_c,
            "dbname": dbname,
            "user": self._db_url.username,
            "password": self._db_url.password,
            "host": self._db_url.hostname,
            "port": self._db_url.port,
            "options": f"-c search_path={schema}",
        }

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = self._pool.getconn()
        return ConnectionPSQLPsycopg2(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        self._pool.putconn(cnx._cnx)

    def dispose(self):  # noqa: D102
        if not self._pool.closed:
            self._pool.closeall()

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"
