"""Shared base classes for PostgreSQL backends."""

from dinao.backend.base import Connection, ConnectionPool
from dinao.backend.errors import ConfigurationError


class ConnectionPSQL(Connection):
    """Shared Connection implementation for PostgreSQL backends."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        """Execute SQL on the given cursor with positional parameters."""
        cursor.execute(sql, params)


class ConnectionPoolPSQL(ConnectionPool):
    """Shared ConnectionPool base for PostgreSQL backends.

    Handles URL parameter parsing and validation common to all PostgreSQL drivers.
    """

    def __init__(self, db_url: str):
        """Construct a connection pool base for the given connection URL.

        The db_url is expected to be in the following format::

            "postgresql+{driver}://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        Supported `optional_args` common to all PostgreSQL drivers:

            * schema, a list of strings that sets the search path of the connections, defaults to "public"
            * pool_min_conn, an integer specifying the minimum connections to keep in the pool, defaults to 1
            * pool_max_conn, an integer specifying the maximum connections to keep in the pool, defaults to 1
            * sslmode, a string ("prefer", "verify-full", etc ...) specifying ssl mode for connections, defaults to None
            * sslrootcert, a string specifying a path to a root CA to use for ssl verification, defaults to None

        :param db_url: a url with the described format
        :raises: ConfigurationError
        """
        super().__init__(db_url)
        self._cnx_kwargs = self._make_cnx_kwargs()

    def _make_cnx_kwargs(self):
        dbname = self._db_url.path.strip("/")
        if not dbname:
            raise ConfigurationError("Database name is required but missing")
        schema = ",".join(self._get_arg("schema", list, ["public"]))
        self._pool_min_conn = self._get_arg("pool_min_conn", int, 1)
        self._pool_max_conn = self._get_arg("pool_max_conn", int, self._pool_min_conn)
        if self._pool_min_conn <= 0 or self._pool_max_conn <= 0:
            raise ConfigurationError("The pool_max_conn and pool_min_conn must be greater than 0")
        if self._pool_max_conn < self._pool_min_conn:
            raise ConfigurationError("The argument pool_max_conn must be greater or equal to pool_min_conn")
        ssl_mode = self._get_arg("sslmode", str, None)
        ssl_root_cert = self._get_arg("sslrootcert", str, None)
        kwargs = {
            "dbname": dbname,
            "user": self._db_url.username,
            "password": self._db_url.password,
            "host": self._db_url.hostname,
            "port": self._db_url.port,
            "options": f"-c search_path={schema}",
        }
        if ssl_mode:
            kwargs["sslmode"] = ssl_mode
        if ssl_root_cert:
            kwargs["sslrootcert"] = ssl_root_cert
        return kwargs

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"
