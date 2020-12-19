"""Implementation of postgres backends."""

from urllib.parse import parse_qs, urlparse

from dinao.backend.base import Connection, ConnectionPool
from dinao.backend.errors import BackendEngineNotInstalled, ConfigurationError


class ConnectionPSQLPsycopg2(Connection):
    """Implementation of Connection for Psycopg2."""

    def _execute(self, cursor, sql: str, params: tuple = None):
        cursor.execute(query=sql, vars=params)


class ConnectionPoolPSQLPsycopg2(ConnectionPool):
    """Implementation of ConnectionPool for Psycopg2."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "postgres+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}?{optional_ars}"

        Supported `optional_ars` include:

            * schema, sets the search path of the connections, defaults to "public"
            * pool_min_conn, an integer specifying the minimum connections to keep in the pool, defaults to 1
            * pool_max_conn, an integer specifying the maximum connections to keep in the pool, defaults to 5

        :param db_url: a url with the described format
        :raises: ConfigurationError, BackendEngineNotInstalled
        """
        super().__init__(db_url)
        try:
            import psycopg2.pool
        except ModuleNotFoundError:  # pragma: no cover
            issue = "Module psycopg2 not installed, cannot create connection pool"
            raise BackendEngineNotInstalled(issue)
        self._cnx_kwargs = self._url_to_cnx_kwargs(db_url)
        self._pool = psycopg2.pool.ThreadedConnectionPool(**self._cnx_kwargs)

    def _url_to_cnx_kwargs(self, url: str):
        db_url = urlparse(url)
        dbname = db_url.path.strip("/")
        if not dbname:
            raise ConfigurationError("Database name is required but missing")
        additional_args = parse_qs(db_url.query)
        if "schema" not in additional_args:
            self.logger.debug("Schema not specified, defaulting to public")
        schema = ",".join(additional_args.get("schema", ["public"]))
        max_c = self._parse_additional_arg_int("pool_max_conn", 10, additional_args)
        min_c = self._parse_additional_arg_int("pool_min_conn", 1, additional_args)
        if max_c < min_c:
            raise ConfigurationError("The argument pool_max_conn must be greater or equal to pool_min_conn")
        self._raise_for_unexpected_ars(["pool_max_conn", "pool_min_conn", "schema"], additional_args)
        return {
            "maxconn": max_c,
            "minconn": min_c,
            "dbname": dbname,
            "user": db_url.username,
            "password": db_url.password,
            "host": db_url.hostname,
            "port": db_url.port,
            "options": f"-c search_path={schema}",
        }

    def lease(self) -> Connection:  # noqa: D102
        inner_cnx = self._pool.getconn()
        return ConnectionPSQLPsycopg2(inner_cnx)

    def release(self, cnx: Connection):  # noqa: D102
        self._pool.putconn(cnx._cnx)

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"
