"""Implementation of postgres backends."""

from contextlib import contextmanager
from typing import List, Tuple
from urllib.parse import parse_qs, urlparse

from dinao.backend.base import Connection, ConnectionPool, ResultSet
from dinao.backend.errors import BackendEngineNotInstalled, ConfigurationError


class ResultSetPSQLPsycopg2(ResultSet):
    """Implementation of ResultSet for Psycopg2."""

    def __init__(self, cursor):
        """Construct a result set.

        :param cursor: the underlying psycopg2 cursor being wrapped by this object.
        """
        super().__init__()
        self._cursor = cursor

    def fetchone(self) -> tuple:  # noqa: D102
        return self._cursor.fetchone()

    def fetchall(self) -> List[tuple]:  # noqa: D102
        return self._cursor.fetchall()

    def columns(self) -> Tuple[str]:  # noqa: D102
        return tuple([str(d[0]) for d in self._cursor.description])


class ConnectionPSQLPsycopg2(Connection):
    """Implementation of Connection for Psycopg2."""

    def __init__(self, cnx, auto_commit: bool = True):
        """Construct a connection for Psycopg2.

        :param cnx: the inner Psycopg2 this object wraps
        :param auto_commit: should calls to execute() be automatically committed, defaults to True
        """
        super().__init__(auto_commit)
        self._cnx = cnx

    def commit(self):  # noqa: D102
        self._cnx.commit()

    def rollback(self):  # noqa: D102
        self._cnx.rollback()

    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSetPSQLPsycopg2:  # noqa: D102
        cursor = self._cnx.cursor()
        cursor.execute(sql, params)
        yield ResultSetPSQLPsycopg2(cursor)
        cursor.close()

    def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:  # noqa: D102
        commit = commit if commit is not None else self._auto_commit
        cursor = self._cnx.cursor()
        cursor.execute(sql, params)
        affected = cursor.rowcount
        if commit:
            self.commit()
        cursor.close()
        return affected


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

    def _parse_additional_arg_int(self, name: str, default: int, additional_args) -> int:
        if name not in additional_args:
            self.logger.debug(f'No "{name}" specified, defaulting to {default}')
            return default
        try:
            assert len(additional_args.get(name)) == 1
            return int(additional_args.get(name)[0])
        except AssertionError:
            raise ConfigurationError(f'Invalid "{name}": only a single value must be specified')
        except ValueError:
            raise ConfigurationError(f'Invalid "{name}": must be integer')

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

    def lease(self) -> ConnectionPSQLPsycopg2:  # noqa: D102
        inner_cnx = self._pool.getconn()
        return ConnectionPSQLPsycopg2(inner_cnx)

    def release(self, cnx: ConnectionPSQLPsycopg2):  # noqa: D102
        self._pool.putconn(cnx._cnx)

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"
