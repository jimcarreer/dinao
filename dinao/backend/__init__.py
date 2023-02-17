"""Functionality abstracting the primitive database backend interface."""

from urllib.parse import urlparse

from dinao.backend.base import Connection, ConnectionPool, ResultSet
from dinao.backend.errors import ConfigurationError, UnsupportedBackendError
from dinao.backend.mariadb import ConnectionPoolMariaDB
from dinao.backend.postgres import ConnectionPoolPSQLPsycopg2
from dinao.backend.sqlite import ConnectionPoolSQLite3


ENGINE_DEFAULTS = {"postgresql": "psycopg2", "sqlite3": None, "mariadb": "mariadbconnector"}


def create_connection_pool(db_url: str) -> ConnectionPool:
    """Create a connection pool for the given database connection URL.

    The db_url is expected to be in the following format::

        "{db_backend}+{driver}://{username}:{password}@{hostname}:{port}/{db_name}"

    With different db_backends / drivers supporting additional arguments.

    :returns: A connection pool based on the given database URL.
    :raises: ConfigurationError, UnsupportedBackendError
    """
    parsed_url = urlparse(db_url)
    backend = parsed_url.scheme
    if not backend:
        raise ConfigurationError("No database backend specified")
    backend = backend.split("+")
    engine = ENGINE_DEFAULTS.get(backend[0]) if len(backend) == 1 else backend[1]
    backend = backend[0]
    if backend == "postgresql" and engine == "psycopg2":
        return ConnectionPoolPSQLPsycopg2(db_url)
    if backend == "sqlite3" and engine is None:
        return ConnectionPoolSQLite3(db_url)
    if backend == "mariadb" and engine == "mariadbconnector":
        return ConnectionPoolMariaDB(db_url)
    raise UnsupportedBackendError(f"The backend+engine '{parsed_url.scheme}' is not supported")


__all__ = ["Connection", "ConnectionPool", "ResultSet", "create_connection_pool", "errors"]
