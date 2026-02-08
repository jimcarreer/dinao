"""Functionality abstracting the primitive database backend interface."""

from urllib.parse import urlparse

from dinao.backend.base import (
    AsyncConnection,
    AsyncConnectionPool,
    AsyncResultSet,
    Connection,
    ConnectionBase,
    ConnectionPool,
    ConnectionPoolBase,
    ResultSet,
)
from dinao.backend.errors import ConfigurationError, UnsupportedBackendError
from dinao.backend.mariadb import ConnectionPoolMariaDB
from dinao.backend.mysql import ConnectionPoolMySQL
from dinao.backend.postgres import (
    AsyncConnectionPoolPSQLPsycopg3,
    ConnectionPoolPSQLPsycopg2,
    ConnectionPoolPSQLPsycopg3,
)
from dinao.backend.sqlite import ConnectionPoolSQLite3

ENGINE_DEFAULTS = {"postgresql": "psycopg2", "sqlite3": None, "mariadb": "mariadbconnector", "mysql": "mysqlconnector"}
VALID_MODES = {"sync", "async"}


def _parse_scheme(scheme: str):
    """Parse the URL scheme into backend, engine, and mode components.

    The scheme is expected to be in the format: ``backend+engine+mode`` where mode is optional and defaults to
    ``sync``.  For example: ``postgresql+psycopg+async`` or ``postgresql+psycopg2``.

    :param scheme: the URL scheme string
    :returns: a tuple of (backend, engine, mode)
    :raises: ConfigurationError
    """
    parts = scheme.split("+")
    backend = parts[0]
    engine = ENGINE_DEFAULTS.get(backend) if len(parts) == 1 else parts[1]
    mode = "sync"
    if len(parts) == 3:
        mode = parts[2]
    elif len(parts) > 3:
        raise ConfigurationError(f"Invalid scheme '{scheme}': too many components")
    if mode not in VALID_MODES:
        raise ConfigurationError(f"Invalid mode '{mode}', must be 'sync' or 'async'")
    return backend, engine, mode


def create_connection_pool(db_url: str) -> ConnectionPoolBase:
    """Create a connection pool for the given database connection URL.

    The db_url is expected to be in the following format::

        "{db_backend}+{driver}://{username}:{password}@{hostname}:{port}/{db_name}"

    Async mode can be specified with a ``+async`` suffix on the driver::

        "{db_backend}+{driver}+async://{username}:{password}@{hostname}:{port}/{db_name}"

    With different db_backends / drivers supporting additional arguments.

    :returns: A connection pool based on the given database URL.
    :raises: ConfigurationError, UnsupportedBackendError
    """
    parsed_url = urlparse(db_url)
    scheme = parsed_url.scheme
    if not scheme:
        raise ConfigurationError("No database backend specified")
    backend, engine, mode = _parse_scheme(scheme)
    if backend == "postgresql" and engine == "psycopg2":
        if mode == "async":
            raise UnsupportedBackendError("The psycopg2 driver does not support async mode")
        return ConnectionPoolPSQLPsycopg2(db_url)
    if backend == "postgresql" and engine == "psycopg":
        if mode == "async":
            return AsyncConnectionPoolPSQLPsycopg3(db_url)
        return ConnectionPoolPSQLPsycopg3(db_url)
    if backend == "sqlite3" and engine is None:
        if mode == "async":
            raise UnsupportedBackendError("The sqlite3 backend does not support async mode")
        return ConnectionPoolSQLite3(db_url)
    if backend == "mariadb" and engine == "mariadbconnector":
        if mode == "async":
            raise UnsupportedBackendError("The mariadb backend does not support async mode")
        return ConnectionPoolMariaDB(db_url)
    if backend == "mysql" and engine == "mysqlconnector":
        if mode == "async":
            raise UnsupportedBackendError("The mysql backend does not support async mode")
        return ConnectionPoolMySQL(db_url)
    raise UnsupportedBackendError(f"The backend+engine '{scheme}' is not supported")


__all__ = [
    "AsyncConnection",
    "AsyncConnectionPool",
    "AsyncResultSet",
    "Connection",
    "ConnectionBase",
    "ConnectionPool",
    "ConnectionPoolBase",
    "ResultSet",
    "create_connection_pool",
    "errors",
]
