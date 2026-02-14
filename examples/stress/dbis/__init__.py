"""Dialect-specific database interface modules for the stress example.

Each supported backend has a dedicated sync and async DBI module with
SQL tailored to the dialect, eliminating the need for runtime template
substitution of dialect-specific fragments.
"""

from . import mariadb_sync, mysql_sync, postgres_async, postgres_sync, sqlite_async, sqlite_sync

_SYNC_BACKENDS = {
    "sqlite": sqlite_sync,
    "postgres": postgres_sync,
    "mariadb": mariadb_sync,
    "mysql": mysql_sync,
}

_ASYNC_BACKENDS = {
    "sqlite": sqlite_async,
    "postgres": postgres_async,
}


def load_sync_dbi(backend: str):
    """Return the synchronous DBI module for the named backend.

    :param backend: backend name (e.g. ``"sqlite"``, ``"postgres"``,
        ``"mariadb"``, ``"mysql"``)
    :returns: the dialect-specific sync DBI module
    """
    return _SYNC_BACKENDS[backend]


def load_async_dbi(backend: str):
    """Return the asynchronous DBI module for the named backend.

    :param backend: backend name (``"sqlite"`` or ``"postgres"``)
    :returns: the dialect-specific async DBI module
    """
    return _ASYNC_BACKENDS[backend]
