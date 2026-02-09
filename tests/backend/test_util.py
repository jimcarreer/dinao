"""Miscellaneous tests for backend utilities."""

import ssl

from dinao.backend import create_connection_pool
from dinao.backend.errors import ConfigurationError, UnsupportedBackendError
from dinao.backend.postgres import (
    AsyncConnectionPoolPSQLAsyncpg,
    AsyncConnectionPoolPSQLPsycopg3,
    ConnectionPoolPSQLPsycopg3,
)

import pytest


@pytest.mark.parametrize(
    "db_uri, match, except_class",
    [
        ("://user:pass@host:4444", "No database backend specified", ConfigurationError),
        ("oracle://user:pass@host:4444", "not supported", UnsupportedBackendError),
        ("postgresql+psycopg3://user:pass@host:4444", "not supported", UnsupportedBackendError),
        ("postgresql://user:pass@host:4444", "name is required but missing", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?pool_max_conn=ABC", "must be int", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?pool_min_conn=ABC", "must be int", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?pool_threaded=ABC", "must be bool", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?pool_min_conn=-1", "must be greater than 0", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?pool_max_conn=0", "must be greater than 0", ConfigurationError),
        ("postgresql://user:pass@host:4444/dbname?weird=XYZ", "Unexpected argument", ConfigurationError),
        (
            "postgresql://user:pass@host:4444/dbname?weird=XYZ&schema=s1&schema=s2&schema=s3",
            "Unexpected argument",
            ConfigurationError,
        ),
        (
            "postgresql://user:pass@host:4444/dbname?weird=JUNK&pool_threaded=True",
            "Unexpected argument",
            ConfigurationError,
        ),
        (
            "postgresql://user:pass@host:4444/dbname?pool_min_conn=2&pool_max_conn=1",
            "pool_max_conn must be greater or equal to pool_min_conn",
            ConfigurationError,
        ),
        (
            "postgresql://user:pass@host:4444/dbname?pool_min_conn=1&pool_min_conn=2",
            "single value",
            ConfigurationError,
        ),
        ("sqlite3+invalid://test.db", "not supported", UnsupportedBackendError),
        ("sqlite3://test.db?schema=test", "Unexpected argument", ConfigurationError),
        ("mariadb://user:pass@host:4444/", "name is required but missing", ConfigurationError),
        ("mariadb://user:pass@host:4444/dbname?pool_size=-1", "must be greater than 0", ConfigurationError),
        ("mariadb://user:pass@host:4444/dbname?pool_name=", "cannot be an empty string", ConfigurationError),
        ("mysql://user:pass@host:4444/", "name is required but missing", ConfigurationError),
        ("mysql://user:pass@host:4444/dbname?pool_size=-1", "must be greater than 0", ConfigurationError),
        ("mysql://user:pass@host:4444/dbname?pool_name=", "cannot be an empty string", ConfigurationError),
        ("postgresql+psycopg2+async://user:pass@host:4444/dbname", "does not support async", UnsupportedBackendError),
        ("sqlite3+none+async://test.db", "not supported", UnsupportedBackendError),
        (
            "mariadb+mariadbconnector+async://user:pass@host:4444/dbname",
            "does not support async",
            UnsupportedBackendError,
        ),
        ("mysql+mysqlconnector+async://user:pass@host:4444/dbname", "does not support async", UnsupportedBackendError),
        ("postgresql+psycopg+bogus://user:pass@host:4444/dbname", "Invalid mode", ConfigurationError),
        ("postgresql+psycopg+a+b://user:pass@host:4444/dbname", "too many components", ConfigurationError),
        ("postgresql+asyncpg+sync://user:pass@host:4444/dbname", "does not support sync", UnsupportedBackendError),
    ],
)
def test_backend_create_rejection(db_uri: str, match: str, except_class):
    """Tests bad db_url are rejected by create_connection_pool."""
    with pytest.raises(except_class, match=match):
        create_connection_pool(db_uri)


def test_async_pool_creation():
    """Tests that async URL mode creates the correct pool type."""
    pool = create_connection_pool("postgresql+psycopg+async://user:pass@host:5432/dbname")
    assert isinstance(pool, AsyncConnectionPoolPSQLPsycopg3)
    # Sync mode should create the sync pool
    pool2 = create_connection_pool("postgresql+psycopg+sync://user:pass@host:5432/dbname")
    assert isinstance(pool2, ConnectionPoolPSQLPsycopg3)


def test_asyncpg_pool_creation():
    """Tests that asyncpg async URL creates the correct pool type."""
    pool = create_connection_pool("postgresql+asyncpg+async://user:pass@host:5432/dbname")
    assert isinstance(pool, AsyncConnectionPoolPSQLAsyncpg)
    # asyncpg should default to async mode when mode is omitted
    pool2 = create_connection_pool("postgresql+asyncpg://user:pass@host:5432/dbname")
    assert isinstance(pool2, AsyncConnectionPoolPSQLAsyncpg)


def test_asyncpg_pool_ssl_kwargs():
    """Tests that asyncpg pool correctly transforms SSL kwargs."""
    pool = create_connection_pool(
        "postgresql+asyncpg+async://user:pass@host:5432/dbname"
        "?sslmode=verify-full&sslrootcert=./tests/vols/tls/ca.crt"
    )
    assert isinstance(pool, AsyncConnectionPoolPSQLAsyncpg)
    assert "ssl" in pool._pool_kwargs
    ctx = pool._pool_kwargs["ssl"]
    assert isinstance(ctx, ssl.SSLContext)
    # verify-full should keep check_hostname enabled
    assert ctx.check_hostname is True


def test_asyncpg_pool_no_options():
    """Tests asyncpg pool with schema stripped from options."""
    pool = create_connection_pool("postgresql+asyncpg+async://user:pass@host:5432/dbname")
    assert isinstance(pool, AsyncConnectionPoolPSQLAsyncpg)
    assert "server_settings" in pool._pool_kwargs
