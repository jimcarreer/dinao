"""Miscellaneous tests for backend utilities."""

from dinao.backend import create_connection_pool
from dinao.backend.errors import ConfigurationError, UnsupportedBackendError

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
    ],
)
def test_backend_create_rejection(db_uri: str, match: str, except_class):
    """Tests bad db_url are rejected by create_connection_pool."""
    with pytest.raises(except_class, match=match):
        create_connection_pool(db_uri)
