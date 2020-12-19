"""Helpful fixtures for testing dinao.backend functionality."""

import os
import uuid

import pytest


@pytest.fixture()
def rand_db_name() -> str:
    """Generate a random database name."""
    return f"test_{str(uuid.uuid4()).replace('-', '')}"


@pytest.fixture()
def tmp_psql_db_url(rand_db_name) -> str:
    """Provide a DB Connection Pool URL for the test postgres instance."""
    import psycopg2
    from tests.backend import postgres_test_sql as test_sql

    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "localhost")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 5432))
    cnx = psycopg2.connect(dbname=database, user=username, password=password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute("commit")
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    yield f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(test_sql.TERMINATE_DB_CONNS, (rand_db_name,))
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_sqlite3_db_url(tmpdir, rand_db_name) -> str:
    """Provide a DB Connection Pool URL for the test sqlite instance."""
    yield f"sqlite3://{tmpdir}/{rand_db_name}"
