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
    username = os.environ.get("DINAO_TEST_PSQL_USER", "psql_test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "psql_test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 15432))
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
def tmp_maria_db_url(rand_db_name) -> str:
    """Provide a DB Connection Pool URL for the test mariadb instance."""
    import mariadb
    from tests.backend import mariadb_test_sql as test_sql

    hostname = os.environ.get("DINAO_TEST_MARIA_HOST", "127.0.0.1")
    password = os.environ.get("DINAO_TEST_MARIA_PASS", "maria_test_pass")
    username = os.environ.get("DINAO_TEST_MARIA_USER", "maria_test_user")
    root_password = os.environ.get("DINAO_TEST_MARIA_ROOT_PASS", "maria_test_root_pass")
    port = int(os.getenv("DINAO_TEST_MARIA_PORT", 13306))
    cnx = mariadb.connect(user="root", password=root_password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    cursor.execute(f"GRANT ALL PRIVILEGES ON {rand_db_name}.* TO {username}")
    yield f"mariadb://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(f"{test_sql.TERMINATE_DB_CONNS} WHERE db = '{rand_db_name}'")
    for row in cursor:  # pragma: no cover
        try:
            cursor.execute(row[0])
        except mariadb.OperationalError as e:
            print(f"Ignoring operational error when killing connections: {e}")
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_sqlite3_db_url(tmpdir, rand_db_name) -> str:
    """Provide a DB Connection Pool URL for the test sqlite instance."""
    yield f"sqlite3://{tmpdir}/{rand_db_name}"
