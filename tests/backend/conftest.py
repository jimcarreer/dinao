"""Helpful fixtures for testing dinao.backend functionality."""

import os
import uuid
from typing import Any, Generator

import mariadb

import psycopg

import psycopg2

import pytest

from tests.backend import mariadb_test_sql
from tests.backend import postgres_test_sql


@pytest.fixture()
def rand_db_name() -> str:
    """Generate a random database name."""
    return f"test_{str(uuid.uuid4()).replace('-', '')}"


@pytest.fixture()
def tmp_psql_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test postgres instance."""
    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "127.0.0.1")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "psql_test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "psql_test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 15432))
    cnx = psycopg2.connect(dbname=database, user=username, password=password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute("commit")
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    yield f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(postgres_test_sql.TERMINATE_DB_CONNS, (rand_db_name,))
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_psycopg3_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test postgres instance using psycopg (v3)."""
    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "127.0.0.1")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "psql_test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "psql_test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 15432))
    cnx = psycopg.connect(dbname=database, user=username, password=password, host=hostname, port=port, autocommit=True)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    yield f"postgresql+psycopg://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(postgres_test_sql.TERMINATE_DB_CONNS, (rand_db_name,))
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_psycopg3_async_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test postgres instance using psycopg (v3) in async mode."""
    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "127.0.0.1")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "psql_test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "psql_test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 15432))
    cnx = psycopg.connect(dbname=database, user=username, password=password, host=hostname, port=port, autocommit=True)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    yield f"postgresql+psycopg+async://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(postgres_test_sql.TERMINATE_DB_CONNS, (rand_db_name,))
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_maria_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test mariadb instance."""
    hostname = os.environ.get("DINAO_TEST_MARIA_HOST", "127.0.0.1")
    password = os.environ.get("DINAO_TEST_MARIA_PASS", "maria_test_pass")
    username = os.environ.get("DINAO_TEST_MARIA_USER", "maria_test_user")
    root_password = os.environ.get("DINAO_TEST_MARIA_ROOT_PASS", "maria_test_root_pass")
    port = int(os.getenv("DINAO_TEST_MARIA_PORT", 13306))
    cnx = mariadb.connect(user="root", password=root_password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    cursor.execute(f"GRANT ALL PRIVILEGES ON {rand_db_name}.* TO {username}")
    yield f"mariadb+mariadbconnector://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(f"{mariadb_test_sql.TERMINATE_DB_CONNS} WHERE db = '{rand_db_name}'")
    rows = cursor.fetchall() if cursor.field_count else []
    for row in rows:  # pragma: no cover
        try:
            cursor.execute(row[0])
        except mariadb.OperationalError as e:
            print(f"Ignoring operational error when killing connections: {e}")
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_mysql_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test mariadb instance."""
    hostname = os.environ.get("DINAO_TEST_MYSQL_HOST", "127.0.0.1")
    password = os.environ.get("DINAO_TEST_MYSQL_PASS", "mysql_test_pass")
    username = os.environ.get("DINAO_TEST_MYSQL_USER", "mysql_test_user")
    root_password = os.environ.get("DINAO_TEST_MYSQL_ROOT_PASS", "mysql_test_root_pass")
    port = int(os.getenv("DINAO_TEST_MYSQL_PORT", 23306))
    cnx = mariadb.connect(user="root", password=root_password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    cursor.execute(f"GRANT ALL PRIVILEGES ON {rand_db_name}.* TO {username}")
    yield f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(f"{mariadb_test_sql.TERMINATE_DB_CONNS} WHERE db = '{rand_db_name}'")
    rows = cursor.fetchall() if cursor.field_count else []
    for row in rows:  # pragma: no cover
        try:
            cursor.execute(row[0])
        except mariadb.OperationalError as e:
            print(f"Ignoring operational error when killing connections: {e}")
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_asyncpg_db_url(rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test postgres instance using asyncpg."""
    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "127.0.0.1")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "psql_test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "psql_test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 15432))
    cnx = psycopg.connect(dbname=database, user=username, password=password, host=hostname, port=port, autocommit=True)
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE {rand_db_name}")
    yield f"postgresql+asyncpg+async://{username}:{password}@{hostname}:{port}/{rand_db_name}"
    cursor.execute(postgres_test_sql.TERMINATE_DB_CONNS, (rand_db_name,))
    cursor.execute(f"DROP DATABASE {rand_db_name}")
    cursor.close()
    cnx.close()


@pytest.fixture()
def tmp_aiosqlite_db_url(tmpdir, rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test aiosqlite instance."""
    yield f"sqlite3+aiosqlite://{tmpdir}/{rand_db_name}"


@pytest.fixture()
def tmp_sqlite3_db_url(tmpdir, rand_db_name) -> Generator[str, Any, None]:
    """Provide a DB Connection Pool URL for the test sqlite instance."""
    yield f"sqlite3://{tmpdir}/{rand_db_name}"
