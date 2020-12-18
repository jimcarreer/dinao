"""Basic test of the postgres implementation of the primitive database interfaces."""

import os
import uuid

from dinao.backend import create_connection_pool
from dinao.backend.base import ResultSet

import pytest

from tests.backend import postgres_test_sql as test_sql


@pytest.fixture()
def tmp_db_cnx_url() -> str:
    """Provide a DB Connection Pool URL for the test postgres instance."""
    import psycopg2

    database = os.environ.get("DINAO_TEST_PSQL_DB", "postgres")
    hostname = os.environ.get("DINAO_TEST_PSQL_HOST", "localhost")
    username = os.environ.get("DINAO_TEST_PSQL_USER", "test_user")
    password = os.environ.get("DINAO_TEST_PSQL_PASS", "test_pass")
    port = int(os.getenv("DINAO_TEST_PSQL_PORT", 5432))
    tmp_db_name = f"test_{str(uuid.uuid4()).replace('-', '')}"
    cnx = psycopg2.connect(dbname=database, user=username, password=password, host=hostname, port=port)
    cursor = cnx.cursor()
    cursor.execute("commit")
    cursor.execute(f"CREATE DATABASE {tmp_db_name}")
    yield f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{tmp_db_name}"
    cursor.execute(test_sql.TERMINATE_DB_CONNS, (tmp_db_name,))
    cursor.execute(f"DROP DATABASE {tmp_db_name}")
    cursor.close()
    cnx.close()


@pytest.mark.parametrize(
    "extra_args",
    ["", "pool_min_conn=1", "pool_min_conn=3", "pool_min_conn=1&pool_max_conn=3"],
)
def test_backend_impls(tmp_db_cnx_url: str, extra_args: str):
    """Tests the basic backend implementations for postgres."""
    cnx_pool = create_connection_pool(f"{tmp_db_cnx_url}{'?'+extra_args if extra_args else ''}")
    assert "%s" == cnx_pool.mung_symbol
    cnx = cnx_pool.lease()
    cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == cnx.execute(test_sql.SIMPLE_INSERT, values)
        cnx.commit()
    with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        res: ResultSet = res
        row = res.fetchone()
        assert ("my_pk_col", "some_uuid", "col_bigint", "col_integer") == res.columns()
        assert (7, 14) == (row[2], row[3])
        assert 2 == len(res.fetchall())
    cnx_pool.release(cnx)
