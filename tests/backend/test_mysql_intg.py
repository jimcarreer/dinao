"""Basic test of the mysql implementation of the primitive database interfaces."""
from pathlib import Path

from dinao.backend import create_connection_pool
from dinao.backend.base import ResultSet
from dinao.backend.errors import ConnectionPoolClosed

import pytest

from tests.backend import mysql_test_sql as test_sql

CA_PATH = Path("./vols/tls/ca.crt").absolute()


@pytest.mark.parametrize(
    "extra_args",
    [
        "",
        "pool_size=10",
        "pool_name=test-pool-name",
        f"ssl_ca={CA_PATH}&ssl_verify_cert=True",
    ],
)
def test_backend_impls(tmp_mysql_db_url: str, extra_args: str):
    """Tests the basic backend implementations for MySQL."""
    cnx_pool = create_connection_pool(f"{tmp_mysql_db_url}{'?'+extra_args if extra_args else ''}")
    assert "%s" == cnx_pool.mung_symbol
    cnx = cnx_pool.lease()
    cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (x, x * 2)
        assert 1 == cnx.execute(test_sql.SIMPLE_INSERT, values)
    with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        res: ResultSet = res
        row = res.fetchone()
        assert ["my_pk_col", "col_big_int", "col_int"] == [r.name for r in res.description]
        assert (7, 14) == (row[1], row[2])
        assert 2 == len(res.fetchall())
    cnx_pool.release(cnx)
    cnx_pool.dispose()


def test_exceptions(tmp_mysql_db_url: str):
    """Tests a closed pool complains about being closed."""
    cnx_pool = create_connection_pool(tmp_mysql_db_url)
    cnx_pool.dispose()
    with pytest.raises(ConnectionPoolClosed):
        cnx_pool.lease()
