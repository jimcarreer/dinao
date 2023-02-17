"""Basic test of the mariadb implementation of the primitive database interfaces."""

import uuid

from dinao.backend import create_connection_pool
from dinao.backend.base import ResultSet

import pytest

from tests.backend import mariadb_test_sql as test_sql


@pytest.mark.parametrize(
    "extra_args",
    ["", "pool_size=10", "pool_name=test-pool-name"],
)
def test_backend_impls(tmp_maria_db_url: str, extra_args: str):
    """Tests the basic backend implementations for postgres."""
    cnx_pool = create_connection_pool(f"{tmp_maria_db_url}{'?'+extra_args if extra_args else ''}")
    assert "?" == cnx_pool.mung_symbol
    cnx = cnx_pool.lease()
    cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == cnx.execute(test_sql.SIMPLE_INSERT, values)
    with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        res: ResultSet = res
        row = res.fetchone()
        assert ["my_pk_col", "some_uuid", "col_big_int", "col_int"] == [r.name for r in res.description]
        assert (7, 14) == (row[2], row[3])
        assert 2 == len(res.fetchall())
    cnx_pool.release(cnx)
    cnx_pool.dispose()
