"""Basic test of the psycopg (v3) implementation of the primitive database interfaces."""

import uuid

from dinao.backend import create_connection_pool
from dinao.backend.base import AsyncResultSet, ResultSet
from dinao.mung import StaticMungSymbolProvider

import pytest

from tests.backend import postgres_test_sql as test_sql


@pytest.mark.parametrize(
    "extra_args",
    [
        "",
        "pool_min_conn=1",
        "pool_min_conn=1&pool_max_conn=3",
        "sslmode=verify-full&sslrootcert=./tests/vols/tls/ca.crt",
        "sslmode=prefer",
    ],
)
def test_backend_impls(tmp_psycopg3_db_url: str, extra_args: str):
    """Tests the basic backend implementations for psycopg (v3)."""
    cnx_pool = create_connection_pool(f"{tmp_psycopg3_db_url}{'?'+extra_args if extra_args else ''}")
    assert isinstance(cnx_pool.mung_symbol, StaticMungSymbolProvider)
    cnx = cnx_pool.lease()
    cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == cnx.execute(test_sql.SIMPLE_INSERT, values)
        cnx.commit()
    with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        res: ResultSet = res
        row = res.fetchone()
        assert ["my_pk_col", "some_uuid", "col_bigint", "col_integer"] == [r.name for r in res.description]
        assert (7, 14) == (row[2], row[3])
        assert 2 == len(res.fetchall())
    cnx_pool.release(cnx)
    cnx_pool.dispose()
    # Disposing an already disposed pool is a no-op
    cnx_pool.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra_args",
    [
        "",
        "pool_min_conn=1",
        "pool_min_conn=1&pool_max_conn=3",
    ],
)
async def test_async_backend_impls(tmp_psycopg3_async_db_url: str, extra_args: str):
    """Tests the basic async backend implementations for psycopg (v3)."""
    cnx_pool = create_connection_pool(f"{tmp_psycopg3_async_db_url}{'?'+extra_args if extra_args else ''}")
    assert isinstance(cnx_pool.mung_symbol, StaticMungSymbolProvider)
    cnx = await cnx_pool.lease()
    await cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == await cnx.execute(test_sql.SIMPLE_INSERT, values)
        await cnx.commit()
    async with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        res: AsyncResultSet = res
        assert res.rowcount > 0
        row = await res.fetchone()
        assert ["my_pk_col", "some_uuid", "col_bigint", "col_integer"] == [r.name for r in res.description]
        # Access description again to exercise the cached path
        assert res.description is res.description
        assert (7, 14) == (row[2], row[3])
        assert 2 == len(await res.fetchall())
    # Exercise execute with commit=False and rollback
    await cnx.execute(test_sql.SIMPLE_INSERT, (str(uuid.uuid4()), str(uuid.uuid4()), 99, 198), commit=False)
    await cnx.rollback()
    async with cnx.query(test_sql.SIMPLE_SELECT, (50,)) as res:
        assert await res.fetchone() is None
    await cnx_pool.release(cnx)
    # Lease again to exercise _open_pool when pool is already open
    cnx2 = await cnx_pool.lease()
    await cnx_pool.release(cnx2)
    await cnx_pool.dispose()
    # Disposing an already disposed pool is a no-op
    await cnx_pool.dispose()
