"""Basic test of the asyncpg implementation of the primitive database interfaces."""

import uuid

from dinao.backend import create_connection_pool
from dinao.mung import NumberedMungSymbolProvider

import pytest

from tests.backend import asyncpg_test_sql as test_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra_args",
    [
        "",
        "pool_min_conn=1",
        "pool_min_conn=1&pool_max_conn=3",
        "sslmode=prefer",
    ],
)
async def test_async_backend_impls(tmp_asyncpg_db_url: str, extra_args: str):
    """Tests the basic async backend implementations for asyncpg."""
    cnx_pool = create_connection_pool(f"{tmp_asyncpg_db_url}{'?' + extra_args if extra_args else ''}")
    assert isinstance(cnx_pool.mung_symbol, NumberedMungSymbolProvider)
    cnx = await cnx_pool.lease()
    await cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == await cnx.execute(test_sql.SIMPLE_INSERT, values)
        await cnx.commit()
    async with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        assert 3 == res.rowcount
        row = await res.fetchone()
        assert ["my_pk_col", "some_uuid", "col_bigint", "col_integer"] == [r.name for r in res.description]
        # Access description again to exercise the cached path
        assert res.description is res.description
        assert (7, 14) == (row[2], row[3])
        remaining = await res.fetchall()
        assert 2 == len(remaining)
        # Verify fetchone returns None when exhausted
        assert await res.fetchone() is None
    await cnx_pool.release(cnx)
    await cnx_pool.dispose()
    # Disposing an already disposed pool is a no-op
    await cnx_pool.dispose()


@pytest.mark.asyncio
async def test_async_transaction_mode(tmp_asyncpg_db_url: str):
    """Tests asyncpg connection in transaction mode (autocommit=False)."""
    cnx_pool = create_connection_pool(tmp_asyncpg_db_url)
    cnx = await cnx_pool.lease()
    cnx.autocommit = False
    # This should start an explicit transaction via _ensure_transaction
    await cnx.execute(test_sql.CREATE_TABLE, commit=False)
    pk = str(uuid.uuid4())
    values = (pk, str(uuid.uuid4()), 1, 2)
    await cnx.execute(test_sql.SIMPLE_INSERT, values, commit=False)
    # Commit the transaction
    await cnx.commit()
    # Verify data was committed by querying
    async with cnx.query(test_sql.SIMPLE_SELECT, (0,)) as res:
        row = await res.fetchone()
        assert row is not None
    # Test rollback path
    await cnx.commit()
    cnx.autocommit = False
    values2 = (str(uuid.uuid4()), str(uuid.uuid4()), 99, 198)
    await cnx.execute(test_sql.SIMPLE_INSERT, values2, commit=False)
    await cnx.rollback()
    # Verify the rolled-back row was not persisted
    cnx.autocommit = True
    async with cnx.query(test_sql.SIMPLE_SELECT, (50,)) as res:
        assert await res.fetchone() is None
    # Test commit/rollback no-ops when no transaction is active
    await cnx.commit()
    await cnx.rollback()
    await cnx_pool.release(cnx)
    await cnx_pool.dispose()
