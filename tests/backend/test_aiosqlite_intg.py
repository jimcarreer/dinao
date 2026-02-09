"""Basic test of the aiosqlite implementation of the primitive database interfaces."""

import uuid

from dinao.backend import create_connection_pool
from dinao.mung import StaticMungSymbolProvider

import pytest

from tests.backend import sqlite_test_sql as test_sql


@pytest.mark.asyncio
async def test_async_backend_impls(tmp_aiosqlite_db_url: str):
    """Tests the basic async backend implementations for aiosqlite."""
    cnx_pool = create_connection_pool(tmp_aiosqlite_db_url)
    assert isinstance(cnx_pool.mung_symbol, StaticMungSymbolProvider)
    cnx = await cnx_pool.lease()
    await cnx.execute(test_sql.CREATE_TABLE, commit=True)
    for x in range(10):
        values = (str(uuid.uuid4()), str(uuid.uuid4()), x, x * 2)
        assert 1 == await cnx.execute(test_sql.SIMPLE_INSERT, values)
        await cnx.commit()
    async with cnx.query(test_sql.SIMPLE_SELECT, (6,)) as res:
        row = await res.fetchone()
        assert ["my_pk_col", "some_uuid", "col_bigint", "col_integer"] == [r.name for r in res.description]
        # Access description again to exercise the cached path
        assert res.description is res.description
        assert (7, 14) == (row[2], row[3])
        remaining = await res.fetchall()
        assert 2 == len(remaining)
    await cnx_pool.release(cnx)
    await cnx_pool.dispose()


@pytest.mark.asyncio
async def test_async_transaction_mode(tmp_aiosqlite_db_url: str):
    """Tests aiosqlite connection in transaction mode (autocommit=False)."""
    cnx_pool = create_connection_pool(tmp_aiosqlite_db_url)
    cnx = await cnx_pool.lease()
    await cnx.execute(test_sql.CREATE_TABLE, commit=True)
    pk = str(uuid.uuid4())
    values = (pk, str(uuid.uuid4()), 1, 2)
    await cnx.execute(test_sql.SIMPLE_INSERT, values)
    await cnx.commit()
    # Verify data was committed by querying
    async with cnx.query(test_sql.SIMPLE_SELECT, (0,)) as res:
        row = await res.fetchone()
        assert row is not None
    # Test rollback path
    cnx.autocommit = False
    values2 = (str(uuid.uuid4()), str(uuid.uuid4()), 99, 198)
    await cnx.execute(test_sql.SIMPLE_INSERT, values2, commit=False)
    await cnx.rollback()
    # Verify the rolled-back row was not persisted
    cnx.autocommit = True
    async with cnx.query(test_sql.SIMPLE_SELECT, (50,)) as res:
        assert await res.fetchone() is None
    await cnx_pool.release(cnx)
    await cnx_pool.dispose()
