"""Integration tests for generating queries with nested and transactional scenarios.

Exercises nested generators (a generating query whose consumer calls another generating
query) and generators inside ``@binder.transaction()`` scoped functions, with various
early-break patterns, against every backend that implements real connection pooling.
"""

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import AsyncGenerator, Generator

from dinao.backend import create_connection_pool
from dinao.binding import AsyncFunctionBinder, FunctionBinder

import pytest

# -- Data model -----------------------------------------------------------


@dataclass
class GenTestRow:
    """A row from the gen_test table."""

    row_id: int
    value: str
    category: int


# -- SQL templates --------------------------------------------------------

_CREATE_TABLE = (
    "CREATE TABLE gen_test (id INTEGER NOT NULL, value TEXT NOT NULL, category INTEGER NOT NULL, PRIMARY KEY (id))"
)
_INSERT_ROW = "INSERT INTO gen_test (id, value, category) VALUES (#{row_id}, #{value}, #{category})"
_SELECT_ALL = "SELECT id AS row_id, value, category FROM gen_test ORDER BY id"
_SELECT_BY_CAT = "SELECT id AS row_id, value, category FROM gen_test WHERE category = #{category} ORDER BY id"
_SELECT_CATS = "SELECT DISTINCT category FROM gen_test ORDER BY category"
_COUNT_ROWS = "SELECT COUNT(*) FROM gen_test"

_SEED_DATA = [{"row_id": i, "value": f"item_{i}", "category": i // 10} for i in range(30)]


# -- Sync binder and bound functions --------------------------------------

_sync_binder = FunctionBinder()


@_sync_binder.execute(_CREATE_TABLE)
def _sync_create_table():
    pass


@_sync_binder.execute(_INSERT_ROW)
def _sync_insert_row(row_id: int, value: str, category: int) -> int:
    pass


@_sync_binder.transaction()
def _sync_seed_data(rows: list):
    for row in rows:
        _sync_insert_row(**row)


@_sync_binder.query(_SELECT_ALL)
def _sync_stream_all() -> Generator[GenTestRow, None, None]:
    pass


@_sync_binder.query(_SELECT_BY_CAT)
def _sync_stream_by_category(category: int) -> Generator[GenTestRow, None, None]:
    pass


@_sync_binder.query(_SELECT_CATS)
def _sync_stream_categories() -> Generator[int, None, None]:
    pass


@_sync_binder.query(_COUNT_ROWS)
def _sync_count_rows() -> int:
    pass


@_sync_binder.transaction()
def _sync_run_in_transaction(work):
    return work()


# -- Async binder and bound functions -------------------------------------

_async_binder = AsyncFunctionBinder()


@_async_binder.execute(_CREATE_TABLE)
async def _async_create_table():
    pass


@_async_binder.execute(_INSERT_ROW)
async def _async_insert_row(row_id: int, value: str, category: int) -> int:
    pass


@_async_binder.transaction()
async def _async_seed_data(rows: list):
    for row in rows:
        await _async_insert_row(**row)


@_async_binder.query(_SELECT_ALL)
async def _async_stream_all() -> AsyncGenerator[GenTestRow, None]:
    pass


@_async_binder.query(_SELECT_BY_CAT)
async def _async_stream_by_category(category: int) -> AsyncGenerator[GenTestRow, None]:
    pass


@_async_binder.query(_SELECT_CATS)
async def _async_stream_categories() -> AsyncGenerator[int, None]:
    pass


@_async_binder.query(_COUNT_ROWS)
async def _async_count_rows() -> int:
    pass


@_async_binder.transaction()
async def _async_run_in_transaction(work):
    return await work()


# -- Fixtures -------------------------------------------------------------


@contextmanager
def _sync_db_lifecycle(url):
    pool = create_connection_pool(url)
    _sync_binder.pool = pool
    _sync_create_table()
    _sync_seed_data(_SEED_DATA)
    try:
        yield
    finally:
        pool.dispose()
        _sync_binder._cnx_pool = None


@pytest.fixture(
    params=[
        pytest.param("tmp_psql_db_url", id="psycopg2"),
        pytest.param("tmp_psycopg3_db_url", id="psycopg3"),
        pytest.param("tmp_mysql_db_url", id="mysql"),
        pytest.param("tmp_maria_db_url", id="mariadb"),
    ]
)
def sync_db(request):
    """Provide a sync pooled database seeded with test data."""
    url = request.getfixturevalue(request.param)
    with _sync_db_lifecycle(url):
        yield


@asynccontextmanager
async def _async_db_context(url):
    pool = create_connection_pool(f"{url}?pool_min_conn=2&pool_max_conn=4")
    _async_binder.pool = pool
    await _async_create_table()
    await _async_seed_data(_SEED_DATA)
    try:
        yield
    finally:
        await pool.dispose()
        _async_binder._cnx_pool = None


@pytest.fixture(
    params=[
        pytest.param("tmp_asyncpg_db_url", id="asyncpg"),
        pytest.param("tmp_psycopg3_async_db_url", id="psycopg3-async"),
    ]
)
def async_db_url(request):
    """Provide the database URL for an async pooled backend."""
    return request.getfixturevalue(request.param)


# -- Sync tests: standalone nested generators -----------------------------


def test_sync_nested_generators_full_exhaust(sync_db):
    """Test nested generators both fully consumed with sync backends."""
    results = []
    for cat in _sync_stream_categories():
        for row in _sync_stream_by_category(cat):
            results.append(row)
    assert len(results) == 30
    assert _sync_count_rows() == 30


def test_sync_nested_generators_inner_early_break(sync_db):
    """Test nested generators where the inner generator breaks early."""
    results = []
    for cat in _sync_stream_categories():
        count = 0
        for row in _sync_stream_by_category(cat):  # pragma: no branch
            results.append(row)
            count += 1
            if count >= 2:
                break
    assert len(results) == 6
    assert _sync_count_rows() == 30


def test_sync_nested_generators_outer_early_break(sync_db):
    """Test nested generators where the outer generator breaks early."""
    results = []
    for cat in _sync_stream_categories():  # pragma: no branch
        for row in _sync_stream_by_category(cat):
            results.append(row)
        break
    assert len(results) == 10
    assert _sync_count_rows() == 30


def test_sync_nested_generators_both_early_break(sync_db):
    """Test nested generators where both generators break early."""
    results = []
    for cat in _sync_stream_categories():  # pragma: no branch
        count = 0
        for row in _sync_stream_by_category(cat):  # pragma: no branch
            results.append(row)
            count += 1
            if count >= 2:
                break
        break
    assert len(results) == 2
    assert _sync_count_rows() == 30


# -- Sync tests: generators inside transactions ---------------------------


def test_sync_generator_in_transaction_full_exhaust(sync_db):
    """Test a single generator fully consumed inside a sync transaction."""

    def work():
        results = []
        for row in _sync_stream_all():
            results.append(row)
        return results, _sync_count_rows()

    results, txn_count = _sync_run_in_transaction(work)
    assert len(results) == 30
    assert txn_count == 30
    assert _sync_count_rows() == 30


def test_sync_generator_in_transaction_early_break(sync_db):
    """Test a single generator with early break inside a sync transaction."""

    def work():
        results = []
        for row in _sync_stream_all():  # pragma: no branch
            results.append(row)
            if len(results) >= 5:
                break
        return results, _sync_count_rows()

    results, txn_count = _sync_run_in_transaction(work)
    assert len(results) == 5
    assert txn_count == 30
    assert _sync_count_rows() == 30


def test_sync_nested_gen_in_transaction_full_exhaust(sync_db):
    """Test nested generators fully consumed inside a sync transaction."""

    def work():
        results = []
        for cat in _sync_stream_categories():
            for row in _sync_stream_by_category(cat):
                results.append(row)
        return results, _sync_count_rows()

    results, txn_count = _sync_run_in_transaction(work)
    assert len(results) == 30
    assert txn_count == 30
    assert _sync_count_rows() == 30


def test_sync_nested_gen_in_transaction_both_early_break(sync_db):
    """Test nested generators both breaking early inside a sync transaction."""

    def work():
        results = []
        for cat in _sync_stream_categories():  # pragma: no branch
            count = 0
            for row in _sync_stream_by_category(cat):  # pragma: no branch
                results.append(row)
                count += 1
                if count >= 2:
                    break
            break
        return results, _sync_count_rows()

    results, txn_count = _sync_run_in_transaction(work)
    assert len(results) == 2
    assert txn_count == 30
    assert _sync_count_rows() == 30


# -- Async tests: standalone nested generators ----------------------------


@pytest.mark.asyncio
async def test_async_nested_generators_full_exhaust(async_db_url):
    """Test nested generators both fully consumed with async backends."""
    async with _async_db_context(async_db_url):
        results = []
        async for cat in _async_stream_categories():
            async for row in _async_stream_by_category(cat):
                results.append(row)
        assert len(results) == 30
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_nested_generators_inner_early_break(async_db_url):
    """Test nested generators where the inner generator breaks early."""
    async with _async_db_context(async_db_url):
        results = []
        async for cat in _async_stream_categories():
            count = 0
            async for row in _async_stream_by_category(cat):  # pragma: no branch
                results.append(row)
                count += 1
                if count >= 2:
                    break
        assert len(results) == 6
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_nested_generators_outer_early_break(async_db_url):
    """Test nested generators where the outer generator breaks early."""
    async with _async_db_context(async_db_url):
        results = []
        async for cat in _async_stream_categories():  # pragma: no branch
            async for row in _async_stream_by_category(cat):
                results.append(row)
            break
        assert len(results) == 10
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_nested_generators_both_early_break(async_db_url):
    """Test nested generators where both generators break early."""
    async with _async_db_context(async_db_url):
        results = []
        async for cat in _async_stream_categories():  # pragma: no branch
            count = 0
            async for row in _async_stream_by_category(cat):  # pragma: no branch
                results.append(row)
                count += 1
                if count >= 2:
                    break
            break
        assert len(results) == 2
        assert await _async_count_rows() == 30


# -- Async tests: generators inside transactions --------------------------


@pytest.mark.asyncio
async def test_async_generator_in_transaction_full_exhaust(async_db_url):
    """Test a single generator fully consumed inside an async transaction."""

    async def work():
        results = []
        async for row in _async_stream_all():
            results.append(row)
        return results, await _async_count_rows()

    async with _async_db_context(async_db_url):
        results, txn_count = await _async_run_in_transaction(work)
        assert len(results) == 30
        assert txn_count == 30
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_generator_in_transaction_early_break(async_db_url):
    """Test a single generator with early break inside an async transaction."""

    async def work():
        results = []
        async for row in _async_stream_all():  # pragma: no branch
            results.append(row)
            if len(results) >= 5:
                break
        return results, await _async_count_rows()

    async with _async_db_context(async_db_url):
        results, txn_count = await _async_run_in_transaction(work)
        assert len(results) == 5
        assert txn_count == 30
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_nested_gen_in_transaction_full_exhaust(async_db_url):
    """Test nested generators fully consumed inside an async transaction."""

    async def work():
        results = []
        async for cat in _async_stream_categories():
            async for row in _async_stream_by_category(cat):
                results.append(row)
        return results, await _async_count_rows()

    async with _async_db_context(async_db_url):
        results, txn_count = await _async_run_in_transaction(work)
        assert len(results) == 30
        assert txn_count == 30
        assert await _async_count_rows() == 30


@pytest.mark.asyncio
async def test_async_nested_gen_in_transaction_both_early_break(async_db_url):
    """Test nested generators both breaking early inside an async transaction."""

    async def work():
        results = []
        async for cat in _async_stream_categories():  # pragma: no branch
            count = 0
            async for row in _async_stream_by_category(cat):  # pragma: no branch
                results.append(row)
                count += 1
                if count >= 2:
                    break
            break
        return results, await _async_count_rows()

    async with _async_db_context(async_db_url):
        results, txn_count = await _async_run_in_transaction(work)
        assert len(results) == 2
        assert txn_count == 30
        assert await _async_count_rows() == 30
