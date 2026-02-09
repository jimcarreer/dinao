"""Tests the async functionality in the dinao.binding module."""

from typing import AsyncGenerator, Generator, Mapping, Optional, Tuple

from dinao.binding.binders.async_ import AsyncFunctionBinder
from dinao.binding.errors import NoPoolSetError, TooManyRowsError

import pytest

from tests.binding.mocks import AsyncMockConnection, AsyncMockConnectionPool, MockDMLCursor, MockDQLCursor


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDMLCursor(1),
            MockDMLCursor(0),
            MockDMLCursor(0),
            MockDMLCursor(1),
            MockDQLCursor([(1,), (2,), (3,)], (("some_num", 99),)),
            MockDMLCursor(1),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_basic_bindings(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests the basic async function binding functionality and behavior."""
    binder, pool = async_binder_and_pool

    @binder.execute("INSERT INTO table VALUES (#{arg1}, #{arg2}, #{arg3}) ON CONFLICT DO NOTHING")
    async def bound_insert(arg1: str, arg2: str, arg3: str = "test") -> int:
        pass  # pragma: no cover

    @binder.query("SELECT some_num FROM table WHERE name = #{arg1.name}")
    async def bound_select(arg1: dict):
        pass  # pragma: no cover

    @binder.query("INSERT INTO some_other_table VALES (#{arg1})")
    async def bound_query_returns_none(arg1: str) -> None:
        pass  # pragma: no cover

    @binder.execute("UPDATE some_table SET some_value = #{arg2} WHERE some_name = #{arg1}")
    async def bound_update(arg1: str, arg2: int) -> None:
        pass  # pragma: no cover

    @binder.transaction()
    async def bound_transaction(param: str) -> int:
        await bound_insert("test1", "test2", param)
        stats = 0
        for x in await bound_select({"name": param}):
            stats += x[0]
        await bound_update(param, stats)
        return stats

    assert await bound_insert("one", "two", "three") == 1
    assert await bound_insert("one", "two") == 0
    assert await bound_query_returns_none("some_value") is None
    assert await bound_transaction("testing") == 6
    assert len(pool.connection_stack) == 4

    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [
        ("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("one", "two", "three"))
    ]

    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("one", "two", "test"))]

    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [("INSERT INTO some_other_table VALES (%s)", ("some_value",))]

    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [
        ("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("test1", "test2", "testing")),
        ("SELECT some_num FROM table WHERE name = %s", ("testing",)),
        ("UPDATE some_table SET some_value = %s WHERE some_name = %s", (6, "testing")),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDQLCursor([(1,), (2,), (3,)], (("some_num", 99),)),
            MockDQLCursor([(4,), (5,), (6,)], (("some_num", 99),)),
            MockDQLCursor([(7,), (8,), (9,)], (("some_num", 99),)),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_generating_query(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests async binder when the result type is a generator."""
    binder, pool = async_binder_and_pool

    @binder.query("SELECT some_num FROM table LIMIT 3")
    async def generating_query() -> Generator:
        pass  # pragma: no cover

    @binder.query("SELECT some_num FROM table LIMIT 3")
    async def generating_query_with_type() -> Generator[int, None, None]:
        pass  # pragma: no cover

    @binder.query("SELECT some_num FROM table LIMIT 3")
    async def async_generating_query_with_type() -> AsyncGenerator[int, None]:
        pass  # pragma: no cover

    results = [x async for x in generating_query()]
    assert results == [(1,), (2,), (3,)]
    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()

    results = [x async for x in generating_query_with_type()]
    assert results == [4, 5, 6]
    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()

    results = [x async for x in async_generating_query_with_type()]
    assert results == [7, 8, 9]
    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDQLCursor([(1, "2", 3.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
            MockDQLCursor([(1, "2", 3.0), (4, "5", 6.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_class_return(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests async binder when the result type is a class."""
    binder, pool = async_binder_and_pool

    class ClassForTest:
        def __init__(self, field_01: int, field_02: str, field_03: float):
            assert field_01 == 1
            assert field_02 == "2"
            assert field_03 == 3.0

    @binder.query("SELECT field_01, field_02, field_03 FROM WHERE arg = #{arg}")
    async def query_class_return(arg: str) -> ClassForTest:
        pass  # pragma: no cover

    result = await query_class_return("test")
    assert isinstance(result, ClassForTest)

    with pytest.raises(TooManyRowsError, match="Only expected one row, but got 2"):
        await query_class_return("test2")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDQLCursor([(1, "2", 3.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
            MockDQLCursor([(1, "2", 3.0), (4, "5", 6.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_dict_return(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests async binder when the result type is a dictionary like object."""
    binder, pool = async_binder_and_pool

    @binder.query("SELECT field_01, field_02, field_03 FROM WHERE arg = #{arg}")
    async def query_dict_return(arg: str) -> Mapping:
        pass  # pragma: no cover

    result = await query_dict_return("test")
    assert result == {"field_01": 1, "field_02": "2", "field_03": 3.0}

    with pytest.raises(TooManyRowsError, match="Only expected one row, but got 2"):
        await query_dict_return("test2")


@pytest.mark.asyncio
async def test_async_binder_rolls_back(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests the async binder rolls back a connection if a bound function raises."""
    binder, pool = async_binder_and_pool

    @binder.transaction()
    async def raises_for_roll_back():
        raise Exception("Will trigger a roll back")

    with pytest.raises(Exception, match="Will trigger a roll back"):
        await raises_for_roll_back()

    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    assert cnx.released
    assert cnx.committed == 0
    assert cnx.rollbacks == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDMLCursor(3),
            MockDMLCursor(1),
            MockDQLCursor([(1,), (2,), (3,)], (("some_num", 99),)),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_binder_passes_cnx(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests that the async binder will pass the active connection if requested."""
    binder, pool = async_binder_and_pool

    @binder.execute("DELETE FROM table")
    async def clear_table() -> int:
        pass  # pragma: no cover

    @binder.transaction()
    async def do_something(my_arg: str, connection: AsyncMockConnection = None) -> int:
        await clear_table()
        count = await connection.execute("INSERT INTO table (%s), (%s)", (1, 2))
        summed = 0
        if count > 0:  # pragma: no branch
            async with connection.query("SELECT * FROM table WHERE thing = %s", (my_arg,)) as results:
                summed = sum([row[0] for row in await results.fetchall()])
        return summed

    assert await do_something("test") == 6
    assert len(pool.connection_stack) == 1
    cnx: AsyncMockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.committed == 1
    assert cnx.query_stack == [
        ("DELETE FROM table", ()),
        ("INSERT INTO table (%s), (%s)", (1, 2)),
        ("SELECT * FROM table WHERE thing = %s", ("test",)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDQLCursor([(1,)], (("exists", 0),)),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_bool_return(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests async binder when the result type is bool."""
    binder, pool = async_binder_and_pool

    @binder.query("SELECT EXISTS(SELECT 1 FROM table WHERE col = #{arg})")
    async def query_bool_return(arg: str) -> bool:
        pass  # pragma: no cover

    result = await query_bool_return("test")
    assert result is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_binder_and_pool",
    [
        [
            MockDQLCursor([("found",)], (("value", 0),)),
            MockDQLCursor([], (("value", 0),)),
            MockDQLCursor([(1, "test", 3.0)], (("field_01", 0), ("field_02", 1), ("field_03", 2))),
            MockDQLCursor([], (("field_01", 0), ("field_02", 1), ("field_03", 2))),
        ],
    ],
    indirect=["async_binder_and_pool"],
)
async def test_async_optional_return(async_binder_and_pool: Tuple[AsyncFunctionBinder, AsyncMockConnectionPool]):
    """Tests async binder when the result type is Optional[X] or X | None."""
    binder, pool = async_binder_and_pool

    @binder.query("SELECT value FROM table WHERE col = #{arg}")
    async def query_optional_str(arg: str) -> Optional[str]:
        pass  # pragma: no cover

    @binder.query("SELECT value FROM table WHERE col = #{arg}")
    async def query_union_none_str(arg: str) -> str | None:
        pass  # pragma: no cover

    class ResultClass:
        def __init__(self, field_01: int, field_02: str, field_03: float):
            self.field_01 = field_01
            self.field_02 = field_02
            self.field_03 = field_03

    @binder.query("SELECT field_01, field_02, field_03 FROM table WHERE col = #{arg}")
    async def query_optional_class(arg: str) -> Optional[ResultClass]:
        pass  # pragma: no cover

    @binder.query("SELECT field_01, field_02, field_03 FROM table WHERE col = #{arg}")
    async def query_union_none_class(arg: str) -> ResultClass | None:
        pass  # pragma: no cover

    # Test Optional[str] with result
    result = await query_optional_str("exists")
    assert result == "found"

    # Test str | None with no result
    result = await query_union_none_str("not_exists")
    assert result is None

    # Test Optional[Class] with result
    result = await query_optional_class("exists")
    assert isinstance(result, ResultClass)
    assert result.field_01 == 1
    assert result.field_02 == "test"
    assert result.field_03 == 3.0

    # Test Class | None with no result
    result = await query_union_none_class("not_exists")
    assert result is None


@pytest.mark.asyncio
async def test_async_binder_raises_for_no_pool():
    """Tests an error is raised when an async binder has no pool but an operation requiring one is performed."""
    binder = AsyncFunctionBinder()

    @binder.execute("INSERT INTO table (#{arg})")
    async def test_bound_execute(arg: str):
        pass  # pragma: no cover

    with pytest.raises(NoPoolSetError, match="No connection pool"):
        await test_bound_execute("testing")

    with pytest.raises(NoPoolSetError, match="No connection pool"):
        async with binder.connection() as cnx:  # noqa: F841
            pass  # pragma: no cover
