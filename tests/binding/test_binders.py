"""Tests the functionality in the dinao.binding module."""

from typing import List, Tuple

from dinao.binding.binders import FunctionBinder
from dinao.binding.errors import BadReturnType

import pytest

from tests.binding.mocks import MockConnection, MockConnectionPool, MockResultSet


@pytest.fixture()
def binder_and_pool(request):
    """Fixture that yields a FunctionBinder (and its MockedConnectionPool) initialized with a set of MockResultSets.

    .. note::
        Can use the `indirect` parametrize functionality in fixture to specify the mocked results.
    """
    result_stack = []
    if hasattr(request, "param"):
        result_stack = request.param
    pool = MockConnectionPool(result_stack)
    binder = FunctionBinder(pool)
    yield binder, pool


@pytest.mark.parametrize(
    "binder_and_pool",
    [
        [
            MockResultSet([], 1, tuple()),
            MockResultSet([], 0, tuple()),
            MockResultSet([], 0, tuple()),
            MockResultSet([], 1, tuple()),
            MockResultSet([(1,), (2,), (3,)], 3, ("some_num",)),
            MockResultSet([], 1, tuple()),
        ],
    ],
    indirect=["binder_and_pool"],
)
def test_basic_bindings(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests the basic function binding functionality and behavior."""
    binder, pool = binder_and_pool

    @binder.execute("INSERT INTO table VALUES (#{arg1}, #{arg2}, #{arg3}) ON CONFLICT DO NOTHING")
    def bounded_insert(arg1: str, arg2: str, arg3: str = "test") -> int:
        pass  # pragma: no cover

    @binder.query("SELECT some_num FROM table WHERE name = #{arg1.name}")
    def bounded_select(arg1: dict):
        pass  # pragma: no cover

    @binder.query("INSERT INTO some_other_table VALES (#{arg1})")
    def bounded_query_returns_none(arg1: str) -> None:
        pass  # pragma: no cover

    @binder.execute("UPDATE some_table SET some_value = #{arg2} WHERE some_name = #{arg1}")
    def bounded_update(arg1: str, arg2: int) -> None:
        pass  # pragma: no cover

    @binder.transaction()
    def bounded_transaction(param: str) -> int:
        bounded_insert("test1", "test2", param)
        stats = 0
        for x in bounded_select({"name": param}):
            stats += x[0]
        bounded_update(param, stats)
        return stats

    assert bounded_insert("one", "two", "three") == 1
    assert bounded_insert("one", "two") == 0
    assert bounded_query_returns_none("some_value") is None
    assert bounded_transaction("testing") == 6
    assert len(pool.connection_stack) == 4

    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [
        ("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("one", "two", "three"))
    ]

    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("one", "two", "test"))]

    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [("INSERT INTO some_other_table VALES (%s)", ("some_value",))]

    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.query_stack == [
        ("INSERT INTO table VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", ("test1", "test2", "testing")),
        ("SELECT some_num FROM table WHERE name = %s", ("testing",)),
        ("UPDATE some_table SET some_value = %s WHERE some_name = %s", (6, "testing")),
    ]


def test_binder_execute_bad_type(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function specifying an invalid return type for execution raises an exception."""
    binder, _ = binder_and_pool

    with pytest.raises(BadReturnType, match="can only return None or int"):

        @binder.execute("INSERT INTO TABLE (#{arg1})")
        def should_raise(arg1: str) -> List:
            pass  # pragma: no cover
