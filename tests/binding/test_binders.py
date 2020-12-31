"""Tests the functionality in the dinao.binding module."""

from typing import Generator, Mapping, Tuple

from dinao.binding.binders import FunctionBinder
from dinao.binding.errors import TooManyRowsError

import pytest

from tests.binding.mocks import MockConnection, MockConnectionPool, MockDMLCursor, MockDQLCursor


@pytest.mark.parametrize(
    "binder_and_pool",
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


@pytest.mark.parametrize(
    "binder_and_pool",
    [
        [
            MockDQLCursor([(1,), (2,), (3,)], (("some_num", 99),)),
            MockDQLCursor([(4,), (5,), (6,)], (("some_num", 99),)),
        ],
    ],
    indirect=["binder_and_pool"],
)
def test_binder_generating_query(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests binder when the result type is a generator."""
    binder, pool = binder_and_pool

    @binder.query("SELECT some_num FROM table LIMIT 3")
    def generating_query() -> Generator:
        pass  # pragma: no cover

    @binder.query("SELECT some_num FROM table LIMIT 3")
    def generating_query_with_type() -> Generator[int, None, None]:
        pass  # pragma: no cover

    results = [x for x in generating_query()]
    assert results == [(1,), (2,), (3,)]
    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()

    results = [x for x in generating_query_with_type()]
    assert results == [4, 5, 6]
    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()


@pytest.mark.parametrize(
    "binder_and_pool",
    [
        [
            MockDQLCursor([(1, "2", 3.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
            MockDQLCursor([(1, "2", 3.0), (4, "5", 6.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
        ],
    ],
    indirect=["binder_and_pool"],
)
def test_binder_class_return(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests binder when the result type is a class."""
    binder, pool = binder_and_pool

    class ClassForTest:
        def __init__(self, field_01: int, field_02: str, field_03: float):
            assert field_01 == 1
            assert field_02 == "2"
            assert field_03 == 3.0

    @binder.query("SELECT field_01, field_02, field_03 FROM WHERE arg = #{arg}")
    def query_class_return(arg: str) -> ClassForTest:
        pass  # pragma: no cover

    result = query_class_return("test")
    assert isinstance(result, ClassForTest)

    with pytest.raises(TooManyRowsError, match="Only expected one row, but got 2"):
        query_class_return("test2")


@pytest.mark.parametrize(
    "binder_and_pool",
    [
        [
            MockDQLCursor([(1, "2", 3.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
            MockDQLCursor([(1, "2", 3.0), (4, "5", 6.0)], (("field_01", 0), ("field_02", 2), ("field_03", 3))),
        ],
    ],
    indirect=["binder_and_pool"],
)
def test_binder_dict_return(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests binder when the result type is a dictionary like object."""
    binder, pool = binder_and_pool

    @binder.query("SELECT field_01, field_02, field_03 FROM WHERE arg = #{arg}")
    def query_dict_return(arg: str) -> Mapping:
        pass  # pragma: no cover

    result = query_dict_return("test")
    assert result == {"field_01": 1, "field_02": "2", "field_03": 3.0}

    with pytest.raises(TooManyRowsError, match="Only expected one row, but got 2"):
        query_dict_return("test2")


def test_binder_roles_back(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests the binder rolls back a connection if a bound function raises."""
    binder, pool = binder_and_pool

    @binder.transaction()
    def raises_for_roll_back():
        raise Exception("Will trigger a roll back")

    with pytest.raises(Exception, match="Will trigger a roll back"):
        raises_for_roll_back()

    cnx: MockConnection = pool.connection_stack.pop(0)
    assert cnx.released
    assert cnx.committed == 0
    assert cnx.rollbacks == 1


@pytest.mark.parametrize(
    "binder_and_pool",
    [
        [
            MockDMLCursor(3),
            MockDMLCursor(1),
            MockDQLCursor([(1,), (2,), (3,)], (("some_num", 99),)),
        ],
    ],
    indirect=["binder_and_pool"],
)
def test_binder_passes_cnx(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that the binder will pass the active connection if requested."""
    binder, pool = binder_and_pool

    @binder.execute("DELETE FROM table")
    def clear_table() -> int:
        pass  # pragma: no cover

    @binder.transaction()
    def do_something(my_arg: str, connection: MockConnection = None) -> int:
        clear_table()
        count = connection.execute("INSERT INTO table (%s), (%s)", (1, 2))
        summed = 0
        if count > 0:
            with connection.query("SELECT * FROM table WHERE thing = %s", (my_arg,)) as results:
                summed = sum([row[0] for row in results.fetchall()])
        return summed

    assert do_something("test") == 6
    assert len(pool.connection_stack) == 1
    cnx: MockConnection = pool.connection_stack.pop(0)
    cnx.assert_clean()
    assert cnx.committed == 1
    assert cnx.query_stack == [
        ("DELETE FROM table", ()),
        ("INSERT INTO table (%s), (%s)", (1, 2)),
        ("SELECT * FROM table WHERE thing = %s", ("test",)),
    ]
