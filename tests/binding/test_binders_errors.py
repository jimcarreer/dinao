"""Tests various errors that can be thrown by binding."""

from typing import Generator, List, Tuple, Union

from dinao.backend import Connection
from dinao.binding import FunctionBinder
from dinao.binding.binders import BoundedGeneratingQuery
from dinao.binding.errors import (
    BadReturnTypeError,
    CannotInferMappingError,
    FunctionAlreadyBoundError,
    MissingTemplateArgumentError,
    MultipleConnectionArgumentError,
    NoPoolSetError,
    PoolAlreadySetError,
    TemplateError,
)
from dinao.binding.templating import Template

import pytest

from tests.binding.mocks import MockConnection, MockConnectionPool


def test_cannot_infer_generic(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function to typed generics raises an error."""
    binder, _ = binder_and_pool

    with pytest.raises(CannotInferMappingError, match="Unable to determine mapper for typing.Union"):

        @binder.query("SELECT * FROM table")
        def raises_cannot_infer() -> Union[str, int]:
            pass  # pragma: no cover


def test_cannot_infer_nested_generic(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function to typed generics as row types raises."""
    binder, _ = binder_and_pool

    with pytest.raises(CannotInferMappingError, match="Unable to determine row mapper for typing.List\\[str\\]"):

        @binder.query("SELECT * FROM table")
        def raises_cannot_infer_row_type() -> List[List[str]]:
            pass  # pragma: no cover


def test_binding_generator_throws(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function to generate when send type and return type are specified."""
    binder, pool = binder_and_pool

    with pytest.raises(CannotInferMappingError, match="Only yield_type"):

        @binder.query("SELECT some_num FROM table LIMIT 3")
        def generating_query_bad() -> Generator[int, int, int]:
            pass  # pragma: no cover


def test_bounded_generating_query_throws(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that BoundedGeneratingQuery raises if not bound to a generator."""
    binder, pool = binder_and_pool

    def not_a_generator() -> int:
        pass  # pragma: no cover

    with pytest.raises(BadReturnTypeError, match="Expected results type to be Generator"):
        BoundedGeneratingQuery(binder, Template("SELECT * FROM table"), not_a_generator)


def test_binder_execute_bad_type(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function specifying an invalid return type for execution raises an exception."""
    binder, _ = binder_and_pool

    with pytest.raises(BadReturnTypeError, match="can only return None or int"):

        @binder.execute("INSERT INTO TABLE (#{arg1})")
        def should_raise(arg1: str) -> List:
            pass  # pragma: no cover


def test_binder_raises_for_template(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that a bad template causes an error at binding time."""
    binder, _ = binder_and_pool

    with pytest.raises(TemplateError, match="#{arg1"):

        @binder.execute("INSERT INTO table #{arg1")
        def should_raise_0(arg1: str) -> int:
            pass  # pragma: no cover


def test_double_binding_raises(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests that binding a function more than once results in an error."""
    binder, _ = binder_and_pool
    match = "has already been bounded by"

    with pytest.raises(FunctionAlreadyBoundError, match=match):

        @binder.execute("UPDATE table SET col = #{arg1}")
        @binder.execute("INSERT INTO TABLE (#{arg1})")
        def should_raise_1(arg1: str):
            pass  # pragma: no cover

    with pytest.raises(FunctionAlreadyBoundError, match=match):

        @binder.execute("UPDATE table SET col = #{arg1}")
        @binder.query("SELECT * FROM table WHERE col = #{arg1})")
        def should_raise_2(arg1: str):
            pass  # pragma: no cover

    with pytest.raises(FunctionAlreadyBoundError, match=match):

        @binder.execute("UPDATE table SET col = #{arg1}")
        @binder.transaction()
        def should_raise_3(arg1: str):
            pass  # pragma: no cover


def test_args_mismatch_raises(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests an error is raised if a template is bound to a function without a matching argument."""
    binder, _ = binder_and_pool

    with pytest.raises(MissingTemplateArgumentError, match="specified in template but is not an argument of"):

        @binder.execute("INSERT INTO table (#{arg})")
        def should_raise_4(some_arg: str):
            pass  # pragma: no cover


def test_binder_raises_for_no_pool():
    """Tests an error is raised when a bind has no pool but an operation requiring one is performed."""
    binder = FunctionBinder()

    @binder.execute("INSERT INTO table (#{arg})")
    def test_bound_execute(arg: str):
        pass  # pragma: no cover

    with pytest.raises(NoPoolSetError, match="No connection pool"):
        test_bound_execute("testing")

    with pytest.raises(NoPoolSetError, match="No connection pool"):
        with binder.connection() as cnx:  # noqa: F841
            pass  # pragma: no cover


def test_binder_raises_for_pool_set_twice(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests an error is raised when a binder has its pool set twice."""
    binder, _ = binder_and_pool
    pool = MockConnectionPool([])

    with pytest.raises(PoolAlreadySetError, match="only be set once"):
        binder.pool = pool


def test_binder_raises_for_double_connection_arg(binder_and_pool: Tuple[FunctionBinder, MockConnectionPool]):
    """Tests an error is raised when a bound function specifies it would like more than one connection."""
    binder, _ = binder_and_pool

    with pytest.raises(MultipleConnectionArgumentError, match="Connection argument specified multiple times for"):

        @binder.transaction()
        def should_raise_5(cnx1: Connection, cnx2: MockConnection):
            pass  # pragma: no cover
