"""Implements synchronous function binding to SQL queries and actions."""

import inspect
import threading
import typing
from contextlib import contextmanager
from typing import Any, Generator

from dinao.backend.base import Connection
from dinao.binding.binders.base import (
    BoundExecutionBase,
    BoundGeneratingQueryBase,
    BoundQueryBase,
    BoundTransactionBase,
    FunctionBinderBase,
)
from dinao.binding.errors import IncompatibleBindingError, NoPoolSetError, TooManyRowsError
from dinao.binding.mappers import GENERATOR_GENERICS


class FunctionBinder(FunctionBinderBase):
    """Implements functionality for binding functions to SQL queries and actions."""

    def __init__(self):
        """Construct a function binder."""
        super().__init__()
        self._context_store = threading.local()

    def _validate_function(self, func):
        """Validate that the function is not a coroutine.

        :raises IncompatibleBindingError
        """
        if inspect.iscoroutinefunction(func):
            error = (
                f"FunctionBinder cannot bind async function"
                f" '{func.__name__}'; use AsyncFunctionBinder"
                f" for async functions"
            )
            self._suppressed_raise(IncompatibleBindingError(error))

    def execute(self, sql: str) -> callable:
        """Binds a given function to a given SQL template.

        .. warning::
            The execution of the SQL given is not expected to return results / rows. Use the ``query``
            decorator for SQL that returns rows.

        .. note::
            The return type of the decorated / bound function may return an int, which will make it return the number of
            rows affected by the execution of the last SQL statement in the template.

        Example::

            @binder.execute("INSERT INTO my_table (col1, col2) VALUES (#{arg1.some_key_or_member}, #{arg2})")
            def my_bound_func(arg1: dict, arg2: str) -> int:
                pass

        :param sql: a SQL template to bind the function execution to
        :returns: a decorator expecting a callable
        :raises: BadReturnTypeError, FunctionAlreadyBoundError
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            template = self._make_template(sql)
            return BoundExecution(self, template, func)
        return decorated
        # fmt: on

    def query(self, sql: str) -> callable:
        """Binds a given function to a given SQL template.

        The return type annotation of the bound function determines how query results are mapped. Supported return
        types include: bare classes and dataclasses (single row), ``Optional[X]`` (single row or None),
        ``List[X]`` (multiple rows), ``dict`` (single row as dict), ``Generator[X, None, None]`` (streaming rows),
        and native types like ``str``, ``int``, etc. (single column value). If no return type is specified, the
        default is ``List[tuple]``.

        Example::

            @binder.query("SELECT * FROM my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            def my_bound_func(arg1: dict, arg2: str):
                pass

        :param sql: a SQL template to bind the function execution to
        :returns: a decorator expecting a callable
        :raises: CannotInferMappingError, FunctionAlreadyBoundError
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            template = self._make_template(sql)
            return_type = typing.get_origin(inspect.signature(func).return_annotation)
            if return_type in GENERATOR_GENERICS:
                return BoundGeneratingQuery(self, template, func)
            return BoundQuery(self, template, func)
        return decorated
        # fmt: on

    def transaction(self):
        """Binds a given function to a given SQL transaction.

        All bound functions called during the call of a function bound in this way will use the same transaction /
        connection.

        Example::

            @binder.query("SELECT * FROM my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            def my_select(arg1: dict, arg2: str):
                pass

            @binder.execute(
                "INSERT INTO stats_table VALUES "
                "(#{stat.name}, #{stat.value}) "
                "ON CONFLICT (name) DO UPDATE SET value = #{stat.value}"
            )
            def my_update(stat: dict) -> int:
                pass

            @binder.transaction()
            def my_transaction(arg1: dict, stat_name: str) -> int:
                my_table_results = my_select(arg1, stat_name)
                stat = {"name": stat_name, "value": 0}
                for row in my_table_results:
                    stat["value"] += row[0]
                return my_update(stat=stat)

        :returns: a decorator expecting a callable
        :raises: MultipleConnectionArgumentError, FunctionAlreadyBoundError
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            return BoundTransaction(self, func)
        return decorated
        # fmt: on

    @contextmanager
    def connection(self) -> Generator[Connection | None | Any, Any, None]:
        """Context manager for database connections used by bound functions.

        An active connection will be kept and yielded, provided to all contexts within the context that initially asked
        for it.

        Example::

            with binder.connection() as cnx_outer:
                with binder.connection() as cnx_inner:
                    assert cnx_outer == cnx_inner  # This assertion is true

        Any exception caught during the context of a connection will trigger a rollback of the transaction.  The
        transaction is automatically committed when execution is yielded back.
        """
        if self._cnx_pool is None:
            raise NoPoolSetError("No connection pool has been set for the binder")
        active_cnx = getattr(self._context_store, "active_cnx", None)
        if active_cnx:
            yield active_cnx
            return
        self._context_store.active_cnx = self.pool.lease()
        try:
            yield self._context_store.active_cnx
            self._context_store.active_cnx.commit()
        except:  # noqa: E722
            self._context_store.active_cnx.rollback()
            raise
        finally:
            self.pool.release(self._context_store.active_cnx)
            self._context_store.active_cnx = None


class BoundQuery(BoundQueryBase):
    """Implementation of a bound function that represents a SQL query that returns rows."""

    def _one_return(self, results):
        """Map a single row result."""
        if results.rowcount > 1:
            raise TooManyRowsError(f"Only expected one row, but got {results.rowcount}")
        raw = results.fetchone()
        return self._row_mapper(raw, results.description) if raw else None

    def _many_return(self, results):
        """Map multiple row results."""
        return [self._row_mapper(row, results.description) for row in results.fetchall()]

    def __call__(self, *args, **kwargs):
        """Execute the templated SQL as a query with results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        with self._binder.connection() as cnx:
            with cnx.query(sql, values) as results:
                return self._return_impl(results)


class BoundGeneratingQuery(BoundGeneratingQueryBase):
    """Implementation of a bound function that represents a SQL query that returns rows via a generator."""

    def __call__(self, *args, **kwargs):
        """Execute the templated SQL as a query with results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        with self._binder.connection() as cnx:
            with cnx.query(sql, values) as results:
                row = results.fetchone()
                while row:
                    yield self._row_mapper(row, results.description)
                    row = results.fetchone()


class BoundExecution(BoundExecutionBase):
    """Implementation of a bound function that represents a SQL query that does not return rows."""

    def __call__(self, *args, **kwargs):
        """Execute the templated SQL as a query without results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        with self._binder.connection() as cnx:
            affected = cnx.execute(sql, values)
            return None if self._returns_none else affected


class BoundTransaction(BoundTransactionBase):
    """Implementation of a bound function whose call is done within a database transaction."""

    _cnx_class = Connection

    def __call__(self, *args, **kwargs):
        """Execute the decorated / bound function with a single database connection / transaction.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        with self._binder.connection() as cnx:
            cnx.autocommit = False
            if self._cnx_arg_name:
                kwargs = kwargs or {}
                kwargs[self._cnx_arg_name] = cnx
            return self._func(*args, **kwargs)
