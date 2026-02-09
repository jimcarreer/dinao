"""Implements asynchronous function binding to SQL queries and actions."""

import contextvars
import inspect
import typing
from contextlib import asynccontextmanager

from dinao.backend.base import AsyncConnection, AsyncConnectionPool
from dinao.backend.errors import AsyncPoolRequiredError
from dinao.binding.binders.base import (
    BoundExecutionBase,
    BoundGeneratingQueryBase,
    BoundQueryBase,
    BoundTransactionBase,
    FunctionBinderBase,
)
from dinao.binding.errors import CannotInferMappingError, IncompatibleBindingError, NoPoolSetError, TooManyRowsError
from dinao.binding.mappers import ASYNC_GENERATOR_GENERICS, GENERATOR_GENERICS


class AsyncFunctionBinder(FunctionBinderBase):
    """Implements functionality for binding functions to async SQL queries and actions."""

    def __init__(self):
        """Construct an async function binder."""
        super().__init__()
        self._context_store = contextvars.ContextVar("active_cnx", default=None)

    def _validate_function(self, func):
        """Validate that the function is a coroutine.

        :raises IncompatibleBindingError
        """
        if not inspect.iscoroutinefunction(func):
            error = (
                f"AsyncFunctionBinder cannot bind sync function"
                f" '{func.__name__}'; use FunctionBinder"
                f" for sync functions"
            )
            self._suppressed_raise(IncompatibleBindingError(error))

    def _validate_pool(self, pool):
        """Validate that the pool is an async pool.

        :raises AsyncPoolRequiredError
        """
        if not isinstance(pool, AsyncConnectionPool):
            raise AsyncPoolRequiredError("AsyncFunctionBinder requires an AsyncConnectionPool")

    def execute(self, sql: str) -> callable:
        """Binds a given function to a given async SQL template.

        .. warning::
            The execution of the SQL given is not expected to return results / rows. Use the ``query``
            decorator for SQL that returns rows.

        .. note::
            The return type of the decorated / bound function may return an int, which will make it return the number of
            rows affected by the execution of the last SQL statement in the template.

        Example::

            @binder.execute("INSERT INTO my_table (col1, col2) VALUES (#{arg1.some_key_or_member}, #{arg2})")
            async def my_bound_func(arg1: dict, arg2: str) -> int:
                pass

        :param sql: a SQL template to bind the function execution to
        :returns: a decorator expecting a callable
        :raises: BadReturnTypeError, FunctionAlreadyBoundError
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            template = self._make_template(sql)
            return AsyncBoundExecution(self, template, func)
        return decorated
        # fmt: on

    def query(self, sql: str) -> callable:
        """Binds a given function to a given async SQL template.

        The return type annotation of the bound function determines how query results are mapped. Supported return
        types include: bare classes and dataclasses (single row), ``Optional[X]`` (single row or None),
        ``List[X]`` (multiple rows), ``dict`` (single row as dict),
        ``AsyncGenerator[X, None]`` (streaming rows), and native types like ``str``, ``int``, etc.
        (single column value). If no return type is specified, the default is ``List[tuple]``.

        Example::

            @binder.query("SELECT * FROM my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            async def my_bound_func(arg1: dict, arg2: str):
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
            if return_type in GENERATOR_GENERICS + ASYNC_GENERATOR_GENERICS:
                return AsyncBoundGeneratingQuery(self, template, func)
            return AsyncBoundQuery(self, template, func)
        return decorated
        # fmt: on

    def transaction(self):
        """Binds a given function to a given async SQL transaction.

        All bound functions called during the call of a function bound in this way will use the same transaction /
        connection.

        Example::

            @binder.query("SELECT * FROM my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            async def my_select(arg1: dict, arg2: str):
                pass

            @binder.execute(
                "INSERT INTO stats_table VALUES "
                "(#{stat.name}, #{stat.value}) "
                "ON CONFLICT (name) DO UPDATE SET value = #{stat.value}"
            )
            async def my_update(stat: dict) -> int:
                pass

            @binder.transaction()
            async def my_transaction(arg1: dict, stat_name: str) -> int:
                my_table_results = await my_select(arg1, stat_name)
                stat = {"name": stat_name, "value": 0}
                for row in my_table_results:
                    stat["value"] += row[0]
                return await my_update(stat=stat)

        :returns: a decorator expecting a callable
        :raises: MultipleConnectionArgumentError, FunctionAlreadyBoundError
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            return AsyncBoundTransaction(self, func)
        return decorated
        # fmt: on

    @asynccontextmanager
    async def connection(self):
        """Async context manager for database connections used by bound functions.

        An active connection will be kept and yielded, provided to all contexts within the context that initially asked
        for it.

        Example::

            async with binder.connection() as cnx_outer:
                async with binder.connection() as cnx_inner:
                    assert cnx_outer == cnx_inner  # This assertion is true

        Any exception caught during the context of a connection will trigger a rollback of the transaction.  The
        transaction is automatically committed when execution is yielded back.
        """
        if self._cnx_pool is None:
            raise NoPoolSetError("No connection pool has been set for the binder")
        active_cnx = self._context_store.get()
        if active_cnx:
            yield active_cnx
            return
        cnx = await self.pool.lease()
        self._context_store.set(cnx)
        try:
            yield cnx
            await cnx.commit()
        except:  # noqa: E722
            await cnx.rollback()
            raise
        finally:
            await self.pool.release(cnx)
            self._context_store.set(None)


class AsyncBoundQuery(BoundQueryBase):
    """Async implementation of a bound function that represents a SQL query that returns rows."""

    def _one_return(self, results):
        """Not used directly - see async _async_one_return."""
        pass  # pragma: no cover

    def _many_return(self, results):
        """Not used directly - see async _async_many_return."""
        pass  # pragma: no cover

    async def _async_one_return(self, results):
        """Map a single row result asynchronously."""
        if results.rowcount > 1:
            raise TooManyRowsError(f"Only expected one row, but got {results.rowcount}")
        raw = await results.fetchone()
        return self._row_mapper(raw, results.description) if raw else None

    async def _async_many_return(self, results):
        """Map multiple row results asynchronously."""
        return [self._row_mapper(row, results.description) for row in await results.fetchall()]

    async def __call__(self, *args, **kwargs):
        """Execute the templated SQL as an async query with results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        async with self._binder.connection() as cnx:
            async with cnx.query(sql, values) as results:
                if self._return_impl == self._none_return:
                    return None
                elif self._return_impl == self._one_return:
                    return await self._async_one_return(results)
                else:
                    return await self._async_many_return(results)


class AsyncBoundGeneratingQuery(BoundGeneratingQueryBase):
    """Async implementation of a bound function that returns rows via an async generator."""

    def _allowed_generics(self):
        """Return the list of allowed generic types for this bound query."""
        return GENERATOR_GENERICS + ASYNC_GENERATOR_GENERICS

    def _validate_generic_args(self, generic_args):
        """Validate the generic arguments of the return type annotation."""
        if generic_args:
            # For Generator[Y, S, R], check S and R are None
            if len(generic_args) == 3 and generic_args[1:3] != (type(None), type(None)):
                raise CannotInferMappingError("Only yield_type should be specified for a generator")
            # For AsyncGenerator[Y, S], check S is None
            if len(generic_args) == 2 and generic_args[1] is not type(None):
                raise CannotInferMappingError("Only yield_type should be specified for an async generator")

    async def __call__(self, *args, **kwargs):
        """Execute the templated SQL as an async query with results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        async with self._binder.connection() as cnx:
            async with cnx.query(sql, values) as results:
                row = await results.fetchone()
                while row:
                    yield self._row_mapper(row, results.description)
                    row = await results.fetchone()


class AsyncBoundExecution(BoundExecutionBase):
    """Async implementation of a bound function that represents a SQL query that does not return rows."""

    async def __call__(self, *args, **kwargs):
        """Execute the templated SQL as an async query without results.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        sql, values = self._sql_template.render(self._binder.mung_symbol, bound.arguments)
        async with self._binder.connection() as cnx:
            affected = await cnx.execute(sql, values)
            return None if self._returns_none else affected


class AsyncBoundTransaction(BoundTransactionBase):
    """Async implementation of a bound function whose call is done within a database transaction."""

    _cnx_class = AsyncConnection

    async def __call__(self, *args, **kwargs):
        """Execute the decorated / bound function with a single database connection / transaction.

        :param args: the positional arguments of the bound / decorated function
        :param kwargs: the keyword arguments of the bound / decorated function
        """
        async with self._binder.connection() as cnx:
            cnx.autocommit = False
            if self._cnx_arg_name:
                kwargs = kwargs or {}
                kwargs[self._cnx_arg_name] = cnx
            return await self._func(*args, **kwargs)
