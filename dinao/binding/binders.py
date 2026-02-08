"""Implements functionality for binding python functions to SQL queries and actions."""

import inspect
import threading
import types
import typing
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Generator

from dinao.backend.base import Connection, ConnectionPool, ResultSet
from dinao.binding.errors import (
    BadReturnTypeError,
    CannotInferMappingError,
    FunctionAlreadyBoundError,
    MissingTemplateArgumentError,
    MultipleConnectionArgumentError,
    NoPoolSetError,
    PoolAlreadySetError,
    TemplateError,
    TooManyRowsError,
)
from dinao.binding.mappers import DICT_GENERICS, GENERATOR_GENERICS, LIST_GENERICS, RowMapper, get_row_mapper
from dinao.binding.templating import Template


def _unwrap_optional(type_hint: typing.Any) -> typing.Optional[typing.Type]:
    """Extract the inner type from Optional[X] or X | None.

    :param type_hint: A type hint that may be Optional[X] or X | None
    :returns: The inner type X if type_hint is Optional[X], None otherwise
    """
    origin = typing.get_origin(type_hint)
    args = typing.get_args(type_hint)
    # Check for Union types (typing.Union or types.UnionType for X | Y syntax)
    if origin not in (typing.Union, types.UnionType) or not args:
        return None
    # Filter out NoneType from the union args
    non_none_args = [a for a in args if a is not type(None)]
    # Only unwrap if there's exactly one non-None type (true Optional)
    if len(non_none_args) == 1:
        return non_none_args[0]
    return None


class FunctionBinder:
    """Implements functionality for binding functions to SQL queries and actions."""

    def __init__(self):
        """Construct a function binder."""
        self._context_store = threading.local()
        self._verbose_trace = False
        self._cnx_pool = None

    def _suppressed_raise(self, exc: Exception):
        if self._verbose_trace:
            raise exc  # pragma: no cover
        # Try to cut down on the traces so the user gets closer to the issue in their code
        raise exc.with_traceback(None) from exc

    def _raise_for_multi_binding(self, func: callable):
        if isinstance(func, BoundFunction):
            name = func.bound_function.__name__
            error = f"The function {name} has already been bound by {func}"
            self._suppressed_raise(FunctionAlreadyBoundError(error))

    def _make_template(self, sql: str):
        try:
            return Template(sql)
        except TemplateError as exc:
            self._suppressed_raise(exc)

    @property
    def pool(self) -> ConnectionPool:
        """Get the connection pool used by this binder."""
        return self._cnx_pool

    @pool.setter
    def pool(self, pool: ConnectionPool):
        """Set the connection pool used by this binder.

        :raises PoolAlreadySetError
        """
        if self._cnx_pool is not None:
            raise PoolAlreadySetError("The connection pool can only be set once")
        self._cnx_pool = pool

    @property
    def mung_symbol(self) -> str:
        """Return the mung symbol used for rendering templates bound by the binder."""
        if self._cnx_pool is None:
            raise NoPoolSetError("No connection pool has been set for the binder")
        return self.pool.mung_symbol

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


class BoundFunction(ABC):
    """Abstract class for common behaviors between bound functions."""

    def __init__(self, binder: FunctionBinder, func: callable):  # noqa: D107
        self._binder = binder
        self._func = func
        self._sig = inspect.signature(func)

    @abstractmethod
    def __call__(self, *args, **kwargs):  # noqa: D102
        pass  # pragma: no cover

    @property
    def bound_function(self) -> callable:
        """Return the original callable this object binds."""
        return self._func


class BoundSQLFunction(BoundFunction):
    """Abstract class for common behaviors between functions bound to templated SQL."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable):  # noqa: D107
        super().__init__(binder, func)
        self._sql_template = sql_template
        for arg_id in sql_template.arguments:
            if arg_id[0] not in self._sig.parameters:
                error = f"Argument '{arg_id[0]}' specified in template but is not an argument of {func.__name__}"
                raise MissingTemplateArgumentError(error)

    def __str__(self) -> str:
        """Return a simple representation of a SQL bound function."""
        return f"{self.__class__.__name__} of {self._sql_template} ({self.bound_function.__name__})"

    @abstractmethod
    def __call__(self, *args, **kwargs):  # noqa: D102
        pass  # pragma: no cover


class BoundQuery(BoundSQLFunction):
    """Implementation of a bound function that represents a SQL query that returns rows."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable, row_mapper: RowMapper = None):
        """Construct a BoundSQLFunction that returns result sets.

        :param binder: the binder to which this bound function belongs
        :param sql_template: the template to use for resolving SQL parameters / munged sql.
        :param func: the callable being bound to the given SQL
        :param row_mapper: a RowMapper implementation that overrides the mapper inferred from the return annotation

        :raises CannotInferMappingError
        """
        super().__init__(binder, sql_template, func)
        self._row_mapper = None
        self._return_impl = None

        return_type = self._sig.return_annotation
        # Default (a.k.a Signature.empty) if no return type given is List[tuple]
        if return_type == inspect.Signature.empty:
            return_type = typing.List[tuple]

        # Handle Optional[X] / X | None by unwrapping to X
        # The _one_return impl already handles None when no row is found
        unwrapped = _unwrap_optional(return_type)
        if unwrapped is not None:
            return_type = unwrapped

        generic_type = typing.get_origin(return_type)
        generic_args = typing.get_args(return_type)
        returns_none = False
        row_type = None

        if return_type in [None, typing.NoReturn]:
            self._return_impl = self._none_return
            returns_none = True
        # Classes / Dataclasses etc ...
        elif return_type and not generic_type:
            self._return_impl = self._one_return
            row_type = return_type
        # Single dictionary returns, no key / value typing suggested
        elif generic_type in DICT_GENERICS and not generic_args:
            self._return_impl = self._one_return
            row_type = dict
        elif generic_type in LIST_GENERICS:
            self._return_impl = self._many_return
            row_type = generic_args[0] if generic_args else tuple
        else:
            raise CannotInferMappingError(f"Unable to determine mapper for {return_type}")

        self._row_mapper = row_mapper or get_row_mapper(row_type)
        if not returns_none and not self._row_mapper:
            raise CannotInferMappingError(f"Unable to determine row mapper for {row_type}")

    @staticmethod
    def _none_return(results: ResultSet):
        return None

    def _one_return(self, results: ResultSet):
        if results.rowcount > 1:
            raise TooManyRowsError(f"Only expected one row, but got {results.rowcount}")
        raw = results.fetchone()
        return self._row_mapper(raw, results.description) if raw else None

    def _many_return(self, results: ResultSet):
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


class BoundGeneratingQuery(BoundSQLFunction):
    """Implementation of a bound function that represents a SQL query that returns rows via a generator."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable, row_mapper: RowMapper = None):
        """Construct a BoundGeneratingQuery that returns result sets.

        :param binder: the binder to which this bound function belongs
        :param sql_template: the template to use for resolving SQL parameters / munged sql.
        :param func: the callable being bound to the given SQL
        :param row_mapper: a RowMapper implementation that overrides the mapper inferred from the return annotation

        :raises CannotInferMappingError, BadReturnTypeError
        """
        super().__init__(binder, sql_template, func)
        self._row_mapper = None
        return_type = self._sig.return_annotation
        generic_type = typing.get_origin(return_type)
        generic_args = typing.get_args(return_type)
        if generic_type not in GENERATOR_GENERICS:
            raise BadReturnTypeError(f"Expected results type to be Generator, got {generic_type}")
        # Generators require 3 args but we only support mapping the yield type
        if generic_args and generic_args[1:3] != (type(None), type(None)):
            raise CannotInferMappingError("Only yield_type should be specified for a generator")
        self._row_mapper = row_mapper or get_row_mapper(generic_args[0] if generic_args else tuple)

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


class BoundExecution(BoundSQLFunction):
    """Implementation of a bound function that represents a SQL query that does not return rows."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable):
        """Construct a BoundSQLFunction that returns no results.

        :param binder: the binder to which this bound function belongs
        :param sql_template: the template to use for resolving SQL parameters / munged sql.
        :param func: the callable being bound to the given SQL

        :raises: BadReturnTypeError
        """
        super().__init__(binder, sql_template, func)
        self._returns_none = False
        if self._sig.return_annotation == self._sig.empty or self._sig.return_annotation is None:
            self._returns_none = True
        elif self._sig.return_annotation != int:
            raise BadReturnTypeError("Bound executions can only return None or int (e.g. affected rows)")

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


class BoundTransaction(BoundFunction):
    """Implementation of a bound function whose call is done within a database transaction."""

    def __init__(self, binder: FunctionBinder, func: callable):
        """Construct a BoundFunction that represents a transaction.

        :param binder: the binder to which this bound function belongs
        :param func: the callable to be executed within a transaction

        :raises: MultipleConnectionArgumentError
        """
        super().__init__(binder, func)
        self._cnx_arg_name = None
        for name in self._sig.parameters:
            if issubclass(self._sig.parameters[name].annotation, Connection):
                if self._cnx_arg_name is not None:
                    error = f"Connection argument specified multiple times for {self.bound_function.__name__}"
                    raise MultipleConnectionArgumentError(error)
                self._cnx_arg_name = name

    def __str__(self) -> str:
        """Return a simple representation of a transaction bound function."""
        return f"{self.__class__.__name__} ({self.bound_function.__name__})"

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
