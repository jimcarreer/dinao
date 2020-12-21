"""Implements functionality for binding python functions to SQL queries and actions."""

import inspect
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Tuple

from dinao.backend.base import Connection, ConnectionPool
from dinao.binding.errors import BadReturnType, FunctionAlreadyBound, MissingTemplateArgument, TemplateError
from dinao.binding.templating import Template


class FunctionBinder:
    """Implements functionality for binding functions to SQL queries and actions."""

    def __init__(self, cnx_pool: ConnectionPool):
        """Construct a function binder utilizing the given connection pool.

        :param cnx_pool: the connection pool the function binder should use when executing bound SQL functions.
        """
        self._cnx_pool = cnx_pool
        self._active_cnx = None
        self._verbose_trace = False

    def _suppressed_raise(self, exc: Exception):
        if self._verbose_trace:
            raise exc
        # Try to cut down on the traces so the user gets closer to the issue in their code
        raise exc.with_traceback(None) from exc

    def _raise_for_multi_binding(self, func: callable):
        if isinstance(func, BoundedFunction):
            name = func.bounded_function.__name__
            error = f"The function {name} has already been bounded by {func}"
            self._suppressed_raise(FunctionAlreadyBound(error))

    def _make_template(self, sql: str):
        try:
            return Template(sql, self._cnx_pool.mung_symbol)
        except TemplateError as exc:
            self._suppressed_raise(exc)

    def execute(self, sql: str) -> callable:
        """Binds a given function to a given SQL template.

        .. warning::
            The execution of the SQL given is not expected to return results / rows. Use the

        .. note::
            The return type of the decorated / bound function may return an int, which will make it return the number of
            rows affected by the execution of the last SQL statement in the template.

        Example::

            @binder.execute("INSERT INTO my_table (col1, col2) VALUES (#{arg1.some_key_or_member}, #{arg2})")
            def my_bounded_func(arg1: dict, arg2: str) -> int:
                pass

        :param sql: a SQL template to bind the function execution to
        :returns: a decorator expecting a callable
        :raises: BadReturnType, FunctionAlreadyBound
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            template = self._make_template(sql)
            return BoundedExecution(self, template, func)
        return decorated
        # fmt: on

    def query(self, sql: str) -> callable:
        """Binds a given function to a given SQL template.

        .. warning::
            Currently return type mapping is not implemented if the return type specified on the bounded function is not
            either empty or None a NotImplementedError is raised.  A list of tuples is returned when the return hint is
            left empty, representing the results from the query.

        Example::

            @binder.query("SELECT * INTO my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            def my_bounded_func(arg1: dict, arg2: str):
                pass

        :param sql: a SQL template to bind the function execution to
        :returns: a decorator expecting a callable
        :raises: NotImplementedError, FunctionAlreadyBound
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            template = self._make_template(sql)
            return BoundedQuery(self, template, func)
        return decorated
        # fmt: on

    def transaction(self):
        """Binds a given function to a given SQL transaction.

        All bounded function called during the call of a function bounded in this way will use the same transaction /
        connection.

        Example::

            @binder.query("SELECT * INTO my_table WHERE col_1 = #{arg1.some_key_or_member} AND col_2 = #{arg2}")
            def my_select(arg1: dict, arg2: str):
                pass

            @binder.execute(
                "INSERT INTO stats_table VALUES "
                "(#{stats.name}, #{stats.value}) "
                "ON CONFLICT (name) DO UPDATE SET value = #{stats.value}"
            )
            def my_update(stat: dict) -> int:
                pass

            @binder.transaction()
            def my_transaction(arg1: dict, stat_name: str) -> int:
                my_table_results = my_select(arg1, stat_name)
                stat = {"name": stat, "value": 0}
                for row in my_table_results:
                    stat["value"] += row[0]
                return my_update(stat=stat)

        :returns: a decorator expecting a callable
        :raises: NotImplementedError, FunctionAlreadyBound
        """
        # fmt: off
        def decorated(func: callable):
            self._raise_for_multi_binding(func)
            return BoundedTransaction(self, func)
        return decorated
        # fmt: on

    @contextmanager
    def connection(self) -> Connection:
        """Context manger for database connections used by bound functions.

        An active connection will be kept and yielded, provided to all contexts within the context that initially asked
        for it.

        Example::

            with binder.connection() as cnx_outer:
                with binder.connection() as cnx_inner:
                    assert cnx_outer == cnx_inner  # This assertion is true

        Any exception caught during the context of a connection will trigger a rollback of the transaction.  The
        transaction is automatically committed when execution is yielded back.
        """
        if self._active_cnx:
            yield self._active_cnx
            return
        self._active_cnx = self._cnx_pool.lease()
        try:
            yield self._active_cnx
            self._active_cnx.commit()
        finally:
            self._cnx_pool.release(self._active_cnx)
            self._active_cnx = None


class BoundedFunction(ABC):
    """Abstract class for common behaviors between bound functions."""

    def __init__(self, binder: FunctionBinder, func: callable):  # noqa: D107
        self._binder = binder
        self._func = func
        self._sig = inspect.signature(func)

    @abstractmethod
    def __call__(self, *args, **kwargs):  # noqa: D102
        pass  # pragma: no cover

    @property
    def bounded_function(self) -> callable:
        """Return the original callable this object binds."""
        return self._func


class BoundedSQLFunction(BoundedFunction):
    """Abstract class for common behaviors between functions bound to templated SQL."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable):  # noqa: D107
        super().__init__(binder, func)
        self._sql_template = sql_template
        for arg_id in sql_template.arguments:
            if arg_id[0] not in self._sig.parameters:
                error = f"Argument '{arg_id[0]}' specified in template but is not an argument of {func.__name__}"
                raise MissingTemplateArgument(error)

    @staticmethod
    def _resolve_value(arg_id: Tuple[str], root_args: dict):
        node = root_args
        for arg_name in arg_id:
            node = node[arg_name] if isinstance(node, dict) else getattr(node, arg_name)
        return node

    def _resolve_values(self, *args, **kwargs):
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        cached = {}
        values = []
        for arg_id in self._sql_template.arguments:
            value = cached.get(arg_id)
            if not value:
                value = self._resolve_value(arg_id, bound.arguments)
                cached[arg_id] = value
            values.append(value)
        return tuple(values)

    def __str__(self) -> str:
        """Return a simple representation of a SQL bound function."""
        return f"{self.__class__.__name__} of {self._sql_template} ({self.bounded_function.__name__})"

    @abstractmethod
    def __call__(self, *args, **kwargs):  # noqa: D102
        pass  # pragma: no cover


class BoundedQuery(BoundedSQLFunction):
    """Implementation of a bounded function that represents a SQL query that returns rows."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable):
        """Construct a BoundSQLFunction that returns result sets.

        :param binder: the binder to which this bound function belongs
        :param sql_template: the template to use for resolving SQL parameters / munged sql.
        :param func: the callable being bound to the given SQL
        """
        super().__init__(binder, sql_template, func)
        self._returns_none = False
        self._returns_directly = False
        if self._sig.return_annotation is None:
            self._returns_none = True
        elif self._sig.return_annotation == self._sig.empty:
            self._returns_directly = True
        else:
            # TODO: Actually implement mapper/mapping support
            raise NotImplementedError("Return mapping not implemented")  # pragma: no cover

    def __call__(self, *args, **kwargs):
        """Execute the templated SQL as a query with results.

        :param args: the positional arguments of the bounded / decorated function
        :param kwargs: the keyword arguments of the bounded / decorated function
        """
        values = self._resolve_values(*args, **kwargs)
        with self._binder.connection() as cnx:
            with cnx.query(self._sql_template.munged_sql, values) as results:
                if self._returns_none:
                    return None
                return results.fetchall()


class BoundedExecution(BoundedSQLFunction):
    """Implementation of a bounded function that represents a SQL query that does not return rows."""

    def __init__(self, binder: FunctionBinder, sql_template: Template, func: callable):
        """Construct a BoundSQLFunction that returns no results.

        :param binder: the binder to which this bound function belongs
        :param sql_template: the template to use for resolving SQL parameters / munged sql.
        :param func: the callable being bound to the given SQL

        :raises: BadReturnType
        """
        super().__init__(binder, sql_template, func)
        self._returns_none = False
        if self._sig.return_annotation == self._sig.empty or self._sig.return_annotation is None:
            self._returns_none = True
        elif self._sig.return_annotation != int:
            raise BadReturnType("Bound executions can only return None or int (e.g. affected rows)")

    def __call__(self, *args, **kwargs):
        """Execute the templated SQL as a query without results.

        :param args: the positional arguments of the bounded / decorated function
        :param kwargs: the keyword arguments of the bounded / decorated function
        """
        values = self._resolve_values(*args, **kwargs)
        with self._binder.connection() as cnx:
            affected = cnx.execute(self._sql_template.munged_sql, values)
            return None if self._returns_none else affected


class BoundedTransaction(BoundedFunction):
    """Implementation of a bounded function whose call is done within a database transaction."""

    def __str__(self) -> str:
        """Return a simple representation of a transaction bound function."""
        return f"{self.__class__.__name__} ({self.bounded_function.__name__})"

    def __call__(self, *args, **kwargs):
        """Execute the decorated / bound function with a single database connection / transaction.

        :param args: the positional arguments of the bounded / decorated function
        :param kwargs: the keyword arguments of the bounded / decorated function
        """
        with self._binder.connection():
            return self._func(*args, **kwargs)
