"""Shared base classes for sync and async function binding."""

import inspect
import types
import typing
from abc import ABC, abstractmethod

from dinao.backend.base import Connection, ConnectionPoolBase, ResultSet
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


class FunctionBinderBase(ABC):
    """Shared base for sync and async function binders."""

    def __init__(self):
        """Construct a function binder base."""
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

    def _validate_pool(self, pool):
        """Validate a pool before assignment. Override in subclasses for type checking."""
        pass

    def _validate_function(self, func):
        """Validate a function before binding. Override in subclasses for type checking."""
        pass  # pragma: no cover

    @property
    def pool(self) -> ConnectionPoolBase:
        """Get the connection pool used by this binder."""
        return self._cnx_pool

    @pool.setter
    def pool(self, pool: ConnectionPoolBase):
        """Set the connection pool used by this binder.

        :raises PoolAlreadySetError
        """
        if self._cnx_pool is not None:
            raise PoolAlreadySetError("The connection pool can only be set once")
        self._validate_pool(pool)
        self._cnx_pool = pool

    @property
    def mung_symbol(self):
        """Return the mung symbol used for rendering templates bound by the binder."""
        if self._cnx_pool is None:
            raise NoPoolSetError("No connection pool has been set for the binder")
        return self.pool.mung_symbol

    @abstractmethod
    def execute(self, sql: str) -> callable:
        """Bind a function to a SQL execution template."""
        pass  # pragma: no cover

    @abstractmethod
    def query(self, sql: str) -> callable:
        """Bind a function to a SQL query template."""
        pass  # pragma: no cover

    @abstractmethod
    def transaction(self) -> callable:
        """Bind a function to a SQL transaction."""
        pass  # pragma: no cover


class BoundFunction(ABC):
    """Abstract class for common behaviors between bound functions."""

    def __init__(self, binder: FunctionBinderBase, func: callable):  # noqa: D107
        binder._validate_function(func)
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

    def __init__(self, binder: FunctionBinderBase, sql_template: Template, func: callable):  # noqa: D107
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


class BoundQueryBase(BoundSQLFunction):
    """Shared base for sync and async bound queries that return rows."""

    def __init__(
        self, binder: FunctionBinderBase, sql_template: Template, func: callable, row_mapper: RowMapper = None
    ):
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

    @abstractmethod
    def _one_return(self, results):
        pass  # pragma: no cover

    @abstractmethod
    def _many_return(self, results):
        pass  # pragma: no cover


class BoundGeneratingQueryBase(BoundSQLFunction):
    """Shared base for sync and async bound generating queries."""

    def __init__(
        self, binder: FunctionBinderBase, sql_template: Template, func: callable, row_mapper: RowMapper = None
    ):
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
        if generic_type not in self._allowed_generics():
            raise BadReturnTypeError(f"Expected results type to be Generator, got {generic_type}")
        self._validate_generic_args(generic_args)
        self._row_mapper = row_mapper or get_row_mapper(generic_args[0] if generic_args else tuple)

    def _allowed_generics(self):
        """Return the list of allowed generic types for this bound query."""
        return GENERATOR_GENERICS

    def _validate_generic_args(self, generic_args):
        """Validate the generic arguments of the return type annotation."""
        # Generators require 3 args but we only support mapping the yield type
        if generic_args and generic_args[1:3] != (type(None), type(None)):
            raise CannotInferMappingError("Only yield_type should be specified for a generator")


class BoundExecutionBase(BoundSQLFunction):
    """Shared base for sync and async bound executions."""

    def __init__(self, binder: FunctionBinderBase, sql_template: Template, func: callable):
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


class BoundTransactionBase(BoundFunction):
    """Shared base for sync and async bound transactions."""

    _cnx_class = Connection

    def __init__(self, binder: FunctionBinderBase, func: callable):
        """Construct a BoundFunction that represents a transaction.

        :param binder: the binder to which this bound function belongs
        :param func: the callable to be executed within a transaction

        :raises: MultipleConnectionArgumentError
        """
        super().__init__(binder, func)
        self._cnx_arg_name = None
        for name in self._sig.parameters:
            if issubclass(self._sig.parameters[name].annotation, self._cnx_class):
                if self._cnx_arg_name is not None:
                    error = f"Connection argument specified multiple times for {self.bound_function.__name__}"
                    raise MultipleConnectionArgumentError(error)
                self._cnx_arg_name = name

    def __str__(self) -> str:
        """Return a simple representation of a transaction bound function."""
        return f"{self.__class__.__name__} ({self.bound_function.__name__})"
