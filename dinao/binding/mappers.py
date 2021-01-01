"""Mapper interface definition and stock implementations."""

import collections.abc
import inspect
import typing
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from dinao.backend.base import ColumnDescriptor
from dinao.binding.errors import TooManyValuesError

TUPLE_GENERICS = [tuple, typing.Tuple]
GENERATOR_GENERICS = [collections.abc.Generator, typing.Generator]
DICT_GENERICS = [dict, typing.Dict, typing.Mapping, collections.abc.Mapping]
LIST_GENERICS = [list, typing.List, typing.Iterable, collections.abc.Sequence, collections.abc.Iterable]
NATIVE_SINGLE = [str, int, float, complex, datetime, UUID]


class RowMapper(ABC):
    """Interface for database results mapper."""

    @abstractmethod
    def __call__(self, row: typing.Tuple, description: typing.Tuple[ColumnDescriptor, ...]):
        """Map a database row to the type this implementation maps to.

        :param row: a tuple of raw values from a database result set
        :param description: the description of the row's columns

        :returns: the row as the mapped type.
        """
        pass  # pragma: no cover


class TupleRowMapper(RowMapper):
    """Implements mapping for simply returning the raw result tuple with no casting."""

    def __call__(self, row: typing.Tuple, description: typing.Tuple[ColumnDescriptor, ...]):  # noqa: D102
        return row


class DictRowMapper(RowMapper):
    """Implements mapping for dictionaries where keys are string and values are not cast."""

    def __call__(self, row: typing.Tuple, description: typing.Tuple[ColumnDescriptor, ...]):  # noqa: D102
        return {d.name: row[col] for col, d in enumerate(description)}


class ClassRowMapper(DictRowMapper):
    """Implements mapping for classes where rows are passed as kwargs."""

    def __init__(self, mapped_class):
        """Construct a class row mapper by passing the row as named key word args to the class constructor.

        :param mapped_class: the class to map a row to
        """
        self._mapped_class = mapped_class

    def __call__(self, row: typing.Tuple, description: typing.Tuple[ColumnDescriptor, ...]):  # noqa: D102
        kwargs = super().__call__(row, description)
        return self._mapped_class(**kwargs)


class SingleValueRowMapper(RowMapper):
    """Implements a row mapper for a primitive type."""

    def __call__(self, row: typing.Tuple, description: typing.Tuple[ColumnDescriptor, ...]):  # noqa: D102
        if len(row) > 1:
            raise TooManyValuesError(f"Too many values, expected 1, got {len(row)}")
        return row[0]


def get_row_mapper(row_type: typing.Type) -> typing.Optional[RowMapper]:
    """Find the row mapper for the given row type.

    :param row_type: the type to find a mapper for

    :returns: a row mapper if one could be determined, None otherwise
    """
    generics_type = typing.get_origin(row_type)
    generics_args = typing.get_args(row_type)
    # Currently we don't support things like Tuple[str, int, ... ], Dict[str, int] etc ...
    if generics_type and generics_args:
        return None
    elif row_type in TUPLE_GENERICS:
        return TupleRowMapper()
    elif row_type in DICT_GENERICS:
        return DictRowMapper()
    elif row_type in NATIVE_SINGLE:
        return SingleValueRowMapper()
    # Finally fall back to kwargs init
    elif inspect.isclass(row_type):
        return ClassRowMapper(row_type)
    # Signal we don't know what to do
    return None
