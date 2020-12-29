"""Tests basic functionality of dinao.binding.mappers module."""

import dataclasses
from datetime import datetime
from typing import Dict, Mapping, Optional, Tuple, Union
from uuid import UUID

from dinao.backend.base import ColumnDescriptor
from dinao.binding.errors import TooManyValuesError
from dinao.binding.mappers import ClassRowMapper, DictRowMapper, SingleValueRowMapper, TupleRowMapper, get_row_mapper

import pytest


@dataclasses.dataclass()
class SomeDataClass:
    """Data class for testing mappers."""

    field_01: str
    field_02: int


class SomeClass:
    """Regular class for testing mappers."""

    pass


@pytest.mark.parametrize(
    "row_type, expected",
    (
        (str, SingleValueRowMapper),
        (int, SingleValueRowMapper),
        (float, SingleValueRowMapper),
        (complex, SingleValueRowMapper),
        (datetime, SingleValueRowMapper),
        (UUID, SingleValueRowMapper),
        (tuple, TupleRowMapper),
        (Tuple, TupleRowMapper),
        (dict, DictRowMapper),
        (Dict, DictRowMapper),
        (Mapping, DictRowMapper),
        (SomeClass, ClassRowMapper),
        (SomeDataClass, ClassRowMapper),
    ),
)
def test_get_row_mapper(row_type, expected):
    """Tests get_row_mapper for known mappings."""
    assert isinstance(get_row_mapper(row_type), expected)


@pytest.mark.parametrize(
    "row_type",
    (
        Mapping[str, int],
        Tuple[str, ...],
        Union,
        Union[str, int],
        Optional,
        Optional[str],
    ),
)
def test_get_unknown_mappers(row_type):
    """Tests get_row_mapper for unknown mappings."""
    assert get_row_mapper(row_type) is None


def test_dict_row_mapper():
    """Tests the DictRowMapper class."""
    mapper = DictRowMapper()
    descriptions = (
        ColumnDescriptor("field_01", 0),
        ColumnDescriptor("field_02", 1),
        ColumnDescriptor("field_03", 3),
    )
    actual = mapper((0, "test", 3.0), descriptions)
    assert actual == {"field_01": 0, "field_02": "test", "field_03": 3.0}


def test_class_row_mapper():
    """Tests the ClassRowMapper class."""
    mapper = ClassRowMapper(SomeDataClass)
    descriptions = (
        ColumnDescriptor("field_01", 0),
        ColumnDescriptor("field_02", 1),
    )
    actual = mapper(("test", 20), descriptions)
    assert isinstance(actual, SomeDataClass)
    assert actual.field_01 == "test"
    assert actual.field_02 == 20


def test_single_value_row_mapper():
    """Tests the SingleValueRowMapper class."""
    mapper = SingleValueRowMapper()
    descriptions = (ColumnDescriptor("field_01", 0),)
    assert mapper(("test",), descriptions) == "test"


def test_single_value_row_mapper_throws():
    """Tests the SingleValueRowMapper class throws when there are to many columns."""
    mapper = SingleValueRowMapper()
    descriptions = (
        ColumnDescriptor("field_01", 0),
        ColumnDescriptor("field_02", 1),
    )
    with pytest.raises(TooManyValuesError, match="expected 1, got 2"):
        mapper((3.0, 4.0), descriptions)
