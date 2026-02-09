"""Tests for the templating implementation of the binding module."""

from typing import Tuple

from dinao.binding.errors import TemplateError
from dinao.binding.templating import Template
from dinao.mung import NumberedMungSymbolProvider, StaticMungSymbolProvider

import pytest

from tests.binding.template_cases import GOOD_CASES, INVALID_CASES


@pytest.mark.parametrize("init, ex_args, r_kwargs, ex_render, ", GOOD_CASES)
def test_valid_templates(init: Tuple, ex_args: Tuple[Tuple[str]], r_kwargs: dict, ex_render: Tuple[str, Tuple]):
    """Tests functionality around well-formed SQL template strings."""
    template = Template(*init)
    assert template.arguments == ex_args
    sql, values = template.render(StaticMungSymbolProvider("%s"), r_kwargs)
    assert sql == ex_render[0]
    assert values == ex_render[1]


@pytest.mark.parametrize("init, error", INVALID_CASES)
def test_invalid_template(init, error):
    """Tests an invalid template raises the appropriate error."""
    with pytest.raises(TemplateError, match=error):
        Template(*init)


def test_static_mung_symbol_provider():
    """Tests that StaticMungSymbolProvider always returns the same symbol."""
    provider = StaticMungSymbolProvider("%s")
    assert provider() == "%s"
    assert provider() == "%s"
    assert provider() == "%s"

    provider2 = StaticMungSymbolProvider("?")
    assert provider2() == "?"
    assert provider2() == "?"


def test_numbered_mung_symbol_provider():
    """Tests that NumberedMungSymbolProvider increments on each call."""
    provider = NumberedMungSymbolProvider()
    assert provider() == "$1"
    assert provider() == "$2"
    assert provider() == "$3"


def test_numbered_mung_symbol_provider_custom():
    """Tests NumberedMungSymbolProvider with custom start and prefix."""
    provider = NumberedMungSymbolProvider(start=5, prefix=":")
    assert provider() == ":5"
    assert provider() == ":6"
    assert provider() == ":7"


def test_numbered_mung_symbol_template_render():
    """Tests rendering a template with NumberedMungSymbolProvider produces numbered placeholders."""
    template = Template("INSERT INTO my_table VALUES(#{a}, #{b}, #{c})")
    sql, values = template.render(NumberedMungSymbolProvider(), {"a": 1, "b": "two", "c": 3.0})
    assert sql == "INSERT INTO my_table VALUES($1, $2, $3)"
    assert values == (1, "two", 3.0)
