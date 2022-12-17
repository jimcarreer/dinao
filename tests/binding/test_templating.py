"""Tests for the templating implementation of the binding module."""

from typing import Tuple

from dinao.binding.errors import TemplateError
from dinao.binding.templating import Template

import pytest

from tests.binding.template_cases import GOOD_CASES, INVALID_CASES


@pytest.mark.parametrize("init, ex_args, r_kwargs, ex_render, ", GOOD_CASES)
def test_valid_templates(init: Tuple, ex_args: Tuple[Tuple[str]], r_kwargs: dict, ex_render: Tuple[str, Tuple]):
    """Tests functionality around well-formed SQL template strings."""
    template = Template(*init)
    assert template.arguments == ex_args
    sql, values = template.render("%s", r_kwargs)
    assert sql == ex_render[0]
    assert values == ex_render[1]


@pytest.mark.parametrize("init, error", INVALID_CASES)
def test_invalid_template(init, error):
    """Tests an invalid template raises the appropriate error."""
    with pytest.raises(TemplateError, match=error):
        Template(*init)
