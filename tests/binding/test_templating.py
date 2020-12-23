"""Tests for the templating implementation in dinao.binding module."""

from typing import Tuple

from dinao.binding.errors import TemplateError
from dinao.binding.templating import Template

import pytest

from tests.binding.template_cases import GOOD_CASES


@pytest.mark.parametrize("init, ex_args, r_kwargs, ex_render, ", GOOD_CASES)
def test_valid_templates(init: Tuple, ex_args: Tuple[Tuple[str]], r_kwargs: dict, ex_render: Tuple[str, Tuple]):
    """Tests functionality around well formed SQL template strings."""
    template = Template(*init)
    assert template.arguments == ex_args
    sql, values = template.render("%s", r_kwargs)
    assert sql == ex_render[0]
    assert values == ex_render[1]


def test_bad_template():
    """Tests a bad template raises the appropariate error."""
    raw_template = [
        "INSERT INTO table VALUES (#{myarg1}, #{myarg2})",
        "  ON CONFLICT DO UPDATE",
        "SET mycol1 = #{myarg1",
        "WHERE mycol2 = #{marg2}",
    ]
    raw_template = "\n".join(raw_template)
    with pytest.raises(TemplateError, match="SET mycol1 = #{myarg1"):
        Template(raw_template)
