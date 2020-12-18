"""Tests for the templating implementation in dinao.binding module."""

from typing import Tuple

from dinao.binding.templating import Template

import pytest

from tests.binding.template_cases import GOOD_CASES


@pytest.mark.parametrize("init_args, expected_sql, expected_args", GOOD_CASES)
def test_valid_templates(init_args: Tuple[str], expected_sql: str, expected_args: Tuple[Tuple[str]]):
    """Tests functionality around well formed SQL template strings."""
    template = Template(*init_args)
    assert template.munged_sql == expected_sql
    assert template.arguments == expected_args
