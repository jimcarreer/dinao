"""Tests for migration script discovery."""

import os
import re

from dinao.migration.discovery import DEFAULT_PATTERN, MigrationScript, ScriptDiscovery
from dinao.migration.errors import DiscoveryError

import pytest


def test_discover_finds_matching_scripts(tmp_path):
    """Verify scripts matching the default pattern are discovered."""
    (tmp_path / "20240101_001_create_users.py").write_text("def upgrade(cnx): pass")
    (tmp_path / "20240102_001_add_email.py").write_text("def upgrade(cnx): pass")
    discovery = ScriptDiscovery(str(tmp_path))
    scripts = discovery.discover()
    assert len(scripts) == 2
    assert scripts[0].name == "20240101_001_create_users"
    assert scripts[1].name == "20240102_001_add_email"


def test_discover_ignores_non_matching_files(tmp_path):
    """Verify files not matching the pattern are ignored."""
    (tmp_path / "20240101_001_valid.py").write_text("def upgrade(cnx): pass")
    (tmp_path / "readme.txt").write_text("not a migration")
    (tmp_path / "helper.py").write_text("def helper(): pass")
    (tmp_path / "__init__.py").write_text("")
    discovery = ScriptDiscovery(str(tmp_path))
    scripts = discovery.discover()
    assert len(scripts) == 1
    assert scripts[0].name == "20240101_001_valid"


def test_discover_sorts_lexicographically(tmp_path):
    """Verify scripts are returned in sorted order."""
    (tmp_path / "20240301_001_third.py").write_text("def upgrade(cnx): pass")
    (tmp_path / "20240101_001_first.py").write_text("def upgrade(cnx): pass")
    (tmp_path / "20240201_001_second.py").write_text("def upgrade(cnx): pass")
    discovery = ScriptDiscovery(str(tmp_path))
    scripts = discovery.discover()
    assert [s.name for s in scripts] == [
        "20240101_001_first",
        "20240201_001_second",
        "20240301_001_third",
    ]


def test_discover_raises_on_duplicate_names(tmp_path):
    """Verify DiscoveryError is raised for duplicate script names."""
    sub1 = tmp_path / "20240101_001_dup.py"
    sub1.write_text("def upgrade(cnx): pass")
    # Create duplicate by adding another with same name - since we sort by filename,
    # duplicates can only happen if the same file appears twice (unlikely with os.listdir).
    # Instead, test the _check_duplicates method directly.
    scripts = [
        MigrationScript(name="20240101_001_dup", path=str(sub1)),
        MigrationScript(name="20240101_001_dup", path=str(sub1)),
    ]
    with pytest.raises(DiscoveryError, match="Duplicate migration script name"):
        ScriptDiscovery._check_duplicates(scripts)


def test_discover_missing_directory():
    """Verify DiscoveryError is raised when the directory does not exist."""
    with pytest.raises(DiscoveryError, match="does not exist"):
        ScriptDiscovery("/nonexistent/path")


def test_discover_empty_directory(tmp_path):
    """Verify empty list is returned for a directory with no matching scripts."""
    discovery = ScriptDiscovery(str(tmp_path))
    scripts = discovery.discover()
    assert scripts == []


def test_discover_custom_pattern(tmp_path):
    """Verify custom pattern overrides the default."""
    (tmp_path / "v001_create_users.py").write_text("def upgrade(cnx): pass")
    (tmp_path / "20240101_001_ignored.py").write_text("def upgrade(cnx): pass")
    discovery = ScriptDiscovery(str(tmp_path), pattern=r"^v\d{3}_.+\.py$")
    scripts = discovery.discover()
    assert len(scripts) == 1
    assert scripts[0].name == "v001_create_users"


def test_migration_script_path(tmp_path):
    """Verify script paths are absolute."""
    (tmp_path / "20240101_001_test.py").write_text("def upgrade(cnx): pass")
    discovery = ScriptDiscovery(str(tmp_path))
    scripts = discovery.discover()
    assert os.path.isabs(scripts[0].path)
    assert scripts[0].path.endswith("20240101_001_test.py")


def test_default_pattern_value():
    """Verify the default pattern matches the expected format."""
    pattern = re.compile(DEFAULT_PATTERN)
    assert pattern.match("20240315_001_add_users.py")
    assert pattern.match("20241231_999_long_description_here.py")
    assert not pattern.match("create_users.py")
    assert not pattern.match("2024_001_short_date.py")
    assert not pattern.match("20240315_01_two_digits.py")
