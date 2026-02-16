"""Tests for the migration error hierarchy."""

from dinao.migration.errors import (
    DiscoveryError,
    MigrationError,
    MigrationInProgressError,
    RevisionError,
    ScriptValidationError,
)


def test_migration_error_is_base():
    """Verify MigrationError is the base for all migration exceptions."""
    assert issubclass(MigrationInProgressError, MigrationError)
    assert issubclass(RevisionError, MigrationError)
    assert issubclass(DiscoveryError, MigrationError)
    assert issubclass(ScriptValidationError, MigrationError)


def test_migration_error_message():
    """Verify exception messages are preserved."""
    err = MigrationError("test message")
    assert str(err) == "test message"


def test_migration_in_progress_error():
    """Verify MigrationInProgressError can be raised and caught."""
    try:
        raise MigrationInProgressError("lock held")
    except MigrationError as exc:
        assert str(exc) == "lock held"


def test_revision_error():
    """Verify RevisionError can be raised and caught."""
    try:
        raise RevisionError("script failed")
    except MigrationError as exc:
        assert str(exc) == "script failed"


def test_discovery_error():
    """Verify DiscoveryError can be raised and caught."""
    try:
        raise DiscoveryError("bad directory")
    except MigrationError as exc:
        assert str(exc) == "bad directory"


def test_script_validation_error():
    """Verify ScriptValidationError can be raised and caught."""
    try:
        raise ScriptValidationError("missing upgrade")
    except MigrationError as exc:
        assert str(exc) == "missing upgrade"
