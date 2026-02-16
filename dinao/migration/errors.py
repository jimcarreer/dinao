"""Migration-specific exception classes."""


class MigrationError(Exception):
    """Base exception for all migration errors."""

    pass


class MigrationInProgressError(MigrationError):
    """Raised when another replica or process already holds the migration lock."""

    pass


class RevisionError(MigrationError):
    """Raised when a migration script fails during execution."""

    pass


class DiscoveryError(MigrationError):
    """Raised when the script directory or pattern is invalid."""

    pass


class ScriptValidationError(MigrationError):
    """Raised when a migration script is missing a valid upgrade function."""

    pass
