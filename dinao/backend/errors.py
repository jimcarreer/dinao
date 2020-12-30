"""Defines common errors raised from database backend code."""


class UnsupportedBackendError(Exception):
    """Raised when an unsupported backend is specified."""

    pass


class ConfigurationError(Exception):
    """Raised when there is a backend configuration connection error."""

    pass


class BackendNotInstalledError(Exception):
    """Raised when a backend engine is not installed."""

    pass
