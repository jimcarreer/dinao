"""Defines common errors raised from database backend code."""


class UnsupportedBackend(Exception):
    """Raised when an unsupported backend is specified."""

    pass


class ConfigurationError(Exception):
    """Raised when theres a backend configuration connection error."""

    pass


class BackendEngineNotInstalled(Exception):
    """Raised when a backend engine is not installed."""

    pass
