"""Defines common errors raised from function binding."""


class BindingError(Exception):
    """Base exception for errors when binding functions."""

    pass


class SignatureError(BindingError):
    """Base exception for binding errors related to function signature inspection."""

    pass


class BadReturnType(SignatureError):
    """Raised when a return hint specifies a type that cannot be used for mapping in the context of the binding."""

    pass
