"""Defines common errors raised from function binding."""


class NoPoolSetError(Exception):
    """Raised when there is no pool set for a function binder before a sql operation is called."""

    pass


class PoolAlreadySetError(Exception):
    """Raised when a pool is set twice on a binder."""

    pass


class BindingError(Exception):
    """Base exception for errors when binding functions."""

    pass


class TemplateError(BindingError):
    """Raised when parsing a template fails."""

    pass


class FunctionAlreadyBound(BindingError):
    """Raised when a function is bound more than once to a query, execution, or transaction."""

    pass


class SignatureError(BindingError):
    """Base exception for binding errors related to function signature inspection."""

    pass


class BadReturnType(SignatureError):
    """Raised when a return hint specifies a type that cannot be used for mapping in the context of the binding."""

    pass


class MissingTemplateArgument(SignatureError):
    """Raised when a template specifies an argument not found in its bounded function's signature."""

    pass
