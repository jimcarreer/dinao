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


class MappingError(Exception):
    """Base exception for errors related to mapping database results to return types."""

    pass


class TemplateError(BindingError):
    """Raised when parsing a template fails."""

    pass


class FunctionAlreadyBoundError(BindingError):
    """Raised when a function is bound more than once to a query, execution, or transaction."""

    pass


class SignatureError(BindingError):
    """Base exception for binding errors related to function signature inspection."""

    pass


class BadReturnTypeError(SignatureError):
    """Raised when a return hint specifies a type that cannot be used for mapping in the context of the binding."""

    pass


class MissingTemplateArgumentError(SignatureError):
    """Raised when a template specifies an argument not found in its bounded function's signature."""

    pass


class CannotInferMappingError(SignatureError):
    """Raised when the return mapping for a bound function cannot be determined."""

    pass


class MultipleConnectionArgumentError(SignatureError):
    """Raised when a bound function specifies multiple connection arguments."""

    pass


class TooManyValuesError(MappingError):
    """Raised when the number of columns does not match the expected number for mapping."""

    pass


class TooManyRowsError(MappingError):
    """Raised when mapping implies a singular return, but many rows are returned."""

    pass
