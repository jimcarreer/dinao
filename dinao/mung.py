"""Provides mung symbol abstractions for parameterized SQL template rendering."""

from abc import ABC, abstractmethod


class MungSymbolProvider(ABC):
    """Abstract base for providing mung symbols during template rendering."""

    @abstractmethod
    def __call__(self) -> str:
        """Return the next mung symbol for a parameterized placeholder.

        :returns: the mung symbol string
        """
        pass  # pragma: no cover


class StaticMungSymbolProvider(MungSymbolProvider):
    """Provides the same static mung symbol on every call."""

    def __init__(self, symbol: str):
        """Construct a static mung symbol provider.

        :param symbol: the symbol to return on every call
        """
        self._symbol = symbol

    def __call__(self) -> str:  # noqa: D102
        return self._symbol


class NumberedMungSymbolProvider(MungSymbolProvider):
    """Provides numbered mung symbols (e.g. $1, $2, $3) that increment on each call."""

    def __init__(self, start: int = 1, prefix: str = "$"):
        """Construct a numbered mung symbol provider.

        :param start: the starting number, defaults to 1
        :param prefix: the prefix before the number, defaults to "$"
        """
        self._counter = start
        self._prefix = prefix

    def __call__(self) -> str:  # noqa: D102
        symbol = f"{self._prefix}{self._counter}"
        self._counter += 1
        return symbol
