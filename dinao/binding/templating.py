"""Implements a simple templating grammar / parser for binding SQL to functions."""

import typing

# fmt: off
from pyparsing import (  # noqa: I101
    alphanums, alphas, printables,
    Combine, Forward, Group, OneOrMore, Suppress, White, Word, ZeroOrMore
)
# fmt: on


class Template:
    """Implements templating supporting only argument replacement."""

    # Grammar is based roughly on problem description from
    # https://github.com/pyparsing/pyparsing/issues/5
    # fmt: off
    OPEN         = Suppress("#{")                                                     # noqa: E221
    CLOSE        = Suppress("}")                                                      # noqa: E221
    LONERS       = ((~OPEN + "#") | (~OPEN + "{") | (~OPEN + "}"))                    # noqa: E221
    IDENTIFIER   = Word(alphas, alphanums + "_")                                      # noqa: E221
    REPLACEMENT  = (IDENTIFIER + ZeroOrMore(Suppress(".") + IDENTIFIER))              # noqa: E221
    PLAIN_TEXT   = Word(printables, excludeChars="#{}")                               # noqa: E221
    NULL_SPACE   = ZeroOrMore(White())                                                # noqa: E221
    LOOKUP       = Forward()                                                          # noqa: E221
    SQL_FRAGMENT = Combine(NULL_SPACE + OneOrMore(PLAIN_TEXT | LONERS) + NULL_SPACE)  # noqa: E221
    LOOKUP      << Group(OPEN + REPLACEMENT + CLOSE)                                  # noqa: E221
    GRAMMAR      = OneOrMore(NULL_SPACE + LOOKUP + NULL_SPACE | SQL_FRAGMENT)         # noqa: E221
    # fmt: on

    def __init__(self, sql_template: str, mung_symbol: str):
        """Construct a simple variable replacement template.

        :param sql_template: the un-munged SQL template as a plain string.
        :param mung_symbol: the symbol to replace variable specifications with within the template.
        """
        self._sql_template = sql_template
        self._mung_symbol = mung_symbol
        self._argument_names = []
        self._munged_template = ""
        # TODO: Try/Except here, rethrow a more descriptive error with explict class
        nodes = self.GRAMMAR.parseString(sql_template, parseAll=True)
        for node in nodes:
            if not isinstance(node, str):
                node = tuple(map(str, node))
                self._argument_names.append(node)
                node = self._mung_symbol
            self._munged_template += node
        self._argument_names = tuple(self._argument_names)

    @property
    def arguments(self) -> typing.Tuple[typing.Tuple[str]]:
        """Return a tuple of tuples representing the argument identifiers used in the template."""
        return self._argument_names

    @property
    def munged_sql(self) -> str:
        """Return the SQL template with the argument specifiers replaced with the mung_symbol."""
        return self._munged_template
