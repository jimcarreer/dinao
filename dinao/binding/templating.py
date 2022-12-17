"""Implements a simple templating grammar / parser for binding SQL to functions."""

from typing import Tuple

from dinao.binding.errors import TemplateError

# fmt: off
from pyparsing import (  # noqa: I101
    alphanums, alphas, printables,
    Combine, Forward, Group, OneOrMore, Suppress, White, Word, ZeroOrMore, ParseBaseException,
    ParseResults
)
# fmt: on


class TemplateParameter:
    """Represents a parameter for prepared statement rendered from a template."""

    def __init__(self, kwarg_path: Tuple[str], is_replace: bool = False):
        """Construct a template parameter.

        :param kwarg_path: an immutable list representing the 'path' of the parameter in a root dictionary of arguments
        :param is_replace: is this parameter a direct replacement
        """
        self.is_replace = is_replace
        self.kwarg_path = kwarg_path


def set_result_type(subject: str, position: int, result: ParseResults):
    """For use with forward lookup groups to inject suppressed characters into the parse result.

    :param subject: string being parsed
    :param position: position in the string the parsing is taking place
    :param result: the parse result object for the given position / subject
    """
    result[0].insert(0, subject[position])


class Template:
    """Implements templating supporting only argument replacement."""

    # Grammar is based roughly on problem description from
    # https://github.com/pyparsing/pyparsing/issues/5
    # fmt: off
    OPEN_VAR     = Suppress("#{")                                                           # noqa: E221
    OPEN_REP     = Suppress("!{")                                                           # noqa: E221
    OPENS        = (OPEN_VAR | OPEN_REP)                                                    # noqa: E221
    CLOSE        = Suppress("}")                                                            # noqa: E221
    LONERS       = ((~OPENS + "#") | (~OPENS + "!") |                                       # noqa: E221
                    (~OPENS + "{") | (~OPENS + "}"))                                        # noqa: E221
    IDENTIFIER   = Word(alphas, alphanums + "_")                                            # noqa: E221
    REPLACEMENT  = (IDENTIFIER + ZeroOrMore(Suppress(".") + IDENTIFIER))                    # noqa: E221
    PLAIN_TEXT   = Word(printables, excludeChars="#!{}")                                    # noqa: E221
    NULL_SPACE   = ZeroOrMore(White())                                                      # noqa: E221
    LOOKUP_VAR   = Forward()                                                                # noqa: E221
    LOOKUP_REP   = Forward()                                                                # noqa: E221
    SQL_FRAGMENT = Combine(NULL_SPACE + OneOrMore(PLAIN_TEXT | LONERS) + NULL_SPACE)        # noqa: E221
    LOOKUP_VAR  << Group(OPEN_VAR + REPLACEMENT + CLOSE).add_parse_action(set_result_type)  # noqa: E221
    LOOKUP_REP  << Group(OPEN_REP + REPLACEMENT + CLOSE).add_parse_action(set_result_type)  # noqa: E221
    LOOKUP       = (LOOKUP_VAR | LOOKUP_REP)                                                # noqa: E221
    GRAMMAR      = OneOrMore(NULL_SPACE + LOOKUP + NULL_SPACE | SQL_FRAGMENT)               # noqa: E221
    # fmt: on

    def __init__(self, sql_template: str):
        """Construct a simple variable replacement template.

        :param sql_template: the raw SQL template before parsing as a plain string.
        """
        self._sql_template = sql_template
        self._parsed_template = []
        self._arguments = []
        try:
            nodes = self.GRAMMAR.parseString(sql_template, parseAll=True)
        except ParseBaseException as x:
            raise TemplateError(f"{x.msg}:\n{x.line}\n{(' '*(x.col -1 ))}^")
        for node in nodes:
            if not isinstance(node, str):
                is_replace = node.pop(0) == "!"
                node = tuple(map(str, node))
                self._arguments.append(node)
                self._parsed_template.append(TemplateParameter(node, is_replace))
                continue
            # If the previous element was a string, concat it with the new str node otherwise append it
            previous = self._parsed_template.pop() if self._parsed_template else ""
            previous = [previous + node] if isinstance(previous, str) else [previous, node]
            self._parsed_template += previous
        self._arguments = tuple(self._arguments)

    @property
    def arguments(self) -> Tuple[Tuple[str]]:
        """Return the arguments expected by the template."""
        return self._arguments

    def __str__(self) -> str:
        """Return a simple representation of the template."""
        return self._sql_template

    @staticmethod
    def _resolve_value(kwarg_path: Tuple[str], root_args: dict):
        node = root_args
        for arg_name in kwarg_path:
            node = node[arg_name] if isinstance(node, dict) else getattr(node, arg_name)
        return node

    def render(self, mung_symbol: str, kwargs: dict) -> [str, tuple]:
        """Render the template to SQL execution arguments.

        :param mung_symbol: the symbol used to replace parameters in the template
        :param kwargs: the root dictionary of key word arguments to resolve parameters from

        :returns: a tuple where the first element is a SQL statement and the second is a tuple of its parameters
        """
        munged = ""
        parameters = []
        cache = {}
        for frag in self._parsed_template:
            if isinstance(frag, TemplateParameter):
                value = cache.get(frag.kwarg_path) or self._resolve_value(frag.kwarg_path, kwargs)
                cache[frag.kwarg_path] = value
                if frag.is_replace:
                    frag = str(value)
                else:
                    parameters.append(value)
                    frag = mung_symbol
            munged += frag
        return munged, tuple(parameters)
