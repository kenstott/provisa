# Copyright (c) 2026 Kenneth Stott
# Canary: 2ba45b8e-40d0-4e54-b0b5-9356877a49aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OData V4 URL expressions: the text of ``$filter`` and ``$orderby`` as a tree (REQ-2193).

A recursive-descent parser over a tokenizer, with no database in it: what a name means and whether
its types fit is ``filter_sql``'s, against the role's entity set. The grammar is the part of OData
ABNF the service serves. A construct of the language it does not serve (a function, an arithmetic
operator, a path) is answered 501 naming it; text that is not the language is answered 400.
"""

# Requirements: REQ-2193

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

from provisa.api.odata.errors import QueryOptionRefused

# Each bound guards a recursion: parsing a nested expression, and every walk of the tree after it.
MAX_DEPTH = 64
MAX_TERMS = 1000

FUNCTIONS: dict[str, int] = {
    "contains": 2,
    "startswith": 2,
    "endswith": 2,
    "tolower": 1,
    "toupper": 1,
    "length": 1,
    "year": 1,
    "month": 1,
    "day": 1,
}
_COMPARISONS = ("eq", "ne", "gt", "ge", "lt", "le")
_NOT_SERVED_OPERATORS = ("add", "sub", "mul", "div", "divby", "mod", "has")


@dataclass(frozen=True)
class Literal:
    kind: str  # null, string, number, boolean, date, datetime, time, guid
    value: object


@dataclass(frozen=True)
class Name:
    name: str


@dataclass(frozen=True)
class Call:
    name: str
    args: tuple[Node, ...]


@dataclass(frozen=True)
class Comparison:
    operator: str
    left: Node
    right: Node


@dataclass(frozen=True)
class In:
    left: Node
    items: tuple[Node, ...]


@dataclass(frozen=True)
class Logical:
    operator: str  # and, or
    operands: tuple[Node, ...]


@dataclass(frozen=True)
class Not:
    operand: Node


Node = Literal | Name | Call | Comparison | In | Logical | Not


@dataclass(frozen=True)
class OrderKey:
    name: str
    descending: bool = False


@dataclass(frozen=True)
class _Token:
    type: str
    text: str
    position: int


_END = "end"
_TOKENS = re.compile(
    r"""
      (?P<space>\s+)
    | (?P<string>'(?:[^']|'')*')
    | (?P<guid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?![\w-]))
    | (?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,12})?)?(?:Z|[+-]\d{2}:\d{2}))
    | (?P<date>\d{4}-\d{2}-\d{2}(?![\w:.-]))
    | (?P<time>\d{2}:\d{2}(?::\d{2}(?:\.\d{1,12})?)?(?![\w:.-]))
    | (?P<special>-?INF(?!\w)|NaN(?!\w))
    | (?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?![\w.]))
    | (?P<name>[A-Za-z_][\w.]*)
    | (?P<symbol>[(),/:=])
    """,
    re.VERBOSE,
)


def _invalid(option: str, message: str) -> QueryOptionRefused:
    return QueryOptionRefused(400, "invalid_query_option", f"{option}: {message}")


def _not_served(what: str) -> QueryOptionRefused:
    return QueryOptionRefused(501, "not_implemented", f"{what} is not served")


def _tokens(option: str, text: str) -> list[_Token]:
    found: list[_Token] = []
    position = 0
    while position < len(text):
        match = _TOKENS.match(text, position)
        if match is None:
            raise _invalid(option, f"unreadable text at position {position}: {text[position:]!r}")
        assert match.lastgroup is not None
        if match.lastgroup != "space":
            found.append(_Token(match.lastgroup, match.group(), position))
        position = match.end()
    found.append(_Token(_END, "", position))
    return found


def _literal(option: str, token: _Token) -> Literal:
    text = token.text
    if token.type == "string":
        return Literal("string", text[1:-1].replace("''", "'"))
    if token.type == "special":
        return Literal("number", float(text.replace("INF", "inf").replace("NaN", "nan")))
    if token.type == "number":
        exact = "." not in text and "e" not in text.lower()
        decimal = "e" not in text.lower()
        value = int(text) if exact else Decimal(text) if decimal else float(text)
        return Literal("number", value)
    try:
        if token.type == "guid":
            return Literal("guid", UUID(text))
        if token.type == "date":
            return Literal("date", date.fromisoformat(text))
        if token.type == "time":
            return Literal("time", time.fromisoformat(text[:15]))
        return Literal("datetime", datetime.fromisoformat(_to_microseconds(text)))
    except ValueError as exc:
        raise _invalid(option, f"{text!r} is not a {token.type}: {exc}") from exc


_FRACTION = re.compile(r"(\.\d{6})\d+")


def _to_microseconds(text: str) -> str:
    """A timestamp's fraction to the microseconds a ``datetime`` holds: OData allows twelve digits."""
    return _FRACTION.sub(r"\1", text)


class _Parser:
    """A cursor over one expression's tokens. Precedence, loosest first: or, and, not, comparison;
    a function, a parenthesis, a name or a literal binds tightest."""

    def __init__(self, option: str, text: str) -> None:
        self.option = option
        self.tokens = _tokens(option, text)
        self.at = 0
        self.depth = 0
        self.terms = 0

    def _peek(self) -> _Token:
        return self.tokens[self.at]

    def _next(self) -> _Token:
        token = self.tokens[self.at]
        self.at += 1
        return token

    def _is_word(self, *words: str) -> bool:
        token = self._peek()
        return token.type == "name" and token.text in words

    def _is_symbol(self, symbol: str) -> bool:
        token = self._peek()
        return token.type == "symbol" and token.text == symbol

    def _expect(self, symbol: str) -> None:
        token = self._next()
        if token.type != "symbol" or token.text != symbol:
            raise self._unexpected(token, f"expected {symbol!r}")

    def _unexpected(self, token: _Token, need: str = "") -> QueryOptionRefused:
        if token.type == "name" and token.text in _NOT_SERVED_OPERATORS:
            return _not_served(f"the {token.text} operator")
        found = "the end of the text" if token.type == _END else repr(token.text)
        said = f"{found} at position {token.position}"
        return _invalid(self.option, f"{need}, found {said}" if need else f"unexpected {said}")

    def _nested(self) -> None:
        self.depth += 1
        if self.depth > MAX_DEPTH:
            raise _invalid(self.option, f"nested deeper than {MAX_DEPTH}")

    def expression(self) -> Node:
        node = self._or()
        token = self._peek()
        if token.type != _END:
            raise self._unexpected(token)
        return node

    def _logical(self, operator: str, operand: Callable[[], Node]) -> Node:
        operands = [operand()]
        while self._is_word(operator):
            self._next()
            operands.append(operand())
        return operands[0] if len(operands) == 1 else Logical(operator, tuple(operands))

    def _or(self) -> Node:
        return self._logical("or", self._and)

    def _and(self) -> Node:
        return self._logical("and", self._not)

    def _not(self) -> Node:
        if not self._is_word("not"):
            return self._comparison()
        self._next()
        self._nested()
        node = Not(self._not())
        self.depth -= 1
        return node

    def _comparison(self) -> Node:
        self.terms += 1
        if self.terms > MAX_TERMS:
            raise _invalid(self.option, f"more than {MAX_TERMS} terms")
        left = self._primary()
        if self._is_word(*_COMPARISONS):
            operator = self._next().text
            return Comparison(operator, left, self._primary())
        if self._is_word("in"):
            self._next()
            return In(left, self._items())
        return left

    def _items(self) -> tuple[Node, ...]:
        self._expect("(")
        items = [self._primary()]
        while self._is_symbol(","):
            self._next()
            items.append(self._primary())
        self._expect(")")
        self.terms += len(items)
        if self.terms > MAX_TERMS:
            raise _invalid(self.option, f"more than {MAX_TERMS} terms")
        return tuple(items)

    def _primary(self) -> Node:
        token = self._next()
        if token.type == "symbol" and token.text == "(":
            self._nested()
            node = self._or()
            self.depth -= 1
            self._expect(")")
            return node
        if token.type == "name":
            return self._named(token)
        if token.type == "symbol" or token.type == _END:
            raise self._unexpected(token, "expected a value")
        return _literal(self.option, token)

    def _named(self, token: _Token) -> Node:
        if token.text == "null":
            return Literal("null", None)
        if token.text in ("true", "false"):
            return Literal("boolean", token.text == "true")
        following = self._peek()
        if following.type == "string" and following.position == token.position + len(token.text):
            raise _not_served(f"the {token.text} literal")
        if self._is_symbol("("):
            return self._call(token)
        if self._is_symbol("/"):
            raise _not_served(f"the path {token.text}/")
        if "." in token.text:
            raise _not_served(f"the qualified name {token.text}")
        return Name(token.text)

    def _call(self, token: _Token) -> Node:
        arity = FUNCTIONS.get(token.text)
        if arity is None:
            raise _not_served(f"the function {token.text}")
        self._expect("(")
        self._nested()
        args = [self._primary()]
        while self._is_symbol(","):
            self._next()
            args.append(self._primary())
        self.depth -= 1
        self._expect(")")
        if len(args) != arity:
            raise _invalid(
                self.option, f"{token.text} takes {arity} argument(s), given {len(args)}"
            )
        return Call(token.text, tuple(args))

    def _key_literal(self) -> Literal:
        token = self._next()
        if token.type == "name" and token.text in ("true", "false"):
            return Literal("boolean", token.text == "true")
        if token.type in ("symbol", "name", _END):
            raise self._unexpected(token, "expected a value")
        return _literal(self.option, token)

    def key_values(self) -> tuple[tuple[str | None, Literal], ...]:
        if self._peek().type != "name" or self._is_word("true", "false"):
            pairs: list[tuple[str | None, Literal]] = [(None, self._key_literal())]
        else:
            pairs = [self._key_pair()]
            while self._is_symbol(","):
                self._next()
                pairs.append(self._key_pair())
        token = self._peek()
        if token.type != _END:
            raise self._unexpected(token)
        return tuple(pairs)

    def _key_pair(self) -> tuple[str | None, Literal]:
        token = self._next()
        if token.type != "name":
            raise self._unexpected(token, "expected a key property")
        self._expect("=")
        return token.text, self._key_literal()

    def order_keys(self) -> tuple[OrderKey, ...]:
        keys = [self._order_key()]
        while self._is_symbol(","):
            self._next()
            keys.append(self._order_key())
        token = self._peek()
        if token.type != _END:
            raise self._unexpected(token)
        return tuple(keys)

    def _order_key(self) -> OrderKey:
        token = self._next()
        if token.type != "name" or token.text in ("null", "true", "false", "not"):
            raise self._unexpected(token, "expected a property")
        if self._is_symbol("("):
            raise _not_served(f"ordering by the function {token.text}")
        if self._is_symbol("/"):
            raise _not_served(f"ordering by the path {token.text}/")
        descending = self._is_word("desc")
        if descending or self._is_word("asc"):
            self._next()
        return OrderKey(token.text, descending)


def parse_key(text: str) -> tuple[tuple[str | None, Literal], ...]:
    """The key predicate inside ``set(...)``: one literal, or ``name=literal`` pairs."""
    return _Parser("key", text).key_values()


def parse_filter(text: str) -> Node:
    """The tree of a ``$filter``."""
    return _Parser("$filter", text).expression()


def parse_orderby(text: str) -> tuple[OrderKey, ...]:
    """The keys of an ``$orderby``. A property named twice orders nothing the first did not."""
    keys = _Parser("$orderby", text).order_keys()
    names = [key.name for key in keys]
    repeated = {name for name in names if names.count(name) > 1}
    if repeated:
        raise _invalid("$orderby", f"names {sorted(repeated)[0]!r} twice")
    return keys
