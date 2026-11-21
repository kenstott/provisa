# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2f4c17-6a3d-4b90-9c51-d07e1ab3f628
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's SQL/JSON path language, as src/backend/utils/adt/jsonpath_exec.c walks it, over the
values provisa.federation.duckdb_pg_json parses: None, bool, Decimal, str, list and dict
(REQ-2167, REQ-2171). The datetime methods, and .keyvalue() of a value inside another, are refused
by name: neither is answered here, and a wrong answer is worse than a refusal."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from typing import Any

from provisa.federation.duckdb_pg_scalars import _error
from provisa.transpiler.function_macros import FunctionRefused

Node = tuple
_DIVISION = Context(prec=60, rounding=ROUND_HALF_EVEN)
_INT4 = (-(2**31), 2**31 - 1)
_INT8 = (-(2**63), 2**63 - 1)


class PathError(Exception):
    """An error of the path's evaluation, which ``silent`` suppresses."""

    def __init__(self, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.message = message


# --- reading a path ------------------------------------------------------------------------------

_TOKEN = re.compile(
    r"""\s+|/\*.*?\*/
    |(?P<number>0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+
        |(?:\d[\d_]*(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)
    |(?P<string>"(?:[^"\\]|\\.)*")
    |(?P<variable>\$(?:[A-Za-z_][\w]*|"(?:[^"\\]|\\.)*"))
    |(?P<word>[A-Za-z_][\w]*)
    |(?P<operator>&&|\|\||==|!=|<>|<=|>=|\.\*\*|\.\*|[$@.,?()\[\]{}!<>+\-*/%])""",
    re.VERBOSE | re.DOTALL,
)
_ESCAPES = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}
_ESCAPED = re.compile(
    r"\\(?:u\{([0-9a-fA-F]+)\}|u([0-9a-fA-F]{4})|x([0-9a-fA-F]{2})|(.))", re.DOTALL
)


def _unquoted(text: str) -> str:
    def plain(found: re.Match[str]) -> str:
        code = found[1] or found[2] or found[3]
        return chr(int(code, 16)) if code else _ESCAPES.get(found[4], found[4])

    return _ESCAPED.sub(plain, text[1:-1])


def _number(text: str) -> Decimal:
    digits = text.replace("_", "")
    if digits[:2].lower() in ("0x", "0o", "0b"):
        return Decimal(int(digits, 0))
    return Decimal(digits)


def _tokens(path: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    position = 0
    while position < len(path):
        match = _TOKEN.match(path, position)
        if match is None:
            raise _error("42601", f'syntax error at or near "{path[position]}" of jsonpath input')
        position = match.end()
        if match.lastgroup is not None:
            found.append((match.lastgroup, match[0]))
    return found


_COMPARISONS = {"==": "==", "!=": "!=", "<>": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
_METHODS = frozenset(
    """type size double ceiling floor abs keyvalue bigint integer number decimal string boolean
    datetime date time time_tz timestamp timestamp_tz""".split()
)


class _Reader:
    def __init__(self, path: str) -> None:
        self.tokens = _tokens(path)
        self.at = 0
        self.subscripts = 0
        self.path = path

    def peek(self) -> str | None:
        return self.tokens[self.at][1] if self.at < len(self.tokens) else None

    def kind(self) -> str | None:
        return self.tokens[self.at][0] if self.at < len(self.tokens) else None

    def take(self) -> str:
        if self.at >= len(self.tokens):
            raise _error("42601", "syntax error at end of jsonpath input")
        self.at += 1
        return self.tokens[self.at - 1][1]

    def accept(self, *texts: str) -> bool:
        if self.peek() in texts:
            self.at += 1
            return True
        return False

    def expect(self, text: str) -> None:
        if not self.accept(text):
            raise _error("42601", f'syntax error at or near "{self.peek()}" of jsonpath input')

    def word(self, text: str) -> bool:
        return self.kind() == "word" and (self.peek() or "").lower() == text

    def accept_word(self, text: str) -> bool:
        if self.word(text):
            self.at += 1
            return True
        return False

    def whole(self) -> tuple[bool, Node]:
        strict = self.accept_word("strict")
        if not strict:
            self.accept_word("lax")
        node = self.or_()
        if self.at != len(self.tokens):
            raise _error("42601", f'syntax error at or near "{self.peek()}" of jsonpath input')
        return strict, node

    def or_(self) -> Node:
        node = self.and_()
        while self.accept("||"):
            node = ("or", node, self.and_())
        return node

    def and_(self) -> Node:
        node = self.not_()
        while self.accept("&&"):
            node = ("and", node, self.not_())
        return node

    def not_(self) -> Node:
        if self.accept("!"):
            self.expect("(")
            node = self.or_()
            self.expect(")")
            return ("not", node)
        return self.comparison()

    def comparison(self) -> Node:
        node = self.additive()
        if self.peek() in _COMPARISONS:
            operator = _COMPARISONS[self.take()]
            return ("compare", operator, node, self.additive())
        if self.accept_word("starts"):
            if not self.accept_word("with"):
                raise _error("42601", "syntax error in jsonpath input: starts with")
            return ("starts", node, self.additive())
        if self.accept_word("like_regex"):
            pattern = _unquoted(self.take())
            flags = _unquoted(self.take()) if self.accept_word("flag") else ""
            return ("like", node, _regex(pattern, flags))
        if self.accept_word("is"):
            if not self.accept_word("unknown"):
                raise _error("42601", "syntax error in jsonpath input: is unknown")
            return ("is_unknown", node)
        return node

    def additive(self) -> Node:
        node = self.multiplicative()
        while self.peek() in ("+", "-"):
            node = ("arithmetic", self.take(), node, self.multiplicative())
        return node

    def multiplicative(self) -> Node:
        node = self.unary()
        while self.peek() in ("*", "/", "%"):
            node = ("arithmetic", self.take(), node, self.unary())
        return node

    def unary(self) -> Node:
        if self.peek() in ("+", "-"):
            operator = self.take()
            return ("unary", operator, self.unary())
        return self.accessors(self.primary())

    def primary(self) -> Node:
        kind, text = self.kind(), self.peek()
        if text is None:
            raise _error("42601", "syntax error at end of jsonpath input")
        if text == "(":
            self.take()
            node = self.or_()
            self.expect(")")
            return node
        self.take()
        if kind == "number":
            return ("value", _number(text))
        if kind == "string":
            return ("value", _unquoted(text))
        if kind == "variable":
            return ("variable", _unquoted(text[1:]) if text[1] == '"' else text[1:])
        if text == "$":
            return ("root",)
        if text == "@":
            return ("current",)
        lowered = text.lower() if kind == "word" else ""
        if text in ("true", "false", "null"):
            return ("value", {"true": True, "false": False, "null": None}[text])
        if lowered == "last":
            if not self.subscripts:
                raise _error("42601", "LAST is allowed only in array subscripts")
            return ("last",)
        if lowered == "exists":
            self.expect("(")
            node = self.or_()
            self.expect(")")
            return ("exists", node)
        raise _error("42601", f'syntax error at or near "{text}" of jsonpath input')

    def accessors(self, node: Node) -> Node:
        while True:
            if self.accept(".**"):
                node = ("any", node, *self.levels())
            elif self.accept(".*"):
                node = ("any_key", node)
            elif self.accept("."):
                node = self.member(node)
            elif self.accept("["):
                node = self.subscript(node)
            elif self.accept("?"):
                self.expect("(")
                node = ("filter", node, self.or_())
                self.expect(")")
            else:
                return node

    def levels(self) -> tuple[int, int | None]:
        if not self.accept("{"):
            return 0, None
        first = self.level()
        last = self.level() if self.accept_word("to") else first
        self.expect("}")
        return first or 0, last

    def level(self) -> int | None:
        return None if self.accept_word("last") else int(self.take())

    def member(self, node: Node) -> Node:
        kind, text = self.kind(), self.take()
        if kind == "string":
            return ("key", node, _unquoted(text))
        if kind == "variable":
            return ("key", node, text)
        if kind == "word" and text in _METHODS and self.peek() == "(":
            self.take()
            arguments: list[Decimal | str] = []
            while not self.accept(")"):
                sign = -1 if self.accept("-") else 1
                argument = self.take()
                is_string = argument[0] == '"'
                arguments.append(_unquoted(argument) if is_string else sign * _number(argument))
                self.accept(",")
            return ("method", node, text, tuple(arguments))
        if kind != "word":
            raise _error("42601", f'syntax error at or near "{text}" of jsonpath input')
        return ("key", node, text)

    def subscript(self, node: Node) -> Node:
        if self.accept("*"):
            self.expect("]")
            return ("any_element", node)
        self.subscripts += 1
        ranges: list[tuple[Node, Node | None]] = []
        while True:
            start = self.or_()
            ranges.append((start, self.or_() if self.accept_word("to") else None))
            if not self.accept(","):
                break
        self.expect("]")
        self.subscripts -= 1
        return ("elements", node, tuple(ranges))


_FLAGS = {"i": re.IGNORECASE, "s": re.DOTALL, "m": re.MULTILINE, "x": re.VERBOSE}


def _regex(pattern: str, flags: str) -> re.Pattern[str]:
    bits = 0
    for flag in flags:
        if flag == "q":
            pattern = re.escape(pattern)
        elif flag in _FLAGS:
            bits |= _FLAGS[flag]
        else:
            raise _error("42601", f'invalid input syntax for type jsonpath: "{flag}"')
    return re.compile(pattern, bits)


# --- walking it ----------------------------------------------------------------------------------


def _type(item: Any) -> str:
    if item is None:
        return "null"
    if isinstance(item, bool):
        return "boolean"
    if isinstance(item, Decimal):
        return "number"
    if isinstance(item, str):
        return "string"
    return "array" if isinstance(item, list) else "object"


def _compared(operator: str, left: Any, right: Any) -> bool | None:
    """compareItems: None where the two are not comparable, which is unknown."""
    if _type(left) != _type(right):
        if left is None or right is None:
            return operator == "!="
        return None
    if isinstance(left, (list, dict)):
        return None
    if left is None:
        order = 0
    elif isinstance(left, str):
        pair = (left.encode("utf-16-be"), right.encode("utf-16-be"))
        order = (pair[0] > pair[1]) - (pair[0] < pair[1])
    else:
        order = (left > right) - (left < right)
    return {
        "==": order == 0,
        "!=": order != 0,
        "<": order < 0,
        "<=": order <= 0,
        ">": order > 0,
        ">=": order >= 0,
    }[operator]


_ARITHMETIC: dict[str, Callable[[Decimal, Decimal], Decimal]] = {
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
}


def _calculated(operator: str, left: Decimal, right: Decimal) -> Decimal:
    if operator in _ARITHMETIC:
        return _ARITHMETIC[operator](left, right)
    if right.is_zero():
        raise PathError("22012", "division by zero")
    if operator == "/":
        return _DIVISION.divide(left, right).normalize(_DIVISION)
    return left - right * (left / right).to_integral_value(rounding="ROUND_DOWN")


def _text_of_number(value: Decimal) -> str:
    return f"{value:f}"


@dataclass
class _Walk:
    root: Any
    variables: dict[str, Any]
    strict: bool
    ignore_structure: bool

    def __post_init__(self) -> None:
        self.last: int | None = None

    def run(self, node: Node, current: Any) -> Iterator[Any]:
        return getattr(self, f"_{node[0]}")(node, current)

    def unwrapped(self, items: Iterator[Any]) -> Iterator[Any]:
        """Each item, an array's elements in place of it in lax mode."""
        for item in items:
            if isinstance(item, list) and not self.strict:
                yield from item
            else:
                yield item

    def targets(self, node: Node, current: Any) -> Iterator[Any]:
        """What an accessor's base gives, an array's elements in place of it in lax mode."""
        return self.unwrapped(self.run(node, current))

    # primaries

    def _value(self, node: Node, _: Any) -> Iterator[Any]:
        yield node[1]

    def _root(self, _: Node, __: Any) -> Iterator[Any]:
        yield self.root

    def _current(self, _: Node, current: Any) -> Iterator[Any]:
        yield current

    def _last(self, _: Node, __: Any) -> Iterator[Any]:
        assert self.last is not None
        yield Decimal(self.last)

    def _variable(self, node: Node, _: Any) -> Iterator[Any]:
        if node[1] not in self.variables:
            raise _error("42704", f'could not find jsonpath variable "{node[1]}"')
        yield self.variables[node[1]]

    # accessors

    def _key(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.targets(node[1], current):
            if not isinstance(item, dict):
                if not self.ignore_structure:
                    raise PathError(
                        "2203A", "jsonpath member accessor can only be applied to an object"
                    )
            elif node[2] in item:
                yield item[node[2]]
            elif not self.ignore_structure:
                raise PathError("2203A", f'JSON object does not contain key "{node[2]}"')

    def _any_key(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.targets(node[1], current):
            if isinstance(item, dict):
                yield from item.values()
            elif not self.ignore_structure:
                raise PathError(
                    "2203C", "jsonpath wildcard member accessor can only be applied to an object"
                )

    def _any_element(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.run(node[1], current):
            if isinstance(item, list):
                yield from item
            elif not self.strict:
                yield item
            elif not self.ignore_structure:
                raise PathError(
                    "22039", "jsonpath wildcard array accessor can only be applied to an array"
                )

    def _elements(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.run(node[1], current):
            if not isinstance(item, list) and self.strict:
                if self.ignore_structure:
                    continue
                raise PathError("22039", "jsonpath array accessor can only be applied to an array")
            elements = item if isinstance(item, list) else [item]
            outer, self.last = self.last, len(elements) - 1
            for start, stop in node[2]:
                first = self._subscript(start, current)
                final = first if stop is None else self._subscript(stop, current)
                wrong = first < 0 or first > final or final >= len(elements)
                if wrong and not self.ignore_structure:
                    raise PathError("22033", "jsonpath array subscript is out of bounds")
                yield from elements[max(first, 0) : min(final, len(elements) - 1) + 1]
            self.last = outer

    def _subscript(self, node: Node, current: Any) -> int:
        values = list(self.unwrapped(self.run(node, current)))
        if len(values) != 1 or not isinstance(values[0], Decimal):
            raise PathError("22033", "jsonpath array subscript is not a single numeric value")
        return int(values[0].to_integral_value(rounding="ROUND_DOWN"))

    def _any(self, node: Node, current: Any) -> Iterator[Any]:
        _, base, first, last = node
        for item in self.run(base, current):
            yield from self._levels(item, 0, first, last)

    def _levels(self, item: Any, level: int, first: int, last: int | None) -> Iterator[Any]:
        if level >= first and (last is None or level <= last):
            yield item
        if last is not None and level >= last:
            return
        children = (
            item if isinstance(item, list) else item.values() if isinstance(item, dict) else ()
        )
        for child in children:
            yield from self._levels(child, level + 1, first, last)

    def _filter(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.targets(node[1], current):
            if self.truth(node[2], item) is True:
                yield item

    # predicates: True, False or None, unknown; a predicate read as a value is its boolean

    def truth(self, node: Node, current: Any) -> bool | None:
        kind = node[0]
        if kind in ("compare", "starts", "like"):
            return self._any_pair(node, current)
        if kind == "and":
            left = self.truth(node[1], current)
            if left is False:
                return False
            right = self.truth(node[2], current)
            return False if right is False else None if None in (left, right) else True
        if kind == "or":
            left = self.truth(node[1], current)
            if left is True:
                return True
            right = self.truth(node[2], current)
            return True if right is True else None if None in (left, right) else False
        if kind == "not":
            inner = self.truth(node[1], current)
            return None if inner is None else not inner
        if kind == "is_unknown":
            return self.truth(node[1], current) is None
        if kind == "exists":
            return self._exists_truth(node, current)
        # any other expression is true or false by its one boolean, as a filter's operand
        found = self._sequence(node, current, unwrap=False)
        if found is None or len(found) != 1 or not isinstance(found[0], bool):
            raise _error("42601", "boolean expression expected in jsonpath filter")
        return found[0]

    def _sequence(self, node: Node, current: Any, *, unwrap: bool) -> list[Any] | None:
        """``node``'s items; None for an error of its evaluation, which is unknown."""
        try:
            items = self.run(node, current)
            return list(self.unwrapped(items) if unwrap else items)
        except PathError:
            return None

    def _exists_truth(self, node: Node, current: Any) -> bool | None:
        try:
            if not self.strict:
                return next(self.run(node[1], current), _NOTHING) is not _NOTHING
            return bool(list(self.run(node[1], current)))
        except PathError:
            return None

    def _pair(self, node: Node, left: Any, right: Any) -> bool | None:
        if node[0] == "compare":
            return _compared(node[1], left, right)
        if not isinstance(left, str):
            return None
        if node[0] == "like":
            return node[2].search(left) is not None
        return left.startswith(right) if isinstance(right, str) else None

    def _any_pair(self, node: Node, current: Any) -> bool | None:
        """executePredicate: in lax mode the first true decides; in strict mode an unknown does."""
        left_node = node[2] if node[0] == "compare" else node[1]
        lefts = self._sequence(left_node, current, unwrap=not self.strict)
        if lefts is None:
            return None
        rights: list[Any] | None = [None]
        if node[0] == "compare":
            rights = self._sequence(node[3], current, unwrap=not self.strict)
        elif node[0] == "starts":
            rights = self._sequence(node[2], current, unwrap=False)
        if rights is None:
            return None
        found = error = False
        for left in lefts:
            for right in rights:
                answer = self._pair(node, left, right)
                if answer is None:
                    if self.strict:
                        return None
                    error = True
                elif answer:
                    if not self.strict:
                        return True
                    found = True
        return True if found else None if error else False

    def _boolean(self, node: Node, current: Any) -> Iterator[Any]:
        yield self.truth(node, current)

    _compare = _and = _or = _not = _is_unknown = _starts = _like = _exists = _boolean

    # arithmetic

    def _arithmetic(self, node: Node, current: Any) -> Iterator[Any]:
        _, operator, left, right = node
        lefts = list(self.unwrapped(self.run(left, current)))
        rights = list(self.unwrapped(self.run(right, current)))
        for side, items in (("left", lefts), ("right", rights)):
            if len(items) != 1 or not isinstance(items[0], Decimal):
                raise PathError(
                    "22038",
                    f"{side} operand of jsonpath operator {operator} is not a single numeric value",
                )
        yield _calculated(operator, lefts[0], rights[0])

    def _unary(self, node: Node, current: Any) -> Iterator[Any]:
        for item in self.unwrapped(self.run(node[2], current)):
            if not isinstance(item, Decimal):
                raise PathError(
                    "2203D", f"operand of unary jsonpath operator {node[1]} is not a numeric value"
                )
            yield -item if node[1] == "-" else item

    # methods

    def _method(self, node: Node, current: Any) -> Iterator[Any]:
        _, base, name, arguments = node
        if name in ("datetime", "date", "time", "time_tz", "timestamp", "timestamp_tz"):
            raise FunctionRefused(f"jsonpath_{name}", "duckdb")
        whole = name in ("type", "size")
        items = self.run(base, current) if whole else self.targets(base, current)
        for item in items:
            yield from _METHOD[name](self, item, arguments)


#: What a lazy search for the first item answers with when there is none.
_NOTHING = object()


def _number_method(name: str, item: Any) -> Decimal:
    if not isinstance(item, Decimal):
        raise PathError(
            "22039", f"jsonpath item method .{name}() can only be applied to a numeric value"
        )
    return item


def _invalid(name: str, item: Any, type_name: str) -> PathError:
    text = _text_of_number(item) if isinstance(item, Decimal) else str(item)
    return PathError(
        "22P02",
        f'argument "{text}" of jsonpath item method .{name}() is invalid for type {type_name}',
    )


def _numeric_of(name: str, item: Any) -> Decimal:
    if isinstance(item, Decimal):
        return item
    if isinstance(item, str):
        try:
            value = Decimal(item.strip())
        except InvalidOperation:
            raise _invalid(name, item, "numeric") from None
        if value.is_finite():
            return value
        raise PathError(
            "22P02", f"NaN or Infinity is not allowed for jsonpath item method .{name}()"
        )
    raise PathError(
        "22039", f"jsonpath item method .{name}() can only be applied to a string or numeric value"
    )


def _integer_of(name: str, item: Any, bounds: tuple[int, int], type_name: str) -> Iterator[Decimal]:
    number = _numeric_of(name, item)
    if isinstance(item, str) and not re.fullmatch(r"\s*[-+]?\d+\s*", item):
        raise _invalid(name, item, type_name)
    whole = int(number.to_integral_value(rounding="ROUND_HALF_UP"))
    if not bounds[0] <= whole <= bounds[1]:
        raise _invalid(name, item, type_name)
    yield Decimal(whole)


def _double(_: _Walk, item: Any, __: tuple) -> Iterator[Decimal]:
    number = float(_numeric_of("double", item))
    if not math.isfinite(number):
        raise _invalid("double", item, "double precision")
    yield Decimal(repr(number))


def _type_method(_: _Walk, item: Any, __: tuple) -> Iterator[str]:
    yield _type(item)


def _size(walk: _Walk, item: Any, _: tuple) -> Iterator[Decimal]:
    if not isinstance(item, list) and walk.strict:
        raise PathError("22039", "jsonpath item method .size() can only be applied to an array")
    yield Decimal(len(item) if isinstance(item, list) else 1)


def _keyvalue(walk: _Walk, item: Any, _: tuple) -> Iterator[dict[str, Any]]:
    if not isinstance(item, dict):
        raise PathError(
            "22039", "jsonpath item method .keyvalue() can only be applied to an object"
        )
    if item is not walk.root:  # its id is its offset in PostgreSQL's binary jsonb
        raise FunctionRefused("jsonpath_keyvalue", "duckdb")
    for key, value in item.items():
        yield {"id": Decimal(0), "key": key, "value": value}


def _string(_: _Walk, item: Any, __: tuple) -> Iterator[str]:
    if isinstance(item, bool):
        yield "true" if item else "false"
    elif isinstance(item, str):
        yield item
    elif isinstance(item, Decimal):
        yield _text_of_number(item)
    else:
        raise PathError(
            "22039",
            "jsonpath item method .string() can only be applied to a bool, string, numeric, or "
            "datetime value",
        )


_TRUE_WORDS = ("t", "tr", "tru", "true", "y", "ye", "yes", "on", "1")
_FALSE_WORDS = ("f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0")


def _boolean_method(_: _Walk, item: Any, __: tuple) -> Iterator[bool]:
    if isinstance(item, bool):
        yield item
    elif isinstance(item, Decimal):
        if item != item.to_integral_value():
            raise _invalid("boolean", item, "boolean")
        yield not item.is_zero()
    elif isinstance(item, str) and item.lower() in (*_TRUE_WORDS, *_FALSE_WORDS):
        yield item.lower() in _TRUE_WORDS
    elif isinstance(item, str):
        raise _invalid("boolean", item, "boolean")
    else:
        raise PathError(
            "22039",
            "jsonpath item method .boolean() can only be applied to a bool, string, or numeric "
            "value",
        )


def _decimal(_: _Walk, item: Any, arguments: tuple) -> Iterator[Decimal]:
    number = _numeric_of("decimal", item)
    if not arguments:
        yield number
        return
    precision = int(arguments[0])
    scale = int(arguments[1]) if len(arguments) > 1 else 0
    if not 1 <= precision <= 1000:
        raise PathError("22023", "NUMERIC precision must be between 1 and 1000")
    rounded = number.quantize(Decimal(1).scaleb(-scale), rounding="ROUND_HALF_UP")
    if rounded.adjusted() + 1 > precision - scale:
        raise _invalid("decimal", item, "numeric")
    yield rounded


def _number_item(_: _Walk, item: Any, __: tuple) -> Iterator[Decimal]:
    yield _numeric_of("number", item)


def _rounded(rounding: str, name: str) -> Callable[[_Walk, Any, tuple], Iterator[Decimal]]:
    def method(_: _Walk, item: Any, __: tuple) -> Iterator[Decimal]:
        yield _number_method(name, item).to_integral_value(rounding=rounding)

    return method


def _abs(_: _Walk, item: Any, __: tuple) -> Iterator[Decimal]:
    yield abs(_number_method("abs", item))


_METHOD: dict[str, Callable[[_Walk, Any, tuple], Iterator[Any]]] = {
    "type": _type_method,
    "size": _size,
    "double": _double,
    "ceiling": _rounded(ROUND_CEILING, "ceiling"),
    "floor": _rounded(ROUND_FLOOR, "floor"),
    "abs": _abs,
    "keyvalue": _keyvalue,
    "string": _string,
    "boolean": _boolean_method,
    "decimal": _decimal,
    "number": _number_item,
    "bigint": lambda _, item, __: _integer_of("bigint", item, _INT8, "bigint"),
    "integer": lambda _, item, __: _integer_of("integer", item, _INT4, "integer"),
}


# --- the functions -------------------------------------------------------------------------------


def _holds_any(node: Any) -> bool:
    if not isinstance(node, tuple):
        return False
    return node[:1] == ("any",) or any(_holds_any(part) for part in node)


def _walk(path: str, target: Any, variables: Any) -> tuple[_Walk, Node]:
    strict, node = _Reader(path).whole()
    if not isinstance(variables, dict):
        raise _error("22023", '"vars" argument is not an object')
    # a .** ignores structural errors for what follows it (jpiAny)
    return _Walk(target, variables, strict, not strict or _holds_any(node)), node


def items(path: str, target: Any, variables: Any, silent: bool) -> list[Any]:
    """The path's items; none where an error is silenced."""
    walk, node = _walk(path, target, variables)
    try:
        return list(walk.run(node, target))
    except PathError as failed:
        if silent:
            return []
        raise _error(failed.sqlstate, failed.message) from failed


def exists(path: str, target: Any, variables: Any, silent: bool) -> bool | None:
    """Whether the path has an item; NULL where an error is silenced."""
    walk, node = _walk(path, target, variables)
    try:
        return next(walk.run(node, target), _NOTHING) is not _NOTHING
    except PathError as failed:
        if silent:
            return None
        raise _error(failed.sqlstate, failed.message) from failed


def match(path: str, target: Any, variables: Any, silent: bool) -> bool | None:
    """The path's one boolean item, or NULL for its one null."""
    walk, node = _walk(path, target, variables)
    try:
        found = list(walk.run(node, target))
    except PathError as failed:
        if silent:
            return None
        raise _error(failed.sqlstate, failed.message) from failed
    if len(found) == 1 and (found[0] is None or isinstance(found[0], bool)):
        return found[0]
    if silent:
        return None
    raise _error("22038", "single boolean result is expected")
