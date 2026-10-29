# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c9e3a-7b42-4f08-9a6e-2c8f0b4d7e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's literal input, said on DuckDB (REQ-2167, PGWIRE CATALOG AND TYPES).

A statement in PostgreSQL's dialect may give a value as a string literal cast to its type, which
PostgreSQL reads with that type's input function. DuckDB reads some of those strings otherwise:

- an array's text form, ``'{1,2}'::int4[]`` (array_in), which DuckDB does not read at all: the
  literal is written as the array of its elements' strings, cast to the array type;
- a bytea's hex form, ``'\\xdeadbeef'::bytea`` (byteain), which DuckDB reads as one escaped byte
  and six characters: the literal is written as the bytes its hex digits name;
- an exact number cast to a numeric of no precision, ``'12345.6789'::numeric``, which DuckDB
  rounds to its default DECIMAL(18,3): the literal is cast to a decimal holding all its digits;

and an untyped NULL a statement's own result column gives is text, as PostgreSQL resolves an
unknown-typed output column (DuckDB types it INTEGER)."""

# Requirements: REQ-2167

from __future__ import annotations

import decimal

import sqlglot
import sqlglot.expressions as exp

#: DuckDB's widest decimal.
_MAX_PRECISION = 38


class _NotAnArrayLiteral(ValueError):
    """The string is not array_in's text form of an array."""


def array_elements(text: str) -> list:
    """The elements of an array's text form (array_in), nested lists for its dimensions: each
    element its own text, or None for an unquoted NULL."""
    elements, end = _array(text, _skip_space(text, 0))
    if _skip_space(text, end) != len(text):
        raise _NotAnArrayLiteral(f"junk after the array in {text!r}")
    return elements


def _skip_space(text: str, i: int) -> int:
    while i < len(text) and text[i].isspace():
        i += 1
    return i


def _array(text: str, i: int) -> tuple[list, int]:
    if i >= len(text) or text[i] != "{":
        raise _NotAnArrayLiteral(f"no '{{' at {i} in {text!r}")
    out: list = []
    i = _skip_space(text, i + 1)
    if i < len(text) and text[i] == "}":
        return out, i + 1
    while True:
        i = _skip_space(text, i)
        if i < len(text) and text[i] == "{":
            element, i = _array(text, i)
        else:
            element, i = _element(text, i)
        out.append(element)
        i = _skip_space(text, i)
        if i >= len(text):
            raise _NotAnArrayLiteral(f"unterminated array in {text!r}")
        if text[i] == "}":
            return out, i + 1
        if text[i] != ",":
            raise _NotAnArrayLiteral(f"no ',' at {i} in {text!r}")
        i += 1


def _element(text: str, i: int) -> tuple[str | None, int]:
    if i < len(text) and text[i] == '"':
        chars: list[str] = []
        i += 1
        while i < len(text) and text[i] != '"':
            if text[i] == "\\":
                i += 1
            if i >= len(text):
                break
            chars.append(text[i])
            i += 1
        if i >= len(text):
            raise _NotAnArrayLiteral(f"unterminated quoted element in {text!r}")
        return "".join(chars), i + 1
    chars = []
    escaped = False
    while i < len(text) and text[i] not in ",}{":
        if text[i] == "\\":
            escaped = True
            i += 1
            if i >= len(text):
                break
        if text[i] == '"':
            raise _NotAnArrayLiteral(f"a quote inside an unquoted element in {text!r}")
        chars.append(text[i])
        i += 1
    element = "".join(chars).rstrip()
    if not element:
        raise _NotAnArrayLiteral(f"an empty element in {text!r}")
    if not escaped and element.upper() == "NULL":
        return None, i
    return element, i


def _as_array(elements: list) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    return exp.Array(
        expressions=[
            _as_array(e)
            if isinstance(e, list)
            else (exp.null() if e is None else exp.Literal.string(e))
            for e in elements
        ]
    )


def _exact_numeric(literal: exp.Literal) -> exp.DataType | None:
    try:
        value = decimal.Decimal(literal.this)
    except decimal.InvalidOperation:
        return None  # not a number: the engine's cast reports it
    if not value.is_finite():
        return None
    _, digits, exponent = value.as_tuple()
    scale = max(0, -int(exponent))
    precision = max(len(digits), scale)
    if precision > _MAX_PRECISION:
        return None  # wider than any engine decimal: left to the engine's own cast
    return exp.DataType.build(f"DECIMAL({_MAX_PRECISION}, {scale})", dialect="postgres")


def _literal_input(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    if not isinstance(node, exp.Cast) or not isinstance(node.this, exp.Literal):
        return node
    literal, to = node.this, node.to
    if literal.is_string and to.is_type(exp.DataType.Type.ARRAY):
        try:
            elements = array_elements(literal.this)
        except _NotAnArrayLiteral:
            return node  # not an array's text form: the engine's cast reports it
        return exp.Cast(this=_as_array(elements), to=to)
    if (
        literal.is_string
        and to.is_type(exp.DataType.Type.VARBINARY)
        and literal.this.startswith("\\x")
    ):
        digits = literal.this[2:]
        if len(digits) % 2 or any(c not in "0123456789abcdefABCDEF" for c in digits):
            return node  # not hex: the engine's cast reports it
        return exp.Unhex(this=exp.Literal.string(digits))
    if to.is_type(exp.DataType.Type.DECIMAL) and not to.expressions:
        exact = _exact_numeric(literal)
        if exact is not None:
            return exp.Cast(this=literal, to=exact)
    return node


def postgres_literals(pg_sql: str) -> str:
    """``pg_sql`` (PostgreSQL's dialect) with its literal input and untyped NULL result columns
    written so DuckDB reads them as PostgreSQL does."""
    lowered = pg_sql.lower()
    if "::" not in pg_sql and "cast" not in lowered and "null" not in lowered:
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    tree = tree.transform(_literal_input)
    if isinstance(tree, exp.Select):
        for select in tree.selects:
            target = select.this if isinstance(select, exp.Alias) else select
            if isinstance(target, exp.Null):
                target.replace(exp.cast(exp.null(), exp.DataType.Type.TEXT))
    return tree.sql(dialect="postgres")
