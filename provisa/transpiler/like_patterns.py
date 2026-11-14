# Copyright (c) 2026 Kenneth Stott
# Canary: b8f491df-d8a3-46ff-a3af-23a80de10aeb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A LIKE pattern on ClickHouse, read as PostgreSQL reads it (REQ-2167, REQ-2236).

ClickHouse's LIKE has no ESCAPE clause. Its pattern escapes with a backslash, and a backslash
escapes only %, _ and itself there: ``'ab' LIKE 'a\\b'`` is false on ClickHouse, true in
PostgreSQL, where a backslash makes any character after it itself.

A pattern the statement writes as a literal is written here in ClickHouse's own form:
``x LIKE p ESCAPE e`` is ``x LIKE like_escape(p, e)`` (like_match.c's do_like_escape), and each
escaped character is written as ClickHouse reads it. A text pattern that ends in its escape,
which PostgreSQL raises at only where a match reaches it, and an ESCAPE whose pattern or escape
a row computes, are refused by name. An escape of more than one character raises PostgreSQL's
22025. A bytea pattern with an ESCAPE is written as the hex input of its bytes, which
provisa.transpiler.polyfill.bytea's LIKE rows read byte by byte; a bytea pattern with no ESCAPE
is those rows' own.

bytea has no ILIKE (PostgreSQL raises 42883 for its operator); ClickHouse's ILIKE answers one.
An ILIKE of a value the statement says is a bytea raises PostgreSQL's error here."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import bytea, trino
from provisa.transpiler.polyfill.notation import Refusal

_DIALECT = "clickhouse"
#: What ClickHouse's LIKE reads as escaped after a backslash: any other character after one is
#: the backslash and the character.
_CLICKHOUSE_ESCAPED = frozenset("%_\\")
_INVALID_ESCAPE = Refusal("22025", "invalid escape string")
#: The meta key of a LIKE whose pattern is already in ClickHouse's form.
_READ = "provisa_like_read"


def text_like_escape(pattern: str, escape: str) -> str | None:
    """like_escape(text, text) as like_match.c's do_like_escape writes it: the pattern with its
    escape written as a backslash. None for an escape of more than one character, which
    PostgreSQL raises (22025)."""
    if not escape:
        return pattern.replace("\\", "\\\\")
    if len(escape) != 1:
        return None
    if escape == "\\":
        return pattern
    out: list[str] = []
    after = False
    for char in pattern:
        if char == escape and not after:
            out.append("\\")
            after = True
        elif char == "\\":
            out.append("\\" if after else "\\\\")
            after = False
        else:
            out.append(char)
            after = False
    return "".join(out)


def clickhouse_pattern(pattern: str) -> str | None:
    """PostgreSQL's LIKE pattern, a backslash its escape, in ClickHouse's form: an escaped %, _ or
    backslash keeps its backslash, any other escaped character is itself. None for a pattern that
    ends in its escape."""
    out: list[str] = []
    escaped = False
    for char in pattern:
        if escaped:
            out.append("\\" + char if char in _CLICKHOUSE_ESCAPED else char)
            escaped = False
        elif char == "\\":
            escaped = True
        else:
            out.append(char)
    return None if escaped else "".join(out)


def _of_bytea(node: exp.Expr) -> bool:
    """Whether the statement says ``node`` is a bytea."""
    return trino.said(node.unnest()) == "bytea"


def _string(node: exp.Expr) -> str | None:
    """The text of a string literal, through a cast of it to text; None for any other value."""
    value = node.unnest()
    if isinstance(value, exp.Cast) and value.to.is_type(*exp.DataType.TEXT_TYPES):
        value = value.this.unnest()
    return value.this if isinstance(value, exp.Literal) and value.is_string else None


def _bytes(node: exp.Expr) -> bytes | None:
    """The bytes of a bytea literal (a string, or one cast to bytea) ClickHouse holds as written:
    hex input, or text with no backslash. None for any other value."""
    value = node.unnest()
    if isinstance(value, exp.Cast) and _of_bytea(value):
        value = value.this.unnest()
    if not (isinstance(value, exp.Literal) and value.is_string):
        return None
    return bytea.literal_bytes(value.this)


def _is_null(node: exp.Expr) -> bool:
    value = node.unnest()
    return isinstance(value, exp.Null) or (
        isinstance(value, exp.Cast) and isinstance(value.this.unnest(), exp.Null)
    )


def _type_name(node: exp.Expr) -> str | None:
    """PostgreSQL's name of an operand's type in its error: unknown for an untyped string."""
    value = node.unnest()
    if isinstance(value, exp.Literal) and value.is_string:
        return "unknown"
    said = trino.said(value)
    return None if said is None else said.split("(")[0]


def _no_ilike_of_bytea(node: exp.ILike) -> None:
    """PostgreSQL's error of an ILIKE of a bytea: no operator ~~* (!~~*) takes one."""
    if not (_of_bytea(node.this) or _of_bytea(node.expression)):
        return
    left, right = _type_name(node.this), _type_name(node.expression)
    if left is None or right is None:
        raise FunctionRefused("ilike", _DIALECT)  # the error names a type no statement says
    operator = "!~~*" if node.args.get("negate") else "~~*"
    raise RowRefused(
        "ilike", _DIALECT, Refusal("42883", f"operator does not exist: {left} {operator} {right}")
    )


def _escaped_bytea(match: exp.Like, escape: exp.Expr) -> exp.Expr:
    """A bytea LIKE of an ESCAPE: its pattern the hex input of like_escape's bytes."""
    pattern = match.expression
    if _is_null(pattern) or _is_null(escape):
        match.set("expression", exp.Cast(this=exp.null(), to=exp.DataType.build("BYTEA")))
        return match
    pattern_bytes, escape_bytes = _bytes(pattern), _bytes(escape)
    if pattern_bytes is None or escape_bytes is None:
        raise FunctionRefused("like_escape", _DIALECT)  # a computed pattern or escape
    written = bytea.like_escape(pattern_bytes, escape_bytes)
    if written is None:
        raise RowRefused("like_escape", _DIALECT, _INVALID_ESCAPE)
    match.meta[_READ] = True
    hex_input = exp.Literal.string("\\x" + written.hex())
    match.set("expression", exp.Cast(this=hex_input, to=exp.DataType.build("BYTEA")))
    return match


def _text_pattern(match: exp.Like | exp.ILike, escape: exp.Expr | None) -> exp.Expr:
    """A text LIKE (ILIKE), its literal pattern in ClickHouse's form."""
    pattern = match.expression
    if escape is not None and (_is_null(pattern) or _is_null(escape)):
        match.set("expression", exp.Cast(this=exp.null(), to=exp.DataType.build("TEXT")))
        return match
    written = _string(pattern)
    if escape is not None:
        escape_text = _string(escape)
        if written is None or escape_text is None:
            raise FunctionRefused("like_escape", _DIALECT)  # a computed pattern or escape
        escaped = text_like_escape(written, escape_text)
        if escaped is None:
            raise RowRefused("like_escape", _DIALECT, _INVALID_ESCAPE)
        written = escaped
    match.meta[_READ] = True
    if written is None:
        return match  # a pattern a row computes: ClickHouse's own reading of its backslashes
    ours = clickhouse_pattern(written)
    if ours is None:
        # PostgreSQL raises 22025 only where a match reaches the escape, which a row decides.
        raise FunctionRefused("like", _DIALECT)
    literal = exp.Literal.string(ours)
    value = pattern.unnest()
    if isinstance(value, exp.Cast):
        value.set("this", literal)
    else:
        match.set("expression", literal)
    return match


def _match(match: exp.Expr, escape: exp.Expr | None) -> exp.Expr:
    """A LIKE or ILIKE (or another match an ESCAPE reads) as ClickHouse reads it, its ESCAPE gone."""
    if isinstance(match, exp.ILike):
        _no_ilike_of_bytea(match)
    if not isinstance(match, (exp.Like, exp.ILike)):
        raise FunctionRefused("similar_to_escape", _DIALECT)  # SIMILAR TO ... ESCAPE
    if _of_bytea(match.this) or _of_bytea(match.expression):
        if escape is None or isinstance(match, exp.ILike):
            return match  # the bytea LIKE rows' own
        return _escaped_bytea(match, escape)
    return _text_pattern(match, escape)


def written(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each LIKE and ILIKE pattern in ClickHouse's form, its ESCAPE read."""
    for escaped in list(tree.find_all(exp.Escape)):
        match = escaped.this
        escaped.replace(_match(match, escaped.expression))
    for match in list(tree.find_all(exp.Like, exp.ILike)):
        if not match.meta.get(_READ):
            _match(match, None)
    return tree
