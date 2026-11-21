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
  rounds to its default DECIMAL(18,3): the literal is cast to a decimal holding all its digits
  (``exact_inputs``, as on every other target);

and an untyped NULL a statement's own result column gives is text, as PostgreSQL resolves an
unknown-typed output column (DuckDB types it INTEGER)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import decimal
import re

import sqlglot
import sqlglot.expressions as exp

#: DuckDB's widest decimal.
_MAX_PRECISION = 38
#: The digits a literal's decimal has beside its own.
_ROOM = 18


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
    # Room to compute in, and not the widest: an engine cannot widen a list of DECIMAL(38,1) and
    # DECIMAL(38,3), and DuckDB refuses a product of a width that is too narrow.
    width = min(_MAX_PRECISION, precision + _ROOM)
    return exp.DataType.build(f"DECIMAL({width}, {scale})", dialect="postgres")


def _number_of(node: exp.Expr) -> exp.Literal | None:
    """The literal of a number written with its sign or in parentheses, as ``(-7.125)::numeric``."""
    while isinstance(node, (exp.Paren, exp.Neg)):
        node = node.this
    return node if isinstance(node, exp.Literal) else None


def _fixed_length(to: exp.DataType) -> int | None:
    """The length a cast to character says: bpchar's input cuts a longer text to it, and character
    with no length is character(1). None for a type of no length."""
    if not to.is_type(exp.DataType.Type.CHAR):
        return None
    return int(to.expressions[0].this.this) if to.expressions else 1


def _exact_input(node: exp.Expr) -> exp.Expr:
    """A literal cast as PostgreSQL's input function reads it, where a target's cast would change
    it: a numeric of no precision keeps every digit (DECIMAL is DECIMAL(38,0) on Trino, (18,3) on
    DuckDB and (10,0) on ClickHouse), and a character(n) keeps n characters."""
    if not isinstance(node, exp.Cast):
        return node
    to = node.to
    number = _number_of(node.this)
    if number is not None and to.is_type(exp.DataType.Type.DECIMAL) and not to.expressions:
        exact = _exact_numeric(number)
        return node if exact is None else exp.Cast(this=node.this, to=exact)
    length = _fixed_length(to)
    if length is not None and isinstance(node.this, exp.Literal) and node.this.is_string:
        kept = exp.Literal.string(node.this.this[:length])
        return exp.Cast(this=kept, to=to)
    return node


#: Whether a statement may cast a literal to a type whose input ``_exact_input`` reads.
_SIZED_CAST = re.compile(r"(::|\bas)\s*\"?(numeric|decimal|char|character)\b", re.IGNORECASE)


def exact_inputs(pg_sql: str) -> str:
    """``pg_sql`` with each literal cast as PostgreSQL's input function reads it (REQ-2171, AN
    ANSWER THAT DIFFERS IS REFUSED). The text of a statement with none is returned as it is."""
    if _SIZED_CAST.search(pg_sql) is None:
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    written = tree.transform(_exact_input)
    return pg_sql if written == tree else written.sql(dialect="postgres")


def values_cast(column: exp.Column) -> exp.Cast | None:
    """The cast the statement says of a column of a VALUES list it reads, by the list's first row:
    the row PostgreSQL resolves its type from, every other row fitting it."""
    select = column.find_ancestor(exp.Select)
    if select is None or select.args.get("joins"):
        return None
    source = select.args.get("from_")
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values):
        return None
    alias = values.args.get("alias")
    names = [c.name for c in alias.columns] if alias is not None else []
    if column.name not in names:
        return None
    value = values.expressions[0].expressions[names.index(column.name)]
    return value if isinstance(value, exp.Cast) else None


def _depth(elements: list) -> int:
    """An array's dimensions, as its text form nests them: '{{a,b}}' has two."""
    inner = [_depth(e) for e in elements if isinstance(e, list)]
    return 1 + (max(inner) if inner else 0)


def _of_depth(to: exp.DataType, depth: int) -> exp.DataType:
    """``to``, an array type, of ``depth`` dimensions: PostgreSQL's text[] holds any number of
    them, an engine's list type one per nesting (a cast of a nested list to TEXT[] writes each
    sublist as a text). The dimensions ``to`` declares are dropped, as PostgreSQL ignores them:
    '{{1,2}}'::int[] and '{{1,2}}'::int[][] are both of two."""
    element = to
    while element.is_type(exp.DataType.Type.ARRAY) and element.expressions:
        element = element.expressions[0]
    typed = element.copy()
    for _ in range(depth):
        typed = exp.DataType(this=exp.DataType.Type.ARRAY, expressions=[typed], nested=True)
    return typed


def array_input(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """An array's text form cast to an array type, ``'{1,2}'::int4[]``, as the array of its
    elements' strings cast to that type: no engine's cast reads array_in's text. Any other node
    is returned as it is."""
    if not isinstance(node, exp.Cast) or not isinstance(node.this, exp.Literal):
        return node
    literal, to = node.this, node.to
    if not literal.is_string or not to.is_type(exp.DataType.Type.ARRAY):
        return node
    try:
        elements = array_elements(literal.this)
    except _NotAnArrayLiteral:
        return node  # not an array's text form: the engine's cast reports it
    return exp.Cast(this=_as_array(elements), to=_of_depth(to, _depth(elements)))


def array_inputs(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each array's text form cast to an array type written as its elements
    (``array_input``), in place."""
    return tree.transform(array_input, copy=False)


def literal_input(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    if not isinstance(node, exp.Cast) or not isinstance(node.this, exp.Literal):
        return node
    literal, to = node.this, node.to
    written = array_input(node)
    if written is not node:
        return written
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
    tree = tree.transform(literal_input, copy=False)
    if isinstance(tree, exp.Select):
        for select in tree.selects:
            target = select.this if isinstance(select, exp.Alias) else select
            if isinstance(target, exp.Null):
                target.replace(exp.cast(exp.null(), exp.DataType.Type.TEXT))
    return tree.sql(dialect="postgres", copy=False)


#: An edit of a statement's text: the characters ``start`` to ``end`` (exclusive) and their
#: replacement.
TextEdit = tuple[int, int, str]


def literal_span(literal: exp.Literal) -> tuple[int, int]:
    """Where a parsed string literal is in the statement's text: its first character and the one
    after its last."""
    return literal.meta["start"], literal.meta["end"] + 1


def pg_string(text: str) -> str:
    """``text`` as a PostgreSQL string literal (standard_conforming_strings: a backslash is
    itself)."""
    return "'" + text.replace("'", "''") + "'"


def spliced(sql: str, edits: list[TextEdit]) -> str:
    """``sql`` with each edit made. A step that changes a few literals edits the statement's text
    rather than write the parsed statement again, which would lose what a parse keeps in a node's
    meta (to_char's template, pg_syntax)."""
    for start, end, text in sorted(edits, reverse=True):
        sql = sql[:start] + text + sql[end:]
    return sql
