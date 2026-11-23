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
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import PostgresError
from provisa.transpiler.metadata_arguments import read_oid

#: DuckDB's widest decimal.
_MAX_PRECISION = 38
#: The digits a literal's decimal has beside its own.
_ROOM = 18
#: The room every target but Trino's gets.
ROOM = _ROOM
#: None beside its own, on Trino: Trino holds a sum or product whose exact type passes 38 digits
#: at a smaller scale (DECIMAL(38, 21) + DECIMAL(20, 1) at scale 19), where its own decimal
#: arithmetic widens a literal's type of its digits alone to the type its answer needs.
TIGHT = 0


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


def _exact_numeric(literal: exp.Literal, room: int = _ROOM) -> exp.DataType | None:
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
    width = min(_MAX_PRECISION, precision + room)
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


def _exact_input(node: exp.Expr, room: int = _ROOM) -> exp.Expr:
    """A literal cast as PostgreSQL's input function reads it, where a target's cast would change
    it: a numeric of no precision keeps every digit (DECIMAL is DECIMAL(38,0) on Trino, (18,3) on
    DuckDB and (10,0) on ClickHouse), and a character(n) keeps n characters."""
    if not isinstance(node, exp.Cast):
        return node
    to = node.to
    number = _number_of(node.this)
    if number is not None and to.is_type(exp.DataType.Type.DECIMAL) and not to.expressions:
        exact = _exact_numeric(number, room)
        return node if exact is None else exp.Cast(this=node.this, to=exact)
    length = _fixed_length(to)
    if length is not None and isinstance(node.this, exp.Literal) and node.this.is_string:
        kept = exp.Literal.string(node.this.this[:length])
        return exp.Cast(this=kept, to=to)
    return node


#: Whether a statement may cast a literal to a type whose input ``_exact_input`` reads.
_SIZED_CAST = re.compile(r"(::|\bas)\s*\"?(numeric|decimal|char|character)\b", re.IGNORECASE)


def exact_inputs(pg_sql: str, room: int = _ROOM) -> str:
    """``pg_sql`` with each literal cast as PostgreSQL's input function reads it (REQ-2171, AN
    ANSWER THAT DIFFERS IS REFUSED). The text of a statement with none is returned as it is.
    ``room``: the digits a literal's decimal has beside its own (``TIGHT`` on Trino)."""
    if _SIZED_CAST.search(pg_sql) is None:
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    written = tree.transform(lambda node: _exact_input(node, room))
    return pg_sql if written == tree else written.sql(dialect="postgres")


def text_literal(node: exp.Expr) -> exp.Literal | None:
    """The text a statement writes as a literal: a string literal, or a literal cast to text
    (``'hex'::text``, ``1::text``), which names its text as plainly; None for any other value."""
    if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.TEXT:
        return exp.Literal.string(node.this.this) if isinstance(node.this, exp.Literal) else None
    return node if isinstance(node, exp.Literal) and node.is_string else None


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


#: Each integer type's input range and the name its input error says (int2in, int4in, int8in).
_INTEGER_INPUTS = {
    exp.DataType.Type.SMALLINT: (-(2**15), 2**15 - 1, "smallint"),
    exp.DataType.Type.INT: (-(2**31), 2**31 - 1, "integer"),
    exp.DataType.Type.BIGINT: (-(2**63), 2**63 - 1, "bigint"),
}
#: An integer's text as its input reads it in base 10: digits, signed or not, between spaces. A
#: text with underscores or a 0x/0o/0b prefix is left to the engine's cast.
_DECIMAL_INTEGER = re.compile(r"\s*([+-]?[0-9]+)\s*", re.ASCII)
#: A date's text in ISO form, of a year after Christ: date_in's range ends at 5874897-12-31.
_ISO_DATE = re.compile(r"\s*([0-9]{4,})-([0-9]{2})-([0-9]{2})\s*", re.ASCII)
_LAST_DATE = (5874897, 12, 31)
#: The first date a timestamp does not hold (END_TIMESTAMP): date2timestamp raises at it.
_FIRST_PAST_TIMESTAMP = (294277, 1, 1)
_OID_MAX = 2**32 - 1
_INT4 = _INTEGER_INPUTS[exp.DataType.Type.INT]
#: The calls that read a numeric into money or pg_lsn, by the argument they read it from, and
#: PostgreSQL's error for a NaN there (numeric_cash's numeric_int8, pg_lsn_pli, pg_lsn_mii).
_NAN_READERS = {
    "money": (0, "cannot convert NaN to bigint"),
    "pg_lsn_pli": (1, "cannot add NaN to pg_lsn"),
    "numeric_pl_pg_lsn": (0, "cannot add NaN to pg_lsn"),
    "pg_lsn_mii": (1, "cannot subtract NaN from pg_lsn"),
}
#: Whether a statement may hold a literal or a call ``refuse_past_inputs`` reads.
_PAST_INPUT = re.compile(
    r"(::|\bas)\s*\"?(oid|smallint|integer|bigint|int[248]?|date)\b"
    r"|\b(money|pg_lsn_pli|pg_lsn_mii|numeric_pl_pg_lsn|oid|date_part)\"?\s*\(",
    re.IGNORECASE,
)


def _string_cast(node: exp.Expr) -> str | None:
    """The text of a string literal ``node`` casts, None for any other node."""
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal) and node.this.is_string:
        return node.this.this
    return None


def _string_cast_to(node: exp.Expr, kind: exp.DType) -> str | None:
    """The text of a string literal ``node`` casts to a type of ``kind``, None otherwise."""
    if not isinstance(node, exp.Cast) or not node.to.is_type(kind):
        return None
    return _string_cast(node)


def _cast_name(cast: exp.Cast) -> str:
    return cast.to.sql(dialect="postgres").lower().removeprefix("pg_catalog.")


def _leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _iso_date(text: str) -> tuple[int, int, int] | None:
    """A date's ISO text as (year, month, day), where the month and the day are a calendar's."""
    matched = _ISO_DATE.fullmatch(text)
    if matched is None:
        return None
    year, month, day = (int(part) for part in matched.groups())
    days = [31, 29 if _leap(year) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if not (1 <= month <= 12 and 1 <= day <= days[month - 1]):
        return None
    return year, month, day


def _int8_constant(node: exp.Expr) -> int | None:
    """The value of a bigint constant: an integer literal cast to bigint, or an integer literal
    past int4, which PostgreSQL types bigint. None for any other node, an int4 among them: int4's
    cast to oid is its bits, read without a check."""
    if isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.BIGINT):
        inner = node.this
        text = inner.this if isinstance(inner, exp.Literal) else None
        matched = _DECIMAL_INTEGER.fullmatch(text) if text is not None else None
        return int(matched[1]) if matched is not None else None
    if isinstance(node, exp.Literal) and node.is_int:
        value = int(node.this)
        return value if not _INT4[0] <= value <= _INT4[1] else None
    return None


def _refuse_input(cast: exp.Cast) -> None:
    """Raise the input function's error for a string literal cast past its type."""
    text = _string_cast(cast)
    if text is None:
        return
    if _cast_name(cast) == "oid":
        read_oid(text)  # oidin's range, -2147483648 to 4294967295
        return
    for kind, (low, high, name) in _INTEGER_INPUTS.items():
        if cast.to.is_type(kind):
            matched = _DECIMAL_INTEGER.fullmatch(text)
            if matched is not None and not low <= int(matched[1]) <= high:
                raise PostgresError("22003", f'value "{text}" is out of range for type {name}')
            return
    if cast.to.is_type(exp.DataType.Type.DATE):
        read = _iso_date(text)
        if read is not None and read > _LAST_DATE:
            raise PostgresError("22008", f'date out of range: "{text}"')


def _refuse_oid_of_int8(node: exp.Expr) -> None:
    """Raise int8's cast to oid (oid(bigint), bigint::oid) of a constant past oid's range."""
    if isinstance(node, exp.Cast) and _cast_name(node) == "oid":
        value = _int8_constant(node.this)
    elif (
        isinstance(node, exp.Anonymous)
        and node.name.lower() == "oid"
        and len(node.expressions) == 1
    ):
        value = _int8_constant(node.expressions[0])
    else:
        return
    if value is not None and not 0 <= value <= _OID_MAX:
        raise PostgresError("22003", "OID out of range")


def _refuse_call(call: exp.Anonymous) -> None:
    """Raise a call's error for a constant argument PostgreSQL refuses: a NaN numeric into money
    or pg_lsn (0A000), a date date_part reads as a timestamp past timestamp's range (22008)."""
    name = call.name.lower()
    args = call.expressions
    if name in _NAN_READERS:
        at, message = _NAN_READERS[name]
        text = _string_cast_to(args[at], exp.DataType.Type.DECIMAL) if at < len(args) else None
        if text is not None and text.strip().lower() == "nan":
            raise PostgresError("0A000", message)
    if name == "date_part" and len(args) == 2:
        text = _string_cast_to(args[1], exp.DataType.Type.DATE)
        read = _iso_date(text) if text is not None else None
        if read is not None and read >= _FIRST_PAST_TIMESTAMP:
            raise PostgresError("22008", "date out of range for timestamp")


def refuse_past_inputs(pg_sql: str) -> None:
    """Raise PostgreSQL's error for a constant of ``pg_sql`` its input refuses (REQ-2236, amended
    2026-11-23: a recognisable edge with a fixed answer raises PostgreSQL's SQLSTATE): an oid,
    integer or date literal past its type, int8's cast to oid past oid's range, a NaN numeric into
    money or pg_lsn, date_part of a date past timestamp's range. Each literal is read before the
    call given it, as PostgreSQL reads a literal when it parses the statement. DuckDB's cast
    answers these (its BIGINT holds an oid, its DATE and INTEGER read further) or fails with an
    error of its own. A TRY_CAST is not refused: it answers NULL where its type refuses."""
    if _PAST_INPUT.search(pg_sql) is None:
        return
    try:
        tree = sqlglot.parse_one(pg_sql, read="postgres")
    except SqlglotError:
        return  # a text sqlglot cannot read: the transpile that follows reports it
    # A TRY_CAST answers NULL for an input its type refuses: it is the lowered form of
    # pg_input_is_valid and pg_input_error_info (catalog_server_answers.input_check) and of a
    # null-on-failure cast, whose job is to report the refusal, not raise it.
    casts = [c for c in tree.find_all(exp.Cast) if not isinstance(c, exp.TryCast)]
    for cast in casts:
        _refuse_input(cast)
    for node in [*casts, *tree.find_all(exp.Anonymous)]:
        _refuse_oid_of_int8(node)
    for call in tree.find_all(exp.Anonymous):
        _refuse_call(call)


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
