# Copyright (c) 2026 Kenneth Stott
# Canary: d2f820b5-e32c-42a4-88b4-ca8ff98afe56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's refusal of a constant its input does not take, raised before DuckDB reads it
(REQ-2236): DuckDB's cast answers an oid, integer or date literal past PostgreSQL's type, or
fails with an error of its own.

Kept apart from provisa/transpiler/pg_literals.py, which the code host's front-end image ships
alone (provisa/codesource/image_build.py PROTOCOL_MODULES): this module needs provisa.core."""

# Requirements: REQ-2236

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import PostgresError
from provisa.transpiler.metadata_arguments import read_oid


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
