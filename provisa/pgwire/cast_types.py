# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2d4b17-6c3a-4f95-a0d8-1b7e9c5f3a62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL type of a result column that is a cast (REQ-2167, PGWIRE CATALOG AND TYPES).

A statement's result column whose expression is, at its root, a cast to a PostgreSQL type is
described as PostgreSQL describes it: with the cast's type OID and typmod. The engine computes the
value; several PostgreSQL types have no engine type of their own (varchar and character are the
engine's text, jsonb its JSON, oid its integer), and the engine names no typmod, so the cast is
what says the type. A cast is described by it only where the engine's value has that type's wire
form (``CastType.served_as``); a character(n) value is padded to its length, as bpchar's input
pads it."""

# Requirements: REQ-2167

from __future__ import annotations

import decimal
import functools
import re
from dataclasses import dataclass
from typing import Any, Callable

from buenavista.core import BVType

#: The header PostgreSQL adds to a typmod that carries a length or a precision (VARHDRSZ).
_VARHDRSZ = 4

_TEXT_FORMS = frozenset({BVType.TEXT})


@dataclass(frozen=True)
class CastType:
    """How a cast's result column is described: its OID, typmod and wire type; ``served_as`` the
    engine wire types whose value has this type's form; ``coerce`` brings the value to it."""

    oid: int
    typmod: int
    bvtype: BVType
    served_as: frozenset[BVType]
    coerce: Callable[[Any], Any] | None = None


def _length(data_type: Any) -> int | None:
    sizes = data_type.expressions
    return int(sizes[0].name) if sizes else None


def _padded(length: int) -> Callable[[Any], Any]:
    """bpchar's input for character(length): cut to the length, then padded with spaces."""

    def pad(value: Any) -> Any:
        return value[:length].ljust(length) if isinstance(value, str) else value

    return pad


def _cast_type(data_type: Any) -> CastType | None:  # noqa: PLR0911 -- one return per cast type
    import sqlglot.expressions as exp

    kind = data_type.this
    t = exp.DataType.Type
    if kind == t.VARCHAR:
        length = _length(data_type)
        typmod = -1 if length is None else length + _VARHDRSZ
        return CastType(1043, typmod, BVType.TEXT, _TEXT_FORMS)
    if kind in (t.CHAR, t.BPCHAR):
        length = _length(data_type)
        if length is None and kind == t.CHAR:
            length = 1  # character with no length is character(1)
        if length is None:
            return CastType(1042, -1, BVType.TEXT, _TEXT_FORMS)
        return CastType(1042, length + _VARHDRSZ, BVType.TEXT, _TEXT_FORMS, _padded(length))
    if kind == t.DECIMAL:
        sizes = [int(e.name) for e in data_type.expressions]
        if not sizes:
            return CastType(1700, -1, BVType.DECIMAL, frozenset({BVType.DECIMAL}))
        precision, scale = sizes[0], sizes[1] if len(sizes) > 1 else 0
        typmod = ((precision << 16) | scale) + _VARHDRSZ
        return CastType(1700, typmod, BVType.DECIMAL, frozenset({BVType.DECIMAL}))
    if kind == t.FLOAT:
        return CastType(700, -1, BVType.REAL, frozenset({BVType.REAL, BVType.FLOAT}))
    if kind == t.JSONB:
        return CastType(3802, -1, BVType.JSONB, frozenset({BVType.JSON, BVType.JSONB}))
    if isinstance(data_type, exp.ObjectIdentifier) and data_type.name.lower() == "oid":
        integers = frozenset({BVType.SMALLINT, BVType.INTEGER, BVType.BIGINT, BVType.OID})
        return CastType(26, -1, BVType.OID, integers)
    return None


def _to_decimal(value: Any) -> Any:
    if value is None or isinstance(value, decimal.Decimal):
        return value
    return decimal.Decimal(str(value))


def _to_int(value: Any) -> Any:
    return None if value is None else int(value)


def _to_float(value: Any) -> Any:
    return None if value is None else float(value)


_INTEGERS = frozenset({BVType.SMALLINT, BVType.INTEGER, BVType.BIGINT})
_NUMBERS = _INTEGERS | {BVType.REAL, BVType.FLOAT, BVType.DECIMAL}

#: How a call of a pg_catalog function is described, by the type PostgreSQL says it returns
#: (function_types): the engine computes the value, in a type of its own where its function's
#: differs (DuckDB's length is a BIGINT, its avg a DOUBLE), brought to PostgreSQL's type. A type
#: absent here is the engine's to get right: its value is not this type's by a conversion of form.
_RETURNED: dict[str, CastType] = {
    "smallint": CastType(21, -1, BVType.SMALLINT, _INTEGERS, _to_int),
    "integer": CastType(23, -1, BVType.INTEGER, _INTEGERS, _to_int),
    "bigint": CastType(20, -1, BVType.BIGINT, _INTEGERS, _to_int),
    # An exact numeric of no fixed scale is served as its text (duckdb_pg_scalars).
    "numeric": CastType(1700, -1, BVType.DECIMAL, _NUMBERS | {BVType.TEXT}, _to_decimal),
    "double precision": CastType(701, -1, BVType.FLOAT, _NUMBERS, _to_float),
    "real": CastType(700, -1, BVType.REAL, frozenset({BVType.REAL, BVType.FLOAT}), _to_float),
    "oid": CastType(26, -1, BVType.OID, _INTEGERS | {BVType.OID}),
    "character": CastType(1042, -1, BVType.TEXT, _TEXT_FORMS),
    '"char"': CastType(18, -1, BVType.TEXT, _TEXT_FORMS),
    "name": CastType(19, -1, BVType.TEXT, _TEXT_FORMS),
    "character varying": CastType(1043, -1, BVType.TEXT, _TEXT_FORMS),
    "bit": CastType(1560, -1, BVType.BIT, frozenset({BVType.BIT, BVType.TEXT})),
    "bit varying": CastType(1562, -1, BVType.VARBIT, frozenset({BVType.BIT, BVType.TEXT})),
    "numrange": CastType(3906, -1, BVType.NUMRANGE, _TEXT_FORMS),
    "tsvector": CastType(3614, -1, BVType.TSVECTOR, _TEXT_FORMS),
    "jsonb": CastType(3802, -1, BVType.JSONB, frozenset({BVType.JSON, BVType.JSONB})),
    "name[]": CastType(1003, -1, BVType.NAMEARRAY, frozenset({BVType.STRINGARRAY})),
    "cidr": CastType(650, -1, BVType.CIDR, frozenset({BVType.INET})),
    "void": CastType(2278, -1, BVType.TEXT, _TEXT_FORMS),
    "cstring": CastType(2275, -1, BVType.TEXT, _TEXT_FORMS),
    # The engine holds money as its base; its encoder sends the value as money.
    "money": CastType(790, -1, BVType.MONEY, _NUMBERS | {BVType.TEXT, BVType.MONEY}),
}


_BIT_VARYING = re.compile(r"::\s*bit varying\b", re.IGNORECASE)


def bit_varying_read(sql: str) -> str:
    """``sql`` as sqlglot reads it: ``::bit varying`` it takes for a bit and an alias."""
    return _BIT_VARYING.sub("::varbit", sql)


def _result_type(expression: Any, columns: Any) -> CastType | None:
    """How a result column is described by its root: a money expression as PostgreSQL's operators
    type it, a cast by the cast's type, a call of a pg_catalog function by the type that function
    returns; None for any other column."""
    import sqlglot.expressions as exp

    from provisa.pgwire.function_types import call_result_type
    from provisa.transpiler.pg_money import money_type

    typed = None if columns is None else money_type(expression, columns)
    if typed is not None:
        return _RETURNED[typed]
    if isinstance(expression, exp.Cast):
        return _cast_type(expression.to)
    returned = call_result_type(expression)
    return None if returned is None else _RETURNED.get(returned)


@functools.lru_cache(maxsize=4096)
def result_casts(sql: str) -> tuple[CastType | None, ...] | None:
    """For each result column of ``sql`` (a single SELECT), the type its root cast gives it, or
    None for a column that is not such a cast; None for a statement that is not a single SELECT
    or that does not parse as PostgreSQL."""
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import ParseError

    from provisa.transpiler.pg_money import names_money, statement_columns

    try:
        tree = sqlglot.parse_one(bit_varying_read(sql), read="postgres")
    except ParseError:
        return None  # not PostgreSQL's grammar: the engine's own types describe it
    if not isinstance(tree, exp.Select):
        return None
    columns = statement_columns(tree) if names_money(sql) else None
    out: list[CastType | None] = []
    for select in tree.selects:
        expression = select.unalias()
        if isinstance(expression, exp.Star):
            return None  # its columns are known only to the engine
        out.append(_result_type(expression, columns))
    return tuple(out)


#: The registered types described with a typmod of their own (REQ-1961, amended 2026-10-28):
#: a sized numeric, and the character types a text column's native type may be.
_CHARACTER_KINDS = ("VARCHAR", "CHAR", "BPCHAR")


@functools.lru_cache(maxsize=4096)
def described_type(spelling: str) -> CastType | None:
    """How a registered type is described (REQ-1961, PGWIRE SENDS NUMERIC(P,S); VARCHAR AND CHAR
    KEEP THEIR TYPE): a numeric(p,s) as numeric with its typmod, and varchar, varchar(n), char(n)
    or bpchar as that type with its length typmod, as a cast to it is. None for any other type,
    which its wire type describes."""
    import sqlglot.expressions as exp
    from sqlglot.errors import SqlglotError

    from provisa.core.numeric_type import numeric_size_of

    try:
        data_type = exp.DataType.build(spelling, dialect="postgres")
    except (SqlglotError, ValueError):
        return None  # no PostgreSQL type: its wire type describes it
    kind = data_type.this
    if not isinstance(kind, exp.DataType.Type):
        return None  # an object identifier type (oid, regclass): sqlglot names it by a string
    if kind == exp.DataType.Type.DECIMAL and numeric_size_of(spelling) is None:
        return None  # an unsized numeric has no typmod
    if kind != exp.DataType.Type.DECIMAL and kind.name not in _CHARACTER_KINDS:
        return None
    return _cast_type(data_type)


def registered_spelling(data_type: str, native_type: str | None) -> str:
    """The type a registered column is described by on pgwire and in its catalog: a text column
    whose native type is a character type (registration keeps it, REQ-1961) is that type;
    any other column is its registered type."""
    if native_type and data_type.lower() == "text":
        native = described_type(native_type)
        if native is not None and native.bvtype == BVType.TEXT:
            return native_type
    return data_type
