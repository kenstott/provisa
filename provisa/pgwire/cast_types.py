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

import functools
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


@functools.lru_cache(maxsize=4096)
def result_casts(sql: str) -> tuple[CastType | None, ...] | None:
    """For each result column of ``sql`` (a single SELECT), the type its root cast gives it, or
    None for a column that is not such a cast; None for a statement that is not a single SELECT
    or that does not parse as PostgreSQL."""
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import ParseError

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return None  # not PostgreSQL's grammar: the engine's own types describe it
    if not isinstance(tree, exp.Select):
        return None
    out: list[CastType | None] = []
    for select in tree.selects:
        expression = select.unalias()
        if isinstance(expression, exp.Star):
            return None  # its columns are known only to the engine
        out.append(_cast_type(expression.to) if isinstance(expression, exp.Cast) else None)
    return tuple(out)
