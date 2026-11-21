# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9a1c5e-48b7-4e02-9f6d-c2e7a0b5d814
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog function's argument, read as PostgreSQL reads it before the function is called
(REQ-2236, REQ-2167): an oid literal past the type is oidin's error whichever function it is given
to, and a constant NULL to a strict function is NULL with no form read. Both are known as the
statement is addressed, so every engine answers them alike."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import re

import sqlglot.expressions as exp

from provisa.core.pg_error import PostgresError
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.pgwire.catalog_pg17_procs import PROCS

#: oidin's range (src/backend/utils/adt/oid.c, uint32in_subr): an unsigned 32-bit value, or a
#: negative one an int4 holds, which it reads as that int4's unsigned value.
_OID_MAX = 2**32 - 1
_OID_MIN = -(2**31)
#: The text read here: decimal digits, signed or not, between spaces. oidin reads its text with
#: strtoul in base 0, so one with a leading zero is octal or hexadecimal: left as it was.
_DECIMAL = re.compile(r"\s*([+-]?(?:0|[1-9][0-9]*))\s*")
_DIGITS = re.compile(r"[0-9]+")


def read_oid(written: str) -> None:
    """Raise oidin's error where ``written``, an oid literal's text, is past the type."""
    read = _DECIMAL.fullmatch(written)
    if read is not None and not _OID_MIN <= int(read[1]) <= _OID_MAX:
        raise PostgresError("22003", f'value "{written}" is out of range for type oid')


def read_oid_literals(tree: exp.Expr) -> None:
    """Raise oidin's error for a string literal of ``tree`` cast to oid that is past the type. An
    object identifier type's input (regclassin and its kin) reads a string of digits with oidin,
    and any other text as a name."""
    for cast in tree.find_all(exp.Cast):
        value = cast.this
        if not (isinstance(value, exp.Literal) and value.is_string):
            continue
        to = cast.to.sql(dialect="postgres").lower().removeprefix("pg_catalog.")
        if to == "oid" or (to.startswith("reg") and _DIGITS.fullmatch(value.this)):
            read_oid(value.this)


_PRONAME, _PROKIND, _PROISSTRICT, _PRORETSET, _PRONARGS, _PRORETTYPE = 1, 8, 11, 12, 15, 17
_TYPTYPE, _FORMAT_TYPE = 3, 12


def _strict_returns() -> dict[tuple[str, int], str]:
    """A plain function's return type by (name, argument count), where every overload of that
    count is strict and returns that one type, which is no pseudo-type."""
    overloads: dict[tuple[str, int], set[tuple[bool, int]]] = {}
    for proc in PROCS:
        strict = proc[_PROISSTRICT] and proc[_PROKIND] == "f" and not proc[_PRORETSET]
        key = (proc[_PRONAME], proc[_PRONARGS])
        overloads.setdefault(key, set()).add((strict, proc[_PRORETTYPE]))
    returns = {}
    for key, seen in overloads.items():
        if len(seen) != 1:
            continue
        ((strict, returned),) = seen
        if strict and TYPES[returned][_TYPTYPE] != "p":
            returns[key] = TYPES[returned][_FORMAT_TYPE]
    return returns


#: (name, argument count) -> the type of the NULL a NULL argument answers.
STRICT_RETURNS = _strict_returns()


def _is_null(value: exp.Expr) -> bool:
    while isinstance(value, (exp.Cast, exp.Paren)):
        value = value.this
    return isinstance(value, exp.Null)


def strict_null(name: str, args: list[exp.Expr]) -> exp.Expr | None:
    """NULL of the function's type where ``name`` is strict in pg_proc and one of ``args`` is the
    constant NULL, cast or not: PostgreSQL does not call it. None for any other call."""
    returns = STRICT_RETURNS.get((name, len(args)))
    if returns is None or not any(_is_null(a) for a in args):
        return None
    return exp.Cast(this=exp.null(), to=exp.DataType.build(returns, dialect="postgres", udt=True))
