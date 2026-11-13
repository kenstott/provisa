# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e1d72-3a6c-4f08-8e5d-c2a7f0b91e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamp with time zone on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND
COMES BEFORE A REFUSAL; maintainer 2026-11-13).

ClickHouse holds no zoned instant. A timestamptz is held there as DateTime64(6, 'UTC'): the
instant's UTC wall time at microseconds, which is PostgreSQL's own value (microseconds since an
epoch, in UTC) wherever DateTime64 reaches: 1900-01-01 00:00:00 to 2299-12-31 23:59:59.999999.
Inside that bound every comparison of two instants, and of an instant with a date or a timestamp
read in the session's zone, is exact.

The bound is enforced where a value enters a call: a literal the statement writes is read here, as
the statement is transpiled, and one outside the bound raises 0A000 naming the function and the
bound. ClickHouse would otherwise hold it at the nearest end of its range, a shifted value. A value
ClickHouse already holds as DateTime64 or Date32 is inside the bound by its type.

A literal with an offset reaches here as its instant's UTC wall time (transpile._instants_as_utc).
One without an offset is read in the session's TimeZone by PostgreSQL. On a session on UTC (the
conformance run's, and pgwire's unless a client sets another), that is the UTC wall time the
literal writes, and the cast to DateTime64(6, 'UTC') reads it so whatever ClickHouse's own zone
is. On a session on another zone, the two literals can no longer be told apart here, so a call of a
literal instant, and a call that reads a date or a timestamp as an instant (in the session's zone),
keeps its refusal by name there rather than answer in UTC.

Only a literal in ISO form (a date, then a time to microseconds) is read: PostgreSQL's other forms,
infinity, and a fraction past microseconds (which PostgreSQL rounds and ClickHouse cuts) keep their
refusal by name, as does a value whose type no statement says. Every other dialect keeps what it
had (provisa.transpiler.function_macros' forms)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import enum
import re
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot.expressions as exp

from provisa.core.session_time_zone import current_zone
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "timestamp with time zone on ClickHouse"

_CLICKHOUSE = "clickhouse"
#: The first and the last instant DateTime64(6, 'UTC') holds, and Date32 the days between.
FIRST = datetime.datetime(1900, 1, 1)
LAST = datetime.datetime(2299, 12, 31, 23, 59, 59, 999999)
BOUND = "1900-01-01 00:00:00 .. 2299-12-31 23:59:59.999999 UTC"

#: The types of a comparison's operands, by PostgreSQL's names a statement says of them.
_INSTANT = frozenset({"timestamptz", "timestamp with time zone"})
_DAY = frozenset({"date"})
_STAMP = frozenset({"timestamp", "timestamp without time zone"})
#: A literal's text, as read here: a date, and a time to microseconds.
_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?")
_DAY_ONLY = re.compile(r"\d{4}-\d{2}-\d{2}")


def out_of_bound(function: str) -> str:
    """The message of a value outside the bound, which the form raises with SQLSTATE 0A000."""
    return f"{function}() of a value outside {BOUND} has no exact form on clickhouse"


def _read(text: str, day_only: bool) -> datetime.datetime | None:
    """The value a literal in ISO form writes; None for any other text, and for one no calendar
    holds (PostgreSQL's error of it is the engine's to give)."""
    if (_DAY_ONLY if day_only else _ISO).fullmatch(text) is None:
        return None
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return None


@dataclass(frozen=True)
class _Operand:
    """What a comparison reads an operand as: its types, and whether its literal is a date."""

    types: frozenset[str]
    day_only: bool = False


_OF = {
    "timestamptz": _Operand(_INSTANT),
    "date": _Operand(_DAY, True),
    "timestamp": _Operand(_STAMP),
}


class _Read(enum.Enum):
    """What a comparison reads of an operand."""

    NULL = "null"  # a NULL: the STRICT function answers NULL
    VALUE = "value"  # a value ClickHouse holds as its type, inside the bound by that type
    LITERAL = "literal"  # a literal in ISO form inside the bound
    OUTSIDE = "outside"  # a literal in ISO form outside the bound


def _checked(argument: exp.Expr, operand: _Operand) -> _Read | None:
    """What the row reads of ``argument``; None where it does not read it (its type is not said,
    or its text is no ISO form)."""
    value = argument.unnest()
    if isinstance(value, exp.Null):
        return _Read.NULL
    said = trino.said(value)
    if said is None or said.split("(")[0] not in operand.types:
        return None
    if not isinstance(value, exp.Cast):
        return _Read.VALUE
    written = value.this.unnest()
    if isinstance(written, exp.Null):
        return _Read.NULL
    if not (isinstance(written, exp.Literal) and written.is_string):
        return _Read.VALUE  # a value ClickHouse computes holds no instant outside its type's range
    read = _read(written.this, operand.day_only)
    if read is None:
        return None
    return _Read.LITERAL if FIRST <= read <= LAST else _Read.OUTSIDE


def raised(function: str, dialect: str) -> exp.Expr:
    """ClickHouse's raise of 0A000 for a value outside the bound, as the call's answer."""
    form = ch_fail("0A000", out_of_bound(function)).format(condition="1")
    return replacement(form, dialect, f"{function}()")


#: Each comparison: PostgreSQL's operator, or the form of cmp, larger and smaller (strict: NULL of
#: a NULL, as PostgreSQL's STRICT functions answer).
_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_CMP = "CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 WHEN {a} = {b} THEN 0 END"
#: timestamp_larger and timestamp_smaller: the first where it compares above (below) the second,
#: else the second.
_LARGER = "CASE WHEN {a} IS NULL OR {b} IS NULL THEN NULL WHEN {a} > {b} THEN {a} ELSE {b} END"
_SMALLER = "CASE WHEN {a} IS NULL OR {b} IS NULL THEN NULL WHEN {a} < {b} THEN {a} ELSE {b} END"
#: Each operand as the instant ClickHouse compares: a date at its midnight, a timestamp as the
#: same wall time, each in UTC (the session's zone, where this row answers).
_AS_INSTANT = {
    "timestamptz": "{}",
    "date": "toDateTime64({}, 6, 'UTC')",
    "timestamp": "{}",
}


def _comparison(function: str, kinds: tuple[str, str], body: str) -> Callable[..., exp.Expr | None]:
    """The rewrite of one comparison of ``kinds``, its form ``body`` of {a} and {b}."""
    a = _AS_INSTANT[kinds[0]].format("$a")
    b = _AS_INSTANT[kinds[1]].format("$b")
    form = body.format(a=a, b=b)
    pattern = f"{function}($a, $b)"
    mixed = kinds != ("timestamptz", "timestamptz")

    def compared(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or function not in call[0] or len(call[1]) != 2:
            return None
        read = [_checked(arg, _OF[kind]) for arg, kind in zip(call[1], kinds)]
        if None in read:
            return None
        literal = _Read.LITERAL in read or _Read.OUTSIDE in read
        if current_zone.get() is not None and (mixed or literal):
            return None  # read in the session's zone: refused by name off UTC
        if _Read.OUTSIDE in read and _Read.NULL not in read:  # STRICT: NULL of a NULL
            return raised(function, dialect)
        return graft(replacement(form, dialect, pattern), dict(zip("ab", call[1])))

    compared.__doc__ = f"{function} of two values held as DateTime64(6, 'UTC'), in their bound."
    compared.__qualname__ = f"compared_{function}"
    compared.__name__ = compared.__qualname__
    return compared


_NAMED = {"date": "date", "timestamp": "timestamp", "timestamptz": "timestamptz"}
#: The comparisons of each pair of operand types, by PostgreSQL's function names.
_PAIRS: dict[tuple[str, str], tuple[str, tuple[str, ...]]] = {
    ("timestamptz", "timestamptz"): ("timestamptz_{}", (*_ORDER, "cmp", "larger", "smaller")),
    ("timestamptz", "date"): ("timestamptz_{}_date", (*_ORDER, "cmp")),
    ("date", "timestamptz"): ("date_{}_timestamptz", (*_ORDER, "cmp")),
    ("timestamptz", "timestamp"): ("timestamptz_{}_timestamp", (*_ORDER, "cmp")),
    ("timestamp", "timestamptz"): ("timestamp_{}_timestamptz", (*_ORDER, "cmp")),
}
_BODIES = {
    **{name: f"{{a}} {op} {{b}}" for name, op in _ORDER.items()},
    "cmp": _CMP,
    "larger": _LARGER,
    "smaller": _SMALLER,
}
_SUMMARIES = {
    "eq": "Whether {} equals {}.",
    "ne": "Whether {} differs from {}.",
    "lt": "Whether {} is before {}.",
    "le": "Whether {} is not after {}.",
    "gt": "Whether {} is after {}.",
    "ge": "Whether {} is not before {}.",
    "cmp": "btree's comparison of {} and {}: -1, 0 or 1.",
    "larger": "The later of {} and {}.",
    "smaller": "The earlier of {} and {}.",
}
_SPOKEN = {"timestamptz": "an instant", "date": "a date", "timestamp": "a timestamp"}

_HELD = (
    "Held as DateTime64(6, 'UTC'), exact from 1900-01-01 to 2299-12-31. A literal outside "
    f"{BOUND} raises 0A000 ({{}}() of a value outside {BOUND} has no exact form on "
    "clickhouse); ClickHouse would hold it at the nearest end of its range. A literal not in ISO "
    "form (a date, then a time to microseconds), infinity, and a value whose type no statement "
    "says are refused by name."
)
_ZONED = (
    " On a session off UTC, a call of a literal instant{} is refused by name ({}() has no form "
    "on clickhouse): a literal with no offset is read in the session's zone, which the "
    "transpiled form no longer tells from one with an offset."
)


def _feature(function: str, kinds: tuple[str, str], body: str, summary: str) -> Feature:
    mixed = kinds != ("timestamptz", "timestamptz")
    zoned = _ZONED.format(
        f", and every call (the {_NAMED[next(k for k in kinds if k != 'timestamptz')]} is read "
        "as an instant in the session's zone)"
        if mixed
        else "",
        function,
    )
    forms = {
        **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
        _CLICKHOUSE: Rewrite(_comparison(function, kinds, body)),
    }
    return Feature(
        function,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(f"{function}($a, $b)", forms),),
            limitations={_CLICKHOUSE: (Limitation(_HELD.format(function) + zoned),)},
        ),
    )


def _features() -> tuple[Feature, ...]:
    features = []
    for kinds, (named, names) in _PAIRS.items():
        for name in names:
            function = named.format(name)
            summary = _SUMMARIES[name].format(*(_SPOKEN[k] for k in kinds))
            features.append(_feature(function, kinds, _BODIES[name], summary))
    return tuple(features)


FEATURES: tuple[Feature, ...] = _features()
