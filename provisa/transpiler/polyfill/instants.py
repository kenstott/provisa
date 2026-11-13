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
import zoneinfo
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot.expressions as exp

from provisa.core.session_time_zone import current_zone
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail
from provisa.transpiler.pg_literals import values_cast
from provisa.transpiler import pg_forms
from provisa.transpiler.engine_refusals import PLAIN_PARTS

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
    "part": _Operand(frozenset({"text"})),
}
#: A part date_part reads of an instant as it reads one of a timestamp (engine_refusals.PLAIN_PARTS).
_PART = "part"


class _Read(enum.Enum):
    """What a comparison reads of an operand."""

    NULL = "null"  # a NULL: the STRICT function answers NULL
    VALUE = "value"  # a value ClickHouse holds as its type, inside the bound by that type
    LITERAL = "literal"  # a literal in ISO form inside the bound
    OUTSIDE = "outside"  # a literal in ISO form outside the bound


def _written(value: exp.Expr, operand: _Operand) -> _Read | None:
    """What the row reads of a value of the operand's type: a NULL, a literal in ISO form inside
    or outside the bound, or a value ClickHouse computes; None for a literal of no ISO form."""
    written = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(written, exp.Null):
        return _Read.NULL
    if not (isinstance(written, exp.Literal) and written.is_string):
        return _Read.VALUE  # a value ClickHouse computes holds no instant outside its type's range
    read = _read(written.this, operand.day_only)
    if read is None:
        return None
    return _Read.LITERAL if FIRST <= read <= LAST else _Read.OUTSIDE


def _of_its_type(said: str | None, operand: _Operand) -> bool:
    return said is not None and said.split("(")[0] in operand.types


#: What a column of a VALUES list reads, by what its rows read: one the row does not read refuses
#: the call, one outside the bound raises it, a literal reads in the session's zone.
_RANK = (_Read.OUTSIDE, _Read.LITERAL, _Read.VALUE, _Read.NULL)


def _column(column: exp.Column, operand: _Operand) -> _Read | None:
    """What the row reads of a column of a VALUES list the statement casts to the operand's type:
    every row's value is read, as each enters the call."""
    cast = values_cast(column)
    if cast is None or not _of_its_type(cast.to.sql(dialect="postgres").lower(), operand):
        return None
    values = cast.find_ancestor(exp.Values)
    assert values is not None  # values_cast reads the cast from a row of a VALUES list
    alias = values.args["alias"]
    index = [c.name for c in alias.columns].index(column.name)
    reads = []
    for row in values.expressions:
        value = row.expressions[index].unnest()
        if isinstance(value, exp.Cast) and not _of_its_type(trino.said(value), operand):
            return None  # a row of another type: PostgreSQL's coercion of it is not read here
        reads.append(_written(value, operand))
    if None in reads:
        return None
    return next(r for r in _RANK if r in reads)


def _part(value: exp.Expr) -> _Read | None:
    """A part of date_part: a NULL, or a literal of one engine_refusals reads as plain."""
    written = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(written, exp.Null):
        return _Read.NULL
    if isinstance(written, exp.Literal) and written.is_string:
        return _Read.VALUE if written.this.lower() in PLAIN_PARTS else None
    return None


def _checked(argument: exp.Expr, operand: _Operand) -> _Read | None:
    """What the row reads of ``argument``; None where it does not read it (its type is not said,
    or its text is no ISO form)."""
    value = argument.unnest()
    if isinstance(value, exp.Null):
        return _Read.NULL
    if isinstance(value, exp.Column):
        return _column(value, operand)
    if operand is _OF[_PART]:
        return _part(value)
    if not _of_its_type(trino.said(value), operand):
        return None
    if not isinstance(value, exp.Cast):
        return _Read.VALUE
    return _written(value, operand)


def raised(function: str, dialect: str, pattern: str | None = None) -> exp.Expr:
    """ClickHouse's raise of 0A000 for a value outside the bound, as the call's answer; ``pattern``
    the call's, where ``function()`` does not parse (an aggregate takes an argument)."""
    form = ch_fail("0A000", out_of_bound(function)).format(condition="1")
    return replacement(form, dialect, pattern or f"{function}()")


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


def _plain(argument: exp.Expr) -> bool:
    """Whether ``argument`` is a column, a NULL or a cast of a literal: what a row of no pattern
    reads, as the walk writes no argument of what such a row writes in its engine's form."""
    value = argument.unnest()
    written = value.this.unnest() if isinstance(value, exp.Cast) else value
    return isinstance(written, (exp.Column, exp.Null, exp.Literal))


def _rewrite(
    function: str,
    kinds: tuple[str, ...],
    form: str,
    *,
    zoned: bool,
    bounded: bool = True,
    strict: bool = True,
    plain: bool = False,
    names: frozenset[str] = frozenset(),
) -> Callable[[exp.Expr, str], exp.Expr | None]:
    """The rewrite of a call of ``function`` of ``kinds``, its ``form`` of $a, $b, $c, $d.
    ``zoned``: its answer reads a value in the session's zone, so it answers on UTC only.
    ``bounded``: its answer reads the value, so a literal outside the bound raises (isfinite reads
    only that there is one). ``strict``: a NULL argument answers NULL, so a literal outside the
    bound beside one is not read; otherwise any literal outside raises. ``plain``: the row names no
    pattern, so it reads only arguments :func:`_plain` holds."""
    holes = "abcd"[: len(kinds)]
    pattern = f"{function}({', '.join('$' + h for h in holes)})"

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or not ({function} | names) & call[0] or len(call[1]) != len(kinds):
            return None
        if plain and not all(_plain(arg) for arg in call[1]):
            return None
        read = [_checked(arg, _OF[kind]) for arg, kind in zip(call[1], kinds)]
        if None in read:
            return None
        literal = _Read.LITERAL in read or _Read.OUTSIDE in read
        if current_zone.get() is not None and (zoned or literal):
            return None  # read in the session's zone: refused by name off UTC
        if bounded and _Read.OUTSIDE in read and not (strict and _Read.NULL in read):
            return raised(function, dialect, pattern)
        return graft(replacement(form, dialect, pattern), dict(zip(holes, call[1])))

    return rewrite


def _comparison(function: str, kinds: tuple[str, str], body: str) -> Callable[..., exp.Expr | None]:
    """The rewrite of one comparison of ``kinds``, its form ``body`` of {a} and {b}."""
    a = _AS_INSTANT[kinds[0]].format("$a")
    b = _AS_INSTANT[kinds[1]].format("$b")
    mixed = kinds != ("timestamptz", "timestamptz")
    compared = _rewrite(function, kinds, body.format(a=a, b=b), zoned=mixed)
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


def zoned_instant(
    argument: exp.Expr, function: str, dialect: str, pattern: str
) -> tuple[bool, exp.Expr | None]:
    """Whether a row answers a call of ``function`` that reads the instant ``argument`` in the
    session's zone (on UTC only, of a value this module reads), and the raise it answers with
    where the value is a literal outside the bound; ``pattern`` the call's."""
    read = _checked(argument, _OF["timestamptz"])
    if read is None or current_zone.get() is not None:
        return False, None
    return True, raised(function, dialect, pattern) if read is _Read.OUTSIDE else None


# --- one instant -------------------------------------------------------------------------------


def _of_one(
    name: str,
    function: str,
    form: str,
    purpose: str,
    *,
    zoned: bool,
    bounded: bool = True,
    plain: bool = False,
) -> Rewrite:
    rewrite = _rewrite(function, ("timestamptz",), form, zoned=zoned, bounded=bounded, plain=plain)
    rewrite.__doc__ = purpose
    rewrite.__qualname__ = name
    rewrite.__name__ = name
    return Rewrite(rewrite)


_ONE_HELD = (
    "Held as DateTime64(6, 'UTC'), exact from 1900-01-01 to 2299-12-31. A literal outside "
    f"{BOUND} raises 0A000 ({{}}() of a value outside {BOUND} has no exact form on "
    "clickhouse). A literal not in ISO form, infinity, and a value whose type no statement says "
    "are refused by name."
)
_ONE_ZONED = (
    " On a session off UTC it is refused by name ({}() has no form on clickhouse): its answer is "
    "read in the session's zone."
)
_ONE_LITERAL = (
    " On a session off UTC a call of a literal is refused by name ({}() has no form on "
    "clickhouse): a literal with no offset is read in the session's zone."
)


def _one_feature(
    feature: str,
    function: str,
    summary: str,
    form: str,
    *,
    zoned: bool,
    pattern: bool,
    bounded: bool = True,
) -> Feature:
    """A row of a function of one instant. A row whose function another row of the grid also
    reads (max, min, date) names no pattern, so the other rows' rules are still read."""
    name = f"{function}_of_an_instant"
    purpose = f"{function} of a value held as DateTime64(6, 'UTC'), in its bound."
    rewrite = _of_one(
        name, function, form, purpose, zoned=zoned, bounded=bounded, plain=not pattern
    )
    stated = (_ONE_HELD.format(function) if bounded else _UNBOUNDED) + (
        _ONE_ZONED if zoned else _ONE_LITERAL
    ).format(function)
    if pattern:
        forms = {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, _CLICKHOUSE: rewrite}
        rule = Rule(f"{function}($a)", forms)
    else:
        rule = Rule(None, {_CLICKHOUSE: rewrite})
    return Feature(
        feature,
        FAMILY,
        summary,
        read=Side(rules=(rule,), limitations={_CLICKHOUSE: (Limitation(stated),)}),
    )


_UNBOUNDED = (
    "Held as DateTime64(6, 'UTC'): exact, as its answer reads only that a value is there "
    "(ClickHouse holds no infinity). A literal not in ISO form, infinity, and a value whose type "
    "no statement says are refused by name."
)

FEATURES: tuple[Feature, ...] = (
    *_features(),
    _one_feature(
        "max(timestamptz)", "max", "The latest of a group's instants.", "max($a)",
        zoned=False, pattern=False,
    ),
    _one_feature(
        "min(timestamptz)", "min", "The earliest of a group's instants.", "min($a)",
        zoned=False, pattern=False,
    ),
    _one_feature(
        "isfinite(timestamptz)", "isfinite", "Whether an instant is finite.",
        "if(isNull($a), NULL, true)", zoned=False, pattern=True, bounded=False,
    ),
    _one_feature(
        "date(timestamptz)", "date", "An instant's day in the session's zone.",
        "toDate32($a)", zoned=True, pattern=False,
    ),
    _one_feature(
        "timestamp(timestamptz)", "timestamp", "An instant's wall time in the session's zone.",
        "$a", zoned=True, pattern=True,
    ),
)  # fmt: skip


# --- a call of an instant and its other arguments ------------------------------------------------


def _call_feature(
    feature: str,
    function: str,
    kinds: tuple[str, ...],
    form: str,
    summary: str,
    stated: str,
    *,
    zoned: bool,
    strict: bool = True,
    pattern: bool = True,
    names: frozenset[str] = frozenset(),
) -> Feature:
    """A row of ``function($a, ...)`` of ``kinds``: ClickHouse's form, every other dialect keeping
    what it had. A call that reaches the grid as no node a pattern names (date_part, quoted, is
    sqlglot's Extract to a pattern) is a row of no pattern."""
    rewrite = _rewrite(
        function, kinds, form, zoned=zoned, strict=strict, plain=not pattern, names=names
    )
    rewrite.__doc__ = f"{function} of values held as DateTime64(6, 'UTC'), in their bound."
    rewrite.__qualname__ = rewrite.__name__ = f"{function}_of_instants"
    if pattern:
        holes = ", ".join("$" + h for h in "abcd"[: len(kinds)])
        forms = {
            **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
            _CLICKHOUSE: Rewrite(rewrite),
        }
        rule = Rule(f"{function}({holes})", forms)
    else:
        rule = Rule(None, {_CLICKHOUSE: Rewrite(rewrite)})
    return Feature(
        feature,
        FAMILY,
        summary,
        read=Side(rules=(rule,), limitations={_CLICKHOUSE: (Limitation(stated),)}),
    )


#: date_part of an instant: ClickHouse's form of a timestamp's (pg_forms.clickhouse_date_part),
#: its value the instant's UTC wall time, which is the session's where this row answers.
_DATE_PART = re.sub(
    r"\bpart\b",
    "$a",
    re.sub(
        r"\bvalue\b",
        "$b",
        pg_forms.clickhouse_date_part().replace("CAST(value AS TIMESTAMP)", "$b"),
    ),
)
#: overlaps of two periods of instants: PostgreSQL's (pg_forms.OVERLAPS), not strict.
_OVERLAPS = re.sub(
    r"\b(s1|e1|s2|e2)\b",
    lambda m: {"s1": "$a", "e1": "$b", "s2": "$c", "e2": "$d"}[m.group(1)],
    # ClickHouse's BOOLEAN holds no NULL: the form's unknown answer is a Nullable(Bool).
    pg_forms.OVERLAPS.replace("CAST(NULL AS BOOLEAN)", "CAST(NULL AS Nullable(Bool))"),
)

_PARTS_STATED = (
    _ONE_HELD.format("{}")
    + " Only the parts "
    + ", ".join(sorted(PLAIN_PARTS))
    + " are read; any other is refused by name."
    + _ONE_ZONED
)


def _parts(function: str, numbered: str) -> Feature:
    return _call_feature(
        f"{function}(text, timestamptz)",
        function,
        (_PART, "timestamptz"),
        _DATE_PART,
        f"A part of an instant in the session's zone, as {numbered}.",
        _PARTS_STATED.format(function, function),
        zoned=True,
        pattern=False,
    )


# --- an instant at a zone, and to a precision ----------------------------------------------------

#: An instant's text, held: a literal the statement writes, or a NULL.
_Held = datetime.datetime | None


def _literal_instants(argument: exp.Expr) -> list[_Held] | None:
    """Each instant ``argument`` holds, as UTC wall time, where each is a literal in ISO form or a
    NULL (a column of a VALUES list: each row's); None where any is another value, whose result
    this row cannot read inside the bound before ClickHouse computes it."""
    value = argument.unnest()
    if isinstance(value, exp.Null):
        return [None]
    if isinstance(value, exp.Column):
        cast = values_cast(value)
        if cast is None or not _of_its_type(
            cast.to.sql(dialect="postgres").lower(), _OF["timestamptz"]
        ):
            return None
        values = cast.find_ancestor(exp.Values)
        assert values is not None  # values_cast reads the cast from a row of a VALUES list
        index = [c.name for c in values.args["alias"].columns].index(value.name)
        rows = [row.expressions[index].unnest() for row in values.expressions]
    elif _of_its_type(trino.said(value), _OF["timestamptz"]):
        rows = [value]
    else:
        return None
    held: list[_Held] = []
    for row in rows:
        if isinstance(row, exp.Cast) and not _of_its_type(trino.said(row), _OF["timestamptz"]):
            return None
        written = row.this.unnest() if isinstance(row, exp.Cast) else row
        if isinstance(written, exp.Null):
            held.append(None)
            continue
        if not (isinstance(written, exp.Literal) and written.is_string):
            return None
        read = _read(written.this, False)
        if read is None:
            return None
        held.append(read)
    return held


def _inside(instant: datetime.datetime) -> bool:
    return FIRST <= instant <= LAST


def _string_literal(argument: exp.Expr) -> str | None:
    """The text of a string literal (cast or not); None for any other value, a NULL among them."""
    value = argument.unnest()
    written = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(written, exp.Literal) and written.is_string:
        return written.this
    return None


_NULL_INSTANT = "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"


def _is_null(argument: exp.Expr) -> bool:
    value = argument.unnest()
    return isinstance(value.this.unnest() if isinstance(value, exp.Cast) else value, exp.Null)


#: A zone ClickHouse and PostgreSQL both read as the same IANA zone: a region's name, or UTC.
#: An abbreviation, a POSIX offset (PostgreSQL's is west of UTC) and any other text are refused.
_ZONES = frozenset(
    z
    for z in zoneinfo.available_timezones()
    if "/" in z and not z.startswith(("Etc/", "posix/", "right/"))
) | {"UTC"}


def at_zone(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """timezone(zone, instant): the instant's wall time at a named zone, a timestamp held as
    DateTime64(6, 'UTC'). Each instant is a literal (or a NULL), its wall time at the zone read
    here, so one outside the bound raises before ClickHouse would hold it at the range's end."""
    call = trino.called(node)
    if call is None or "timezone" not in call[0] or len(call[1]) != 2:
        return None
    zone, value = call[1]
    instants = _literal_instants(value)
    if instants is None:
        return None
    if current_zone.get() is not None and any(i is not None for i in instants):
        return None  # a literal with no offset is read in the session's zone
    pattern = "timezone($z, $a)"
    if _is_null(zone):
        return replacement(_NULL_INSTANT, dialect, pattern)
    name = _string_literal(zone)
    if name not in _ZONES:
        return None
    at = zoneinfo.ZoneInfo(name)
    for instant in instants:
        if instant is None:
            continue
        if not _inside(instant):
            return raised("timezone", dialect, pattern)
        wall = instant.replace(tzinfo=datetime.timezone.utc).astimezone(at).replace(tzinfo=None)
        if not _inside(wall):
            return raised("timezone", dialect, pattern)
    form = f"toDateTime64(toString(toTimeZone($a, '{name}')), 6, 'UTC')"
    return graft(replacement(form, dialect, pattern), {"a": value})


#: PostgreSQL's epoch, at which a timestamp's microseconds are 0: its rounding to a precision is
#: half away from that instant (AdjustTimestampForTypmod).
_PG_EPOCH = datetime.datetime(2000, 1, 1)
_PG_EPOCH_MICROS = 946684800000000
_MICRO = datetime.timedelta(microseconds=1)


def _rounded(instant: datetime.datetime, scale: int) -> datetime.datetime:
    micros = (instant - _PG_EPOCH) // _MICRO
    half = scale // 2
    step = (micros + half) // scale * scale if micros >= 0 else -((-micros + half) // scale * scale)
    return _PG_EPOCH + step * _MICRO


def to_precision(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """timestamptz(instant, precision): the instant rounded to 0 to 6 digits of a second, half away
    from 2000-01-01 as PostgreSQL rounds. Each instant is a literal (or a NULL), rounded here
    first, so one the rounding takes past the bound raises."""
    call = trino.called(node)
    if call is None or "timestamptz" not in call[0] or len(call[1]) != 2:
        return None
    value, digits = call[1]
    instants = _literal_instants(value)
    if instants is None:
        return None
    if current_zone.get() is not None and any(i is not None for i in instants):
        return None  # a literal with no offset is read in the session's zone
    pattern = "timestamptz($a, $n)"
    if _is_null(digits):
        return replacement(_NULL_INSTANT, dialect, pattern)
    written = digits.unnest()
    written = written.this.unnest() if isinstance(written, exp.Cast) else written
    if not (isinstance(written, exp.Literal) and written.is_int and 0 <= int(written.this) <= 6):
        return None  # a negative precision is PostgreSQL's error; past 6 its warning
    scale = 10 ** (6 - int(written.this))
    for instant in instants:
        if instant is not None and not (_inside(instant) and _inside(_rounded(instant, scale))):
            return raised("timestamptz", dialect, pattern)
    if scale == 1:
        return graft(replacement("$a", dialect, pattern), {"a": value})
    micros = f"(toUnixTimestamp64Micro($a) - {_PG_EPOCH_MICROS})"
    half = scale // 2
    form = (
        f"fromUnixTimestamp64Micro(if({micros} >= 0, intDiv({micros} + {half}, {scale}) * {scale}, "
        f"-(intDiv(-{micros} + {half}, {scale}) * {scale})) + {_PG_EPOCH_MICROS}, 'UTC')"
    )
    return graft(replacement(form, dialect, pattern), {"a": value})


_LITERALS_STATED = (
    "Held as DateTime64(6, 'UTC'), exact from 1900-01-01 to 2299-12-31, of an instant written as "
    "a literal (or a VALUES list of them): its answer is read here first, and one outside "
    f"{BOUND} raises 0A000 ({{}}() of a value outside {BOUND} has no exact form on clickhouse). "
    "An instant ClickHouse computes, a literal not in ISO form and infinity are refused by name. "
    "On a session off UTC a literal is refused by name: one with no offset is read in the "
    "session's zone."
)
_AT_ZONE_STATED = (
    _LITERALS_STATED.format("timezone")
    + " The zone is a region's IANA name or UTC, as a literal; an abbreviation, a POSIX offset and "
    "any other text are refused by name."
)
_PRECISION_STATED = (
    _LITERALS_STATED.format("timestamptz")
    + " The precision is a literal of 0 to 6; any other is refused by name."
)


def _literal_feature(
    feature: str, pattern: str, rewrite: Callable[..., exp.Expr | None], summary: str, stated: str
) -> Feature:
    forms = {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, _CLICKHOUSE: Rewrite(rewrite)}
    return Feature(
        feature,
        FAMILY,
        summary,
        read=Side(rules=(Rule(pattern, forms),), limitations={_CLICKHOUSE: (Limitation(stated),)}),
    )


FEATURES += (
    _parts("date_part", "a double precision"),
    _parts("extract", "a numeric"),
    _call_feature(
        "overlaps(timestamptz)", "overlaps", ("timestamptz",) * 4, _OVERLAPS,
        "Whether two periods of instants overlap.",
        _HELD.format("overlaps") + " A NULL end of a period is read as PostgreSQL reads it; a "
        "literal outside the bound raises beside a NULL too.",
        zoned=False, strict=False, names=frozenset({"overlaps_timestamp"}),
    ),
    _one_feature(
        "timezone(timestamptz)", "timezone", "An instant's wall time in the session's zone.",
        "$a", zoned=True, pattern=True,
    ),
    _literal_feature(
        "timestamptz(timestamptz, integer)", "timestamptz($a, $n)", to_precision,
        "An instant rounded to a number of digits of a second.", _PRECISION_STATED,
    ),
)  # fmt: skip

#: zones' timezone(text, value) row's ClickHouse form, and its limitation there.
AT_ZONE = Rewrite(at_zone)
AT_ZONE_LIMITATION = Limitation(_AT_ZONE_STATED)
