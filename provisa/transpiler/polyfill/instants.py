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
from provisa.transpiler.engine_refusals import PLAIN_PARTS, plain_format

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


def out_of_bound(function: str, bound: str = BOUND) -> str:
    """The message of a value outside the bound, which the form raises with SQLSTATE 0A000;
    ``bound`` the text of another type's (a range's bound of a timestamp or a date)."""
    return f"{function}() of a value outside {bound} has no exact form on clickhouse"


#: A timestamp's input text as a cast reads it here: a date (its year of any digits), a time, an
#: offset (read of a timestamptz only: a timestamp's input drops it) and an era.
_INPUT = re.compile(
    r"\s*(\d{4,})-(\d{1,2})-(\d{1,2})"
    r"(?:(?:\s+|T)(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?"
    r"\s*(?:(Z|UTC|GMT)|([+-])(\d{1,2})(?::?(\d{2}))?(?::?(\d{2}))?)?"
    r"\s*(BC|AD)?\s*",
    re.IGNORECASE | re.ASCII,
)
_INFINITIES = frozenset({"infinity", "+infinity", "-infinity"})


#: PostgreSQL's input function of each timestamp type a cast names, and whether it is zoned.
_INPUT_FUNCTIONS = {
    exp.DataType.Type.TIMESTAMP: ("timestamp_in", False),
    exp.DataType.Type.TIMESTAMPTZ: ("timestamptz_in", True),
}
#: A timestamp's bound as DateTime64(6, 'UTC') holds it: a wall time, of the same instants.
STAMP_BOUND = "1900-01-01 00:00:00 .. 2299-12-31 23:59:59.999999"


def wall_inputs(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each string literal cast to a timestamp without its offset, as PostgreSQL's
    input drops it: ClickHouse's cast reads it. Read before an instant's literal is written as its
    UTC wall time (transpile._instants_as_utc)."""
    for cast in tree.find_all(exp.Cast):
        literal = cast.this
        if cast.to.this != exp.DataType.Type.TIMESTAMP or not (
            isinstance(literal, exp.Literal) and literal.is_string
        ):
            continue
        wall = _offset_dropped(literal.this)
        if wall is not None:
            literal.replace(exp.Literal.string(wall))
    return tree


#: The meta key of a literal a row of this module reads: one it raises for with its own name, or
#: one it reads only that it is there (isfinite), where ClickHouse's value of it answers as
#: PostgreSQL's does.
ROW_READ = "provisa_row_read"


def _read_by_row(*arguments: exp.Expr) -> None:
    """Each literal ``arguments`` write, or a VALUES list's column holds, marked :data:`ROW_READ`."""
    for argument in arguments:
        value = argument.unnest()
        if isinstance(value, exp.Cast):
            value.meta[ROW_READ] = True
            continue
        cast = values_cast(value) if isinstance(value, exp.Column) else None
        values = None if cast is None else cast.find_ancestor(exp.Values)
        if values is None:
            continue
        index = [c.name for c in values.args["alias"].columns].index(value.name)
        for row in values.expressions:
            written = row.expressions[index].unnest()
            if isinstance(written, exp.Cast):
                written.meta[ROW_READ] = True


def outside_cast(node: exp.Expr) -> exp.Expr | None:
    """PostgreSQL's 0A000 in place of a string literal cast to a timestamp or a timestamptz that
    DateTime64(6, 'UTC') does not hold, which ClickHouse's cast would hold at its type's end; None
    for any other node. Read last (transpile.clickhouse_semantics): a row of this module raises one
    its call reads with its own name, or answers it where it reads only that it is there."""
    named = _INPUT_FUNCTIONS.get(node.to.this) if isinstance(node, exp.Cast) else None
    if named is None or node.meta.get(ROW_READ):
        return None
    literal = node.this
    if not (isinstance(literal, exp.Literal) and literal.is_string):
        return None
    function, zoned = named
    if not input_outside(literal.this, zoned):
        return None
    return raised(function, _CLICKHOUSE, returns=INSTANT, bound=BOUND if zoned else STAMP_BOUND)


#: The groups of _INPUT that write an offset: a zone of none (Z, UTC, GMT), or a sign and its parts.
_OFFSET_GROUPS = range(8, 13)


def _offset_dropped(text: str) -> str | None:
    """A timestamp's input text without its offset; None for one that writes none, or a text not
    read here."""
    written = _INPUT.fullmatch(text)
    if written is None:
        return None
    present = [g for g in _OFFSET_GROUPS if written.group(g) is not None]
    if not present:
        return None
    start, end = written.start(present[0]), written.end(present[-1])
    return (text[:start] + text[end:]).strip()


def input_outside(text: str, zoned: bool) -> bool | None:
    """Whether a timestamp's (``zoned``: a timestamptz's) input text is a value outside what
    DateTime64(6, 'UTC') holds, which ClickHouse's cast holds at its type's end: an infinity, a year
    BC or past 9999, or an instant outside the bound (a timestamptz's offset read, one with none
    as UTC, as the cast reads it). None for a text not read here, whose cast is ClickHouse's own."""
    if text.strip().lower() in _INFINITIES:
        return True
    written = _INPUT.fullmatch(text)
    if written is None:
        return None
    year, month, day, hour, minute, second, fraction, utc, sign, oh, om, os_, era = written.groups()
    if (era or "").upper() == "BC" or int(year) > 9999:  # noqa: PLR2004 -- datetime's last year
        return True
    micros = round(int((fraction or "0")[:7].ljust(7, "0")) / 10)
    try:
        wall = datetime.datetime(
            int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(second or 0)
        ) + datetime.timedelta(microseconds=micros)
    except ValueError:
        return None  # no day of the calendar: PostgreSQL's error of it is the engine's to give
    if zoned and sign is not None:
        offset = datetime.timedelta(hours=int(oh), minutes=int(om or 0), seconds=int(os_ or 0))
        wall -= offset if sign == "+" else -offset
    del utc  # Z, UTC and GMT are an offset of none
    return not FIRST <= wall <= LAST


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


#: ClickHouse's types of what a call answers, where a raise in its place is read as a value.
INSTANT = "Nullable(DateTime64(6, 'UTC'))"
DAY = "Nullable(Date32)"


def raised(
    function: str,
    dialect: str,
    pattern: str | None = None,
    returns: str | None = None,
    bound: str = BOUND,
) -> exp.Expr:
    """ClickHouse's raise of 0A000 for a value outside the bound, as the call's answer; ``pattern``
    the call's, where ``function()`` does not parse (an aggregate takes an argument). ``returns``:
    the type of the call's answer, so a call that reads it (date_trunc, toYear) types the raise as
    the value it stands for, and ClickHouse raises it rather than refuse the statement's types.
    ``bound``: the text of the bound the value is outside (a timestamp's, a wall time)."""
    form = ch_fail("0A000", out_of_bound(function, bound)).format(condition="1")
    if returns is not None:
        form = f"CAST({form} AS {returns})"
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
    returns: str | None = None,
) -> Callable[[exp.Expr, str], exp.Expr | None]:
    """The rewrite of a call of ``function`` of ``kinds``, its ``form`` of $a, $b, $c, $d.
    ``zoned``: its answer reads a value in the session's zone, so it answers on UTC only.
    ``bounded``: its answer reads the value, so a literal outside the bound raises (isfinite reads
    only that there is one). ``strict``: a NULL argument answers NULL, so a literal outside the
    bound beside one is not read; otherwise any literal outside raises. ``plain``: the row names no
    pattern, so it reads only arguments :func:`_plain` holds. ``returns``: ClickHouse's type of
    its answer, where it is no boolean (:func:`raised`)."""
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
            _read_by_row(*call[1])
            return raised(function, dialect, pattern, returns)
        if not bounded:
            _read_by_row(*call[1])
        return graft(replacement(form, dialect, pattern), dict(zip(holes, call[1])))

    return rewrite


def _comparison(function: str, kinds: tuple[str, str], body: str) -> Callable[..., exp.Expr | None]:
    """The rewrite of one comparison of ``kinds``, its form ``body`` of {a} and {b}."""
    a = _AS_INSTANT[kinds[0]].format("$a")
    b = _AS_INSTANT[kinds[1]].format("$b")
    mixed = kinds != ("timestamptz", "timestamptz")
    returns = INSTANT if body in (_LARGER, _SMALLER) else None
    compared = _rewrite(function, kinds, body.format(a=a, b=b), zoned=mixed, returns=returns)
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
            rules=(Rule(f"{function}($a, $b)", forms, unlisted_keep=True),),
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
    where the value is a literal outside the bound, an instant; ``pattern`` the call's."""
    read = _checked(argument, _OF["timestamptz"])
    if read is None or current_zone.get() is not None:
        return False, None
    if read is not _Read.OUTSIDE:
        return True, None
    _read_by_row(argument)
    return True, raised(function, dialect, pattern, INSTANT)


# --- one instant -------------------------------------------------------------------------------


#: What a function of one instant answers, where it is no boolean.
_RETURNS = {"max": INSTANT, "min": INSTANT, "date": DAY, "timestamp": INSTANT, "timezone": INSTANT}


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
    rewrite = _rewrite(
        function, ("timestamptz",), form, zoned=zoned, bounded=bounded, plain=plain,
        returns=_RETURNS.get(function),
    )  # fmt: skip
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
        rule = Rule(f"{function}($a)", forms, unlisted_keep=True)
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
        rule = Rule(f"{function}({holes})", forms, unlisted_keep=True)
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
            return raised("timezone", dialect, pattern, INSTANT)
        wall = instant.replace(tzinfo=datetime.timezone.utc).astimezone(at).replace(tzinfo=None)
        if not _inside(wall):
            return raised("timezone", dialect, pattern, INSTANT)
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
            return raised("timestamptz", dialect, pattern, INSTANT)
    if scale == 1:
        return graft(replacement("$a", dialect, pattern), {"a": value})
    micros = f"(toUnixTimestamp64Micro($a) - {_PG_EPOCH_MICROS})"
    half = scale // 2
    form = (
        f"fromUnixTimestamp64Micro(if({micros} >= 0, intDiv({micros} + {half}, {scale}) * {scale}, "
        f"-(intDiv(-{micros} + {half}, {scale}) * {scale})) + {_PG_EPOCH_MICROS}, 'UTC')"
    )
    return graft(replacement(form, dialect, pattern), {"a": value})


#: date_trunc's units of an instant, PostgreSQL's names (timestamptz_trunc_internal), and those
#: whose start is read in the zone's offset at that start (redotz): a day and longer.
_TRUNC_UNITS = frozenset(
    {"microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month",
     "quarter", "year", "decade", "century", "millennium"}
)  # fmt: skip
_REDONE = frozenset({"day", "week", "month", "quarter", "year", "decade", "century", "millennium"})


def _offset_of_wall(wall: datetime.datetime, zone: zoneinfo.ZoneInfo) -> datetime.timedelta:
    """The offset PostgreSQL reads a wall time at ``zone`` in (DetermineTimeZoneOffset): in a gap
    the offset before the transition, in an overlap the one after it."""
    before = wall.replace(tzinfo=zone, fold=0).utcoffset()
    after = wall.replace(tzinfo=zone, fold=1).utcoffset()
    assert before is not None and after is not None  # a ZoneInfo answers every wall time
    if before == after:
        return before
    overlap = before > after  # a fall-back: the later reading is after the transition
    return after if overlap else before


def _started_at_zone(
    instant: datetime.datetime, unit: str, zone: zoneinfo.ZoneInfo
) -> datetime.datetime:
    """date_trunc(unit, instant, zone) as PostgreSQL computes it, the instant a UTC wall time:
    its wall time at the zone truncated, then read back at the zone's offset (the instant's own,
    for a unit shorter than a day)."""
    utc = instant.replace(tzinfo=datetime.timezone.utc)
    local = utc.astimezone(zone)
    offset = local.utcoffset()
    assert offset is not None
    wall = local.replace(tzinfo=None)
    year = wall.year
    if unit == "millennium":
        wall = datetime.datetime((year + 999) // 1000 * 1000 - 999, 1, 1)
    elif unit == "century":
        wall = datetime.datetime((year + 99) // 100 * 100 - 99, 1, 1)
    elif unit == "decade":
        wall = datetime.datetime(year // 10 * 10, 1, 1)
    elif unit == "year":
        wall = datetime.datetime(year, 1, 1)
    elif unit == "quarter":
        wall = datetime.datetime(year, 3 * ((wall.month - 1) // 3) + 1, 1)
    elif unit == "month":
        wall = datetime.datetime(year, wall.month, 1)
    elif unit == "week":
        day = wall.date() - datetime.timedelta(days=wall.weekday())
        wall = datetime.datetime(day.year, day.month, day.day)
    elif unit == "day":
        wall = datetime.datetime(year, wall.month, wall.day)
    elif unit == "hour":
        wall = wall.replace(minute=0, second=0, microsecond=0)
    elif unit == "minute":
        wall = wall.replace(second=0, microsecond=0)
    elif unit == "second":
        wall = wall.replace(microsecond=0)
    elif unit == "milliseconds":
        wall = wall.replace(microsecond=wall.microsecond // 1000 * 1000)
    if unit in _REDONE:
        offset = _offset_of_wall(wall, zone)
    return wall - offset


def started_at_zone(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """date_trunc(unit, instant, zone) of a literal instant, unit and zone: the start computed as
    the statement is translated (the zone's rules are Python's tzdata), held as DateTime64(6,
    'UTC'); one inside the bound, else the raise. A NULL of any answers NULL. Any value a row
    computes keeps the refusal by name."""
    unit_node, zone_node = node.args.get("unit"), node.args.get("zone")
    if unit_node is None or zone_node is None:
        return None
    pattern = "date_trunc($u, $t)"
    instants = _literal_instants(node.this)
    if instants is None or len(instants) != 1:
        return None
    (instant,) = instants
    unit = _unit_text(unit_node)
    if unit is None:
        return None
    if _is_null(zone_node) or instant is None or unit == "":
        return replacement(_NULL_INSTANT, dialect, pattern)
    if current_zone.get() is not None:
        return None  # a literal with no offset is read in the session's zone
    name = _string_literal(zone_node)
    if name not in _ZONES or unit.lower() not in _TRUNC_UNITS:
        return None
    if not _inside(instant):
        return raised("date_trunc", dialect, pattern, INSTANT)
    start = _started_at_zone(instant, unit.lower(), zoneinfo.ZoneInfo(name))
    if not _inside(start):
        return raised("date_trunc", dialect, pattern, INSTANT)
    held = f"CAST('{start.isoformat(sep=' ', timespec='microseconds')}' AS {INSTANT})"
    return replacement(held, dialect, pattern)


def _unit_text(unit: exp.Expr) -> str | None:
    """A unit's text ('' for a NULL); None for one the statement does not write."""
    node = unit.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Null):
        return ""
    if isinstance(node, exp.Var) or (isinstance(node, exp.Literal) and node.is_string):
        return node.name
    return None


_AT_ZONE_TRUNC_STATED = (
    "Of a literal instant, unit and zone (a region's IANA name or UTC): the start is computed as "
    "the statement is translated, by PostgreSQL's rules (its wall time at the zone truncated, a "
    "day or longer read back at the zone's offset then, a gap's before and an overlap's after), "
    "with Python's tzdata; held as DateTime64(6, 'UTC'), exact from 1900-01-01 to 2299-12-31. "
    f"An instant or a start outside {BOUND} raises 0A000 ({out_of_bound('date_trunc')}). A NULL "
    "of any answers NULL. An instant, unit or zone a row computes, a VALUES list, an "
    "abbreviation or a POSIX zone is refused by name; so is any literal on a session off UTC."
)


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
        read=Side(
            rules=(Rule(pattern, forms, unlisted_keep=True),),
            limitations={_CLICKHOUSE: (Limitation(stated),)},
        ),
    )


#: ClickHouse's instant as pg_ranges writes it: a type of its own, which sqlglot holds by name.
_CLICKHOUSE_INSTANT = re.compile(r"(?:Nullable\()?DateTime64\(6, 'UTC'\)\)?")


def held_instant(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A cast of an instant to ClickHouse's own (DateTime64(6, 'UTC')), as pg_ranges writes a
    tstzrange's bounds: the instant as it is held, a literal outside the bound raised. On a
    session off UTC a literal keeps its refusal by name."""
    if not (
        isinstance(node, exp.Cast) and _CLICKHOUSE_INSTANT.fullmatch(node.to.sql("clickhouse"))
    ):
        return None
    if not _plain(node.this):
        return None
    read = _checked(node.this, _OF["timestamptz"])
    if read is None:
        return None
    if current_zone.get() is not None and read in (_Read.LITERAL, _Read.OUTSIDE):
        return None
    pattern = "f($a)"
    if read is _Read.OUTSIDE:
        _read_by_row(node.this)
        return raised("tstzrange", dialect, pattern, INSTANT)
    return graft(replacement(f"CAST($a AS {INSTANT})", dialect, pattern), {"a": node.this})


def to_char_of_an_instant(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """to_char(timestamptz, text): NULL of a NULL instant or a NULL template, as the STRICT
    function answers; ClickHouse's format of a template made only of YYYY, MM, DD, HH24, MI and SS
    and the characters ``-/: `` (engine_refusals.plain_format reads the template the statement
    wrote, which pg_syntax keeps), of an instant held as DateTime64(6, 'UTC') in its bound, on a
    session on UTC: PostgreSQL's text is the session zone's. A literal outside the bound raises.
    Any other call keeps its refusal by name."""
    if not isinstance(node, exp.TimeToStr):
        return None
    value, template = node.this, node.args.get("format")
    if template is None:
        return None
    read = _checked(value, _OF["timestamptz"])
    if read is None:
        return None
    pattern = "to_char($a, $b)"
    if read is _Read.NULL or _is_null(template):
        return replacement("CAST(NULL AS Nullable(String))", dialect, pattern)
    if current_zone.get() is not None or not plain_format(node, _CLICKHOUSE):
        return None
    if read is _Read.OUTSIDE:
        _read_by_row(value)
        return raised("to_char", dialect, pattern, "Nullable(String)")
    form = replacement(f"formatDateTime($a, {_quoted(template.this)})", dialect, pattern)
    return graft(form, {"a": value})


def _quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


_TO_CHAR_STATED = (
    "Of a NULL instant or a NULL template: NULL, as the strict function answers. Of a template "
    "made only of YYYY, MM, DD, HH24, MI and SS and the characters -/: (read as the statement "
    "writes it, not as sqlglot's strftime text of it, in which PostgreSQL's YYYY and a literal %Y "
    f"are the same), on a session on UTC: ClickHouse's format of it. {_ONE_HELD.format('to_char')}"
    " MI is refused by name there as well (ClickHouse's %M is a month's name). Any other template, "
    "and any call on a session off UTC (its text is the session zone's), is refused by name "
    "(to_char() has no form on clickhouse)."
)


_HELD_INSTANT_STATED = (
    _ONE_HELD.format("tstzrange")
    + " The cast is the one a tstzrange's form writes of its bounds (pg_ranges); a NULL, a column "
    "of a VALUES list and a literal are read, any other value is refused by name."
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
    Feature(
        "to_char(timestamptz, text)", FAMILY, "An instant's text in a template.",
        read=Side(
            rules=(
                Rule(
                    "to_char($a, $b)",
                    {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                     _CLICKHOUSE: Rewrite(to_char_of_an_instant)},
                    unlisted_keep=True,
                ),
            ),
            limitations={_CLICKHOUSE: (Limitation(_TO_CHAR_STATED),)},
        ),
    ),
    Feature(
        "cast(timestamptz as DateTime64)", FAMILY,
        "An instant as ClickHouse holds one, as a tstzrange's form reads its bounds.",
        read=Side(
            rules=(Rule(None, {_CLICKHOUSE: Rewrite(held_instant)}),),
            limitations={_CLICKHOUSE: (Limitation(_HELD_INSTANT_STATED),)},
        ),
    ),
)  # fmt: skip

#: clickhouse's date_trunc row's limitation of a call that names a zone.
AT_ZONE_TRUNC_STATED = _AT_ZONE_TRUNC_STATED

#: zones' timezone(text, value) row's ClickHouse form, and its limitation there.
AT_ZONE = Rewrite(at_zone)
AT_ZONE_LIMITATION = Limitation(_AT_ZONE_STATED)
