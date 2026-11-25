# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e4c92-5a18-4f3d-9c26-e8d1a7f4b503
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's answers that differed from PostgreSQL's (REQ-2236; the function support measure,
run 43361055823, REQ-2171).

array_agg: ClickHouse's groupArray skips a NULL, and answers an empty array of no rows, where
PostgreSQL keeps each NULL and answers NULL of no rows. Its form aggregates each value in a tuple,
which groupArray keeps whatever it holds, and answers NULL where the aggregate counted no row. Of an
order (array_agg(x ORDER BY y)) it is refused by name: ClickHouse takes no ORDER BY in a call.

A NULL array: ClickHouse has no NULL of an array (an Array is never Nullable), so a call PostgreSQL
answers NULL for its NULL array argument is that NULL, read before any rule's pattern: a function
STRICT at its argument count, or one that tests its array for NULL first. A set-returning function
answers no rows of a NULL argument, not one NULL: it is left to its own row (generate_subscripts'
refuses it on ClickHouse), but unnest's, which is ClickHouse's arrayJoin of an empty array of the
type. max and min of a NULL array answer NULL too: each skips a NULL. A call sqlglot reads as a
typed node (array_to_string as ArrayToString) is read by the names that node answers to.

date_trunc: ClickHouse's dateTrunc answers a DateTime (a Date for a month or a year), which reaches
Arrow as its count of seconds and holds no day before 1970. Its start is a DateTime64(6) of the
value's zone: :func:`truncated`, the one builder every writer of it calls (this row, for a
statement's call; provisa.transpiler.semantic_dialect, for sqlglot's; zoned_instants, for an
instant's start in the session's zone).

A text cast to a date: ClickHouse reads a day its month does not have as a day of the next month
('2024-02-30' as 2024-03-01). A text the statement writes is read here: an ISO date (YYYY-MM-DD)
is that day, written for toDate32; a day that does not exist is PostgreSQL's error (22008), and
NULL for a cast that answers NULL on failure (pg_input_is_valid's check, accurateCastOrNull by the
time the grid reads it); any other text, and a year Date32 does not hold, is refused by name.

Literals of a time, an integer and a numeric are read from their text here too, as PostgreSQL's
input reads them (J1366's found-not-fixed 1 to 3). A time: ClickHouse's toTime64 reads '12:30' as
00:12:30, so the text is written as time_out writes it (12:30:00), a field out of range raising
22008, any text not of hours, minutes, seconds and a fraction refused by name. An integer:
ClickHouse's cast wraps a number past the type (2147483648::integer as -2147483648), truncates a
fraction (2.5 as 2) and answers NULL of a text past it; a number is rounded half away from zero, a
text read by the integer input (0x, 0o and 0b prefixes, underscores), and past the type PostgreSQL's
22003, a text the input does not read 22P02. A numeric: ClickHouse reads a number with a fraction as
a Float64 (1.005 as 1.00499..., 0.1 at scale 30 as 0.1000000000000000090...) and truncates a text
past the scale; the literal is cast from its text, rounded half away from zero to the scale,
22003 past the precision, a value of no precision said held up to 38 digits (0A000 past them).

A date and a number of days by their operator (date + integer, integer + date, date - integer):
date_pli's, integer_pl_date's or date_mii's ClickHouse form (polyfill.dates.ch_moved), of operands
whose types the statement says; the engine's own wrapped Date32 at int4's extremes, and the row
then failed to decode.

lag, lead, first_value, last_value and nth_value: ClickHouse answers the type's default (0, '')
where PostgreSQL answers NULL, past the partition or of an empty frame, and its first_value and
last_value skip a NULL. lag and lead are lagInFrame and leadInFrame over the whole partition (as
PostgreSQL's ignore the frame), each of the value as Nullable; first_value and last_value respect
NULLs; nth_value reads the value as Nullable. What a rule writes is not walked again, so a value
that holds a call is refused by name: each is a literal, a column or a cast of one.

current_schemas: ClickHouse has its own currentSchemas, which answers its database's name, where
PostgreSQL answers the search path, and NULL of a NULL. No form names PostgreSQL's search path on
ClickHouse: refused by name there, as on Trino."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import calendar
import decimal
import functools
import math
import re
import struct

import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire import function_types
from provisa.transpiler.polyfill import clocks, dates, instants, trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.server import plain
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "ClickHouse answers"

_ARRAY_AGG = "array_agg($a)"
_KEEPS_NULLS = "if(count() = 0, NULL, arrayMap(_t -> _t.1, groupArray(tuple($a))))"


def _aggregated(node: exp.Expr) -> exp.Expr | None:
    """The value a plain array_agg call aggregates; None for any other node, and for a call of an
    order, of DISTINCT, under a FILTER or over a window, whose form is not this row's."""
    if isinstance(node, exp.ArrayAgg):
        value = node.this
    elif isinstance(node, exp.Anonymous) and node.name.lower() == "array_agg":
        value = node.expressions[0] if len(node.expressions) == 1 else None
    else:
        return None
    if isinstance(value, exp.Order):
        raise Refused(
            Refusal(
                "0A000",
                "array_agg() of an order has no form on clickhouse: its aggregate takes none",
            )
        )
    if value is None or isinstance(value, exp.Distinct):
        return None
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if isinstance(outer, (exp.Filter, exp.Window)):
        return None
    return value


#: An array's shape as ClickHouse writes it, its brackets and commas: its quoted texts dropped first,
#: then everything else. Two arrays of one type have one shape exactly where PostgreSQL's would have
#: the same dimensions.
_SHAPE = (
    "replaceRegexpAll(replaceRegexpAll(toString({}), '''(?:[^''\\\\\\\\]|\\\\\\\\.)*''', ''), "
    "'[^\\\\[\\\\],]', '')"
)
_NO_ROWS = ch_fail("0A000", "array_agg of no rows does not read this value")
_EMPTY = ch_fail("2202E", "cannot accumulate empty arrays")
_DIMENSIONS = ch_fail("2202E", "cannot accumulate arrays of different dimensionality")
#: array_agg of arrays, PostgreSQL 17's accumArrayResultArr: each array in order, the first empty
#: one, or the first whose dimensions are not the first array's, raising as it raises. Its NULL of
#: no rows is a NULL of an array, which ClickHouse 24.3 holds none of (no Nullable(Array), and NULL
#: has no common type with an array): that edge raises by name.
_OF_ARRAYS = (
    "arrayMap(_g -> arrayMap(_f -> if("
    + " + ".join(
        (
            _NO_ROWS.format(condition="length(_g) = 0"),
            _EMPTY.format(condition="_f = 1"),
            _DIMENSIONS.format(condition="_f = 2"),
        )
    )
    + " = 0, _g, []), "
    "[arrayFilter(_c -> _c > 0, arrayMap(_x -> if(length(_x) = 0, 1, "
    f"if({_SHAPE.format('_x')} <> {_SHAPE.format('_g[1]')}, 2, 0)), _g))[1]])[1], "
    "[groupArray($a)])[1]"
)


def _first_row(column: exp.Column) -> exp.Expr | None:
    """The value of a VALUES list's first row that ``column`` names: the row PostgreSQL resolves
    the column's type from."""
    select = column.find_ancestor(exp.Select)
    source = select.args.get("from_") if select is not None else None
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values) or select is None or select.args.get("joins"):
        return None
    alias = values.args.get("alias")
    names = [c.name for c in alias.columns] if alias is not None else []
    if column.name not in names:
        return None
    return values.expressions[0].expressions[names.index(column.name)]


def _an_array(value: exp.Expr) -> bool:
    """Whether the statement says ``value`` is an array: an ARRAY, a cast to an array type, or a
    column of a VALUES list whose first row is one."""
    node = value.unnest()
    if isinstance(node, exp.Column):
        node = (_first_row(node) or node).unnest()
    if isinstance(node, exp.Array):
        return True
    return isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.ARRAY)


def kept_nulls(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """array_agg of every value, a NULL kept, and NULL of no rows; of arrays, their array."""
    value = _aggregated(node)
    if value is None:
        return None
    form = _OF_ARRAYS if _an_array(value) else _KEEPS_NULLS
    return graft(replacement(form, dialect, _ARRAY_AGG), {"a": value})


ARRAY_AGG = Feature(
    "array_agg",
    FAMILY,
    "The values of an aggregate as an array, a NULL kept; NULL of no rows.",
    read=Side(
        rules=(Rule(_ARRAY_AGG, {"clickhouse": Rewrite(kept_nulls)}, unlisted_keep=True),),
        limitations={
            "clickhouse": (
                Limitation(
                    "Of arrays, no rows have no answer: ClickHouse holds no NULL of an array, "
                    "so the call raises.",
                    Refusal("0A000", "array_agg of no rows does not read this value"),
                ),
            )
        },
    ),
)

#: The functions not STRICT in pg_proc that answer NULL of a NULL array all the same: each tests
#: its array argument for NULL first (PostgreSQL 17's array_userfuncs.c and arrayfuncs.c).
_NULL_OF_A_NULL_ARRAY = frozenset(
    {"array_position", "array_positions", "array_remove", "array_replace", "array_to_string"}
)
#: The functions not STRICT in pg_proc that answer NULL where every argument is a NULL array:
#: array_cat answers the other argument where one is not (array_userfuncs.c, and its grid row).
_NULL_OF_NULL_ARRAYS = frozenset({"array_cat"})
#: The aggregates not STRICT in pg_proc whose answer of a NULL array is NULL: each skips a NULL,
#: and answers NULL of no value.
_OF_NO_VALUE = frozenset({"max", "min"})


@functools.cache
def _arities() -> tuple[frozenset[tuple[str, int]], frozenset[tuple[str, int]]]:
    """Each (name, argument count) whose every overload pg_proc lists as STRICT, and each of which
    an overload returns a set (proretset)."""
    from provisa.pgwire.catalog_pg17_procs import PROCS

    strict: dict[tuple[str, int], bool] = {}
    sets: set[tuple[str, int]] = set()
    for row in PROCS:
        key = (row[1], row[15])
        strict[key] = strict.get(key, True) and bool(row[11])
        if row[12]:
            sets.add(key)
    return frozenset(key for key, every in strict.items() if every), frozenset(sets)


def _is_null_array(value: exp.Expr) -> bool:
    """Whether an argument is a NULL cast to an array type."""
    node = value.unnest()
    return (
        isinstance(node, exp.Cast)
        and node.to.is_type(exp.DataType.Type.ARRAY)
        and isinstance(node.this.unnest(), exp.Null)
    )


def _called(target: exp.Expr, dialect: str) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of ``target``, a call: as the translator's table reads it, or as
    the grid's rule it matches binds them (array_length is sqlglot's ArraySize)."""
    from provisa.transpiler.function_macros import call_of
    from provisa.transpiler.polyfill import GRID

    call = call_of(target)
    if call is not None:
        return call
    typed = trino.called(target) if isinstance(target, exp.Func) else None
    if typed is not None:  # a typed node sqlglot reads the name as (ArrayToString)
        names, args = typed
        strict, sets = _arities()
        known = _NULL_OF_A_NULL_ARRAY | _NULL_OF_NULL_ARRAYS | _OF_NO_VALUE
        for name in sorted(names):
            if name in known or (name, len(args)) in strict or (name, len(args)) in sets:
                return name, args
    try:
        applied = GRID.read(target, dialect)
    except Refused:
        return None  # a rewrite's refusal: the translator raises it as the row's
    if applied is None or applied.rule.pattern is None:
        return None
    return applied.feature.name, [applied.bindings[p] for p in applied.rule.holes()]


def null_of_a_null_array(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call PostgreSQL answers NULL for its NULL array argument, as that NULL, named for the
    call; a set-returning call is left to its own row."""
    call = _called(node, dialect)
    if call is None:
        return None
    name, args = call
    strict, sets = _arities()
    if name == "unnest" and len(args) == 1 and _is_null_array(args[0]):
        # No rows: ClickHouse's arrayJoin of an empty array of the type.
        held = args[0].unnest()
        assert isinstance(held, exp.Cast)  # _is_null_array
        empty = exp.Cast(this=exp.Array(expressions=[]), to=held.to.copy())
        joined = exp.Anonymous(this="arrayJoin", expressions=[empty])
        return exp.alias_(joined, name)
    if (name, len(args)) in sets:
        return None
    if name in _NULL_OF_A_NULL_ARRAY:
        nulled = bool(args) and _is_null_array(args[0])
    elif name in _NULL_OF_NULL_ARRAYS:
        nulled = bool(args) and all(_is_null_array(a) for a in args)
    elif name in _OF_NO_VALUE:
        nulled = len(args) == 1 and _is_null_array(args[0])
    else:
        nulled = (name, len(args)) in strict and any(_is_null_array(a) for a in args)
    return exp.alias_(exp.null(), name) if nulled else None


NULL_ARRAY = Feature(
    "a NULL array",
    FAMILY,
    "A call PostgreSQL answers NULL for its NULL array argument, as that NULL.",
    read=Side(
        # A NULL array is written by its type: the statement holds an array type's brackets.
        rules=(
            Rule(
                None,
                {"clickhouse": Rewrite(null_of_a_null_array, first=True, mentions=r"\[\]")},
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A set-returning call of a NULL array is its own row's: generate_subscripts "
                    "and pg_options_to_table are refused by name."
                ),
            )
        },
    ),
)

#: percentile_cont and percentile_disc keep the old table's forms (function_macros._EXACT): the
#: sorted keys, and the first past each fraction or the line between the two either side of it.
PERCENTILE = Feature(
    "percentile_cont and percentile_disc",
    FAMILY,
    "The percentile of a group's keys at a fraction, or at each of an array of them.",
    read=Side(
        rules=tuple(
            Rule(f"{name}($f) WITHIN GROUP (ORDER BY $k)", {"clickhouse": trino.KEPT}, True)
            for name in ("percentile_cont", "percentile_disc")
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "Of an array of fractions, no keys (no rows, or only NULL ones) have no "
                    "answer: ClickHouse holds no NULL of an array, so the call raises.",
                    Refusal(
                        "0A000",
                        "percentile of an array of fractions over no keys does not read this value",
                    ),
                ),
            )
        },
    ),
)


#: The units shorter than a minute, by the microseconds in one: dateTrunc keeps no fraction.
SUBMINUTE = {
    "second": 1_000_000,
    "millisecond": 1_000,
    "milliseconds": 1_000,
    "microsecond": 1,
    "microseconds": 1,
}


def truncated(unit: str, value: exp.Expr, zone: str = "UTC") -> exp.Expr:
    """The start of ``unit`` of ``value`` as DateTime64(6, ``zone``): dateTrunc's, or for a unit
    shorter than a minute the microseconds since the epoch floored to it; a NULL of it for no unit
    (a NULL part)."""
    if not unit:
        to = exp.DataType.build(f"Nullable(DateTime64(6, '{zone}'))", dialect="clickhouse")
        return exp.Cast(this=exp.null(), to=to)
    step = SUBMINUTE.get(unit.lower())
    if step is not None:
        micros = exp.Anonymous(
            this="toUnixTimestamp64Micro",
            expressions=[
                exp.Anonymous(this="toDateTime64", expressions=[value, exp.Literal.number(6)])
            ],
        )
        modulo = exp.Anonymous(
            this="positiveModulo", expressions=[micros.copy(), exp.Literal.number(step)]
        )
        floored = exp.Sub(this=micros, expression=modulo)
        return exp.Anonymous(
            this="fromUnixTimestamp64Micro", expressions=[floored, exp.Literal.string(zone)]
        )
    start = exp.Anonymous(this="dateTrunc", expressions=[exp.Literal.string(unit.lower()), value])
    return exp.Anonymous(
        this="toDateTime64", expressions=[start, exp.Literal.number(6), exp.Literal.string(zone)]
    )


def _unit_said(unit: exp.Expr | None) -> str | None:
    """A unit's text as the statement writes it ('' for NULL); None for one it does not write."""
    node = unit.unnest() if unit is not None else None
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Null):
        return ""
    if isinstance(node, exp.Var) or (isinstance(node, exp.Literal) and node.is_string):
        return node.name
    return None


#: What a statement says of an instant.
_INSTANTS = frozenset({"timestamptz", "timestamp with time zone"})


def started(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """date_trunc of a timestamp: its start as DateTime64(6, 'UTC'), whatever the unit. Of a
    timestamp with time zone, on a session on UTC, the same (polyfill.instants: held as
    DateTime64(6, 'UTC') in its bound). An interval is not this row's: it keeps its refusal by
    name (provisa.transpiler.engine_refusals)."""
    if not isinstance(node, exp.TimestampTrunc):
        return None
    said = trino.said(node.this)
    if said == "interval":
        return None
    if said in _INSTANTS:
        if node.args.get("zone") is not None:
            return instants.started_at_zone(node, dialect)  # of literals, or refused by name
        answers, raised = instants.zoned_instant(
            node.this, "date_trunc", dialect, "date_trunc($u, $t)"
        )
        if not answers:
            return None
        if raised is not None:
            return raised
    elif node.args.get("zone") is not None:
        return None  # a timestamp PostgreSQL reads as an instant to truncate at the zone
    unit = _unit_said(node.args.get("unit"))
    return None if unit is None else truncated(unit, node.this)


DATE_TRUNC = Feature(
    "date_trunc",
    FAMILY,
    "The start of a unit of a timestamp.",
    read=Side(
        rules=(
            Rule("date_trunc($u, $t)", {"clickhouse": Rewrite(started)}, unlisted_keep=True),
            Rule("date_trunc($u, $t, $z)", {"clickhouse": Rewrite(started)}, unlisted_keep=True),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "Of a timestamp with time zone: held as DateTime64(6, 'UTC'), exact from "
                    "1900-01-01 to 2299-12-31; a literal outside "
                    f"{instants.BOUND} raises 0A000 ({instants.out_of_bound('date_trunc')}). On "
                    "a session off UTC it is refused by name (date_trunc() has no form on "
                    "clickhouse): the start of a unit is the session zone's. Of an interval, "
                    "refused by name."
                ),
                Limitation("At a zone the call names: " + instants.AT_ZONE_TRUNC_STATED),
            )
        },
    ),
)

_ISO_DATE = re.compile(r"\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*", re.ASCII)
#: The years ClickHouse's Date32 holds.
_DATE32_YEARS = range(1900, 2300)


def _text_of(value: exp.Expr) -> str | None:
    """The text a statement writes, through casts of it to text; None for any other value."""
    node = value.unnest()
    while isinstance(node, exp.Cast) and node.to.is_type(*exp.DataType.TEXT_TYPES):
        node = node.this.unnest()
    return node.this if isinstance(node, exp.Literal) and node.is_string else None


def _date_cast(node: exp.Expr) -> tuple[exp.Expr, bool] | None:
    """The text a node casts to a date, and whether it answers NULL on failure; None for any other
    node."""
    if isinstance(node, (exp.Cast, exp.TryCast)) and node.to.is_type(exp.DataType.Type.DATE):
        return node.this, isinstance(node, exp.TryCast)
    if isinstance(node, exp.Anonymous) and node.name == "accurateCastOrNull":
        value, to = (node.expressions + [None, None])[:2]
        if isinstance(to, exp.Literal) and to.name.upper() == "DATE" and value is not None:
            return value, True
    return None


def dated(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text the statement writes cast to a date: its day, PostgreSQL's error of one that does
    not exist, or a refusal by name of a text not in ISO form."""
    cast = _date_cast(node)
    text = None if cast is None else _text_of(cast[0])
    if cast is None or text is None:
        return None
    iso = _ISO_DATE.fullmatch(text)
    if iso is None or int(iso[1]) not in _DATE32_YEARS:
        raise Refused(
            Refusal(
                "0A000",
                f'date_in() of "{text}" has no form on {dialect}: only an ISO date '
                "(YYYY-MM-DD) of 1900 to 2299 is read",
            )
        )
    year, month, day = (int(part) for part in iso.groups())
    if not (1 <= month <= 12 and 1 <= day <= calendar.monthrange(year, month)[1]):
        if cast[1]:
            return exp.Cast(this=exp.null(), to=exp.DataType.build("DATE"))
        raise Refused(Refusal("22008", f'date/time field value out of range: "{text}"'))
    day_text = f"{year:04d}-{month:02d}-{day:02d}"
    return exp.Anonymous(this="toDate32", expressions=[exp.Literal.string(day_text)])


DATE = Feature(
    "date",
    FAMILY,
    "A text cast to a date.",
    read=Side(
        rules=(
            Rule(
                None,
                {"clickhouse": Rewrite(dated, mentions=r"::\s*date\b|\bas\s+date\b|\bdate\s+'")},
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A text the statement writes: an ISO date of 1900 to 2299, any other refused "
                    "by name. A text column's cast keeps ClickHouse's own read."
                ),
            )
        },
    ),
)

#: A time of day's text as PostgreSQL's time input reads it (DecodeTimeOnly): hours, minutes,
#: seconds and a fraction of any digits. Any other text (AM or PM, a zone, a date, 'allballs') is
#: refused by name.
_CLOCK_TEXT = re.compile(r"\s*(\d+):(\d{1,2})(?::(\d{1,2})(?:(\.\d+))?)?\s*", re.ASCII)
_SECOND_US = 1_000_000
_DAY_US = 86_400 * _SECOND_US


def _plain_cast(node: exp.Expr, *types: exp.DType) -> exp.Cast | None:
    """``node``, a cast to one of ``types`` that raises on failure; None for any other node (a cast
    that answers NULL on failure, pg_input_is_valid's check, is ClickHouse's accurateCastOrNull,
    which already answers NULL past the type)."""
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and node.to.is_type(*types):
        return node
    return None


def clock_micros(text: str) -> int | None:
    """The microseconds of the day of a time's text, as time_in reads it; None for a text not of
    hours, minutes, seconds and a fraction. A field out of range is PostgreSQL's 22008."""
    clock = _CLOCK_TEXT.fullmatch(text)
    if clock is None:
        return None
    hour, minute, second = int(clock[1]), int(clock[2]), int(clock[3] or 0)
    # ParseFractionalSecond: rint(strtod(fraction) * 1000000), as a double rounds to even.
    fraction = round(float(clock[4]) * _SECOND_US) if clock[4] else 0
    micros = ((hour * 60 + minute) * 60 + second) * _SECOND_US + fraction
    if minute > 59 or second > 60 or hour > 24 or micros > _DAY_US:
        raise Refused(Refusal("22008", f'date/time field value out of range: "{text}"'))
    return micros


def timed(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text the statement writes cast to a time: its time of day as time_out writes it, which
    ClickHouse reads exactly; PostgreSQL's error of a field out of range, or a refusal by name of
    a text not of hours, minutes, seconds and a fraction."""
    cast = _plain_cast(node, exp.DataType.Type.TIME)
    text = None if cast is None else _text_of(cast.this)
    if cast is None or text is None:
        return None
    micros = clock_micros(text)
    if micros is None:
        raise Refused(
            Refusal(
                "0A000",
                f'time_in() of "{text}" has no form on {dialect}: only a time of hours, minutes, '
                "seconds and a fraction is read",
            )
        )
    written = clocks.text_of(micros)
    if written == text:
        return None  # ClickHouse reads it as it is (provisa.transpiler.transpile)
    return exp.Cast(this=exp.Literal.string(written), to=cast.to.copy())


TIME = Feature(
    "time",
    FAMILY,
    "A text cast to a time of day.",
    read=Side(
        rules=(
            Rule(
                None,
                {"clickhouse": Rewrite(timed, mentions=r"(?:::|\bas)\s*time\b|\btime\s+'")},
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A text the statement writes: hours, minutes, seconds and a fraction, each "
                    "field read as PostgreSQL reads it (a seventh fractional digit rounded, 24:00:00 "
                    "the last, a field out of range 22008); any other text (AM or PM, a zone, a "
                    "date) is refused by name. ClickHouse's server has no Time64: a time is held as "
                    "an Int64 of its microseconds of the day (0 to 86400000000), compared as "
                    "integers, and the select list, or a cast to text, shows it as time_out's text. "
                    "24:00:00 is held and compared; pgwire reads no time of 24:00:00 back, so a "
                    "column that shows it raises. A value whose type the statement does not say "
                    "(a table's column) is read from its text at run time: a date before the clock "
                    "is read past, up to six fractional digits; any other text raises 22007, a "
                    "field past its range 22008, each without the value in its message. A "
                    "timestamp is read as its wall time's time of day."
                ),
            )
        },
    ),
)

#: Each integer type: PostgreSQL's name of it and its bits.
_INTEGERS = {
    exp.DataType.Type.SMALLINT: ("smallint", 16),
    exp.DataType.Type.INT: ("integer", 32),
    exp.DataType.Type.BIGINT: ("bigint", 64),
}
#: An integer's text as PostgreSQL 16's input reads it (pg_strtoint32 and its kin): a sign, then
#: decimal digits or a 0x, 0o or 0b prefix and its digits, an underscore only between digits (or
#: after a prefix), white space around.
_INTEGER_TEXT = re.compile(
    r"\s*([+-]?)(?:0[xX]((?:_?[0-9a-fA-F])+)|0[oO]((?:_?[0-7])+)|0[bB]((?:_?[01])+)|(\d(?:_?\d)*))\s*",
    re.ASCII,
)
_BASES = (16, 8, 2, 10)


def _integer_of_text(text: str) -> int | None:
    """The integer of a text as PostgreSQL's integer input reads it; None for a text it refuses."""
    read = _INTEGER_TEXT.fullmatch(text)
    if read is None:
        return None
    digits, base = next((d, b) for d, b in zip(read.groups()[1:], _BASES) if d is not None)
    value = int(digits.replace("_", ""), base)
    return -value if read[1] == "-" else value


def _signed_number(node: exp.Expr) -> tuple[exp.Literal, bool] | None:
    """A number literal written with its signs or in parentheses, and whether it is negated."""
    negative = False
    while isinstance(node, (exp.Paren, exp.Neg)):
        negative ^= isinstance(node, exp.Neg)
        node = node.this
    if isinstance(node, exp.Literal) and not node.is_string:
        return node, negative
    return None


def _number(node: exp.Expr) -> decimal.Decimal | None:
    """The exact value of a number literal (as PostgreSQL reads it: a numeric, never a double)."""
    signed = _signed_number(node)
    if signed is None:
        return None
    try:
        value = decimal.Decimal(signed[0].this)
    except decimal.InvalidOperation:
        return None
    return -value if signed[1] else value


def _as_number(value: int | decimal.Decimal) -> exp.Expr:
    """A number literal of ``value``, its sign a negation as PostgreSQL's SQL writes one."""
    literal = exp.Literal.number(str(abs(value)))
    return exp.Neg(this=literal) if value < 0 else literal


def integer_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A literal cast to an integer, read as PostgreSQL reads it: a number rounded half away from
    zero, a text by the integer input; PostgreSQL's 22003 past the type and 22P02 of a text it does
    not read, where ClickHouse's cast wraps, truncates or answers NULL."""
    cast = _plain_cast(node, *_INTEGERS)
    if cast is None:
        return None
    name, bits = _INTEGERS[cast.to.this]
    lowest, highest = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    text = _text_of(cast.this)
    if text is not None:
        value = _integer_of_text(text)
        if value is None:
            raise Refused(Refusal("22P02", f'invalid input syntax for type {name}: "{text}"'))
        if not lowest <= value <= highest:
            raise Refused(Refusal("22003", f'value "{text}" is out of range for type {name}'))
        return exp.Cast(this=_as_number(value), to=cast.to.copy())
    number = _number(cast.this)
    if number is None or not number.is_finite():
        return None
    rounded = int(number.to_integral_value(rounding=decimal.ROUND_HALF_UP))
    if not lowest <= rounded <= highest:
        raise Refused(Refusal("22003", f"{name} out of range"))
    if rounded == number:
        return None  # an integer ClickHouse casts as it is
    return exp.Cast(this=_as_number(rounded), to=cast.to.copy())


INTEGER = Feature(
    "integer",
    FAMILY,
    "A literal cast to smallint, integer or bigint.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        integer_read,
                        mentions=r"[\d'.]\s*\)?\s*(?:::|\bas)\s*\"?(?:smallint|integer|bigint|"
                        r"int[248]?)\b|\b(?:smallint|integer|bigint|int[248])\s+'",
                    )
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A number or a text the statement writes is read as PostgreSQL reads it: a "
                    "number rounded half away from zero, a text by the integer input (a 0x, 0o or "
                    "0b prefix, underscores); past the type it raises 22003, a text the input does "
                    "not read 22P02. A column's cast keeps ClickHouse's own, which wraps past the "
                    "type."
                ),
            )
        },
    ),
)

#: A real's text as float4in reads it: a decimal number with an exponent, white space around.
_REAL_TEXT = re.compile(r"\s*[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\s*", re.ASCII)
#: Half a unit past a real's largest value (2^128 - 2^104): strtof reads a value of this magnitude
#: or more as infinity, which float4in raises as out of range.
_REAL_PAST = decimal.Decimal((1 << 128) - (1 << 103))
_REAL_LARGEST = struct.unpack("<f", struct.pack("<I", 0x7F7FFFFF))[0]
#: Enough digits that a real's distance from a literal's digits is exact (a real has up to 149
#: digits after the point, a literal is kept to a few hundred).
_EXACT = decimal.Context(prec=1000)
#: The integers a real holds exactly, which ClickHouse's cast reads as they are.
_REAL_EXACT_INTEGERS = 1 << 24


def _real_bits(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


def _real_of_bits(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def _real_steps(value: float) -> list[float]:
    """``value``, a real, and the reals either side of it (none past the largest)."""
    if value == 0:
        return [value, _real_of_bits(1), -_real_of_bits(1)]
    bits = _real_bits(value)
    steps = [value, _real_of_bits(bits - 1)]
    if abs(value) < _REAL_LARGEST:
        steps.append(_real_of_bits(bits + 1))
    return steps


def nearest_real(value: decimal.Decimal) -> float | None:
    """The real nearest ``value``, a tie to the even one, as strtof reads its digits; None past the
    type. A double's nearest value rounded to a real may be a tie the digits are not (rounding
    twice), so the reals either side of it are compared with the digits too."""
    if abs(value) >= _REAL_PAST:
        return None
    near = min(max(float(value), -_REAL_LARGEST), _REAL_LARGEST)
    guess = struct.unpack("<f", struct.pack("<f", near))[0]
    with decimal.localcontext(_EXACT):
        return min(
            _real_steps(guess),
            key=lambda real: (abs(decimal.Decimal(real) - value), _real_bits(real) & 1),
        )


def real_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A literal cast to a real, read as float4in reads its digits: ClickHouse's cast of a text to
    a Float32 is no nearest value ('3.4028235e+38' read as 3.4028233e+38), and of a number goes by
    way of a double. The real nearest the digits is written as its exact double, cast to a
    Float32, which ClickHouse reads exactly. Past the type, and a value that is no zero read as
    zero, PostgreSQL's 22003. NaN and infinity ClickHouse reads as they are."""
    cast = _plain_cast(node, exp.DataType.Type.FLOAT)
    if cast is None:
        return None
    text = _text_of(cast.this)
    if text is not None:
        if not _REAL_TEXT.fullmatch(text):
            return None  # NaN, infinity, or a text the input refuses: ClickHouse's own
        value = decimal.Decimal(text.strip())
    else:
        number = _number(cast.this)
        if number is None:
            return None
        value = number
    if value == value.to_integral_value() and abs(value) <= _REAL_EXACT_INTEGERS:
        return None  # an integer a real holds, which ClickHouse casts as it is
    real = nearest_real(value)
    if real is None or (real == 0 and value != 0):
        said = text if text is not None else format(value, "f")
        raise Refused(Refusal("22003", f'"{said.strip()}" is out of range for type real'))
    return exp.Cast(this=_as_number(decimal.Decimal(repr(real))), to=cast.to.copy())


REAL = Feature(
    "real",
    FAMILY,
    "A literal cast to a real.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        real_read,
                        mentions=r"[\d'.]\s*\)?\s*(?:::|\bas)\s*\"?(?:real|float4)\b"
                        r"|\b(?:real|float4)\s+'",
                    )
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A number or a text the statement writes is read as float4in reads it: the "
                    "real nearest its digits, a tie to the even one; past the type, or a value "
                    "that is no zero read as zero, it raises 22003. A column's cast keeps "
                    "ClickHouse's own, which reads a text to a Float32 less exactly."
                ),
            )
        },
    ),
)

#: The integers a double holds exactly, which ClickHouse's cast reads as they are.
_DOUBLE_EXACT_INTEGERS = 1 << 53


def double_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A literal cast to a double, read as float8in reads its digits (strtod's nearest double, a
    tie to the even one): ClickHouse's cast of a text to a Float64 is no nearest value without
    precise_float_parsing ('4.029840426088479e+17' read as 4.0298404260884794e+17), where its
    number literal is. The nearest double is written as its shortest digits, a number literal.
    Past the type, and a value that is no zero read as zero, PostgreSQL's 22003. NaN, infinity
    and a zero (its sign kept) ClickHouse reads as they are."""
    cast = _plain_cast(node, exp.DataType.Type.DOUBLE)
    if cast is None:
        return None
    text = _text_of(cast.this)
    if text is not None:
        if not _REAL_TEXT.fullmatch(text):
            return None  # NaN, infinity, or a text the input refuses: ClickHouse's own
        value = decimal.Decimal(text.strip())
    else:
        number = _number(cast.this)
        if number is None:
            return None
        value = number
    if value == value.to_integral_value() and abs(value) <= _DOUBLE_EXACT_INTEGERS:
        return None  # an integer a double holds (a zero too), which ClickHouse casts as it is
    double = float(value)  # the nearest double, as Python's decimal-to-float conversion rounds
    if math.isinf(double) or double == 0:
        said = text if text is not None else format(value, "f")
        raise Refused(
            Refusal("22003", f'"{said.strip()}" is out of range for type double precision')
        )
    return exp.Cast(this=_as_number(decimal.Decimal(repr(double))), to=cast.to.copy())


DOUBLE = Feature(
    "double precision",
    FAMILY,
    "A literal cast to a double.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        double_read,
                        mentions=r"[\d'.]\s*\)?\s*(?:::|\bas)\s*\"?(?:double\s+precision|float8)\b"
                        r"|\b(?:double\s+precision|float8)\s+'",
                    )
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A number or a text the statement writes is read as float8in reads it: the "
                    "double nearest its digits, a tie to the even one; past the type, or a value "
                    "that is no zero read as zero, it raises 22003. A column's cast keeps "
                    "ClickHouse's own, which reads a text to a Float64 less exactly."
                ),
            )
        },
    ),
)

#: An oid's text as PostgreSQL 16's oidin reads it (uint32in_subr): white space, a sign, decimal
#: digits, white space.
_OID_TEXT = re.compile(r"\s*([+-]?\d+)\s*", re.ASCII)


def oid_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to an oid, read as oidin reads it: from -2147483648 to 4294967295, a negative
    value its 32 bits unsigned; PostgreSQL's 22003 past them and 22P02 of a text it does not read,
    where ClickHouse's cast of the text to the bigint an oid is held as answers any number."""
    if not (
        isinstance(node, exp.Cast)
        and not isinstance(node, exp.TryCast)
        and isinstance(node.to, exp.ObjectIdentifier)
        and node.to.name.lower() == "oid"
    ):
        return None
    text = _text_of(node.this)
    if text is None:
        return None
    read = _OID_TEXT.fullmatch(text)
    if read is None:
        raise Refused(Refusal("22P02", f'invalid input syntax for type oid: "{text}"'))
    value = int(read[1])
    if not -(1 << 31) <= value < (1 << 32):
        raise Refused(Refusal("22003", f'value "{text}" is out of range for type oid'))
    return exp.Cast(this=exp.Literal.number(value % (1 << 32)), to=node.to.copy())


OID_LITERAL = Feature(
    "an oid literal",
    FAMILY,
    "A text cast to an oid.",
    read=Side(
        rules=(Rule(None, {"clickhouse": Rewrite(oid_read, mentions=r"(?:::|\bas)\s*\"?oid\b")}),),
        limitations={
            "clickhouse": (
                Limitation(
                    "A text the statement writes is read as PostgreSQL reads it: past -2147483648 "
                    "to 4294967295 it raises 22003, a text the input does not read 22P02, and a "
                    "negative value is its 32 bits unsigned. A column's cast keeps ClickHouse's "
                    "own."
                ),
            )
        },
    ),
)


#: A tid held as its text, '(block,offset)', as the number that orders it (block, then offset).
_TID_KEY = (
    "(toUInt64OrZero(splitByChar(',', replaceRegexpAll(ifNull($a, ''), '[() ]', ''))[1]) * 65536 + "
    "toUInt64OrZero(splitByChar(',', replaceRegexpAll(ifNull($a, ''), '[() ]', ''))[2]))"
)


def _tid_extreme(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """max or min of tids, held as their text, in PostgreSQL's order (block, then offset); NULL of
    no value. Over a window or under a FILTER it is no row's."""
    if not isinstance(node, (exp.Max, exp.Min)) or isinstance(
        node.parent, (exp.Window, exp.Filter)
    ):
        return None
    value = node.this
    if trino.said(value) != "tid":
        return None
    kind = "argMaxIf" if isinstance(node, exp.Max) else "argMinIf"
    form = f"if(count($a) = 0, NULL, {kind}($a, {_TID_KEY}, $a IS NOT NULL))"
    return graft(replacement(form, dialect, "f($a)"), {"a": value})


TID_EXTREMES = Feature(
    "max and min of tids",
    FAMILY,
    "The last or first tid of a group, by block and then offset.",
    read=Side(
        rules=(
            Rule(None, {"clickhouse": Rewrite(_tid_extreme, mentions=r"::\s*tid\b", first=True)}),
        ),
    ),
)


#: The digits of a decimal ClickHouse holds where a literal says none (numeric with no precision).
_MOST_DIGITS = 38
_NUMERIC_OUT_OF_BOUND = (
    f"numeric_in() of a value of more than {_MOST_DIGITS} digits has no exact form on clickhouse"
)


def _numeric_of_text(text: str) -> decimal.Decimal | None:
    """The value of a text as PostgreSQL's numeric input reads it (an integer of a 0x, 0o or 0b
    prefix too); None for a text it refuses."""
    if re.fullmatch(r"\s*[+-]?0[xXoObB].*", text, re.ASCII):
        integer = _integer_of_text(text)
        return None if integer is None else decimal.Decimal(integer)
    if not re.fullmatch(
        r"\s*[+-]?(?:\d(?:_?\d)*(?:\.(?:\d(?:_?\d)*)?)?|\.\d(?:_?\d)*)"
        r"(?:[eE][+-]?\d+)?\s*|\s*[+-]?(?:nan|inf|infinity)\s*",
        text,
        re.ASCII | re.IGNORECASE,
    ):
        return None
    return decimal.Decimal(text.strip().replace("_", ""))


#: The refusal of a numeric NaN or infinity, which ClickHouse's Decimal does not hold.
_NUMERIC_NOT_FINITE = "numeric_in() of NaN or infinity has no form on clickhouse"

#: A numeric's text ClickHouse's cast reads exactly where its digits fit the scale.
_EXACT_TEXT = re.compile(r"[+-]?\d+(?:\.\d+)?", re.ASCII)


def _precision(to: exp.DataType) -> tuple[int, int] | None:
    """The precision and scale a cast to numeric says; None where it says none."""
    if not to.expressions:
        return None
    said = [int(param.this.this) for param in to.expressions]
    return said[0], said[1] if len(said) > 1 else 0


def numeric_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A literal cast to a numeric, cast from its text rounded as PostgreSQL rounds it: ClickHouse
    reads a number with a fraction as a Float64 (1.005 as 1.00499...), and truncates a text past
    the scale. Past the precision, PostgreSQL's 22003; a text the input does not read, 22P02."""
    cast = _plain_cast(node, exp.DataType.Type.DECIMAL)
    if cast is None:
        return None
    text = _text_of(cast.this)
    if text is not None:
        value = _numeric_of_text(text)
        if value is None:
            raise Refused(Refusal("22P02", f'invalid input syntax for type numeric: "{text}"'))
    else:
        value = _number(cast.this)
    if value is None:
        return None
    if not value.is_finite():
        # ClickHouse's Decimal holds no NaN or infinity: its cast answers NULL for either.
        raise Refused(Refusal("0A000", _NUMERIC_NOT_FINITE))
    said = _precision(cast.to)
    to = cast.to.copy()
    if said is None:
        _, digits, exponent = value.as_tuple()
        scale = max(0, -int(exponent))
        if max(len(digits), scale) > _MOST_DIGITS:
            raise Refused(Refusal("0A000", _NUMERIC_OUT_OF_BOUND))
        to = exp.DataType.build(
            f"DECIMAL({min(_MOST_DIGITS, max(len(digits), scale) + 18)}, {scale})"
        )
    else:
        precision, scale = said
        if not 0 <= scale <= precision:
            return None
        overflow = Refused(Refusal("22003", "numeric field overflow"))
        if value and value.adjusted() > precision - scale:
            raise overflow
        quantum = decimal.Decimal(1).scaleb(-scale)
        value = value.quantize(
            quantum, rounding=decimal.ROUND_HALF_UP, context=decimal.Context(prec=precision + 2)
        )
        if value and value.adjusted() >= precision - scale:
            raise overflow
    if said is not None and text is not None and _EXACT_TEXT.fullmatch(text):
        if decimal.Decimal(text) == value:
            return None  # a text of the scale's digits or fewer, which ClickHouse reads exactly
    return exp.Cast(this=exp.Literal.string(format(value, "f")), to=to)


NUMERIC = Feature(
    "numeric input",
    FAMILY,
    "A literal cast to a numeric.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        numeric_read,
                        mentions=r"[\d'.]\s*\)?\s*(?:::|\bas)\s*\"?(?:numeric|decimal)\b"
                        r"|\b(?:numeric|decimal)\s+'",
                    )
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A number or a text the statement writes is cast from its text, rounded half "
                    "away from zero to the scale, as PostgreSQL rounds it; past the precision it "
                    "raises 22003, a text the input does not read 22P02. A value of no precision "
                    f"said holds up to {_MOST_DIGITS} digits; one of more raises 0A000 "
                    f"({_NUMERIC_OUT_OF_BOUND}). NaN and infinity raise 0A000 ({_NUMERIC_NOT_FINITE}): "
                    "ClickHouse's Decimal holds neither. A column's cast keeps ClickHouse's own."
                ),
            )
        },
    ),
)


def _not_finite_numeric(value: exp.Expr) -> bool:
    """Whether ``value`` is a literal NaN or infinity cast to a numeric."""
    cast = _plain_cast(value.unnest(), exp.DataType.Type.DECIMAL)
    text = None if cast is None else _text_of(cast.this)
    if text is None:
        return False
    read = _numeric_of_text(text)
    return read is not None and not read.is_finite()


def _of_a_not_finite_numeric(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """scale, min_scale, hash_numeric and hash_numeric_extended of a literal NaN or infinity, as
    numeric.c answers them (NULL; NULL; 0; the seed): the literal reader refuses the value itself,
    which ClickHouse's Decimal does not hold."""
    call = trino.called(node)
    if call is None or not call[1] or not _not_finite_numeric(call[1][0]):
        return None
    names, args = call
    if names & {"scale", "min_scale"} and len(args) == 1:
        return exp.Null()
    if "hash_numeric" in names and len(args) == 1:
        return exp.Literal.number(0)
    if "hash_numeric_extended" in names and len(args) == 2:  # noqa: PLR2004 -- value and seed
        return exp.Cast(this=args[1].copy(), to=exp.DataType.build("BIGINT"))
    return None


NOT_FINITE_NUMERIC = Feature(
    "numeric NaN's scale and hash",
    FAMILY,
    "The scale and the hash of a numeric NaN or infinity, which ClickHouse does not hold.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        _of_a_not_finite_numeric,
                        mentions=r"\b(?:scale|min_scale|hash_numeric|hash_numeric_extended)\s*\(",
                        # Before the literal's reader refuses the value it is read of.
                        first=True,
                    )
                },
            ),
        )
    ),
)

#: What a date's or a day count's operand may hold: a value the walk has nothing in to write.
_PLAIN = (
    exp.Cast,
    exp.Column,
    exp.Identifier,
    exp.Literal,
    exp.Null,
    exp.Neg,
    exp.Paren,
    exp.DataType,
    exp.DataTypeParam,
)
_DAY_COUNTS = frozenset({"integer", "smallint"})
#: Each operator of a date and a day count: the row it is, by its operands' kinds.
_DAY_OPERATORS: dict[type[exp.Expr], dict[str, str]] = {
    exp.Add: {"dn": "date_pli", "nd": "integer_pl_date"},
    exp.Sub: {"dn": "date_mii"},
}


def _day_kind(operand: exp.Expr) -> str | None:
    said = function_types.argument_type(operand.unnest())
    return "d" if said == "date" else "n" if said in _DAY_COUNTS else None


def _day_operand(operand: exp.Expr, read, dialect: str) -> exp.Expr | None:
    """An operand of a date and a day count, its literal read; None for one holding a call."""
    written = read(operand.unnest(), dialect)
    if written is not None:
        return written
    if not all(isinstance(n, _PLAIN) for n in operand.walk()):
        return None
    number = _number(operand)
    if number is not None and not -(1 << 31) <= number < 1 << 31:
        return None  # a bigint or a numeric: PostgreSQL has no operator of a date and one
    return operand


def days_moved(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A date plus or less a number of days, by its operator: date_pli, date_mii or
    integer_pl_date, of operands whose types the statement says."""
    rows = _DAY_OPERATORS.get(type(node))
    if rows is None:
        return None
    left, right = node.this, node.expression
    name = rows.get(f"{_day_kind(left)}{_day_kind(right)}")
    if name is None:
        return None
    date, count = (right, left) if name == "integer_pl_date" else (left, right)
    date = _day_operand(date, dated, dialect)
    count = _day_operand(count, integer_read, dialect)
    if date is None or count is None:
        return None
    sign = "-" if name == "date_mii" else "+"
    form = dates.ch_moved(name, sign)
    return graft(replacement(form, dialect, f"{name}($a, $b)"), {"a": date, "b": count})


DATE_DAYS = Feature(
    "date + integer",
    FAMILY,
    "A date plus or less a number of days, by its operator (date_pli, date_mii, integer_pl_date).",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        days_moved, mentions=r"::\s*date\b|\bas\s+date\b|\bdate\s+'"
                    )
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    dates.ch_moved_stated("date_pli")
                    + " Of operands whose types the statement says (a literal, a cast); a date "
                    "column's keeps ClickHouse's own arithmetic, exact within Date32's range."
                ),
            )
        },
    ),
)

STAMP = Feature(
    "timestamp",
    FAMILY,
    "A text cast to a timestamp, or to a timestamp with time zone.",
    read=Side(
        limitations={
            "clickhouse": (
                Limitation(
                    "Held as DateTime64(6, 'UTC'). A text the statement writes is read where the "
                    "statement is transpiled (provisa.transpiler.transpile). Of a timestamp, an "
                    "infinity, a year BC or past 9999, and a value outside "
                    f"{instants.STAMP_BOUND} raise 0A000 "
                    f"({instants.out_of_bound('timestamp_in', instants.STAMP_BOUND)}); of a "
                    "timestamptz, an infinity, a year BC or past 9999, and a value outside "
                    f"{instants.BOUND} (its offset read) raise 0A000 "
                    f"({instants.out_of_bound('timestamptz_in')}). ClickHouse would hold each at "
                    "its type's end. A timestamp's offset is dropped, as PostgreSQL's "
                    "input drops it (ClickHouse's cast reads it). A text in another form, and a "
                    "text column's cast, keep ClickHouse's own read."
                ),
            )
        },
    ),
)

_IN_FRAME = {"lag": "lagInFrame", "lead": "leadInFrame"}
_RESPECTING = ("first_value", "last_value")
WINDOW_VALUES = (*_IN_FRAME, *_RESPECTING, "nth_value")


def _window_call(window: exp.Window) -> tuple[str, list[exp.Expr]] | None:
    """The value function a window computes and its arguments; None for any other, and for one
    that ignores NULLs."""
    call = window.this
    if isinstance(call, exp.RespectNulls):
        call = call.this
    if isinstance(call, exp.Dot):
        call = call.expression
    named = trino.called(call)
    if named is None:
        return None
    names = named[0] & set(WINDOW_VALUES)
    return (next(iter(names)), named[1]) if len(names) == 1 else None


def _is_null(value: exp.Expr) -> bool:
    """Whether a value is NULL, or a cast of NULL."""
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return isinstance(node, exp.Null)


def _a_count(value: exp.Expr) -> bool:
    """Whether a window's offset is a bare unsigned literal, which ClickHouse takes as written."""
    node = value.unnest()
    return isinstance(node, exp.Literal) and not node.is_string and node.this.isdigit()


#: What each window function's offset below its least raises: PostgreSQL's error, or a refusal
#: of a backwards lag or lead, which no form here reads.
_BELOW = {
    "nth_value": ("22016", "argument of nth_value must be greater than zero", 1),
    "lag": ("0A000", "lag does not read this value", 0),
    "lead": ("0A000", "lead does not read this value", 0),
}


def _counted(offset: exp.Expr, name: str) -> exp.Expr:
    """``offset`` as ClickHouse's unsigned count: its value where it is not NULL, and a raise of
    one below the least the function reads."""
    sqlstate, message, least = _BELOW[name]
    below = exp.LT(this=offset.copy(), expression=exp.Literal.number(least))
    thrown = exp.Anonymous(
        this="throwIf",
        expressions=[
            exp.Anonymous(this="ifNull", expressions=[below, exp.Literal.number(0)]),
            exp.Literal.string(f"postgresql-error {sqlstate}: {message}<end of postgresql-error>"),
        ],
    )
    count = exp.Add(
        this=exp.Anonymous(this="assumeNotNull", expressions=[offset.copy()]), expression=thrown
    )
    return exp.Anonymous(this="toUInt64", expressions=[count])


def _null_of(value: exp.Expr) -> exp.Expr:
    """A NULL of ``value``'s type: nullIf of the value and itself."""
    return exp.Anonymous(this="nullIf", expressions=[_nullable(value), _nullable(value)])


def _nullable(value: exp.Expr) -> exp.Expr:
    return exp.Anonymous(this="toNullable", expressions=[value.copy()])


_WHOLE_PARTITION = exp.WindowSpec(
    kind="ROWS", start="UNBOUNDED", start_side="PRECEDING", end="UNBOUNDED", end_side="FOLLOWING"
)


def framed(function: str) -> Rewrite:
    """The rewrite of ``function`` over a window, NULL where ClickHouse answers its default."""

    def frame(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if not isinstance(node, exp.Window):
            return None
        call = _window_call(node)
        if call is None or call[0] != function:
            return None
        name, args = call
        if len(args) > 1 and _is_null(args[1]):
            # PostgreSQL's offset or row number of NULL answers NULL (WinGetFuncArgCurrent's
            # isnull), which ClickHouse takes as no integer: a NULL of the value's type.
            return _null_of(args[0])
        if not args or not all(plain(arg) for arg in args):
            raise Refused(
                Refusal(
                    "0A000",
                    f"{name}() of a value holding a call has no form on {dialect}: "
                    "only a literal, a column or a cast of one",
                )
            )
        window = node.copy()
        value, rest = _nullable(args[0]), [arg.copy() for arg in args[1:]]
        offset = args[1] if len(args) > 1 else None
        if offset is not None and not _a_count(offset):
            # ClickHouse takes an offset only as an integer no NULL: a row's NULL answers NULL,
            # and one PostgreSQL refuses (or reads backwards) is raised.
            rest[0] = _counted(offset, name)
        if name in _IN_FRAME:
            window.set("this", exp.Anonymous(this=_IN_FRAME[name], expressions=[value, *rest]))
            window.set("spec", _WHOLE_PARTITION.copy())
        elif name in _RESPECTING:
            kind = exp.FirstValue if name == "first_value" else exp.LastValue
            window.set("this", exp.RespectNulls(this=kind(this=value)))
        else:
            window.set("this", exp.Anonymous(this="nth_value", expressions=[value, *rest]))
        if offset is None or _a_count(offset):
            return window
        return exp.If(
            this=exp.Is(this=offset.copy(), expression=exp.Null()),
            true=exp.Null(),
            false=window,
        )

    frame.__doc__ = f"{function} over a window: NULL where ClickHouse answers its type's default."
    frame.__qualname__ = frame.__name__ = f"framed_{function}"
    return Rewrite(frame, mentions=rf"\b{function}\b")


HOLDS_A_CALL = Limitation(
    "Of a value holding a call, refused by name: what a rule writes is not walked again."
)
WINDOW_FEATURES = tuple(
    Feature(
        name,
        FAMILY,
        f"The value of {'a row of' if name not in _IN_FRAME else 'another row of'} a window.",
        read=Side(
            rules=(Rule(None, {"clickhouse": framed(name)}),),
            limitations={"clickhouse": (HOLDS_A_CALL,)},
        ),
    )
    for name in (*_RESPECTING, "nth_value")
)


def _constant_count(count: exp.Expr) -> int | None:
    """The integer a count of buckets the statement writes is, through casts; None otherwise."""
    node = count.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    negated = isinstance(node, exp.Neg)
    node = node.this.unnest() if negated else node
    if isinstance(node, exp.Literal) and not node.is_string and node.this.isdigit():
        return -int(node.this) if negated else int(node.this)
    return None


def bucketed(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """ntile over a window, as PostgreSQL's integer: every engine's is a bigint. On ClickHouse a
    NULL count of buckets answers NULL, as PostgreSQL's does; a count below 1 is PostgreSQL's
    error; any other constant count is ClickHouse's unsigned one over the whole partition (as
    PostgreSQL's ignores the frame); a count that is no constant is refused by name, as ClickHouse
    takes none."""
    if not (isinstance(node, exp.Window) and isinstance(node.this, exp.Ntile)):
        return None
    count = node.this.this
    if dialect != "clickhouse" or count is None:
        return exp.cast(node, "INT")
    if _is_null(count):
        return exp.cast(exp.null(), "INT")
    buckets = _constant_count(count)
    if buckets is None:
        raise Refused(
            Refusal(
                "0A000",
                "ntile() of a count that is no constant has no form on clickhouse: "
                "its ntile takes a constant",
            )
        )
    if buckets < 1:
        raise Refused(Refusal("22014", "argument of ntile must be greater than zero"))
    window = node.copy()
    window.this.set("this", exp.Literal.number(buckets))
    window.set("spec", _WHOLE_PARTITION.copy())
    if window.args.get("order") is None:  # ClickHouse's ntile reads a frame of an order only
        unordered = exp.Ordered(this=exp.Anonymous(this="tuple", expressions=[]))
        window.set("order", exp.Order(expressions=[unordered]))
    return exp.cast(window, "INT")


NTILE = Feature(
    "ntile",
    FAMILY,
    "The bucket of a row in a window, as an integer.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    dialect: Rewrite(bucketed, mentions=r"\bntile\b")
                    for dialect in DIALECTS
                    if dialect not in NATIVE
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A count of buckets that is no constant is refused by name: ClickHouse's "
                    "ntile takes a constant."
                ),
            )
        },
    ),
)


def _scale_of(value: str) -> str:
    """The scale of ``value``'s Decimal type (0 for any other): ClickHouse holds a numeric's scale
    in its type, and writes its text without trailing zeros."""
    return f"toUInt8OrZero(extract(toTypeName({value}), 'Decimal\\\\(\\\\d+, (\\\\d+)\\\\)'))"


#: numeric_mod: the remainder at 38 places in a Decimal256 (an operand's 38 digits fit on either
#: side of the point), which keeps it exact, written at the larger scale of
#: its operands (toDecimalString writes a scale of 0 with its point, trimmed); by nullIf of the
#: divisor, as ClickHouse folds a constant remainder by 0 whichever branch is chosen (the old table's form cut the text, which ClickHouse writes without trailing zeros:
#: 1000 % 1000 answered '').
_NUMERIC_MOD = (
    "CASE WHEN $a IS NULL OR $b IS NULL THEN CAST(NULL AS Nullable(String)) "
    "WHEN $b = 0 THEN CAST(throwIf(1, 'postgresql-error 22012: division by zero"
    "<end of postgresql-error>') AS Nullable(String)) "
    "ELSE trimRight(toDecimalString(CAST($a AS Nullable(Decimal(76, 38))) % "
    "CAST(nullIf($b, 0) AS Nullable(Decimal(76, 38))), "
    f"greatest({_scale_of('$a')}, {_scale_of('$b')})), '.') END"
)

NUMERIC_MOD = Feature(
    "numeric_mod",
    FAMILY,
    "The remainder of one numeric divided by another, at the larger scale of the two.",
    read=Side(
        rules=(
            Rule(
                "numeric_mod($a, $b)",
                # Every other dialect keeps the old table's form: a rule of no form there would
                # leave the call to the engine, which has no numeric_mod (run 43424649612).
                {**trino.trino_only(trino.KEPT), "clickhouse": _NUMERIC_MOD},
                unlisted_keep=True,
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "The scale is each operand's type's: a column holds one scale for all its "
                    "values, where PostgreSQL's numeric holds each value's own."
                ),
            )
        },
    ),
)

_SEARCH_PATH = (
    "Refused by name: ClickHouse's own currentSchemas answers its database, not PostgreSQL's "
    "search path."
)
#: Refused wherever a statement names it: unqualified, ClickHouse would answer its own.
_NO_SEARCH_PATH = Rewrite(
    trino.refusing(
        "current_schemas",
        "current_schemas: refused by name, as no form names the search path.",
        lambda *_: True,
    ).function,
    mentions=r"\bcurrent_schemas\b",
)
CURRENT_SCHEMAS = Feature(
    "current_schemas",
    FAMILY,
    "The schemas of the search path.",
    read=Side(
        rules=(Rule(None, dict.fromkeys((*trino.TRINO, "clickhouse"), _NO_SEARCH_PATH)),),
        limitations={
            **dict.fromkeys(
                trino.TRINO, (Limitation("Refused by name: Trino has no form of it."),)
            ),
            "clickhouse": (Limitation(_SEARCH_PATH),),
        },
    ),
)

_SPG_POLY = "spg_poly_quad_compress($v)"


SPG_POLY_QUAD_COMPRESS = Feature(
    "spg_poly_quad_compress",
    FAMILY,
    "A polygon's bounding box, as the SP-GiST quad tree's compress function answers it.",
    read=Side(
        rules=(
            Rule(_SPG_POLY, dict.fromkeys((*trino.OTHERS, *trino.TRINO, "clickhouse"), trino.KEPT)),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "make_bound_box's box as poly_box answers it (pg_forms_geometric.bounding_box, "
                    "as on DuckDB and Trino): float8out's text of each extreme, NaN and infinities "
                    "as float8 orders them."
                ),
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 260
FEATURES: tuple[Feature, ...] = (
    NULL_ARRAY,
    ARRAY_AGG,
    PERCENTILE,
    DATE_TRUNC,
    DATE,
    TIME,
    INTEGER,
    REAL,
    DOUBLE,
    OID_LITERAL,
    TID_EXTREMES,
    NUMERIC,
    NOT_FINITE_NUMERIC,
    DATE_DAYS,
    STAMP,
    *WINDOW_FEATURES,
    NTILE,
    NUMERIC_MOD,
    CURRENT_SCHEMAS,
    SPG_POLY_QUAD_COMPRESS,
)


#: The type an untyped NULL column PostgreSQL types by no call or cast is sent as: ClickHouse
#: types a bare NULL Nullable(Nothing), which the server's Arrow output has no type for (24.3: "The
#: type 'Nothing' of a column is not supported for conversion into Arrow data format"). A NULL of
#: String reaches the wire as text, PostgreSQL's type of an unknown result column.
_UNTYPED_NULL = "Nullable(String)"

#: The type an untyped NULL column PostgreSQL types is sent as (REQ-2236): one Arrow writes as a
#: binary, as embedded ClickHouse writes a Nothing, which pgwire reads as a column the engine
#: could not type (server._UNTYPED_RESULTS) and describes by PostgreSQL's type of the column
#: (cast_types.result_casts). No ClickHouse type of PostgreSQL's would do in its place: an Array
#: is never Nullable, and 24.3 has no Variant.
_DESCRIBED_NULL = "Nullable(FixedString(1))"


def _untyped_null(value: exp.Expr) -> bool:
    """Whether ClickHouse types ``value`` Nullable(Nothing): a NULL, or a comparison one operand
    of which is a NULL (in_range of NULLs is NULL = 1)."""
    node = value.unnest()
    if isinstance(node, exp.Null):
        return True
    return isinstance(node, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)) and any(
        isinstance(side.unnest(), exp.Null) for side in (node.this, node.expression)
    )


def _described(pg_sql: str, count: int) -> tuple[bool, ...]:
    """For each of a ``count``-column result of ``pg_sql``, whether pgwire describes it by
    PostgreSQL's type of its expression: where cast_types.result_casts types it, read as pgwire
    reads it (server._describe_casts), which describes no column of a statement whose columns it
    does not count alike."""
    from provisa.pgwire.cast_types import result_casts

    casts = result_casts(pg_sql)
    if casts is None or len(casts) != count:
        return (False,) * count
    return tuple(cast is not None for cast in casts)


def typed_null_columns(written: str, pg_sql: str) -> str:
    """``written``, ``pg_sql`` in ClickHouse's SQL, with each column of its select list that
    ClickHouse types Nullable(Nothing) a NULL the server's Arrow output types: the server sends its
    result as Arrow (REQ-986), and 24.3's Arrow output has no type for Nothing. A column pgwire
    describes by PostgreSQL's type is a NULL of ``_DESCRIBED_NULL``, any other a NULL of
    ``_UNTYPED_NULL``. Read on the statement as written, for the generator folds a CASE of NULLs
    to NULL. Only a plain SELECT's list: a set operation's branch takes its type from the other
    branches, which a typed NULL would not share. Unchanged where no column is such a NULL."""
    if not _MENTIONS_NULL.search(written):
        return written
    tree = sqlglot.parse_one(written, read="clickhouse")
    if not isinstance(tree, exp.Select):
        return written
    nulls = [
        (c.unalias(), described)
        for c, described in zip(tree.selects, _described(pg_sql, len(tree.selects)))
        if _untyped_null(c.unalias())
    ]
    if not nulls:
        return written
    for value, described in nulls:
        typed = exp.DataType.build(
            _DESCRIBED_NULL if described else _UNTYPED_NULL, dialect="clickhouse"
        )
        value.replace(exp.cast(exp.null(), typed))
    return tree.sql(dialect="clickhouse")


_MENTIONS_NULL = re.compile(r"\bNULL\b", re.IGNORECASE)
