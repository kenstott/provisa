# Copyright (c) 2026 Kenneth Stott
# Canary: 2f4f06b8-9593-4f2b-a022-b7a461d45edb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's temporal values as openCypher defines them, lowered onto the engine's own types
(REQ-2173).

A value's kind is read off the statement (its constructor, the function that returns it, the
arithmetic that combines two, a variable a WITH or UNWIND carries): date, localtime, time,
localdatetime, datetime or duration. A duration is the engine's interval: months, days and
microseconds, as openCypher's duration is months, days, seconds and nanoseconds. It leaves the
engine as those three numbers (an engine's driver would hand back a timedelta, its months read as
30 days) and is written in the answer in ISO-8601, P..T.. (:func:`iso_duration`)."""

# Requirements: REQ-2173

from __future__ import annotations

import re
from dataclasses import dataclass
from fractions import Fraction
from typing import TYPE_CHECKING, Callable, Mapping

from sqlglot import exp

from provisa.gql.expr_ast import (
    Binary,
    CypherExpr,
    FunctionCall,
    ListLiteral,
    Literal,
    MapLiteral,
    Parameter,
    Paren,
    Property,
    Unary,
    Variable,
)
from provisa.gql import spatial
from provisa.gql.translator_types import CypherTranslateError

if TYPE_CHECKING:
    import datetime

DATE = "date"
LOCALTIME = "localtime"
TIME = "time"
LOCALDATETIME = "localdatetime"
DATETIME = "datetime"
DURATION = "duration"

#: The engine type each kind is held as.
SQL_TYPES = {
    DATE: "DATE",
    LOCALTIME: "TIME",
    TIME: "TIMETZ",
    LOCALDATETIME: "TIMESTAMP",
    DATETIME: "TIMESTAMPTZ",
    DURATION: "INTERVAL",
}
_DATED = {DATE, LOCALDATETIME, DATETIME}

#: The functions on durations this module lowers (lower-cased).
DURATION_FUNCTIONS = frozenset(
    {"duration", "duration.between", "duration.inmonths", "duration.indays", "duration.inseconds"}
)

# openCypher's average month and day, for a fractional month or day of a duration.
_DAYS_PER_MONTH = Fraction("30.436875")
_SECONDS_PER_DAY = 86400
_MICROS = 1_000_000
#: A zone written as an offset, +hh:mm[:ss] (_named_zone's form of one).
_OFFSET = re.compile(r"[+-]\d{2}:\d{2}(?::\d{2})?")


# --------------------------------------------------------------------------- kinds


def parameter_key(name: str) -> str:
    """A ``$name`` parameter's entry among the kinds and zones of variables (``$`` keeps it apart
    from a variable of that name)."""
    return f"${name}"


def kind_of(
    expr: CypherExpr,
    variables: Mapping[str, str],
    property_kind: Callable[[Property], str | None],
) -> str | None:
    """The temporal kind ``expr`` has, or None when it is no temporal value."""
    if isinstance(expr, Paren):
        return kind_of(expr.inner, variables, property_kind)
    if isinstance(expr, Variable):
        return variables.get(expr.name)
    if isinstance(expr, Parameter):
        return variables.get(parameter_key(expr.name))
    if isinstance(expr, Property):
        return property_kind(expr)
    if isinstance(expr, FunctionCall):
        name = expr.name.lower()
        if name in SQL_TYPES:
            return name
        if name == spatial.POINT:
            return spatial.POINT
        if name in DURATION_FUNCTIONS:
            return DURATION
        prefix = name.split(".", 1)[0]
        return prefix if "." in name and prefix in SQL_TYPES else None
    if isinstance(expr, Unary) and expr.op == "-":
        inner = kind_of(expr.operand, variables, property_kind)
        return inner if inner == DURATION else None
    if isinstance(expr, Binary) and expr.op in ("+", "-", "*", "/"):
        left = kind_of(expr.left, variables, property_kind)
        right = kind_of(expr.right, variables, property_kind)
        if expr.op in ("*", "/"):
            return DURATION if DURATION in (left, right) else None
        if left == DURATION and right == DURATION:
            return DURATION
        if right == DURATION and left in SQL_TYPES:
            return left
        if expr.op == "+" and left == DURATION and right in SQL_TYPES:
            return right
    return None


def element_kind(
    expr: CypherExpr,
    variables: Mapping[str, str],
    property_kind: Callable[[Property], str | None],
) -> str | None:
    """The kind every item of a list literal has, or None (an UNWIND's variable)."""
    if not isinstance(expr, ListLiteral) or not expr.items:
        return None
    written = [i for i in expr.items if not (isinstance(i, Literal) and i.kind == "null")]
    kinds = {kind_of(item, variables, property_kind) for item in written}
    return kinds.pop() if len(kinds) == 1 else None


# --------------------------------------------------------------------------- durations


def _fold(months: Fraction, days: Fraction, seconds: Fraction) -> tuple[int, int, int]:
    """(months, days, microseconds) of a duration built from whole or fractional parts: a
    fraction of a month is days (openCypher's average month), a fraction of a day is seconds."""
    whole_months = int(months)  # toward zero
    days += (months - whole_months) * _DAYS_PER_MONTH
    whole_days = int(days)
    seconds += (days - whole_days) * _SECONDS_PER_DAY
    return whole_months, whole_days, int(seconds * _MICROS)


_UNITS = {
    "years": ("months", 12),
    "quarters": ("months", 3),
    "months": ("months", 1),
    "weeks": ("days", 7),
    "days": ("days", 1),
    "hours": ("seconds", 3600),
    "minutes": ("seconds", 60),
    "seconds": ("seconds", 1),
    "milliseconds": ("seconds", Fraction(1, 1000)),
    "microseconds": ("seconds", Fraction(1, 1_000_000)),
    "nanoseconds": ("seconds", Fraction(1, 1_000_000_000)),
}


def duration_of_map(entries: Mapping[str, Fraction]) -> tuple[int, int, int]:
    """A duration({..}) whose every component is a number written in the statement."""
    parts = {"months": Fraction(0), "days": Fraction(0), "seconds": Fraction(0)}
    for key, value in entries.items():
        unit = _UNITS.get(key.lower())
        if unit is None:
            raise CypherTranslateError(f"duration() has no component {key!r}")
        parts[unit[0]] += value * unit[1]
    return _fold(parts["months"], parts["days"], parts["seconds"])


_ISO = re.compile(
    r"^(?P<sign>[-+]?)P(?:(?P<years>[-+]?[\d.]+)Y)?(?:(?P<months>[-+]?[\d.]+)M)?"
    r"(?:(?P<weeks>[-+]?[\d.]+)W)?(?:(?P<days>[-+]?[\d.]+)D)?"
    r"(?:T(?:(?P<hours>[-+]?[\d.]+)H)?(?:(?P<minutes>[-+]?[\d.]+)M)?"
    r"(?:(?P<seconds>[-+]?[\d.]+)S)?)?$",
    re.I,
)
_ISO_DATETIME = re.compile(
    r"^P(?P<years>\d{4})-(?P<months>\d{2})-(?P<days>\d{2})"
    r"T(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2}(?:\.\d+)?)$"
)


def duration_of_text(text: str) -> tuple[int, int, int]:
    """A duration('P..') written in the statement: ISO-8601's PnYnMnWnDTnHnMnS, or its
    PYYYY-MM-DDThh:mm:ss form."""
    m = _ISO.match(text.strip()) or _ISO_DATETIME.match(text.strip())
    if m is None or not any(v for k, v in m.groupdict().items() if k != "sign"):
        raise CypherTranslateError(f"{text!r} is not an ISO-8601 duration, e.g. 'P1Y2M10DT2H30M'")
    entries = {k: Fraction(v) for k, v in m.groupdict().items() if k != "sign" and v}
    if m.groupdict().get("sign") == "-":
        entries = {k: -v for k, v in entries.items()}
    return duration_of_map(entries)


def interval(months: int, days: int, micros: int) -> exp.Expression:
    """The engine's interval of a duration's three parts."""
    text = f"{months} months {days} days {micros} microseconds"
    return exp.Cast(this=exp.Literal.string(text), to=exp.DataType.build("INTERVAL"))


def _unit(name: str) -> exp.Expression:
    return exp.Cast(this=exp.Literal.string(f"1 {name}"), to=exp.DataType.build("INTERVAL"))


def _times(value: exp.Expression, unit: str) -> exp.Expression:
    """``value`` of ``unit`` as an interval. A count of microseconds is written into the
    interval's text: an engine multiplies an interval by a 32-bit number (DuckDB), and a day's
    microseconds are past that."""
    if unit == "microsecond":
        whole = exp.Cast(this=value, to=exp.DataType.build("BIGINT"))
        text = exp.DPipe(
            this=exp.Cast(this=whole, to=exp.DataType.build("VARCHAR")),
            expression=exp.Literal.string(" microseconds"),
        )
        return exp.Cast(this=text, to=exp.DataType.build("INTERVAL"))
    return exp.Mul(this=exp.Paren(this=value), expression=_unit(unit))


def duration_of_parts(entries: Mapping[str, exp.Expression]) -> exp.Expression:
    """A duration({..}) of computed components: each as that many of its unit. A fraction of a
    month or a day is the engine's own (PostgreSQL and DuckDB read a month as 30 days)."""
    units = {
        "years": "year",
        "quarters": "month",
        "months": "month",
        "weeks": "day",
        "days": "day",
        "hours": "hour",
        "minutes": "minute",
        "seconds": "second",
        "milliseconds": "millisecond",
        "microseconds": "microsecond",
        "nanoseconds": "microsecond",
    }
    scale = {"quarters": 3, "weeks": 7}
    total: exp.Expression | None = None
    for key, value in entries.items():
        unit = units.get(key.lower())
        if unit is None:
            raise CypherTranslateError(f"duration() has no component {key!r}")
        if key.lower() in scale:
            value = exp.Mul(
                this=exp.Paren(this=value), expression=exp.Literal.number(scale[key.lower()])
            )
        if key.lower() == "nanoseconds":
            value = exp.Div(this=exp.Paren(this=value), expression=exp.Literal.number(1000))
        term = _times(value, unit)
        total = term if total is None else exp.Add(this=total, expression=term)
    return total if total is not None else interval(0, 0, 0)


def parts(value: exp.Expression) -> tuple[exp.Expression, exp.Expression, exp.Expression]:
    """A duration's (months, days, microseconds), each a whole number."""

    def field(unit: str) -> exp.Expression:
        return exp.Extract(this=exp.var(unit), expression=value.copy())

    def whole(e: exp.Expression) -> exp.Expression:
        return exp.Cast(this=e, to=exp.DataType.build("BIGINT"))

    months = exp.Add(
        this=exp.Mul(this=field("YEAR"), expression=exp.Literal.number(12)),
        expression=field("MONTH"),
    )
    micros = exp.Add(
        this=exp.Add(
            this=exp.Mul(this=field("HOUR"), expression=exp.Literal.number(3_600_000_000)),
            expression=exp.Mul(this=field("MINUTE"), expression=exp.Literal.number(60_000_000)),
        ),
        expression=field("MICROSECOND"),
    )
    return whole(exp.Paren(this=months)), whole(field("DAY")), whole(exp.Paren(this=micros))


def returned(value: exp.Expression) -> exp.Expression:
    """A duration as it leaves the engine: its three parts (:func:`iso_duration` writes them).
    A null duration leaves as null."""
    if isinstance(value, exp.Null):
        return value
    return exp.Array(expressions=list(parts(value)))


def _trunc_div(a: int, b: int) -> int:
    q = abs(a) // abs(b)
    return q if (a >= 0) == (b >= 0) else -q


def iso_duration(months: int, days: int, micros: int) -> str:
    """openCypher's ISO-8601 text of a duration: P1Y2M3DT4H5M6.5S, each part with its own sign
    and left out when zero, PT0S for none."""
    years, months = _trunc_div(months, 12), months - 12 * _trunc_div(months, 12)
    hours = _trunc_div(micros, 3600 * _MICROS)
    micros -= hours * 3600 * _MICROS
    minutes = _trunc_div(micros, 60 * _MICROS)
    micros -= minutes * 60 * _MICROS
    out = "P"
    out += f"{years}Y" if years else ""
    out += f"{months}M" if months else ""
    out += f"{days}D" if days else ""
    time = f"{hours}H" if hours else ""
    time += f"{minutes}M" if minutes else ""
    if micros:
        whole = _trunc_div(micros, _MICROS)
        frac = abs(micros - whole * _MICROS)
        sign = "-" if micros < 0 and whole == 0 else ""
        text = f"{sign}{whole}" + (f".{frac:06d}".rstrip("0") if frac else "")
        time += f"{text}S"
    if time:
        out += "T" + time
    return out if out != "P" else "PT0S"


# --------------------------------------------------------------------------- arithmetic


def _months_and_days(duration: exp.Expression) -> exp.Expression:
    months, days = parts(duration)[:2]
    return exp.Paren(this=exp.Add(this=_times(months, "month"), expression=_times(days, "day")))


def _seconds_part(duration: exp.Expression) -> exp.Expression:
    return _times(parts(duration)[2], "microsecond")


def arithmetic(
    op: str,
    left: exp.Expression,
    left_kind: str | None,
    right: exp.Expression,
    right_kind: str | None,
    zone: str | None = None,
) -> exp.Expression | None:
    """``left op right`` where one side is a duration, as openCypher reads it; None where
    neither is."""
    if DURATION not in (left_kind, right_kind):
        return None
    if op in ("*", "/"):
        cls = exp.Mul if op == "*" else exp.Div
        return cls(this=exp.Paren(this=left), expression=exp.Paren(this=right))
    left, right = exp.Paren(this=left), exp.Paren(this=right)
    if left_kind == DURATION and right_kind == DURATION:
        return (exp.Add if op == "+" else exp.Sub)(this=left, expression=right)
    if left_kind == DURATION and op == "+":
        left, left_kind, right, right_kind = right, right_kind, left, left_kind
    if right_kind != DURATION or left_kind not in SQL_TYPES:
        raise CypherTranslateError(
            f"a duration cannot be taken {'from' if op == '-' else 'with'} that"
        )
    cls = exp.Add if op == "+" else exp.Sub
    if left_kind == DATE:
        # A date moves by the duration's months and days; its time of day is no part of a date.
        return exp.Cast(
            this=cls(this=left, expression=_months_and_days(right)),
            to=exp.DataType.build("DATE"),
        )
    if left_kind in (LOCALTIME, TIME):
        # A time of day moves by the duration's time, round the clock.
        return exp.Cast(
            this=cls(this=left, expression=_seconds_part(right)),
            to=exp.DataType.build(SQL_TYPES[left_kind]),
        )
    if left_kind == DATETIME and zone is not None:
        # Moved on its wall clock in its zone, by the zone's rules (REQ-2173).
        return zoned(cls(this=wall_clock(left, zone), expression=right), zone)
    return cls(this=left, expression=right)


# --------------------------------------------------------------------------- functions


def _as_timestamp(value: exp.Expression) -> exp.Expression:
    return exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))


def _micros_of_day(value: exp.Expression, kind: str) -> exp.Expression:
    """The time of day in microseconds; a date's is midnight. (An engine's SECOND field drops the
    fraction; its MICROSECOND field is the seconds with it.)"""
    if kind == DATE:
        return exp.Literal.number(0)

    def field(unit: str) -> exp.Expression:
        return exp.Extract(this=exp.var(unit), expression=value.copy())

    return exp.Paren(
        this=exp.Add(
            this=exp.Add(
                this=exp.Mul(this=field("HOUR"), expression=exp.Literal.number(3_600_000_000)),
                expression=exp.Mul(this=field("MINUTE"), expression=exp.Literal.number(60_000_000)),
            ),
            expression=field("MICROSECOND"),
        )
    )


def _at_utc(micros: exp.Expression, value: exp.Expression) -> exp.Expression:
    """A time with an offset's microseconds of the day, moved to UTC."""
    offset = exp.Extract(this=exp.var("TIMEZONE"), expression=value.copy())
    return exp.Paren(
        this=exp.Sub(
            this=micros, expression=exp.Mul(this=offset, expression=exp.Literal.number(_MICROS))
        )
    )


def _epoch(value: exp.Expression) -> exp.Expression:
    return exp.Extract(this=exp.var("EPOCH"), expression=value)


def duration_function(
    name: str,
    written: list[CypherExpr],
    kinds: list[str | None],
    lower: Callable[[CypherExpr], exp.Expression],
    zones: list[str | None] | None = None,
) -> exp.Expression:
    """duration(..), duration.between, duration.inMonths, duration.inDays, duration.inSeconds.
    ``lower`` lowers an argument (a date time as its instant), ``zones`` each one's zone; a
    duration written wholly in the statement is folded here."""
    name = name.lower()
    if name == "duration":
        if len(written) != 1:
            raise CypherTranslateError("duration() takes one argument, a string or a map")
        given = written[0]
        if isinstance(given, Literal) and given.kind == "string":
            return interval(*duration_of_text(str(given.value)))
        if isinstance(given, MapLiteral):
            if all(isinstance(v, Literal) and v.kind == "number" for _k, v in given.entries):
                return interval(
                    *duration_of_map({k: Fraction(str(v.value)) for k, v in given.entries})  # type: ignore[union-attr]
                )
            return duration_of_parts({k: lower(v) for k, v in given.entries})
        return exp.Cast(this=lower(given), to=exp.DataType.build("INTERVAL"))
    args = [lower(a) for a in written]
    if len(args) != 2 or None in kinds or DURATION in kinds:
        raise CypherTranslateError(f"{name}() takes two temporal values")
    (start, end), (start_kind, end_kind) = args, kinds
    start_zone, end_zone = (zones or [None, None])[:2]
    assert start_kind is not None and end_kind is not None
    zoned_kinds = (TIME, DATETIME)
    dated = start_kind in _DATED and end_kind in _DATED
    if name in ("duration.inmonths", "duration.indays") and not dated:
        return interval(0, 0, 0)
    if not dated:
        # Without a date on both, only the times of day are compared: two values with zones as
        # the same instant of the day, a value with a zone beside a local one on its wall clock.
        both_zoned = start_kind in zoned_kinds and end_kind in zoned_kinds

        def of_day(value: exp.Expression, kind: str, zone: str | None) -> exp.Expression:
            if kind == DATETIME:
                value = (
                    exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
                    if both_zoned
                    else wall_clock(value, zone)
                )
                return _micros_of_day(value, LOCALDATETIME)
            micros = _micros_of_day(value, kind)
            return _at_utc(micros, value) if kind == TIME and both_zoned else micros

        kinds_given = {start_kind, end_kind}
        if DATETIME in kinds_given and kinds_given & {LOCALTIME}:
            # A local time of day beside a date time is that time on its date, in its zone: the
            # instants are compared, so a change of offset between them counts.
            dt, dt_zone = (start, start_zone) if start_kind == DATETIME else (end, end_zone)

            def instant_of(value: exp.Expression, kind: str) -> exp.Expression:
                if kind == DATETIME:
                    return exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
                day = exp.Cast(this=wall_clock(dt.copy(), dt_zone), to=exp.DataType.build("DATE"))
                wall = exp.Add(
                    this=exp.Cast(this=day, to=exp.DataType.build("TIMESTAMP")),
                    expression=_times(_micros_of_day(value, kind), "microsecond"),
                )
                return exp.Cast(this=zoned(wall, dt_zone), to=exp.DataType.build("TIMESTAMP"))

            between = exp.Sub(
                this=_epoch(instant_of(end, end_kind)),
                expression=_epoch(instant_of(start, start_kind)),
            )
            micros = exp.Cast(
                this=exp.Anonymous(
                    this="ROUND",
                    expressions=[
                        exp.Mul(
                            this=exp.Paren(this=between), expression=exp.Literal.number(_MICROS)
                        )
                    ],
                ),
                to=exp.DataType.build("BIGINT"),
            )
            return _times(micros, "microsecond")
        later = of_day(end, end_kind, end_zone)
        earlier = of_day(start, start_kind, start_zone)
        return _times(exp.Sub(this=later, expression=earlier), "microsecond")
    if start_kind == DATETIME and end_kind == DATETIME:
        # Two instants, compared as such.
        wall_a = instant_a = exp.Cast(this=start, to=exp.DataType.build("TIMESTAMP"))
        wall_b = instant_b = exp.Cast(this=end, to=exp.DataType.build("TIMESTAMP"))
    else:
        # A date time beside a local value: the local value is read in the date time's zone.
        zone = start_zone if start_kind == DATETIME else end_zone

        def wall(value: exp.Expression, kind: str, own: str | None) -> exp.Expression:
            if kind == DATETIME:
                return exp.Cast(this=wall_clock(value, own), to=exp.DataType.build("TIMESTAMP"))
            return _as_timestamp(value)

        def instant(value: exp.Expression, kind: str) -> exp.Expression:
            if kind == DATETIME:
                return exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
            moment = zoned(_as_timestamp(value), zone)
            return exp.Cast(this=moment, to=exp.DataType.build("TIMESTAMP"))

        wall_a, wall_b = wall(start, start_kind, start_zone), wall(end, end_kind, end_zone)
        instant_a, instant_b = instant(start, start_kind), instant(end, end_kind)
    age = exp.Anonymous(this="AGE", expressions=[wall_b, wall_a])
    seconds = exp.Sub(this=_epoch(instant_b.copy()), expression=_epoch(instant_a.copy()))
    if name == "duration.inmonths":
        return _times(parts(age)[0], "month")
    if name == "duration.indays":
        days = exp.Cast(
            this=exp.Anonymous(
                this="TRUNC",
                expressions=[
                    exp.Div(
                        this=exp.Paren(this=seconds),
                        expression=exp.Literal.number(_SECONDS_PER_DAY),
                    )
                ],
            ),
            to=exp.DataType.build("BIGINT"),
        )
        return _times(days, "day")

    def micros_of(e: exp.Expression) -> exp.Expression:
        return exp.Cast(
            this=exp.Anonymous(
                this="ROUND",
                expressions=[
                    exp.Mul(this=exp.Paren(this=e), expression=exp.Literal.number(_MICROS))
                ],
            ),
            to=exp.DataType.build("BIGINT"),
        )

    if name == "duration.inseconds":
        return _times(micros_of(seconds), "microsecond")
    # between: the calendar difference of the wall clocks, and the hours a zone's change of
    # offset (daylight saving) adds or takes between the two instants.
    walls = exp.Sub(this=_epoch(wall_b.copy()), expression=_epoch(wall_a.copy()))
    shift = exp.Sub(this=exp.Paren(this=seconds.copy()), expression=exp.Paren(this=walls))
    return exp.Add(this=age, expression=_times(micros_of(shift), "microsecond"))


# --------------------------------------------------------------------------- properties

_DURATION_PROPERTIES = {
    # name: (part index 0 months / 1 days / 2 micros, divisor, modulo)
    "years": (0, 12, None),
    "quarters": (0, 3, None),
    "months": (0, 1, None),
    "weeks": (1, 7, None),
    "days": (1, 1, None),
    "hours": (2, 3600 * _MICROS, None),
    "minutes": (2, 60 * _MICROS, None),
    "seconds": (2, _MICROS, None),
    "milliseconds": (2, 1000, None),
    "microseconds": (2, 1, None),
    "nanoseconds": (2, Fraction(1, 1000), None),
    "quartersofyear": (0, 3, 4),
    "monthsofquarter": (0, 1, 3),
    "monthsofyear": (0, 1, 12),
    "daysofweek": (1, 1, 7),
    "minutesofhour": (2, 60 * _MICROS, 60),
    "secondsofminute": (2, _MICROS, 60),
    "millisecondsofsecond": (2, 1000, 1000),
    "microsecondsofsecond": (2, 1, 1_000_000),
    "nanosecondsofsecond": (2, Fraction(1, 1000), 1_000_000_000),
}


def duration_property(value: exp.Expression, name: str) -> exp.Expression:
    """``d.name`` of a duration: a whole number of that unit, or of that unit within the next."""
    found = _DURATION_PROPERTIES.get(name.lower())
    if found is None:
        raise CypherTranslateError(f"a duration has no property {name!r}")
    index, divisor, modulo = found
    if index == 2:
        return _time_property(parts(value)[2], name.lower())
    part = parts(value)[index]
    if isinstance(divisor, Fraction):
        counted: exp.Expression = exp.Mul(
            this=part, expression=exp.Literal.number(int(1 / divisor))
        )
    elif divisor == 1:
        counted = part
    else:
        counted = exp.Cast(
            this=exp.Anonymous(
                this="TRUNC",
                expressions=[exp.Div(this=part, expression=exp.Literal.number(divisor))],
            ),
            to=exp.DataType.build("BIGINT"),
        )
    if modulo is None:
        return counted
    return exp.Mod(this=exp.Paren(this=counted), expression=exp.Literal.number(modulo))


def _time_property(micros: exp.Expression, key: str) -> exp.Expression:
    """A duration's time part in a unit, as openCypher counts it: its seconds the whole seconds
    at or below it, the part of a second after them never negative (-0.9s is -1s and 0.1s)."""
    seconds = _whole(
        exp.Anonymous(
            this="FLOOR",
            expressions=[exp.Div(this=micros, expression=exp.Literal.number(float(_MICROS)))],
        )
    )
    fraction = exp.Paren(
        this=exp.Sub(
            this=micros.copy(),
            expression=exp.Mul(this=seconds.copy(), expression=exp.Literal.number(_MICROS)),
        )
    )

    def mod(e: exp.Expression, n: int) -> exp.Expression:
        return exp.Mod(this=exp.Paren(this=e), expression=exp.Literal.number(n))

    def times(e: exp.Expression, n: int) -> exp.Expression:
        return exp.Mul(this=e, expression=exp.Literal.number(n))

    built = {
        "hours": lambda: _int_div(seconds, 3600),
        "minutes": lambda: _int_div(seconds, 60),
        "seconds": lambda: seconds,
        "milliseconds": lambda: exp.Add(
            this=times(seconds, 1000), expression=_int_div(fraction, 1000)
        ),
        "microseconds": lambda: micros,
        "nanoseconds": lambda: times(micros, 1000),
        "minutesofhour": lambda: mod(_int_div(seconds, 60), 60),
        "secondsofminute": lambda: mod(seconds, 60),
        "millisecondsofsecond": lambda: _int_div(fraction, 1000),
        "microsecondsofsecond": lambda: fraction,
        "nanosecondsofsecond": lambda: times(fraction, 1000),
    }
    return built[key]()


#: A registered column's type -> the temporal kind its values have.
_TYPE_KINDS = {
    "date": DATE,
    "time": LOCALTIME,
    "time without time zone": LOCALTIME,
    "timetz": TIME,
    "time with time zone": TIME,
    "timestamp": LOCALDATETIME,
    "timestamp without time zone": LOCALDATETIME,
    "datetime": LOCALDATETIME,
    "timestamptz": DATETIME,
    "timestamp with time zone": DATETIME,
    "interval": DURATION,
}


def kind_of_type(data_type: str) -> str | None:
    """The temporal kind of a registered column's values, by its type."""
    return _TYPE_KINDS.get(re.sub(r"\(\d+\)", "", data_type).strip().lower())


# --------------------------------------------------------------------------- text and equality


def _int_div(a: exp.Expression, b: int) -> exp.Expression:
    """``a / b`` toward zero, a whole number on every engine (DuckDB's / of integers is a float)."""
    quotient = exp.Div(this=exp.Paren(this=a), expression=exp.Literal.number(b))
    return exp.Cast(
        this=exp.Anonymous(this="TRUNC", expressions=[quotient]), to=exp.DataType.build("BIGINT")
    )


def _text(e: exp.Expression) -> exp.Expression:
    return exp.Cast(this=e, to=exp.DataType.build("VARCHAR"))


def _part_text(value: exp.Expression, suffix: str) -> exp.Expression:
    """``<value><suffix>``, or nothing when the value is zero."""
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.NEQ(this=value, expression=exp.Literal.number(0)),
                true=exp.DPipe(this=_text(value), expression=exp.Literal.string(suffix)),
            )
        ],
        default=exp.Literal.string(""),
    )


def iso_text(value: exp.Expression) -> exp.Expression:
    """toString(d) of a duration: :func:`iso_duration`'s text, built by the engine."""
    months, days, micros = parts(value)
    years = _int_div(months, 12)
    month_rest = exp.Sub(
        this=months.copy(), expression=exp.Mul(this=years.copy(), expression=exp.Literal.number(12))
    )
    hours = _int_div(micros, 3600 * _MICROS)
    rest = exp.Sub(
        this=micros.copy(),
        expression=exp.Mul(this=hours.copy(), expression=exp.Literal.number(3600 * _MICROS)),
    )
    minutes = _int_div(rest, 60 * _MICROS)
    rest = exp.Paren(
        this=exp.Sub(
            this=exp.Paren(this=rest.copy()),
            expression=exp.Mul(this=minutes.copy(), expression=exp.Literal.number(60 * _MICROS)),
        )
    )
    whole = _int_div(rest, _MICROS)
    fraction = exp.Anonymous(
        this="ABS",
        expressions=[
            exp.Sub(
                this=rest.copy(),
                expression=exp.Mul(this=whole.copy(), expression=exp.Literal.number(_MICROS)),
            )
        ],
    )
    digits = exp.Anonymous(
        this="RTRIM",
        expressions=[
            exp.Anonymous(
                this="LPAD",
                expressions=[
                    _text(fraction.copy()),
                    exp.Literal.number(6),
                    exp.Literal.string("0"),
                ],
            ),
            exp.Literal.string("0"),
        ],
    )
    sign = exp.Case(
        ifs=[
            exp.If(
                this=exp.And(
                    this=exp.LT(this=rest.copy(), expression=exp.Literal.number(0)),
                    expression=exp.EQ(this=whole.copy(), expression=exp.Literal.number(0)),
                ),
                true=exp.Literal.string("-"),
            )
        ],
        default=exp.Literal.string(""),
    )
    seconds = exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=rest.copy(), expression=exp.Literal.number(0)),
                true=exp.Literal.string(""),
            ),
            exp.If(
                this=exp.EQ(this=fraction.copy(), expression=exp.Literal.number(0)),
                true=exp.DPipe(this=_text(whole.copy()), expression=exp.Literal.string("S")),
            ),
        ],
        default=exp.DPipe(
            this=exp.DPipe(
                this=exp.DPipe(this=sign, expression=_text(whole.copy())),
                expression=exp.DPipe(this=exp.Literal.string("."), expression=digits),
            ),
            expression=exp.Literal.string("S"),
        ),
    )
    time = exp.DPipe(
        this=exp.DPipe(this=_part_text(hours, "H"), expression=_part_text(minutes, "M")),
        expression=seconds,
    )
    date = exp.DPipe(
        this=exp.DPipe(
            this=_part_text(years, "Y"), expression=_part_text(exp.Paren(this=month_rest), "M")
        ),
        expression=_part_text(days, "D"),
    )
    with_time = exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=time.copy(), expression=exp.Literal.string("")),
                true=exp.Literal.string(""),
            )
        ],
        default=exp.DPipe(this=exp.Literal.string("T"), expression=time),
    )
    text = exp.DPipe(
        this=exp.DPipe(this=exp.Literal.string("P"), expression=date), expression=with_time
    )
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=text.copy(), expression=exp.Literal.string("P")),
                true=exp.Literal.string("PT0S"),
            )
        ],
        default=text,
    )


def equality(op: str, left: exp.Expression, right: exp.Expression) -> exp.Expression:
    """Two durations are equal when each part is: 40 hours is not 1 day and 16 hours, as an
    engine's interval would have it."""
    same = exp.EQ(this=returned(left), expression=returned(right))
    return same if op == "=" else exp.Not(this=exp.Paren(this=same))


# --------------------------------------------------------------------------- answers


def _clock(value: "datetime.time | datetime.datetime") -> str:
    """hh:mm, with :ss when there are seconds and the fraction without its trailing zeros."""
    text = f"{value.hour:02d}:{value.minute:02d}"
    if value.second or value.microsecond:
        text += f":{value.second:02d}"
    if value.microsecond:
        text += f".{value.microsecond:06d}".rstrip("0")
    return text


def _offset(value: "datetime.time | datetime.datetime") -> str:
    delta = value.utcoffset()
    if delta is None:
        return ""
    if not delta:
        return "Z"
    total = int(delta.total_seconds())
    sign = "-" if total < 0 else "+"
    minutes, seconds = divmod(abs(total), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{sign}{hours:02d}:{minutes:02d}" + (f":{seconds:02d}" if seconds else "")


def cypher_text(value: "datetime.date | datetime.time | datetime.datetime") -> str:
    """A date, time or date time as openCypher writes it (Temporal6): 1984-10-11, 12:00,
    21:40:32.142, 2015-07-21T21:40:32.142Z."""
    import datetime as _dt

    if isinstance(value, _dt.datetime):
        text = f"{value.date().isoformat()}T{_clock(value)}{_offset(value)}"
        from zoneinfo import ZoneInfo

        # A date time in a region names the region after its offset.
        return f"{text}[{value.tzinfo.key}]" if isinstance(value.tzinfo, ZoneInfo) else text
    if isinstance(value, _dt.time):
        return f"{_clock(value)}{_offset(value)}"
    return value.isoformat()


# --------------------------------------------------------------------------- values written whole


def _offset_of(text: str) -> "datetime.tzinfo | None":
    """An offset written after a time: Z, +01:00, +0100, +01; a zone name read from the zone
    database. None for none."""
    import datetime as _dt

    if not text:
        return None
    if text == "Z":
        return _dt.timezone.utc
    m = re.fullmatch(r"([-+])(\d{2}):?(\d{2})?(?::?(\d{2}))?", text)
    if m is not None:
        delta = _dt.timedelta(
            hours=int(m.group(2)), minutes=int(m.group(3) or 0), seconds=int(m.group(4) or 0)
        )
        return _dt.timezone(-delta if m.group(1) == "-" else delta)
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise CypherTranslateError(f"{text!r} is no time zone or offset") from exc


_DATE_TEXT = [
    (re.compile(r"(?P<y>[-+]?\d{4})-?W(?P<w>\d{2})-?(?P<dow>\d)?"), "week"),
    (re.compile(r"(?P<y>[-+]?\d{4})-?Q(?P<q>\d)(?:-?(?P<doq>\d{2}))?"), "quarter"),
    (re.compile(r"(?P<y>[-+]?\d{4})-(?P<o>\d{3})"), "ordinal"),
    (re.compile(r"(?P<y>[-+]?\d{4})(?P<o>\d{3})"), "ordinal"),
    (re.compile(r"(?P<y>[-+]?\d{4})-?(?P<m>\d{2})?-?(?P<d>\d{2})?"), "calendar"),
]
_TIME_TEXT = re.compile(
    r"(?P<h>\d{2})(?::?(?P<mi>\d{2})(?::?(?P<s>\d{2})(?:[.,](?P<f>\d{1,9}))?)?)?"
    r"(?P<zone>Z|[-+]\d{2}(?::?\d{2})?)?(?:\[(?P<region>[^\]]+)\])?"
)


def _date_of(fields: Mapping[str, int]) -> "datetime.date":
    """A date from its components: calendar (year, month, day), week (year, week, dayOfWeek),
    ordinal (year, ordinalDay) or quarter (year, quarter, dayOfQuarter)."""
    import datetime as _dt

    year = fields["year"]
    if "week" in fields:
        return _dt.date.fromisocalendar(year, fields["week"], fields.get("dayOfWeek", 1))
    if "ordinalDay" in fields:
        return _dt.date(year, 1, 1) + _dt.timedelta(days=fields["ordinalDay"] - 1)
    if "quarter" in fields:
        start = _dt.date(year, 3 * (fields["quarter"] - 1) + 1, 1)
        return start + _dt.timedelta(days=fields.get("dayOfQuarter", 1) - 1)
    return _dt.date(year, fields.get("month", 1), fields.get("day", 1))


def _time_of(fields: Mapping[str, int]) -> "datetime.time":
    import datetime as _dt

    micros = (
        fields.get("millisecond", 0) * 1000
        + fields.get("microsecond", 0)
        + fields.get("nanosecond", 0) // 1000
    )
    return _dt.time(fields.get("hour", 0), fields.get("minute", 0), fields.get("second", 0), micros)


def _parse_date_text(text: str) -> "datetime.date":
    for pattern, form in _DATE_TEXT:
        m = pattern.fullmatch(text)
        if m is None:
            continue
        g = m.groupdict()
        fields = {"year": int(g["y"])}
        if form == "week":
            fields |= {"week": int(g["w"]), "dayOfWeek": int(g["dow"] or 1)}
        elif form == "quarter":
            fields |= {"quarter": int(g["q"]), "dayOfQuarter": int(g["doq"] or 1)}
        elif form == "ordinal":
            fields |= {"ordinalDay": int(g["o"])}
        else:
            fields |= {"month": int(g["m"] or 1), "day": int(g["d"] or 1)}
        return _date_of(fields)
    raise CypherTranslateError(f"{text!r} is no date, e.g. '2015-07-21'")


def _parse_time_text(text: str) -> tuple["datetime.time", str]:
    """The time of day a string writes, and its offset or zone as written ('' for none)."""
    m = _TIME_TEXT.fullmatch(text)
    if m is None:
        raise CypherTranslateError(f"{text!r} is no time, e.g. '21:40:32.142'")
    g = m.groupdict()
    fraction = (g["f"] or "").ljust(9, "0")
    fields = {
        "hour": int(g["h"]),
        "minute": int(g["mi"] or 0),
        "second": int(g["s"] or 0),
        "nanosecond": int(fraction or 0),
    }
    zone = g["region"] or g["zone"] or ""
    return _time_of(fields), zone


def _at_zone(date: "datetime.date", time: "datetime.time", zone: str) -> "datetime.datetime":
    import datetime as _dt

    value = _dt.datetime.combine(date, time)
    tz = _offset_of(zone)
    return value.replace(tzinfo=tz) if tz is not None else value.replace(tzinfo=_dt.timezone.utc)


def text_value(kind: str, text: str) -> str:
    """A value of ``kind`` written as text in the statement, in the form the engine reads:
    date('2015-W30-2') is '2015-07-21'."""
    import datetime as _dt

    text = text.strip()
    if kind == DATE:
        return _parse_date_text(text).isoformat()
    if kind in (LOCALTIME, TIME):
        value, zone = _parse_time_text(text)
        if kind == LOCALTIME:
            return value.isoformat()
        tz = _offset_of(zone) or _dt.timezone.utc
        offset = _dt.datetime.combine(_dt.date.today(), value, tz).utcoffset()
        assert offset is not None
        return value.replace(tzinfo=_dt.timezone(offset)).isoformat()
    date_text, _, time_text = text.partition("T")
    date = _parse_date_text(date_text)
    value, zone = _parse_time_text(time_text) if time_text else (_dt.time(), "")
    if kind == LOCALDATETIME:
        return _dt.datetime.combine(date, value).isoformat()
    return _at_zone(date, value, zone).isoformat()


def map_value(kind: str, entries: Mapping[str, object]) -> str:
    """A value of ``kind`` from a map of components written in the statement, in the form the
    engine reads: date({year: 1984, week: 10, dayOfWeek: 3}) is '1984-03-07'."""
    import datetime as _dt

    zone = entries.get("timezone")
    numbers = {k: int(v) for k, v in entries.items() if k != "timezone"}  # type: ignore[call-overload]
    if kind == DATE:
        return _date_of(numbers).isoformat()
    if kind in (LOCALTIME, TIME):
        value = _time_of(numbers)
        if kind == LOCALTIME:
            return value.isoformat()
        tz = _offset_of(str(zone)) if zone else _dt.timezone.utc
        assert tz is not None
        offset = _dt.datetime.combine(_dt.date.today(), value, tz).utcoffset()
        assert offset is not None
        return value.replace(tzinfo=_dt.timezone(offset)).isoformat()
    date = _date_of(numbers)
    value = _time_of(numbers)
    if kind == LOCALDATETIME:
        return _dt.datetime.combine(date, value).isoformat()
    return _at_zone(date, value, str(zone) if zone else "").isoformat()


# --------------------------------------------------------------------------- values built from others

_DATE_FIELDS = ("year", "month", "day")
_TIME_FIELDS = ("hour", "minute", "second", "millisecond", "microsecond", "nanosecond")
_OTHER_DATE_FORMS = ("week", "dayOfWeek", "ordinalDay", "quarter", "dayOfQuarter")


def _field(value: exp.Expression, unit: str) -> exp.Expression:
    return exp.Extract(this=exp.var(unit), expression=value.copy())


def _offset_text(seconds: exp.Expression) -> exp.Expression:
    """An offset in seconds as +hh:mm."""
    magnitude = exp.Anonymous(this="ABS", expressions=[seconds.copy()])
    hours = _int_div(magnitude.copy(), 3600)
    minutes = _int_div(
        exp.Sub(
            this=magnitude,
            expression=exp.Mul(this=hours.copy(), expression=exp.Literal.number(3600)),
        ),
        60,
    )

    def two(e: exp.Expression) -> exp.Expression:
        return exp.Anonymous(
            this="LPAD", expressions=[_text(e), exp.Literal.number(2), exp.Literal.string("0")]
        )

    sign = exp.Case(
        ifs=[
            exp.If(
                this=exp.LT(this=seconds.copy(), expression=exp.Literal.number(0)),
                true=exp.Literal.string("-"),
            )
        ],
        default=exp.Literal.string("+"),
    )
    return exp.DPipe(
        this=exp.DPipe(this=sign, expression=two(hours)),
        expression=exp.DPipe(this=exp.Literal.string(":"), expression=two(minutes)),
    )


def _offset_seconds(text: str) -> int:
    import datetime as _dt

    tz = _offset_of(text)
    assert tz is not None
    offset = _dt.datetime.combine(_dt.date.today(), _dt.time(), tz).utcoffset()
    assert offset is not None
    return int(offset.total_seconds())


def composed_value(
    kind: str,
    entries: Mapping[str, CypherExpr],
    kinds: Mapping[str, str | None],
    lower: Callable[[CypherExpr], exp.Expression],
    *,
    base: exp.Expression | None = None,
    base_kind: str | None = None,
    zone: str | None = None,
    source_zone: str | None = None,
    source_offset: exp.Expression | None = None,
) -> exp.Expression:
    """A value of ``kind`` from a map whose components are computed or taken from another value
    ({date: d, day: 28}, {time: t, timezone: '+05:00'}): each component the map names, else the
    other value's, else its default. ``base`` is a value already lowered whose date and time the
    map's components replace (a truncated value and its overrides)."""
    given = {k: v for k, v in entries.items() if k not in ("date", "time", "datetime", "timezone")}
    source_date = entries.get("date", entries.get("datetime"))
    source_time = entries.get("time", entries.get("datetime"))
    date_sql = lower(source_date) if source_date is not None else base
    time_sql = lower(source_time) if source_time is not None else base
    time_kind = kinds.get("time") or kinds.get("datetime") or base_kind

    def part(name: str, unit: str, source: exp.Expression | None, default: int) -> exp.Expression:
        if name in given:
            return lower(given[name])
        if source is not None:
            return _field(source, unit)
        return exp.Literal.number(default)

    def whole(e: exp.Expression) -> exp.Expression:
        return exp.Cast(this=e, to=exp.DataType.build("BIGINT"))

    year = whole(part("year", "YEAR", date_sql, 1970))
    month = whole(part("month", "MONTH", date_sql, 1))
    day = whole(part("day", "DAY", date_sql, 1))
    hour = whole(part("hour", "HOUR", time_sql, 0))
    minute = whole(part("minute", "MINUTE", time_sql, 0))
    # Seconds with their fraction: a second the map names keeps the other value's fraction
    # unless the map names that too.
    if any(k in given for k in ("millisecond", "microsecond", "nanosecond")):
        fraction: exp.Expression = exp.Add(
            this=exp.Add(
                this=exp.Mul(
                    this=exp.Paren(this=part("millisecond", "", None, 0)),
                    expression=exp.Literal.number(1000),
                ),
                expression=part("microsecond", "", None, 0),
            ),
            expression=_int_div(part("nanosecond", "", None, 0), 1000),
        )
    elif time_sql is not None:
        fraction = exp.Mod(
            this=_field(time_sql, "MICROSECOND"), expression=exp.Literal.number(_MICROS)
        )
    else:
        fraction = exp.Literal.number(0)
    if "second" in given:
        whole_seconds: exp.Expression = lower(given["second"])
    elif time_sql is not None:
        whole_seconds = _int_div(_field(time_sql, "MICROSECOND"), _MICROS)
    else:
        whole_seconds = exp.Literal.number(0)
    seconds = exp.Add(
        this=exp.Paren(this=whole_seconds),
        expression=exp.Div(
            this=exp.Paren(this=fraction), expression=exp.Literal.number(float(_MICROS))
        ),
    )
    calendar: exp.Expression = exp.Anonymous(this="MAKE_DATE", expressions=[year, month, day])
    if any(k in given for k in _OTHER_DATE_FORMS):
        calendar = _other_date_form(given, date_sql, lower, part, whole)
    if kind == DATE:
        return calendar
    if kind in (LOCALTIME, TIME):
        local = exp.Anonymous(this="MAKE_TIME", expressions=[hour, minute, seconds])
    else:
        y, m, d = (whole(_field(calendar, unit)) for unit in ("YEAR", "MONTH", "DAY"))
        local = exp.Anonymous(this="MAKE_TIMESTAMP", expressions=[y, m, d, hour, minute, seconds])
    if kind in (LOCALTIME, LOCALDATETIME):
        return local
    if kind == DATETIME:
        # The wall clock the map gives (its time of day the other value's, in that value's own
        # zone), as an instant; it is presented in the zone the map names (REQ-2173).
        if time_kind == TIME and source_offset is not None:
            text = exp.DPipe(this=_text(local), expression=_offset_text(source_offset))
            return exp.Cast(this=text, to=exp.DataType.build("TIMESTAMPTZ"))
        if time_kind == DATETIME:
            return zoned(local, source_zone)
        return zoned(local, zone)
    # A time: at the zone the map names, else the other value's, else UTC. A value
    # that has a zone, given another, is the same instant there.
    named = entries.get("timezone")
    zoned_source = time_sql is not None and time_kind in (TIME, DATETIME)
    if named is not None:
        if not (isinstance(named, Literal) and named.kind == "string"):
            raise CypherTranslateError(f"{kind}() takes its timezone written in the statement")
        target = _offset_seconds(str(named.value))
        if zoned_source and source_offset is not None:
            shift = exp.Sub(this=exp.Literal.number(target), expression=source_offset)
            local = exp.Add(
                this=local,
                expression=_times(
                    exp.Mul(this=exp.Paren(this=shift), expression=exp.Literal.number(_MICROS)),
                    "microsecond",
                ),
            )
            if kind == TIME:
                local = exp.Cast(this=local, to=exp.DataType.build("TIME"))
        offset = exp.Literal.string(_offset_text_of(target))
    elif zoned_source and source_offset is not None:
        offset = _offset_text(source_offset)
    else:
        offset = exp.Literal.string("+00:00")
    text = exp.DPipe(this=_text(local), expression=offset)
    return exp.Cast(this=text, to=exp.DataType.build(SQL_TYPES[kind]))


def _days_after(start: exp.Expression, days: exp.Expression) -> exp.Expression:
    return exp.Cast(
        this=exp.Add(this=start, expression=_times(days, "day")), to=exp.DataType.build("DATE")
    )


def _other_date_form(
    given: Mapping[str, CypherExpr],
    source: exp.Expression | None,
    lower: Callable[[CypherExpr], exp.Expression],
    part: Callable[[str, str, exp.Expression | None, int], exp.Expression],
    whole: Callable[[exp.Expression], exp.Expression],
) -> exp.Expression:
    """A date by week (week-year, week, dayOfWeek), ordinal day or quarter, each component the
    map names, else the other value's."""
    one = exp.Literal.number(1)
    if "week" in given or "dayOfWeek" in given:
        week_year = whole(part("year", "ISOYEAR", source, 1970))
        fourth = exp.Anonymous(
            this="MAKE_DATE", expressions=[week_year, one, exp.Literal.number(4)]
        )
        first_monday = exp.Cast(
            this=exp.Anonymous(this="DATE_TRUNC", expressions=[exp.Literal.string("week"), fourth]),
            to=exp.DataType.build("DATE"),
        )
        week = whole(part("week", "WEEK", source, 1))
        dow = whole(part("dayOfWeek", "ISODOW", source, 1))
        days = exp.Add(
            this=exp.Mul(
                this=exp.Paren(this=exp.Sub(this=week, expression=one)),
                expression=exp.Literal.number(7),
            ),
            expression=exp.Paren(this=exp.Sub(this=dow, expression=one.copy())),
        )
        return _days_after(first_monday, days)
    year = whole(part("year", "YEAR", source, 1970))
    if "ordinalDay" in given:
        start = exp.Anonymous(this="MAKE_DATE", expressions=[year, one, one.copy()])
        return _days_after(
            start, exp.Sub(this=whole(lower(given["ordinalDay"])), expression=one.copy())
        )
    quarter = whole(part("quarter", "QUARTER", source, 1))
    start = exp.Anonymous(
        this="MAKE_DATE",
        expressions=[
            year,
            exp.Add(
                this=exp.Mul(
                    this=exp.Paren(this=exp.Sub(this=quarter, expression=one)),
                    expression=exp.Literal.number(3),
                ),
                expression=one.copy(),
            ),
            one.copy(),
        ],
    )
    if "dayOfQuarter" in given:
        offset = exp.Sub(this=whole(lower(given["dayOfQuarter"])), expression=one.copy())
    elif source is not None:
        quarter_start = exp.Cast(
            this=exp.Anonymous(
                this="DATE_TRUNC", expressions=[exp.Literal.string("quarter"), source.copy()]
            ),
            to=exp.DataType.build("DATE"),
        )
        offset = exp.Sub(
            this=exp.Cast(this=source.copy(), to=exp.DataType.build("DATE")),
            expression=quarter_start,
        )
    else:
        offset = exp.Literal.number(0)
    return _days_after(start, offset)


def _offset_text_of(seconds: int) -> str:
    sign = "-" if seconds < 0 else "+"
    minutes, rest = divmod(abs(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{sign}{hours:02d}:{minutes:02d}" + (f":{rest:02d}" if rest else "")


def temporal_value(
    kind: str,
    written: list[CypherExpr],
    lower: Callable[[CypherExpr], exp.Expression],
    entry_kind: Callable[[CypherExpr], str | None],
    *,
    zone: str | None = None,
    lower_wall: Callable[[CypherExpr, str | None], exp.Expression] | None = None,
    zone_of_entry: Callable[[CypherExpr], str | None] | None = None,
) -> exp.Expression | None:
    """date(..), localtime(..), time(..), localdatetime(..) and datetime(..) of text, a map of
    components or another value; None for the forms the engine's own cast reads (no argument, a
    value with no zone of its own). ``zone`` is the zone the value names (:func:`zone_of`);
    ``lower_wall`` lowers a date time as its wall clock in a zone (None for its own)."""
    if len(written) != 1:
        return None
    given = written[0]
    if isinstance(given, Literal) and given.kind == "null":
        return exp.Null()
    if isinstance(given, Literal) and given.kind == "string":
        value = text_value(kind, str(given.value))
        return exp.Cast(this=exp.Literal.string(value), to=exp.DataType.build(SQL_TYPES[kind]))

    def lowered(e: CypherExpr, at: str | None) -> exp.Expression:
        if lower_wall is not None and entry_kind(e) == DATETIME:
            return lower_wall(e, at)
        return lower(e)

    if not isinstance(given, MapLiteral):
        if entry_kind(given) != DATETIME or kind == DATETIME:
            return None
        # A date time's date, time of day or wall clock, read in its own zone.
        wall = lowered(given, None)
        if kind != TIME:
            return exp.Cast(this=wall, to=exp.DataType.build(SQL_TYPES[kind]))
        clock = _text(exp.Cast(this=wall, to=exp.DataType.build("TIME")))
        offset = _offset_text(offset_seconds_of(lower(given), zone))
        return exp.Cast(
            this=exp.DPipe(this=clock, expression=offset), to=exp.DataType.build("TIMETZ")
        )
    entries = dict(given.entries)
    written_whole = all(
        isinstance(v, Literal) and (v.kind == "number" or (k == "timezone" and v.kind == "string"))
        for k, v in entries.items()
    )
    if written_whole:
        value = map_value(kind, {k: v.value for k, v in entries.items()})  # type: ignore[union-attr]
        return exp.Cast(this=exp.Literal.string(value), to=exp.DataType.build(SQL_TYPES[kind]))
    sources = {k: entry_kind(v) for k, v in entries.items() if k in ("date", "time", "datetime")}
    clock = entries.get("time", entries.get("datetime"))
    clock_kind = entry_kind(clock) if clock is not None else None
    source_offset: exp.Expression | None = None
    if clock is not None and clock_kind == DATETIME:
        source_offset = offset_seconds_of(
            lower(clock), zone_of_entry(clock) if zone_of_entry else None
        )
    elif clock is not None and clock_kind == TIME:
        source_offset = _whole(_field(lower(clock), "TIMEZONE"))
    return composed_value(
        kind,
        entries,
        sources,
        lambda e: lowered(e, None),
        zone=zone,
        source_zone=zone_of_entry(clock) if zone_of_entry and clock is not None else None,
        source_offset=source_offset,
    )


# --------------------------------------------------------------------------- truncate

_TRUNCATED_BY_ENGINE = {
    "quarter": "quarter",
    "month": "month",
    "week": "week",
    "day": "day",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
    "millisecond": "milliseconds",
    "microsecond": "microseconds",
}
_YEARS = {"millennium": 1000, "century": 100, "decade": 10, "year": 1}
_TIME_UNITS = {"hour", "minute", "second", "millisecond", "microsecond"}


def truncated(
    kind: str,
    written: list[CypherExpr],
    value_kind: str | None,
    lower: Callable[[CypherExpr], exp.Expression],
    *,
    zone: str | None = None,
) -> exp.Expression:
    """<kind>.truncate(unit, value[, components]): ``value`` cut to the start of its ``unit``
    (millennium, century, decade, year, weekYear, quarter, month, week, day, hour, minute,
    second, millisecond, microsecond), as a ``kind``, then the components given put in. ``lower``
    gives a date time's wall clock; a date time result is in ``zone``."""
    if len(written) not in (2, 3):
        raise CypherTranslateError(f"{kind}.truncate() takes a unit, a value and a map")
    unit_given, value_given = written[0], written[1]
    if not (isinstance(unit_given, Literal) and unit_given.kind == "string"):
        raise CypherTranslateError(f"{kind}.truncate() takes its unit written in the statement")
    unit = str(unit_given.value)
    if value_kind is None:
        raise CypherTranslateError(f"{kind}.truncate() takes a temporal value")
    value = lower(value_given)
    one = exp.Literal.number(1)
    if value_kind in (LOCALTIME, TIME):
        if unit not in _TIME_UNITS and unit != "day":
            raise CypherTranslateError(f"a time has no {unit} to truncate to")
        moment: exp.Expression = exp.Anonymous(
            this="MAKE_TIMESTAMP",
            expressions=[
                exp.Literal.number(1970),
                one,
                one.copy(),
                exp.Cast(this=_field(value, "HOUR"), to=exp.DataType.build("BIGINT")),
                exp.Cast(this=_field(value, "MINUTE"), to=exp.DataType.build("BIGINT")),
                exp.Div(
                    this=_field(value, "MICROSECOND"), expression=exp.Literal.number(float(_MICROS))
                ),
            ],
        )
    else:
        moment = exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
    if unit in _YEARS:
        year = exp.Cast(this=_field(moment, "YEAR"), to=exp.DataType.build("BIGINT"))
        if _YEARS[unit] > 1:
            year = exp.Sub(
                this=year,
                expression=exp.Mod(this=year.copy(), expression=exp.Literal.number(_YEARS[unit])),
            )
        cut: exp.Expression = exp.Anonymous(
            this="MAKE_TIMESTAMP",
            expressions=[
                year,
                one,
                one.copy(),
                exp.Literal.number(0),
                exp.Literal.number(0),
                exp.Literal.number(0),
            ],
        )
    elif unit == "weekYear":
        week_year = exp.Cast(this=_field(moment, "ISOYEAR"), to=exp.DataType.build("BIGINT"))
        fourth = exp.Anonymous(
            this="MAKE_DATE", expressions=[week_year, one, exp.Literal.number(4)]
        )
        cut = exp.Anonymous(this="DATE_TRUNC", expressions=[exp.Literal.string("week"), fourth])
    elif unit in _TRUNCATED_BY_ENGINE:
        cut = exp.Anonymous(
            this="DATE_TRUNC", expressions=[exp.Literal.string(_TRUNCATED_BY_ENGINE[unit]), moment]
        )
    else:
        raise CypherTranslateError(f"{unit!r} is no unit to truncate to")
    cut = exp.Cast(this=cut, to=exp.DataType.build("TIMESTAMP"))
    components = written[2] if len(written) == 3 else None
    if components is not None and not isinstance(components, MapLiteral):
        raise CypherTranslateError(f"{kind}.truncate() takes its components as a map")
    entries = dict(components.entries) if components is not None else {}
    if not entries:
        if kind == DATETIME:
            return zoned(cut, zone)
        if kind == TIME and zone is not None and _OFFSET.fullmatch(zone):
            # A time keeps its value's offset: the wall clock cut, at that offset.
            local = exp.Cast(this=cut, to=exp.DataType.build("TIME"))
            text = exp.DPipe(this=_text(local), expression=exp.Literal.string(zone))
            return exp.Cast(this=text, to=exp.DataType.build(SQL_TYPES[TIME]))
        return exp.Cast(this=cut, to=exp.DataType.build(SQL_TYPES[kind]))
    return composed_value(kind, entries, {}, lower, base=cut, base_kind=LOCALDATETIME, zone=zone)


# --------------------------------------------------------------------------- date and time properties


def _whole(e: exp.Expression) -> exp.Expression:
    return exp.Cast(this=e, to=exp.DataType.build("BIGINT"))


def value_property(
    kind: str, value: exp.Expression, name: str, zone: str | None = None
) -> exp.Expression:
    """``v.name`` of a date, time or date time (openCypher's accessors): year, quarter, month,
    week, weekYear, day, ordinalDay, weekDay/dayOfWeek, dayOfQuarter, hour, minute, second,
    millisecond, microsecond, nanosecond, offsetSeconds, offsetMinutes, offset, epochSeconds,
    epochMillis."""
    key = name.lower()
    instant = value
    if kind == DATETIME:
        if key == "timezone":
            return exp.Literal.string(zone or "Z")
        if key in ("offsetseconds", "offsetminutes", "offset"):
            seconds = offset_seconds_of(instant, zone)
            if key == "offsetseconds":
                return seconds
            if key == "offsetminutes":
                return _int_div(seconds, 60)
            return _offset_text(seconds) if zone else exp.Literal.string("Z")
        if key not in ("epochseconds", "epochmillis"):
            # Its parts are its wall clock's, in its zone.
            value = wall_clock(value, zone)
    if kind == TIME and key == "timezone":
        return _offset_text(_whole(_field(value, "TIMEZONE")))
    fields = {
        "year": "YEAR",
        "quarter": "QUARTER",
        "month": "MONTH",
        "week": "WEEK",
        "weekyear": "ISOYEAR",
        "day": "DAY",
        "ordinalday": "DOY",
        "weekday": "ISODOW",
        "dayofweek": "ISODOW",
        "hour": "HOUR",
        "minute": "MINUTE",
    }
    dated = kind in _DATED
    timed = kind != DATE
    if key in fields and (dated if fields[key] not in ("HOUR", "MINUTE") else timed):
        return _whole(_field(value, fields[key]))
    if key == "dayofquarter" and dated:
        start = exp.Cast(
            this=exp.Anonymous(
                this="DATE_TRUNC", expressions=[exp.Literal.string("quarter"), value.copy()]
            ),
            to=exp.DataType.build("DATE"),
        )
        days = exp.Sub(this=exp.Cast(this=value, to=exp.DataType.build("DATE")), expression=start)
        return exp.Add(this=exp.Paren(this=days), expression=exp.Literal.number(1))
    micros = _field(value, "MICROSECOND")
    if timed and key == "second":
        return _int_div(micros, _MICROS)
    if timed and key in ("millisecond", "microsecond", "nanosecond"):
        within = exp.Mod(this=micros, expression=exp.Literal.number(_MICROS))
        if key == "millisecond":
            return _int_div(within, 1000)
        if key == "microsecond":
            return _whole(within)
        return exp.Mul(this=_whole(within), expression=exp.Literal.number(1000))
    if kind == TIME and key in ("offsetseconds", "offsetminutes", "offset"):
        seconds = _whole(_field(value, "TIMEZONE"))
        if key == "offsetseconds":
            return seconds
        if key == "offsetminutes":
            return _int_div(seconds, 60)
        return _offset_text(seconds)
    if kind in (DATETIME, LOCALDATETIME) and key in ("epochseconds", "epochmillis"):
        epoch = _field(instant, "EPOCH")
        if key == "epochseconds":
            return _whole(exp.Anonymous(this="FLOOR", expressions=[epoch]))
        return _whole(
            exp.Anonymous(
                this="FLOOR", expressions=[exp.Mul(this=epoch, expression=exp.Literal.number(1000))]
            )
        )
    raise CypherTranslateError(f"a {kind} has no property {name!r}")


# --------------------------------------------------------------------------- zones
#
# A zone a query names (an offset or a region, in a literal or with timezone:) is computed with by
# the engine and travels with the expression only so the answer presents it as openCypher writes
# it: 2015-07-21T21:40:32.142+01:00[Europe/Stockholm]. A column's value names none (REQ-2173).


def _named_zone(text: str) -> str | None:
    """A zone as a query writes it, in one form: a region name, or an offset +hh:mm[:ss]; None
    for UTC, which the answer writes as Z."""
    import datetime as _dt

    if text in ("Z", "UTC") or re.fullmatch(r"[-+]00(:?00)?(:?00)?", text):
        return None
    tz = _offset_of(text)
    if isinstance(tz, _dt.timezone):
        offset = tz.utcoffset(None)
        return _offset_text_of(int(offset.total_seconds())) if offset else None
    return text


def _zone_of_text(text: str) -> str | None:
    m = re.search(r"\[([^\]]+)\]$", text)
    if m is not None:
        return _named_zone(m.group(1))
    m = re.search(r"(Z|[-+]\d{2}(?::?\d{2}(?::?\d{2})?)?)$", text.split("T")[-1])
    return _named_zone(m.group(1)) if m is not None else None


def zone_of(
    expr: CypherExpr,
    zones: Mapping[str, str],
    kind: Callable[[CypherExpr], str | None],
) -> str | None:
    """The zone ``expr`` names, for a time or date time a query builds; None for UTC or none."""
    if isinstance(expr, Paren):
        return zone_of(expr.inner, zones, kind)
    if isinstance(expr, Variable):
        return zones.get(expr.name)
    if isinstance(expr, Parameter):
        return zones.get(parameter_key(expr.name))
    if isinstance(expr, Binary) and expr.op in ("+", "-"):
        for side in (expr.left, expr.right):
            if kind(side) in (TIME, DATETIME):
                return zone_of(side, zones, kind)
        return None
    if not isinstance(expr, FunctionCall):
        return None
    name, _, clock = expr.name.lower().partition(".")
    if name not in (TIME, DATETIME):
        return None
    args = expr.args
    if clock == "truncate":
        given = args[2] if len(args) == 3 else None
        if isinstance(given, MapLiteral):
            for key, value in given.entries:
                if key == "timezone" and isinstance(value, Literal):
                    return _named_zone(str(value.value))
        return zone_of(args[1], zones, kind) if len(args) > 1 else None
    if clock or len(args) != 1:
        return None
    given = args[0]
    if isinstance(given, Literal) and given.kind == "string":
        return _zone_of_text(str(given.value))
    if isinstance(given, MapLiteral):
        entries = dict(given.entries)
        zone = entries.get("timezone")
        if isinstance(zone, Literal) and zone.kind == "string":
            return _named_zone(str(zone.value))
        source = entries.get("datetime", entries.get("time"))
        return zone_of(source, zones, kind) if source is not None else None
    return zone_of(given, zones, kind)


def is_region(zone: str | None) -> bool:
    return zone is not None and not re.match(r"[-+]", zone)


def wall_clock(value: exp.Expression, zone: str | None) -> exp.Expression:
    """A date time's wall clock in its zone (a timestamp without one): the engine's zone rules for
    a region, its offset added for an offset, UTC for none."""
    if is_region(zone):
        return exp.AtTimeZone(this=exp.Paren(this=value), zone=exp.Literal.string(str(zone)))
    as_utc = exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
    if zone is None:
        return as_utc
    seconds = _offset_seconds(zone)
    return exp.Add(
        this=as_utc, expression=_times(exp.Literal.number(seconds * _MICROS), "microsecond")
    )


def zoned(wall: exp.Expression, zone: str | None) -> exp.Expression:
    """A wall clock (a timestamp without a zone) in ``zone``: the instant it is there."""
    if is_region(zone):
        return exp.AtTimeZone(this=exp.Paren(this=wall), zone=exp.Literal.string(str(zone)))
    text = exp.DPipe(this=_text(wall), expression=exp.Literal.string(zone or "+00:00"))
    return exp.Cast(this=text, to=exp.DataType.build("TIMESTAMPTZ"))


def returned_in_zone(value: exp.Expression, zone: str) -> exp.Expression:
    """A date time with a zone as it leaves the engine: its instant (microseconds since the epoch)
    and the zone, which :func:`zoned_text` writes it in."""
    if isinstance(value, exp.Null):
        return value
    micros = exp.Cast(
        this=exp.Anonymous(
            this="ROUND",
            expressions=[
                exp.Mul(this=_field(value, "EPOCH"), expression=exp.Literal.number(_MICROS))
            ],
        ),
        to=exp.DataType.build("BIGINT"),
    )
    return exp.Array(expressions=[_text(micros), exp.Literal.string(zone)])


def zoned_value(micros: int, zone: str) -> "datetime.datetime":
    """An instant in a zone: the date time in that region, or at that offset."""
    import datetime as _dt

    instant = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc) + _dt.timedelta(microseconds=micros)
    tz = _offset_of(zone)
    assert tz is not None
    return instant.astimezone(tz)


def zoned_text(micros: int, zone: str) -> str:
    """openCypher's text of an instant in a zone: 2015-07-21T21:40:32.142+01:00[Europe/Stockholm]."""
    return cypher_text(zoned_value(micros, zone))


def offset_seconds_of(value: exp.Expression, zone: str | None) -> exp.Expression:
    """A date time's offset from UTC, in seconds, in its zone at its instant."""
    if zone is None:
        return exp.Literal.number(0)
    wall = exp.Cast(this=wall_clock(value, zone), to=exp.DataType.build("TIMESTAMP"))
    utc = exp.Cast(this=value.copy(), to=exp.DataType.build("TIMESTAMP"))
    return _whole(exp.Sub(this=_field(wall, "EPOCH"), expression=_field(utc, "EPOCH")))


# --------------------------------------------------------------------------- the value


@dataclass(frozen=True)
class Duration:
    """A duration as a value (Bolt's Duration structure): months, days and microseconds, the
    engine's interval (REQ-2173)."""

    months: int
    days: int
    micros: int

    def iso(self) -> str:
        return iso_duration(self.months, self.days, self.micros)


def value_kind(value: object) -> str | None:
    """The kind a client's value has (a temporal or spatial parameter), or None for neither."""
    import datetime as _dt

    if isinstance(value, Duration):
        return DURATION
    if isinstance(value, spatial.Point):
        return spatial.POINT
    if isinstance(value, _dt.datetime):
        return LOCALDATETIME if value.tzinfo is None else DATETIME
    if isinstance(value, _dt.date):
        return DATE
    if isinstance(value, _dt.time):
        return LOCALTIME if value.tzinfo is None else TIME
    return None


def value_zone(value: object) -> str | None:
    """The zone a client's date time names: its region, or its offset; None for UTC or none."""
    import datetime as _dt
    from zoneinfo import ZoneInfo

    if not isinstance(value, _dt.datetime) or value.tzinfo is None:
        return None
    if isinstance(value.tzinfo, ZoneInfo):
        return value.tzinfo.key
    return _named_zone(_offset(value))


def bindable(value: object) -> object:
    """A client's value as the engine is handed it: a temporal value as the text its placeholder
    is cast from (:func:`value_kind`), a point as its list, anything else as it is."""
    if isinstance(value, Duration):
        return f"{value.months} months {value.days} days {value.micros} microseconds"
    if isinstance(value, spatial.Point):
        return spatial.bindable(value)
    if value_kind(value) is not None:
        return value.isoformat()  # type: ignore[attr-defined]
    return value
