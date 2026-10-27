# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's temporal values as openCypher defines them, lowered onto the engine's own types
(REQ-2172).

A value's kind is read off the statement (its constructor, the function that returns it, the
arithmetic that combines two, a variable a WITH or UNWIND carries): date, localtime, time,
localdatetime, datetime or duration. A duration is the engine's interval: months, days and
microseconds, as openCypher's duration is months, days, seconds and nanoseconds. It leaves the
engine as those three numbers (an engine's driver would hand back a timedelta, its months read as
30 days) and is written in the answer in ISO-8601, P..T.. (:func:`iso_duration`)."""

# Requirements: REQ-2172

from __future__ import annotations

import re
from fractions import Fraction
from typing import Callable, Mapping

from sqlglot import exp

from provisa.gql.expr_ast import (
    Binary,
    CypherExpr,
    FunctionCall,
    ListLiteral,
    Literal,
    MapLiteral,
    Paren,
    Property,
    Unary,
    Variable,
)
from provisa.gql.translator_types import CypherTranslateError

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


# --------------------------------------------------------------------------- kinds


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
    if isinstance(expr, Property):
        return property_kind(expr)
    if isinstance(expr, FunctionCall):
        name = expr.name.lower()
        if name in SQL_TYPES:
            return name
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
) -> exp.Expression:
    """duration(..), duration.between, duration.inMonths, duration.inDays, duration.inSeconds.
    ``lower`` lowers an argument; a duration written wholly in the statement is folded here."""
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
    assert start_kind is not None and end_kind is not None
    dated = start_kind in _DATED and end_kind in _DATED
    if name == "duration.inmonths" and not dated:
        return interval(0, 0, 0)
    if name == "duration.indays" and not dated:
        return interval(0, 0, 0)
    if not dated:
        # Without a date on both, only the times of day are compared.
        later, earlier = _micros_of_day(end, end_kind), _micros_of_day(start, start_kind)
        if start_kind == TIME and end_kind == TIME:
            # Two times with offsets are compared as the same instant of the day.
            later, earlier = _at_utc(later, end), _at_utc(earlier, start)
        micros = exp.Sub(this=later, expression=earlier)
        return _times(micros, "microsecond")
    a, b = _as_timestamp(start), _as_timestamp(end)
    age = exp.Anonymous(this="AGE", expressions=[b, a])
    if name == "duration.between":
        return age
    if name == "duration.inmonths":
        return _times(parts(age)[0], "month")
    seconds = exp.Sub(this=_epoch(b.copy()), expression=_epoch(a.copy()))
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
    micros = exp.Cast(
        this=exp.Anonymous(
            this="ROUND",
            expressions=[
                exp.Mul(this=exp.Paren(this=seconds), expression=exp.Literal.number(_MICROS))
            ],
        ),
        to=exp.DataType.build("BIGINT"),
    )
    return _times(micros, "microsecond")


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
