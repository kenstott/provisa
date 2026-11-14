# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7d1c-93a4-4f2e-8c61-2e7f4a90b3d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's interval input, read in Python as interval_in reads it under IntervalStyle
postgres (REQ-2236; REQ-2167): DecodeInterval's amounts and their unit words, a clock
(h:mm[:ss[.ffffff]]), signs, fractions, '@' and 'ago', into the three fields PostgreSQL holds
(months, days, microseconds). The one reader of an interval's literal: the PostgreSQL parser reads
the INTERVAL keyword's literal with it (pg_syntax), and the ClickHouse rows read a cast's literal
with it (polyfill.intervals). It imports nothing of Provisa's, so both may."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import re
from dataclasses import dataclass

I32_MIN, I32_MAX = -(2**31), 2**31 - 1
I64_MIN, I64_MAX = -(2**63), 2**63 - 1
DAY_US = 86_400_000_000
HOUR_US = 3_600_000_000
MINUTE_US = 60_000_000
SECOND_US = 1_000_000
#: Each unit word of an interval's input (datetime.c's deltatktbl), by its first ten characters
#: (TOKMAXLEN), and the unit it names. quarter and the zone words are no unit of an interval's
#: input, and are refused with any other word.
INPUT_UNITS = {
    **dict.fromkeys(("c", "cent", "centuries", "century"), "century"),
    **dict.fromkeys(("d", "day", "days"), "day"),
    **dict.fromkeys(("dec", "decade", "decades", "decs"), "decade"),
    **dict.fromkeys(("h", "hour", "hours", "hr", "hrs"), "hour"),
    **dict.fromkeys(("m", "min", "mins", "minute", "minutes"), "minute"),
    **dict.fromkeys(("microsecon", "us", "usec", "usecs", "useconds"), "microsecond"),
    **dict.fromkeys(("mil", "millennia", "millennium", "mils"), "millennium"),
    **dict.fromkeys(("millisecon", "ms", "msec", "msecs", "mseconds"), "millisecond"),
    **dict.fromkeys(("mon", "mons", "month", "months"), "month"),
    **dict.fromkeys(("s", "sec", "second", "seconds", "secs"), "second"),
    **dict.fromkeys(("w", "week", "weeks"), "week"),
    **dict.fromkeys(("y", "year", "years", "yr", "yrs"), "year"),
}
#: The microseconds of each unit read as microseconds (AdjustMicroseconds' scale).
_MICRO_SCALES = {
    "microsecond": 1,
    "millisecond": 1000,
    "second": SECOND_US,
    "minute": MINUTE_US,
    "hour": HOUR_US,
}
#: The years of each unit read as years (AdjustYears' scale).
_YEAR_SCALES = {"year": 1, "decade": 10, "century": 100, "millennium": 1000}
#: The units a clock field sets (DTK_TIME_M), and a second with a fraction (DTK_ALL_SECS_M).
_ALL_SECONDS = frozenset({"second", "millisecond", "microsecond"})
_CLOCK_UNITS = frozenset({"hour", "minute"}) | _ALL_SECONDS

_NUMBER = re.compile(r"([+-]?\d+)(\.\d+)?", re.ASCII)
_GLUED = re.compile(r"([+-]?\d+(?:\.\d+)?)([a-z]+)", re.ASCII)
_WORD = re.compile(r"[a-z]+", re.ASCII)
_CLOCK = re.compile(r"([+-]?)(\d+):(\d+)(?::(\d+)(\.\d+)?)?", re.ASCII)


@dataclass
class _Fields:
    """An interval's fields as DecodeInterval builds them (pg_itm_in): months, days and
    microseconds, each exact, and the units given so far (fmask)."""

    months: int = 0
    days: int = 0
    micros: int = 0


def _fract_micros(frac: float, scale: int) -> int:
    """AdjustFractMicroseconds: a fraction of ``scale`` microseconds, its own fraction of one
    rounded half away from... only past a half, as PostgreSQL's C does it in doubles."""
    if frac == 0:
        return 0
    frac *= scale
    usec = int(frac)
    frac -= usec
    if frac > 0.5:
        usec += 1
    elif frac < -0.5:
        usec -= 1
    return usec


def _fract_days(fields: _Fields, frac: float, scale: int) -> None:
    """AdjustFractDays: a fraction of ``scale`` days, its whole days and then its microseconds."""
    if frac == 0:
        return
    frac *= scale
    extra = int(frac)
    fields.days += extra
    frac -= extra
    fields.micros += _fract_micros(frac, DAY_US)


def _amount(fields: _Fields, unit: str, val: int, frac: float) -> None:
    """One amount and its unit (DecodeInterval's DTK_NUMBER), into ``fields``."""
    if unit in _MICRO_SCALES:
        scale = _MICRO_SCALES[unit]
        fields.micros += val * scale + _fract_micros(frac, scale)
    elif unit == "day":
        fields.days += val
        fields.micros += _fract_micros(frac, DAY_US)
    elif unit == "week":
        fields.days += val * 7
        _fract_days(fields, frac, 7)
    elif unit == "month":
        fields.months += val
        _fract_days(fields, frac, 30)
    else:
        scale = _YEAR_SCALES[unit]
        fields.months += val * scale * 12
        # AdjustFractYears: rint, in doubles, of the fraction's months.
        fields.months += int(round(frac * scale * 12))


def _clock(text: str) -> int | None:
    """A clock field's microseconds (DecodeTimeForInterval): h:mm, h:mm:ss or h:mm:ss.ffffff, its
    sign the whole field's. None for a minute or a second past 59, or a fraction PostgreSQL rounds
    to a whole second."""
    matched = _CLOCK.fullmatch(text)
    if matched is None:
        return None
    sign, hours, minutes, seconds, fraction = matched.groups()
    if int(minutes) > 59 or int(seconds or 0) > 59:
        return None
    # ParseFractionalSecond: rint of the fraction's double times a million.
    fsec = int(round(float(fraction) * 1_000_000)) if fraction else 0
    if fsec >= SECOND_US:
        return None
    micros = ((int(hours) * 60 + int(minutes)) * 60 + int(seconds or 0)) * SECOND_US + fsec
    return -micros if sign == "-" else micros


def _tokens(text: str) -> list[str] | None:
    """The fields of an interval's text, as ParseDateTime splits them: words lowercased, an amount
    glued to its unit word apart. None for a field of no form read here."""
    tokens: list[str] = []
    for chunk in text.lower().split():
        glued = _GLUED.fullmatch(chunk)
        if glued is not None:
            tokens.extend(glued.groups())
        elif chunk == "@" or _NUMBER.fullmatch(chunk) or _WORD.fullmatch(chunk):
            tokens.append(chunk)
        elif _CLOCK.fullmatch(chunk):
            tokens.append(chunk)
        else:
            return None
    return tokens


def interval_read(text: str) -> tuple[int, int, int] | None:
    """An interval literal's (months, days, microseconds), as PostgreSQL's interval_in reads it
    under IntervalStyle postgres; None for a form not read here (refused by name), or one
    PostgreSQL raises for (an overflow, a unit given twice)."""
    tokens = _tokens(text)
    if not tokens:
        return None
    if tokens[0] == "@":
        tokens = tokens[1:]
    before = bool(tokens) and tokens[-1] == "ago"
    if before:
        tokens = tokens[:-1]
    if not tokens:
        return None
    fields = _Fields()
    given: set[str] = set()
    at = 0
    while at < len(tokens):
        token = tokens[at]
        micros = _clock(token) if ":" in token else None
        if micros is not None:
            if given & _CLOCK_UNITS:
                return None
            given |= _CLOCK_UNITS
            fields.micros += micros
            at += 1
            continue
        number = _NUMBER.fullmatch(token)
        unit = INPUT_UNITS.get(tokens[at + 1][:10]) if at + 1 < len(tokens) else None
        if number is None or unit is None:
            return None  # a bare number, a word alone, or a word that is no unit here
        whole, fraction = number.groups()
        val = int(whole)
        frac = float(fraction) if fraction else 0.0
        if whole.startswith("-"):
            frac = -frac
        units = {unit}
        if unit == "second" and frac != 0:
            units = set(_ALL_SECONDS)
        if given & units:
            return None  # PostgreSQL's invalid input: a unit given twice
        given |= units
        _amount(fields, unit, val, frac)
        at += 2
    months, days, micros = fields.months, fields.days, fields.micros
    if before:
        months, days, micros = -months, -days, -micros
    if not (I32_MIN <= months <= I32_MAX and I32_MIN <= days <= I32_MAX):
        return None
    if not I64_MIN <= micros <= I64_MAX or _not_finite(months, days, micros):
        return None
    return months, days, micros


def _not_finite(months: int, days: int, micros: int) -> bool:
    """PostgreSQL 17's infinities: the three fields each at its type's end."""
    return (months, days, micros) in (
        (I32_MAX, I32_MAX, I64_MAX),
        (I32_MIN, I32_MIN, I64_MIN),
    )


#: The unit words an amount alone may keep as sqlglot's INTERVAL 'n' UNIT: each engine's own
#: interval literal reads them as PostgreSQL does (a week is no unit of Trino's, nor a fraction).
_KEPT_UNITS = {
    "year": "YEAR",
    "month": "MONTH",
    "day": "DAY",
    "hour": "HOUR",
    "minute": "MINUTE",
    "second": "SECOND",
}
_ONE_AMOUNT = re.compile(r"\s*([+-]?)(\d+)\s*([a-z]+)\s*", re.ASCII)


def one_amount(text: str) -> tuple[str, str] | None:
    """The amount and the unit of an interval's text that is one whole amount of a unit every
    engine's INTERVAL 'n' UNIT reads as PostgreSQL does ('1 day', '3 mons', '-2 hours'), as
    sqlglot writes it: None for any other text."""
    matched = _ONE_AMOUNT.fullmatch(text.lower())
    if matched is None:
        return None
    sign, amount, word = matched.groups()
    unit = _KEPT_UNITS.get(INPUT_UNITS.get(word[:10], ""))
    if unit is None or interval_read(text) is None:
        return None
    return ("-" if sign == "-" else "") + amount, unit


def canonical(months: int, days: int, micros: int) -> str:
    """An interval's text every engine here reads as these fields exactly: an integer amount of
    months, days and microseconds, each given once (DuckDB's interval input, PostgreSQL's, and
    ClickHouse's through ``interval_read``)."""
    return f"{months} months {days} days {micros} microseconds"


__all__ = [
    "DAY_US",
    "HOUR_US",
    "I32_MAX",
    "I32_MIN",
    "I64_MAX",
    "I64_MIN",
    "INPUT_UNITS",
    "MINUTE_US",
    "SECOND_US",
    "canonical",
    "interval_read",
    "one_amount",
]
