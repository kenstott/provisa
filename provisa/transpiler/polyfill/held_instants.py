# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e7a94-2d3b-4f86-9e0a-b47d13c2f685
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A date or a timestamp outside 1900..2299 on ClickHouse, held exactly (REQ-2236, maintainer
2026-11-25: hold rather than refuse; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A
REFUSAL).

ClickHouse's Date32 holds the days 1900-01-01 to 2299-12-31 and clamps any other to its ends; its
DateTime64(6) holds any Int64 count of microseconds from the epoch (fromUnixTimestamp64Micro writes
one as it is, and its comparisons, its order and its Arrow output read the count as it is), but
its calendar functions read only 1900..2299. PostgreSQL's own values are those integers: a date
is a count of days (4714-11-24 BC to 5874897-12-31, infinity at the ends), a timestamp a count of
microseconds (4714-11-24 BC to 294276-12-31 23:59:59.999999, infinity at the ends).

A call whose every argument is a literal (or a NULL), one of them a date or a timestamp outside
1900..2299, a year BC, or an infinity, is read here as the statement is transpiled, as interval
literals are (polyfill.intervals): each date and timestamp as PostgreSQL's integer, the call's
answer computed from them by PostgreSQL's own C rules (date.c, timestamp.c), and written as a
ClickHouse value of the answer's type. A timestamp answer is that count of microseconds as
DateTime64(6, 'UTC') (infinity its Int64 ends but one, as DuckDB marks them); a number, a boolean
and a time of day are themselves. A date answer is a Date32 within 1900..2299; outside it no
ClickHouse type holds a date, so that answer is refused by name. A timestamp past
294247-01-10 04:00:54.775806, the last microsecond an Int64 from 1970 holds (as on DuckDB), is
refused by name. Only a session on UTC is read here: a literal with no offset is read in the
session's zone. Every other call keeps its row's form."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import sqlglot
import sqlglot.expressions as exp

from provisa.core.session_time_zone import current_zone
from provisa.transpiler.polyfill import clocks, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.trino_literals import days_from_civil

FAMILY = "dates and timestamps outside 1900..2299 on ClickHouse"

_CLICKHOUSE = "clickhouse"
DAY_US = 86_400_000_000
#: The days of PostgreSQL's date (IS_VALID_DATE) from 1970-01-01: 4714-11-24 BC, and the first
#: day past 5874897-12-31.
PG_FIRST_DAY = days_from_civil(-4713, 11, 24)
PG_DATE_END = days_from_civil(5874898, 1, 1)
#: The microseconds of PostgreSQL's timestamp (IS_VALID_TIMESTAMP) from 1970: its first, and the
#: first past it (END_TIMESTAMP, 294277-01-01).
PG_FIRST_US = PG_FIRST_DAY * DAY_US
PG_STAMP_END_US = days_from_civil(294277, 1, 1) * DAY_US
#: The last microsecond DateTime64(6) holds from 1970 that is no infinity (2^63 - 1 marks it).
HELD_LAST_US = 2**63 - 2
HELD_LAST = "294247-01-10 04:00:54.775806"
#: PostgreSQL's epoch (2000-01-01) from 1970, in days.
_PG_EPOCH_DAYS = 10957
#: Date32's days from 1970: 1900-01-01 to 2299-12-31.
_DATE32 = (days_from_civil(1900, 1, 1), days_from_civil(2299, 12, 31))
_STAMP32 = (_DATE32[0] * DAY_US, (_DATE32[1] + 1) * DAY_US - 1)


@dataclass(frozen=True)
class Infinite:
    """PostgreSQL's infinity (``sign`` 1) or -infinity (-1) of a date or a timestamp."""

    sign: int


Instant = int | Infinite


# --- reading a literal ------------------------------------------------------------------------

_DATE_TEXT = re.compile(r"\s*(\d{4,})-(\d{1,2})-(\d{1,2})\s*(BC|AD)?\s*", re.ASCII | re.IGNORECASE)
_STAMP_TEXT = re.compile(
    r"\s*(\d{4,})-(\d{1,2})-(\d{1,2})"
    r"(?:(?:\s+|T)(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?"
    r"\s*(?:(Z|UTC|GMT)|([+-])(\d{1,2})(?::?(\d{2}))?(?::?(\d{2}))?)?"
    r"\s*(BC|AD)?\s*",
    re.ASCII | re.IGNORECASE,
)
_CLOCK_TEXT = re.compile(r"\s*(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?\s*", re.ASCII)
_INFINITIES = {"infinity": 1, "+infinity": 1, "-infinity": -1}
_MONTH_DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _month_days(year: int, month: int) -> int:
    return 29 if month == 2 and _leap(year) else _MONTH_DAYS[month - 1]  # noqa: PLR2004


def _day_of(year: str, month: str, day: str, era: str | None) -> int | None:
    """The days from 1970 of a date's fields, its year astronomical where ``era`` is BC; None for
    a day no calendar has."""
    y = int(year)
    if (era or "").upper() == "BC":
        if y == 0:
            return None
        y = 1 - y
    m, d = int(month), int(day)
    if not (1 <= m <= 12 and 1 <= d <= _month_days(y, m)):  # noqa: PLR2004
        return None
    return days_from_civil(y, m, d)


def date_value(text: str) -> Instant | None:
    """A date literal's value as PostgreSQL holds it: its days from 1970, or an infinity. None for
    a text not read here (its form keeps the engine's reading) and for one outside PostgreSQL's
    range, whose 22008 is raised before any form reads it (instants.refuse_past_range)."""
    infinite = _INFINITIES.get(text.strip().lower())
    if infinite is not None:
        return Infinite(infinite)
    read = _DATE_TEXT.fullmatch(text)
    if read is None:
        return None
    days = _day_of(read[1], read[2], read[3], read[4])
    if days is None or not PG_FIRST_DAY <= days < PG_DATE_END:
        return None
    return days


def stamp_value(text: str, zoned: bool) -> Instant | None:
    """A timestamp literal's value as PostgreSQL holds it: its microseconds from 1970 (of a
    timestamptz the instant, its offset read, one with none in UTC), or an infinity. None for a
    text not read here (a fraction past microseconds among them, which PostgreSQL rounds) and for
    one outside PostgreSQL's range."""
    infinite = _INFINITIES.get(text.strip().lower())
    if infinite is not None:
        return Infinite(infinite)
    read = _STAMP_TEXT.fullmatch(text)
    if read is None:
        return None
    days = _day_of(read[1], read[2], read[3], read[13])
    hour, minute, second = int(read[4] or 0), int(read[5] or 0), int(read[6] or 0)
    if days is None or hour > 24 or minute > 59 or second > 59:  # noqa: PLR2004
        return None
    clock = ((hour * 60 + minute) * 60 + second) * 1_000_000 + int((read[7] or "").ljust(6, "0"))
    if clock > DAY_US:
        return None
    micros = days * DAY_US + clock
    if zoned and read[9] is not None:
        offset = (int(read[10]) * 60 + int(read[11] or 0)) * 60 + int(read[12] or 0)
        micros -= (offset if read[9] == "+" else -offset) * 1_000_000
    if not PG_FIRST_US <= micros < PG_STAMP_END_US:
        return None
    return micros


def _clock_value(text: str) -> int | None:
    read = _CLOCK_TEXT.fullmatch(text)
    if read is None:
        return None
    hour, minute, second = int(read[1]), int(read[2]), int(read[3] or 0)
    micros = ((hour * 60 + minute) * 60 + second) * 1_000_000 + int((read[4] or "").ljust(6, "0"))
    if minute > 59 or second > 59 or micros > DAY_US:  # noqa: PLR2004
        return None
    return micros


def civil(days: int) -> tuple[int, int, int]:
    """The proleptic Gregorian date of a count of days from 1970 (Hinnant's civil_from_days), its
    year astronomical (1 BC is 0)."""
    z = days + 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    day = doy - (153 * mp + 2) // 5 + 1
    month = mp + 3 if mp < 10 else mp - 9  # noqa: PLR2004
    return yoe + era * 400 + (month <= 2), month, day  # noqa: PLR2004


def _outside(value: Instant | None, day: bool) -> bool:
    """Whether a value is one ClickHouse's calendar does not read: an infinity, or one outside
    1900..2299."""
    if value is None:
        return False
    if isinstance(value, Infinite):
        return True
    low, high = _DATE32 if day else _STAMP32
    return not low <= value <= high


# --- the arguments of a call ----------------------------------------------------------------------

_DAY_TYPES = frozenset({"date"})
_STAMP_TYPES = frozenset({"timestamp", "timestamp without time zone"})
_ZONED_TYPES = frozenset({"timestamptz", "timestamp with time zone"})
_TIME_TYPES = frozenset({"time", "time without time zone"})
_TEXT_TYPES = frozenset({"text", "varchar", "character varying"})
_INTEGER_TYPES = frozenset({"integer", "int", "int4", "smallint", "bigint"})


@dataclass(frozen=True)
class _Arg:
    """An argument read here: its kind (date, timestamp, timestamptz, time, text, integer) and
    value; ``null`` for a NULL of any type."""

    kind: str
    value: object
    null: bool = False


def _said(node: exp.Expr) -> str | None:
    said = trino.said(node)
    return None if said is None else said.split("(")[0].strip()


def _argument(node: exp.Expr) -> _Arg | None:
    """An argument as a literal of a type read here, or a NULL; None for any other value."""
    value = node.unnest()
    if isinstance(value, exp.Null):
        return _Arg("null", None, null=True)
    said = _said(value) if isinstance(value, exp.Cast) else None
    inner = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(inner, exp.Null):
        return _Arg(said or "null", None, null=True)
    if isinstance(inner, exp.Neg) and isinstance(inner.this, exp.Literal):
        inner = exp.Literal.number(f"-{inner.this.this}")
    if not isinstance(inner, exp.Literal):
        return None
    text = inner.this
    if said is None:
        said = "text" if inner.is_string else "integer"
    if said in _DAY_TYPES:
        day = date_value(text)
        return None if day is None else _Arg("date", day)
    if said in _STAMP_TYPES or said in _ZONED_TYPES:
        zoned = said in _ZONED_TYPES
        stamp = stamp_value(text, zoned)
        return None if stamp is None else _Arg("timestamptz" if zoned else "timestamp", stamp)
    if said in _TIME_TYPES:
        clock = _clock_value(text)
        return None if clock is None else _Arg("time", clock)
    if said in _TEXT_TYPES:
        return _Arg("text", text)
    if said in _INTEGER_TYPES and re.fullmatch(r"\s*[+-]?\d+\s*", text, re.ASCII):
        return _Arg("integer", int(text))
    return None


# --- the answers --------------------------------------------------------------------------------


def _refused(message: str) -> Refused:
    return Refused(Refusal("0A000", message), ruling=True)


def _raise(sqlstate: str, message: str) -> Refused:
    return Refused(Refusal(sqlstate, message), ruling=True)


def _form(text: str) -> exp.Expr:
    return replacement(text, _CLICKHOUSE, "f()").copy()


def _stamp(function: str, micros: Instant) -> exp.Expr:
    """A timestamp answer, held as DateTime64(6, 'UTC') of its microseconds; an infinity at the
    Int64 ends but one."""
    if isinstance(micros, Infinite):
        return _form(f"fromUnixTimestamp64Micro(toInt64({micros.sign * (2**63 - 1)}), 'UTC')")
    if not PG_FIRST_US <= micros < PG_STAMP_END_US:
        raise _raise("22008", "timestamp out of range")
    if micros > HELD_LAST_US:
        raise _refused(
            f"{function}() of a timestamp past {HELD_LAST} has no exact form on clickhouse"
        )
    return _form(f"fromUnixTimestamp64Micro(toInt64({micros}), 'UTC')")


def _date(function: str, days: Instant) -> exp.Expr:
    """A date answer: a Date32 within 1900..2299; any other is held by no ClickHouse type."""
    if not isinstance(days, Infinite) and _DATE32[0] <= days <= _DATE32[1]:
        year, month, day = civil(days)
        return _form(f"toDate32('{year:04d}-{month:02d}-{day:02d}')")
    raise _refused(
        f"{function}() of a date answer outside {_DATE32_BOUND} has no form on clickhouse"
    )


_DATE32_BOUND = "1900-01-01 .. 2299-12-31"


def _float(value: float) -> exp.Expr:
    text = "inf" if value == float("inf") else "-inf" if value == float("-inf") else repr(value)
    return _form(f"CAST('{text}' AS Nullable(Float64))")


def _numeric(value: Decimal) -> exp.Expr:
    _, digits, exponent = value.as_tuple()
    scale = max(0, -int(exponent))
    return _form(
        f"CAST('{value}' AS Nullable(Decimal({max(len(digits), scale + 1, 20)}, {scale})))"
    )


def _integer(value: int) -> exp.Expr:
    return _form(f"CAST({value} AS Nullable(Int32))")


def _boolean(value: bool) -> exp.Expr:
    return _form("true" if value else "false")


def _null(returns: str) -> exp.Expr:
    if returns == "time":
        return clocks.held_null()
    types = {
        "timestamp": "Nullable(DateTime64(6, 'UTC'))", "date": "Nullable(Date32)",
        "float": "Nullable(Float64)", "numeric": "Nullable(Decimal(20, 0))",
        "integer": "Nullable(Int32)", "boolean": "Nullable(Bool)",
    }  # fmt: skip
    return _form(f"CAST(NULL AS {types[returns]})")


# --- PostgreSQL's rules ---------------------------------------------------------------------------


def _cdiv(a: int, b: int) -> int:
    """C's integer division, truncated toward zero."""
    quotient = abs(a) // abs(b)
    return quotient if (a < 0) == (b < 0) else -quotient


def _pg_days(value: Instant) -> int:
    """A date as PostgreSQL's DateADT: days from 2000-01-01, an infinity INT32_MAX or INT32_MIN."""
    if isinstance(value, Infinite):
        return 2**31 - 1 if value.sign > 0 else -(2**31)
    return value - _PG_EPOCH_DAYS


def _pg_micros(value: Instant) -> int:
    """A timestamp as PostgreSQL's Timestamp: microseconds from 2000-01-01, an infinity INT64_MAX
    or INT64_MIN."""
    if isinstance(value, Infinite):
        return 2**63 - 1 if value.sign > 0 else -(2**63)
    return value - _PG_EPOCH_DAYS * DAY_US


@dataclass(frozen=True)
class _Fields:
    """timestamp2tm's fields: the year astronomical (tm_year), the time of day in microseconds."""

    year: int
    month: int
    day: int
    clock: int
    days: int


def _fields(micros: int) -> _Fields:
    days, clock = divmod(micros, DAY_US)
    year, month, day = civil(days)
    return _Fields(year, month, day, clock, days)


#: A unit's text as PostgreSQL's DecodeUnits reads it, by the unit it names.
_UNITS = {
    "microsecond": "microseconds", "microseconds": "microseconds", "us": "microseconds",
    "usec": "microseconds", "usecs": "microseconds", "usecond": "microseconds",
    "useconds": "microseconds",
    "millisecond": "milliseconds", "milliseconds": "milliseconds", "ms": "milliseconds",
    "msec": "milliseconds", "msecs": "milliseconds", "msecond": "milliseconds",
    "mseconds": "milliseconds",
    "second": "second", "seconds": "second", "s": "second", "sec": "second", "secs": "second",
    "minute": "minute", "minutes": "minute", "m": "minute", "min": "minute", "mins": "minute",
    "hour": "hour", "hours": "hour", "h": "hour", "hr": "hour", "hrs": "hour",
    "day": "day", "days": "day", "d": "day",
    "month": "month", "months": "month", "mon": "month", "mons": "month",
    "quarter": "quarter", "qtr": "quarter",
    "year": "year", "years": "year", "y": "year", "yr": "year", "yrs": "year",
    "decade": "decade", "decades": "decade", "dec": "decade", "decs": "decade",
    "century": "century", "centuries": "century", "c": "century", "cent": "century",
    "millennium": "millennium", "millennia": "millennium", "millenniums": "millennium",
    "mil": "millennium", "mils": "millennium",
    "dow": "dow", "isodow": "isodow", "doy": "doy", "epoch": "epoch",
}  # fmt: skip
#: The units whose part of an infinity is that infinity (the rest are NULL of it).
_MONOTONIC = frozenset({"year", "decade", "century", "millennium", "epoch"})
_CLOCK_UNITS = frozenset({"microseconds", "milliseconds", "second", "minute", "hour"})


def _year_part(year: int) -> int:
    return year if year > 0 else year - 1


def _part(unit: str, f: _Fields, micros: int) -> Decimal | None:
    """timestamp_part_common's (and extract's) part of a finite timestamp, exact; None for a unit
    not read here."""
    second, fraction = divmod(f.clock % 60_000_000, 1_000_000)
    parts: dict[str, Callable[[], Decimal]] = {
        "microseconds": lambda: Decimal(second * 1_000_000 + fraction),
        "milliseconds": lambda: Decimal(second * 1_000_000 + fraction).scaleb(-3),
        "second": lambda: Decimal(second * 1_000_000 + fraction).scaleb(-6),
        "minute": lambda: Decimal(f.clock // 60_000_000 % 60),
        "hour": lambda: Decimal(f.clock // 3_600_000_000),
        "day": lambda: Decimal(f.day),
        "month": lambda: Decimal(f.month),
        "quarter": lambda: Decimal((f.month - 1) // 3 + 1),
        "year": lambda: Decimal(_year_part(f.year)),
        "decade": lambda: Decimal(
            _cdiv(f.year, 10) if f.year >= 0 else -_cdiv(8 - (f.year - 1), 10)
        ),
        "century": lambda: Decimal(
            _cdiv(f.year + 99, 100) if f.year > 0 else -_cdiv(99 - (f.year - 1), 100)
        ),
        "millennium": lambda: Decimal(
            _cdiv(f.year + 999, 1000) if f.year > 0 else -_cdiv(999 - (f.year - 1), 1000)
        ),
        "dow": lambda: Decimal((f.days + 4) % 7),
        "isodow": lambda: Decimal((f.days + 3) % 7 + 1),
        "doy": lambda: Decimal(f.days - days_from_civil(f.year, 1, 1) + 1),
        "epoch": lambda: Decimal(micros).scaleb(-6),
    }
    read = parts.get(unit)
    return None if read is None else read()


def _unit(arg: _Arg) -> str | None:
    if arg.kind != "text" or not isinstance(arg.value, str):
        return None
    return _UNITS.get(arg.value.strip().lower())


def _date_part(function: str, args: list[_Arg]) -> exp.Expr | None:
    """date_part(text, timestamp[tz]) and of a date (read as its timestamp, date_part's SQL
    function): a double. extract(text, ...): a numeric, a date's of its own fields."""
    unit, value = _unit(args[0]), args[1]
    if unit is None or value.kind not in ("date", "timestamp", "timestamptz"):
        return None
    numeric = function == "extract"
    returns = "numeric" if numeric else "float"
    if isinstance(value.value, Infinite):
        if unit not in _MONOTONIC:
            return _null(returns)
        if numeric:
            raise _refused(
                f"{function}() of an infinite value answers an infinite numeric, which has no "
                "form on clickhouse"
            )
        return _float(float("inf") if value.value.sign > 0 else float("-inf"))
    assert isinstance(value.value, int)
    is_date = value.kind == "date"
    micros = value.value * DAY_US if is_date else value.value
    if is_date and numeric and unit in _CLOCK_UNITS:
        raise _raise("0A000", f'unit "{args[0].value}" not supported for type date')
    part = _part(unit, _fields(micros), micros)
    if part is None:
        return None
    if numeric and is_date:
        part = part.quantize(Decimal(1)) if unit == "epoch" else part
    return _numeric(part) if numeric else _float(float(part))


def _truncated(unit: str, f: _Fields) -> int | None:
    """timestamp_trunc's start of ``unit`` (its fall-through of fields), in microseconds."""
    year, month, day, clock = f.year, f.month, f.day, f.clock
    order = ["millennium", "century", "decade", "year", "quarter", "month", "day", "hour",
             "minute", "second", "milliseconds", "microseconds"]  # fmt: skip
    if unit not in order:
        return None
    at = order.index(unit)
    if unit == "millennium":
        year = (
            (_cdiv(year + 999, 1000) * 1000 - 999)
            if year > 0
            else (-_cdiv(999 - (year - 1), 1000) * 1000 + 1)
        )
    if unit == "century":
        year = (
            (_cdiv(year + 99, 100) * 100 - 99)
            if year > 0
            else (-_cdiv(99 - (year - 1), 100) * 100 + 1)
        )
    if unit == "decade":
        year = _cdiv(year, 10) * 10 if year > 0 else -_cdiv(8 - (year - 1), 10) * 10
    if at <= order.index("year"):
        month = 1
    if at <= order.index("quarter"):
        month = 3 * ((month - 1) // 3) + 1
    if at <= order.index("month"):
        day = 1
    if at <= order.index("day"):
        clock = 0
    elif at <= order.index("hour"):
        clock -= clock % 3_600_000_000
    elif at <= order.index("minute"):
        clock -= clock % 60_000_000
    elif at <= order.index("second"):
        clock -= clock % 1_000_000
    elif unit == "milliseconds":
        clock -= clock % 1000
    return days_from_civil(year, month, day) * DAY_US + clock


def _date_trunc(function: str, args: list[_Arg]) -> exp.Expr | None:
    unit, value = _unit(args[0]), args[1]
    if unit is None or value.kind not in ("timestamp", "timestamptz"):
        return None
    if isinstance(value.value, Infinite):
        return _stamp(function, value.value)
    assert isinstance(value.value, int)
    start = _truncated(unit, _fields(value.value))
    return None if start is None else _stamp(function, start)


def _as_stamp(value: _Arg) -> Instant:
    """A date or a timestamp as a timestamp (date2timestamp): a date at its midnight."""
    if value.kind == "date" and isinstance(value.value, int):
        return value.value * DAY_US
    assert isinstance(value.value, (int, Infinite))
    return value.value


def _with_time(function: str, day: _Arg, time: _Arg) -> exp.Expr | None:
    """A date and a time of day, a timestamp (datetime_timestamp): an infinite date's infinity."""
    if day.kind != "date" or time.kind != "time":
        return None
    if isinstance(day.value, Infinite):
        return _stamp(function, day.value)
    assert isinstance(day.value, int) and isinstance(time.value, int)
    micros = day.value * DAY_US + time.value
    if not micros < PG_STAMP_END_US:
        raise _raise("22008", "timestamp out of range")
    return _stamp(function, micros)


def _moved(function: str, day: _Arg, days: _Arg, sign: int) -> exp.Expr | None:
    """date_pli and date_mii: an infinite date is itself; past a date's range 22008."""
    if day.kind != "date" or days.kind != "integer":
        return None
    if isinstance(day.value, Infinite):
        return _date(function, day.value)
    assert isinstance(day.value, int) and isinstance(days.value, int)
    moved = day.value + sign * days.value
    if not PG_FIRST_DAY <= moved < PG_DATE_END:
        raise _raise("22008", "date out of range")
    return _date(function, moved)


def _difference(day_a: _Arg, day_b: _Arg) -> exp.Expr | None:
    if day_a.kind != "date" or day_b.kind != "date":
        return None
    if isinstance(day_a.value, Infinite) or isinstance(day_b.value, Infinite):
        raise _raise("22008", "cannot subtract infinite dates")
    assert isinstance(day_a.value, int) and isinstance(day_b.value, int)
    return _integer(day_a.value - day_b.value)


def _subdiff(kind: str, a: _Arg, b: _Arg) -> exp.Expr | None:
    """daterange_subdiff and ts(tz)range_subdiff: PostgreSQL's integers subtracted as doubles."""
    if a.kind != kind or b.kind != kind:
        return None
    if kind == "date":
        assert isinstance(a.value, (int, Infinite)) and isinstance(b.value, (int, Infinite))
        return _float(float(_pg_days(a.value)) - float(_pg_days(b.value)))
    assert isinstance(a.value, (int, Infinite)) and isinstance(b.value, (int, Infinite))
    return _float((float(_pg_micros(a.value)) - float(_pg_micros(b.value))) / 1_000_000.0)


def _clock_of(value: _Arg) -> exp.Expr | None:
    """time(timestamp): its time of day; NULL of an infinity."""
    if value.kind not in ("timestamp", "timestamptz"):
        return None
    if isinstance(value.value, Infinite):
        return clocks.held_null()
    assert isinstance(value.value, int)
    return clocks.held_literal(value.value % DAY_US)


def _day_of_stamp(function: str, value: _Arg) -> exp.Expr | None:
    """date(timestamp[tz]): its day (timestamp2date floors), an infinity's infinity."""
    if value.kind not in ("timestamp", "timestamptz"):
        return None
    if isinstance(value.value, Infinite):
        return _date(function, value.value)
    assert isinstance(value.value, int)
    return _date(function, value.value // DAY_US)


def _same_instant(function: str, value: _Arg, kinds: tuple[str, ...]) -> exp.Expr | None:
    """A cast between a date, a timestamp and a timestamptz in a session on UTC: the same count."""
    if value.kind not in kinds:
        return None
    return _stamp(function, _as_stamp(value))


def _finite(value: _Arg) -> exp.Expr | None:
    if value.kind not in ("date", "timestamp", "timestamptz"):
        return None
    return _boolean(not isinstance(value.value, Infinite))


def _answer(function: str, args: list[_Arg]) -> exp.Expr | None:  # noqa: C901, PLR0911, PLR0912
    """PostgreSQL's answer of ``function`` of ``args``; None for a call not read here."""
    count = len(args)
    if function == "isfinite" and count == 1:
        return _finite(args[0])
    if function == "date_mi" and count == 2:  # noqa: PLR2004
        return _difference(args[0], args[1])
    if function in ("date_pli", "date_mii") and count == 2:  # noqa: PLR2004
        return _moved(function, args[0], args[1], 1 if function == "date_pli" else -1)
    if function == "integer_pl_date" and count == 2:  # noqa: PLR2004
        return _moved(function, args[1], args[0], 1)
    if function in ("date_part", "extract") and count == 2:  # noqa: PLR2004
        return _date_part(function, args)
    if function == "date_trunc" and count == 2:  # noqa: PLR2004
        return _date_trunc(function, args)
    if function in ("timestamp", "timestamptz") and count == 1:
        return _same_instant(function, args[0], ("date", "timestamp", "timestamptz"))
    if function in ("timestamp", "timestamptz", "datetime_pl") and count == 2:  # noqa: PLR2004
        return _with_time(function, args[0], args[1])
    if function == "timedate_pl" and count == 2:  # noqa: PLR2004
        return _with_time(function, args[1], args[0])
    if function == "timezone" and count == 1:
        return _same_instant(function, args[0], ("timestamptz", "timestamp"))
    if function == "timezone" and count == 2 and args[0].kind == "text":  # noqa: PLR2004
        zone = str(args[0].value).strip().upper()
        if zone not in ("UTC", "GMT", "Z", "ETC/UTC"):
            return None
        return _same_instant(function, args[1], ("timestamptz", "timestamp"))
    if function == "time" and count == 1:
        return _clock_of(args[0])
    if function == "date" and count == 1:
        return _day_of_stamp(function, args[0])
    if function in _SUBDIFFS and count == 2:  # noqa: PLR2004
        return _subdiff(_SUBDIFFS[function], args[0], args[1])
    return None


_SUBDIFFS = {
    "daterange_subdiff": "date",
    "tsrange_subdiff": "timestamp",
    "tstzrange_subdiff": "timestamptz",
}
#: The functions read here, and what each answers of a NULL (each is STRICT).
_RETURNS = {
    "isfinite": "boolean", "date_mi": "integer", "date_pli": "date", "date_mii": "date",
    "integer_pl_date": "date", "date_part": "float", "extract": "numeric",
    "date_trunc": "timestamp", "timestamp": "timestamp", "timestamptz": "timestamp",
    "datetime_pl": "timestamp", "timedate_pl": "timestamp", "timezone": "timestamp",
    "time": "time", "date": "date", "daterange_subdiff": "float", "tsrange_subdiff": "float",
    "tstzrange_subdiff": "float",
}  # fmt: skip


def held(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call of literals, one a date or a timestamp outside 1900..2299 or an infinity: its
    answer, computed as PostgreSQL computes it from its integers. None for any other node."""
    if dialect != _CLICKHOUSE or current_zone.get() is not None:
        return None
    call = trino.called(node)
    if call is None:
        return None
    names = call[0] & _RETURNS.keys()
    if len(names) != 1:
        return None
    (function,) = names
    args = [_argument(a) for a in call[1]]
    if any(a is None for a in args):
        return None
    read = [a for a in args if a is not None]
    if not any(
        isinstance(a.value, (int, Infinite)) and _outside(a.value, a.kind == "date")
        for a in read
        if a.kind in ("date", "timestamp", "timestamptz") and not a.null
    ):
        return None
    if any(a.null for a in read):
        return _null(_RETURNS[function])
    return _answer(function, read)


def folded(sql: str, tree: exp.Expr | None) -> exp.Expr | None:
    """``tree`` (the parse of ``sql``, or None for it not yet parsed) with each call :func:`held`
    answers in its ClickHouse form, read before any step reads an instant of it (zoned_instants
    writes date_trunc's ClickHouse form, which reads its value by ClickHouse's calendar); ``tree``
    as it was where no call is read. Each form is ClickHouse's SQL already (zoned_instants.WRITTEN),
    as a zoned form is."""
    from provisa.transpiler import zoned_instants  # it reads this package's families

    if re.search(_MENTIONS, sql, re.IGNORECASE) is None:
        return tree
    parsed = tree if tree is not None else sqlglot.parse_one(sql, read="postgres")
    wrote = False
    from provisa.transpiler.function_macros import RowRefused

    for node in reversed(list(parsed.find_all(exp.Func))):  # the deepest first
        try:
            answer = held(node, _CLICKHOUSE)
        except Refused as refused:
            call = trino.called(node)
            name = min(call[0] & _RETURNS.keys()) if call is not None else "held"
            raise RowRefused(name, _CLICKHOUSE, refused.refusal) from refused
        if answer is None:
            continue
        answer.meta[zoned_instants.WRITTEN] = True
        if node.parent is not None and isinstance(node.parent, exp.Dot):
            node.parent.replace(answer)  # pg_catalog."f"(...): the qualified call
        else:
            node.replace(answer)
        wrote = True
    return parsed if wrote else tree


#: What a statement this reads holds: a year outside 1900..2299 (of five digits, or of four before
#: 1900 or after 2299), a year BC, or an infinity.
_MENTIONS = r"'\s*(?:\d{5,}|0\d{3}|1[0-8]\d\d|2[3-9]\d\d|[3-9]\d{3})-|\bBC\b|infinity'"

_STATED = (
    "Of a call whose every argument is a literal or a NULL, one a date or a timestamp outside "
    "1900..2299, a year BC or an infinity, on a session on UTC: each value is read as "
    "PostgreSQL's integer (a date's days, a timestamp's microseconds) and the answer computed as "
    "PostgreSQL computes it, exact: isfinite, date_mi, date_pli, date_mii, integer_pl_date, "
    "date_part and extract (microseconds to millennium, dow, isodow, doy, epoch), date_trunc "
    "(microseconds to millennium), timestamp, timestamptz, datetime_pl, timedate_pl, timezone "
    "(of UTC), time, date, and the range subdiff functions. A timestamp answer is held as "
    "DateTime64(6, 'UTC') of its microseconds, an infinity at the Int64 ends; one past "
    f"{HELD_LAST} raises 0A000. A date answer outside {_DATE32_BOUND} raises 0A000: no "
    "ClickHouse type holds that date. An infinite extract (a numeric) raises 0A000."
)

FEATURES: tuple[Feature, ...] = (
    Feature(
        "a date or timestamp outside 1900..2299 on clickhouse",
        FAMILY,
        "A call of literals, one a date or a timestamp ClickHouse's calendar does not read.",
        read=Side(
            rules=(
                Rule(
                    None,
                    {_CLICKHOUSE: Rewrite(held, mentions=_MENTIONS, first=True)},
                ),
            ),
            limitations={_CLICKHOUSE: (Limitation(_STATED),)},
        ),
    ),
)

#: The family's place in the grid (polyfill.GRID): read before any row reads its call.
ORDER = 15
