# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f84c0-3b9e-4e16-a5c7-9e0b61d3f428
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dates on Trino (REQ-2236; the function support measure, run 43307392116, REQ-2171).

Trino's difference of two dates is an interval, which it casts to no integer, and it adds no
integer to a date: the old table's forms (provisa.transpiler.pg_forms_types) of date_mi, date_pli
and date_mii count days so. Trino's forms count them with date_diff and date_add. time_mi_time's
old form adds a time to a date, which Trino does not: it is refused by name there. make_date and
transaction_timestamp have no Trino function of the name: each is Trino's form here. Nor has
ClickHouse (run 43410717058): make_date is its makeDate32 of a year Date32 holds (1900 to 2299) and
a day the month has, any other refused by name; transaction_timestamp is the statement's now64, as
current_timestamp is there.

date_pli, date_mii and integer_pl_date on ClickHouse: the old table's form moved the day in a Date
(1970 to 2149), which wrapped any other day (date_pli('2299-12-31', 1) answered 2120-07-27), and
integer_pl_date's was DuckDB's, refused by name there. Each moves the day in Date32: past a date's
range PostgreSQL's 22008, within it but outside Date32's 1900-01-01 .. 2299-12-31 0A000.

to_char of a timestamp on ClickHouse and Trino, and of an instant on ClickHouse on a session on
UTC, is written from PostgreSQL's own tokens (provisa.transpiler.to_char,
instants.to_char_written)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import datetime
import math
import re

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import raising
from provisa.transpiler.polyfill import instants, trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite
from provisa.transpiler.polyfill.numbers import ch_fail, fail, refused

FAMILY = "dates"

#: The days a date holds, from 4713 BC, as date2j counts them from 2000-01-01.
_DATE_DAYS = (-2451545, 2145031948)
_DAYS_FROM_2000 = "date_diff('day', DATE '2000-01-01', CAST($a AS DATE))"


def _moved(sign: str) -> str:
    """date_pli or date_mii: the day moved, or PostgreSQL's refusal of one past a date's range."""
    days = f"({_DAYS_FROM_2000} {sign} CAST($b AS BIGINT))"
    return (
        f"CASE WHEN {days} < {_DATE_DAYS[0]} OR {days} >= {_DATE_DAYS[1]} "
        f"THEN {fail('22008', 'date out of range')} "
        f"ELSE date_add('day', {'' if sign == '+' else '-'}CAST($b AS BIGINT), CAST($a AS DATE)) END"
    )


DATE_MI = Feature(
    "date_mi",
    FAMILY,
    "The days from one date to another.",
    read=Side(
        rules=(
            Rule(
                "date_mi($a, $b)",
                {
                    **trino.trino_only(
                        "CAST(date_diff('day', CAST($b AS DATE), CAST($a AS DATE)) AS INTEGER)"
                    ),
                    # DuckDB subtracts an infinity's sentinel; PostgreSQL refuses it (REQ-2236).
                    "duckdb": (
                        "CASE WHEN isinf(CAST($a AS DATE)) OR isinf(CAST($b AS DATE)) THEN "
                        + raising("22008", "'cannot subtract infinite dates'")
                        + " ELSE CAST(CAST($a AS DATE) - CAST($b AS DATE) AS INTEGER) END"
                    ),
                },
            ),
        )
    ),
)

#: date_bin on DuckDB: its time_bucket, which answers NULL of an infinite origin. PostgreSQL
#: (timestamp_bin, timestamptz_bin) answers an infinite source as it is, and then refuses an
#: infinite origin (REQ-2236).
DATE_BIN = Feature(
    "date_bin",
    FAMILY,
    "A timestamp truncated to a stride counted from an origin.",
    read=Side(
        rules=(
            Rule(
                "date_bin($s, $t, $o)",
                {
                    "duckdb": (
                        "CASE WHEN isinf($t) THEN $t WHEN isinf($o) THEN "
                        + raising("22008", "'origin out of range'")
                        + " ELSE time_bucket($s, $t, $o) END"
                    ),
                },
                unlisted_keep=True,
            ),
        )
    ),
)
#: The days from 1970-01-01 a date holds (IS_VALID_DATE): from 4714-11-24 BC to 5874897-12-31.
_EPOCH_DAYS = (-2440588, 2145042906)
#: ClickHouse's Date32, the days from 1970-01-01 it holds exactly: 1900-01-01 to 2299-12-31.
DATE32_DAYS = (-25567, 120529)
DATE32_BOUND = "1900-01-01 .. 2299-12-31"


def ch_moved(name: str, sign: str, date: str = "$a", days: str = "$b") -> str:
    """date_pli, date_mii or integer_pl_date on ClickHouse: the day moved in Date32, exact; a day
    past a date's range raises PostgreSQL's 22008, and one within it that Date32 does not hold
    exactly raises 0A000. The old table's form moved the day in a Date (1970 to 2149), which wrapped
    a day outside it."""
    moved = f"(toInt64(toInt32(toDate32({date}))) {sign} toInt64({days}))"
    past = f"({moved} < {_EPOCH_DAYS[0]} OR {moved} >= {_EPOCH_DAYS[1]})"
    unheld = f"({moved} < {DATE32_DAYS[0]} OR {moved} > {DATE32_DAYS[1]})"
    return (
        f"toDate32({date}) {sign} toInt32({days}) + "
        + ch_fail("22008", "date out of range").format(condition=past)
        + " + "
        + ch_fail("0A000", instants.out_of_bound(name, DATE32_BOUND)).format(
            condition=f"NOT {past} AND {unheld}"
        )
    )


def ch_moved_stated(name: str) -> str:
    """The limitation of a row whose ClickHouse form is :func:`ch_moved`."""
    return (
        f"A day moved past a date's range raises 22008, as PostgreSQL does; a day within it but "
        f"outside {DATE32_BOUND} (ClickHouse's Date32) raises 0A000 "
        f"({instants.out_of_bound(name, DATE32_BOUND)})."
    )


DATE_PLI = Feature(
    "date_pli",
    FAMILY,
    "A date moved forward by a number of days.",
    read=Side(
        rules=(
            Rule(
                "date_pli($a, $b)",
                trino.trino_only(_moved("+")) | {"clickhouse": ch_moved("date_pli", "+")},
            ),
        ),
        limitations={"clickhouse": (Limitation(ch_moved_stated("date_pli")),)},
    ),
)
DATE_MII = Feature(
    "date_mii",
    FAMILY,
    "A date moved back by a number of days.",
    read=Side(
        rules=(
            Rule(
                "date_mii($a, $b)",
                trino.trino_only(_moved("-")) | {"clickhouse": ch_moved("date_mii", "-")},
            ),
        ),
        limitations={"clickhouse": (Limitation(ch_moved_stated("date_mii")),)},
    ),
)
#: integer_pl_date: PostgreSQL's SQL function of date_pli, its arguments the other way round. On
#: ClickHouse the old table's form is DuckDB's (isfinite), so the call was refused by name there.
INTEGER_PL_DATE = Feature(
    "integer_pl_date",
    FAMILY,
    "A number of days plus a date (+).",
    read=Side(
        rules=(
            Rule(
                "integer_pl_date($n, $d)",
                {
                    **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                    "clickhouse": ch_moved("integer_pl_date", "+", date="$d", days="$n"),
                },
            ),
        ),
        limitations={"clickhouse": (Limitation(ch_moved_stated("integer_pl_date")),)},
    ),
)
TIME_MI_TIME = trino.by_name(
    "time_mi_time",
    FAMILY,
    "The interval from one time of day to another.",
    "Refused by name: the old table's form adds a time to a date, which Trino does not.",
)

#: A date of its parts, from 0001-01-01: Trino's dates are the proleptic Gregorian calendar's, as
#: PostgreSQL's are.
_FIRST_OF_MONTH = "date_add('month', $m - 1, date_add('year', $y - 1, DATE '0001-01-01'))"

#: ClickHouse's make_date: makeDate32 of a year Date32 holds and a day the month has.
_CH_MAKE_DATE = (
    "CASE WHEN $y IS NULL OR $m IS NULL OR $d IS NULL THEN CAST(NULL AS Nullable(Date32)) "
    "WHEN $y BETWEEN 1900 AND 2299 AND $m BETWEEN 1 AND 12 "
    "AND $d BETWEEN 1 AND toDayOfMonth(toLastDayOfMonth(makeDate32($y, $m, 1))) "
    "THEN makeDate32($y, $m, $d) "
    "ELSE CAST(throwIf(1, 'postgresql-error 0A000: make_date does not read this value"
    "<end of postgresql-error>') AS Nullable(Date32)) END"
)

MAKE_DATE = Feature(
    "make_date",
    FAMILY,
    "A date of a year, a month and a day.",
    read=Side(
        rules=(
            Rule(
                "make_date($y, $m, $d)",
                trino.trino_only(
                    "CASE WHEN $y IS NULL OR $m IS NULL OR $d IS NULL THEN CAST(NULL AS DATE) "
                    "WHEN $y BETWEEN 1 AND 9999 AND $m BETWEEN 1 AND 12 THEN "
                    f"CASE WHEN $d BETWEEN 1 AND day_of_month(last_day_of_month({_FIRST_OF_MONTH}))"
                    f" THEN date_add('day', $d - 1, {_FIRST_OF_MONTH}) "
                    f"ELSE {refused('make_date')} END "
                    f"ELSE {refused('make_date')} END"
                )
                | {"clickhouse": _CH_MAKE_DATE},
            ),
        ),
        limitations={
            **dict.fromkeys(
                trino.TRINO,
                (
                    Limitation(
                        "Only a year from 1 to 9999 and a day the month has: any other is refused "
                        "with 0A000, where PostgreSQL answers a year BC or past 9999, and refuses a "
                        "day the month lacks with 22008."
                    ),
                ),
            ),
            "clickhouse": (
                Limitation(
                    "Only a year from 1900 to 2299 (ClickHouse's Date32) and a day the month has: "
                    "any other is refused with 0A000."
                ),
            ),
        },
    ),
)

TRANSACTION_TIMESTAMP = Feature(
    "transaction_timestamp",
    FAMILY,
    "The time the transaction began: now(), as Provisa answers it.",
    read=Side(
        rules=(
            Rule(
                "transaction_timestamp()",
                {**trino.trino_only("current_timestamp"), "clickhouse": "now64(6, 'UTC')"},
            ),
        ),
    ),
)

CLOCK_TIMESTAMP = Feature(
    "clock_timestamp",
    FAMILY,
    "The time now, read as the statement runs.",
    read=Side(
        rules=(
            Rule(
                "clock_timestamp()",
                {
                    **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                    "clickhouse": "now64(6, 'UTC')",
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "The time the statement began (now64): ClickHouse has no clock read again for "
                    "each row, so every row of a statement reads the same instant."
                ),
            )
        },
    ),
)

#: float8in's text: a decimal number, an exponent, NaN or infinity, white space around.
_FLOAT_TEXT = re.compile(
    r"\s*[+-]?(?:(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|nan|inf|infinity)\s*", re.I
)
_POSTGRES_EPOCH = datetime.datetime(2000, 1, 1)
_POSTGRES_EPOCH_SECONDS = 946684800.0


def _seconds(node: exp.Expr, dialect: str) -> float | None:
    """A double the statement writes: its value, None for a NULL; 22P02 of a text float8in does
    not read; refused by name for any other value."""
    value = node.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    negative = False
    while isinstance(value, (exp.Neg, exp.Paren)):
        negative ^= isinstance(value, exp.Neg)
        value = value.this.unnest()
        while isinstance(value, exp.Cast):
            value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    if not isinstance(value, exp.Literal):
        raise Refused(trino.no_form("to_timestamp", dialect))
    if value.is_string and not _FLOAT_TEXT.fullmatch(value.this):
        raise Refused(
            Refusal("22P02", f'invalid input syntax for type double precision: "{value.this}"'),
            ruling=True,
        )
    number = float(value.this.strip())
    return -number if negative else number


def unix_seconds(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """to_timestamp of a double the statement writes (float8_timestamptz): the instant that many
    seconds after 1970-01-01 UTC, its microseconds rounded half to even, computed as the statement
    is read; 22008 of NaN; an instant outside DateTime64's bound, infinity's included, 0A000."""
    if dialect != "clickhouse" or not isinstance(node, exp.UnixToTime):
        return None
    if node.args.get("scale") is not None or node.args.get("zone") is not None:
        return None
    seconds = _seconds(node.this, dialect)
    if seconds is None:
        return sqlglot.parse_one(f"CAST(NULL AS {instants.INSTANT})", read="clickhouse")
    if math.isnan(seconds):
        raise Refused(Refusal("22008", "timestamp cannot be NaN"), ruling=True)
    outside = Refusal(
        "0A000",
        f"to_timestamp() of a value outside {instants.BOUND} has no exact form on clickhouse",
    )
    if math.isinf(seconds) or abs(seconds) > 1e13:
        raise Refused(outside, ruling=True)
    # As timestamp.c computes it: seconds after 2000-01-01 first, then scaled and rint (half to
    # even), so the double rounds where PostgreSQL's does.
    since_2000 = seconds - _POSTGRES_EPOCH_SECONDS
    stamp = _POSTGRES_EPOCH + datetime.timedelta(microseconds=round(since_2000 * 1_000_000.0))
    if not instants.FIRST <= stamp <= instants.LAST:
        raise Refused(outside, ruling=True)
    text = stamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    return sqlglot.parse_one(f"CAST('{text}' AS {instants.INSTANT})", read="clickhouse")


_STAMPS = frozenset({"timestamp without time zone", "timestamp"})


def _precision(node: exp.Expr) -> int | None | bool:
    """A precision the statement writes: its value, None for a NULL, False for any other value."""
    value = node.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    negative = False
    while isinstance(value, (exp.Neg, exp.Paren)):
        negative ^= isinstance(value, exp.Neg)
        value = value.this.unnest()
        while isinstance(value, exp.Cast):
            value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    text = value.this.strip() if isinstance(value, exp.Literal) else ""
    if not text.lstrip("+-").isdigit():
        return False
    return -int(text) if negative else int(text)


def stamp_scaled(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """timestamp of a timestamp and a precision (timestamp_scale, AdjustTimestampForTypmod): its
    microseconds since 2000-01-01 rounded half away from zero to the precision's digits; 22023 of
    a precision outside 0 to 6 where the timestamp is not NULL; NULL of a NULL."""
    if dialect != "clickhouse":
        return None
    call = trino.called(node)
    if call is None or "timestamp" not in call[0] or len(call[1]) != 2:
        return None
    stamp, precision = call[1]
    if trino.said(stamp) not in _STAMPS:
        return None
    digits = _precision(precision)
    if digits is False:
        raise Refused(trino.no_form("timestamp", dialect))
    if digits is None:
        return sqlglot.parse_one(f"CAST(NULL AS {instants.INSTANT})", read="clickhouse")
    assert isinstance(digits, int)
    if not 0 <= digits <= 6:
        raised = ch_fail("22023", f"timestamp({digits}) precision must be between 0 and 6").format(
            condition="provisa_s IS NOT NULL"
        )
        form = f"if({raised} = 0, CAST(NULL AS {instants.INSTANT}), NULL)"
    else:
        scale = 10 ** (6 - digits)
        since = "(toUnixTimestamp64Micro(provisa_s) - 946684800000000)"
        rounded = (
            f"if({since} >= 0, intDiv({since} + {scale // 2}, {scale}) * {scale}, "
            f"-(intDiv(-{since} + {scale // 2}, {scale}) * {scale}))"
        )
        form = (
            f"CAST(fromUnixTimestamp64Micro({rounded} + 946684800000000, 'UTC') "
            f"AS {instants.INSTANT})"
        )
    written = sqlglot.parse_one(form, read="clickhouse")
    return written.transform(
        lambda n: stamp.copy() if isinstance(n, exp.Column) and n.name == "provisa_s" else n
    )


STAMP_SCALED = Feature(
    "timestamp(timestamp, integer)",
    FAMILY,
    "A timestamp rounded to a precision (timestamp_scale).",
    read=Side(
        rules=(Rule(None, {"clickhouse": Rewrite(stamp_scaled, mentions=r"\btimestamp\s*\(")}),),
        limitations={
            "clickhouse": (
                Limitation(
                    "The precision is read as a literal or a NULL; any other is refused by name. "
                    "The timestamp is held as DateTime64(6, 'UTC'); its microseconds since "
                    "2000-01-01 are rounded half away from zero, as AdjustTimestampForTypmod "
                    "rounds them."
                ),
            )
        },
    ),
)


_UTC_NAMES = frozenset({"UTC", "Etc/UTC", "GMT", "Etc/GMT", "UCT", "Zulu", "Universal"})


def _session_is_utc() -> bool:
    from provisa.core.session_time_zone import current_zone

    zone = current_zone.get()
    return zone is None or zone.name in _UTC_NAMES or zone.offset in ("+00:00", "+00")


_DAYS = frozenset({"date"})
_CLOCKS = frozenset({"time without time zone", "time"})


def _day_read(node: exp.Expr, dialect: str) -> exp.Expr:
    """A date argument as the statement's own date literal is read (clickhouse.dated: 0A000 of
    one outside Date32's years or not ISO, 22008 of a day that does not exist): what a rule
    writes is not walked again."""
    from provisa.transpiler.polyfill import clickhouse

    try:
        read = clickhouse.dated(node.unnest(), dialect)
    except Refused as refused:
        raise Refused(refused.refusal, ruling=True) from refused
    return node.copy() if read is None else read


def _written_day(node: exp.Expr) -> bool:
    inner = node.unnest()
    return isinstance(inner, exp.Anonymous) and inner.name == "toDate32"


_LAST_US = (instants.LAST - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)


def _day_and_clock(day: exp.Expr, clock: exp.Expr) -> exp.Expr:
    """A written date (Date32) and a held time (Int64 microseconds) as the instant of their wall
    time in UTC; one past DateTime64's bound (2299-12-31 24:00:00) raises 0A000."""
    sum_us = "(toInt64(provisa_d) * 86400000000 + toInt64(provisa_c))"
    raised = ch_fail(
        "0A000",
        f"timestamptz() of a value outside {instants.BOUND} has no exact form on clickhouse",
    ).format(condition=f"{sum_us} > {_LAST_US}")
    form = f"CAST(fromUnixTimestamp64Micro({sum_us} + {raised}, 'UTC') AS {instants.INSTANT})"
    written = sqlglot.parse_one(form, read="clickhouse")
    nodes = {"provisa_d": day, "provisa_c": clock}
    return written.transform(
        lambda n: nodes[n.name].copy() if isinstance(n, exp.Column) and n.name in nodes else n
    )


def instant_of_wall(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """timestamptz of a date, a timestamp, or a date and a time: the wall time read in the
    session's zone, the instant it is; as a cast to timestamptz of it (or of their sum), which
    ClickHouse is told as its instant. In the session zone UTC only: another zone's wall time is
    refused by name."""
    if dialect != "clickhouse":
        return None
    call = trino.called(node)
    if call is None or "timestamptz" not in call[0] or not 1 <= len(call[1]) <= 2:
        return None
    said = [trino.said(a) for a in call[1]]
    if len(said) == 2 and said[0] not in _DAYS and _written_day(call[1][0]):
        # the arguments as the pattern row read them (a date's toDate32, a held time)
        if not _session_is_utc():
            raise Refused(trino.no_form("timestamptz", dialect))
        return _day_and_clock(call[1][0], call[1][1])
    if not (
        (len(said) == 1 and said[0] in _DAYS | _STAMPS)
        or (len(said) == 2 and said[0] in _DAYS and said[1] in _CLOCKS)
    ):
        return None
    args = [_day_read(a, dialect) if s == "date" else a.copy() for a, s in zip(call[1], said)]
    value = args[0] if len(args) == 1 else exp.Paren(this=exp.Add(this=args[0], expression=args[1]))
    if not _session_is_utc():
        raise Refused(trino.no_form("timestamptz", dialect))
    return exp.Cast(this=value, to=exp.DataType.build("timestamptz", dialect="postgres"))


INSTANT_OF_WALL = Feature(
    "timestamptz(date|timestamp|date, time)",
    FAMILY,
    "The instant a date, a timestamp, or a date and a time is in the session's zone.",
    read=Side(
        rules=(
            Rule(None, {"clickhouse": Rewrite(instant_of_wall, mentions=r"\btimestamptz\s*\(")}),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "In the session TimeZone UTC, where the wall time is the instant: in any "
                    "other zone refused by name. The answer is held as DateTime64(6, 'UTC'); one "
                    f"outside {instants.BOUND} raises 0A000."
                ),
            )
        },
    ),
)


UNIX_SECONDS = Feature(
    "to_timestamp(double precision)",
    FAMILY,
    "The instant a count of seconds after 1970-01-01 UTC is.",
    read=Side(
        rules=(Rule(None, {"clickhouse": Rewrite(unix_seconds, mentions=r"\bto_timestamp\b")}),),
        limitations={
            "clickhouse": (
                Limitation(
                    "Of a double the statement writes as a literal, computed as the statement is "
                    f"read; an instant outside {instants.BOUND} (infinity's too) raises 0A000; any "
                    "other value is refused by name."
                ),
            )
        },
    ),
)

TO_CHAR = Feature(
    "to_char(timestamp, text)",
    FAMILY,
    "A timestamp's or an instant's text in a template.",
    read=Side(
        rules=(
            Rule(
                "to_char($a, $b)",
                {
                    **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                    "clickhouse": Rewrite(instants.to_char_written),
                    "trino": Rewrite(instants.to_char_written),
                },
                unlisted_keep=True,
            ),
        ),
        limitations={
            "clickhouse": (Limitation(instants.TO_CHAR_STATED),),
            "trino": (Limitation(instants.TO_CHAR_TRINO_STATED),),
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 160
FEATURES: tuple[Feature, ...] = (
    DATE_MI,
    DATE_BIN,
    DATE_PLI,
    DATE_MII,
    INTEGER_PL_DATE,
    TIME_MI_TIME,
    MAKE_DATE,
    TRANSACTION_TIMESTAMP,
    CLOCK_TIMESTAMP,
    UNIX_SECONDS,
    STAMP_SCALED,
    INSTANT_OF_WALL,
    TO_CHAR,
)
