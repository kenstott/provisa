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

from provisa.transpiler.polyfill import instants, trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Rewrite
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
                trino.trino_only(
                    "CAST(date_diff('day', CAST($b AS DATE), CAST($a AS DATE)) AS INTEGER)"
                ),
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
ORDER = 120
FEATURES: tuple[Feature, ...] = (
    DATE_MI,
    DATE_PLI,
    DATE_MII,
    INTEGER_PL_DATE,
    TIME_MI_TIME,
    MAKE_DATE,
    TRANSACTION_TIMESTAMP,
    TO_CHAR,
)
