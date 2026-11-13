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
transaction_timestamp have no Trino function of the name: each is Trino's form here."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.numbers import fail, refused

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
DATE_PLI = Feature(
    "date_pli",
    FAMILY,
    "A date moved forward by a number of days.",
    read=Side(rules=(Rule("date_pli($a, $b)", trino.trino_only(_moved("+"))),)),
)
DATE_MII = Feature(
    "date_mii",
    FAMILY,
    "A date moved back by a number of days.",
    read=Side(rules=(Rule("date_mii($a, $b)", trino.trino_only(_moved("-"))),)),
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
                ),
            ),
        ),
        limitations=dict.fromkeys(
            trino.TRINO,
            (
                Limitation(
                    "Only a year from 1 to 9999 and a day the month has: any other is refused "
                    "with 0A000, where PostgreSQL answers a year BC or past 9999, and refuses a "
                    "day the month lacks with 22008."
                ),
            ),
        ),
    ),
)

TRANSACTION_TIMESTAMP = Feature(
    "transaction_timestamp",
    FAMILY,
    "The time the transaction began: now(), as Provisa answers it.",
    read=Side(
        rules=(Rule("transaction_timestamp()", trino.trino_only("current_timestamp")),),
    ),
)

FEATURES: tuple[Feature, ...] = (
    DATE_MI,
    DATE_PLI,
    DATE_MII,
    TIME_MI_TIME,
    MAKE_DATE,
    TRANSACTION_TIMESTAMP,
)
