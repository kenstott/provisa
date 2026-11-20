# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4f1d7e-3c82-4b6a-b5e9-0d27c6f8a413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A time of day on Trino to the microsecond (REQ-2236; the function support measure's edge
values, run 44670380355, REQ-2171).

Trino's TIME and TIMESTAMP of no precision hold milliseconds: a form that casts a time to TIME
rounded 23:59:59.999999 to 00:00:00, so it compared below 12:30:45, and a date plus it was the
date's midnight. PostgreSQL's time holds microseconds. The comparisons of two times (time_cmp,
time_lt and the others, time_larger, time_smaller) compare them as TIME(6), and a date plus a time
(datetime_pl, timedate_pl, timestamp(date, time)) is the TIMESTAMP(6) of the date's and the time's
texts. Of times the statement says (a cast, a literal); any other keeps what it had."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite

FAMILY = "time of day on Trino"

_DATE = "date"
_A, _B = "CAST($a AS TIME(6))", "CAST($b AS TIME(6))"

#: Each comparison of two times and its Trino form of ``$a`` and ``$b``.
_COMPARED = {
    "time_eq": f"{_A} = {_B}",
    "time_ne": f"{_A} <> {_B}",
    "time_lt": f"{_A} < {_B}",
    "time_le": f"{_A} <= {_B}",
    "time_gt": f"{_A} > {_B}",
    "time_ge": f"{_A} >= {_B}",
    "time_cmp": f"CASE WHEN {_A} < {_B} THEN -1 WHEN {_A} > {_B} THEN 1 WHEN {_A} = {_B} THEN 0 END",
    "time_larger": f"CASE WHEN $a IS NULL OR $b IS NULL THEN NULL WHEN {_A} > {_B} THEN {_A} "
    f"ELSE {_B} END",
    "time_smaller": f"CASE WHEN $a IS NULL OR $b IS NULL THEN NULL WHEN {_A} < {_B} THEN {_A} "
    f"ELSE {_B} END",
}
#: A date ``$d`` plus a time ``$t``: the timestamp of their texts, to the microsecond.
_STAMP = (
    "CAST(concat(CAST($d AS VARCHAR), ' ', CAST(CAST($t AS TIME(6)) AS VARCHAR)) AS TIMESTAMP(6))"
)
#: The calls of a date and a time, and the place of the time among their two arguments.
_SUMS = {"datetime_pl": 1, "timestamp": 1, "timedate_pl": 0}


def _is_time(node: exp.Expr) -> bool:
    """Whether the statement says ``node`` is a time without time zone, of any precision."""
    said = trino.said(node)
    return said is not None and said.startswith("time") and said.endswith("without time zone")


def _uncast(node: exp.Expr) -> exp.Expr:
    """``node`` with a cast of it to a time of no precision taken off: the form casts what it casts
    to TIME(6), and a cast to Trino's TIME first would round it to the millisecond."""
    value = node.unnest()
    if (
        isinstance(value, exp.Cast)
        and value.to.is_type(exp.DataType.Type.TIME)
        and not value.to.expressions
    ):
        return value.this
    return node


def _call(node: exp.Expr, names: frozenset[str]) -> tuple[str, list[exp.Expr]] | None:
    """The one name of ``names`` a call of two arguments is written by, and its arguments."""
    call = trino.called(node)
    if call is None or len(call[1]) != 2:  # noqa: PLR2004 -- each of these takes two
        return None
    named = call[0] & names
    return (next(iter(named)), call[1]) if len(named) == 1 else None


def compared(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A comparison of two times the statement says, as TIME(6)."""
    call = _call(node, frozenset(_COMPARED))
    if call is None or not all(_is_time(argument) for argument in call[1]):
        return None
    name, (a, b) = call
    bindings = {"a": _uncast(a), "b": _uncast(b)}
    return trino.written(_COMPARED[name], f"{name}($a, $b)", bindings)


def summed(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A date plus a time the statement says, as the TIMESTAMP(6) of their texts."""
    call = _call(node, frozenset(_SUMS))
    if call is None:
        return None
    name, arguments = call
    clock = arguments[_SUMS[name]]
    day = arguments[1 - _SUMS[name]]
    if not _is_time(clock) or trino.said(day) != _DATE:
        return None
    return trino.written(_STAMP, f"{name}($d, $t)", {"d": day, "t": _uncast(clock)})


#: extract of a date by each part PostgreSQL's date reads, as Trino's EXTRACT of the DATE itself:
#: the form of a timestamp's part casts the date to a TIMESTAMP, which holds no day past 294247.
_DATE_PARTS = {
    "year": "year($d)",
    "quarter": "quarter($d)",
    "month": "month($d)",
    "day": "day($d)",
    "dow": "day_of_week($d) % 7",
    "doy": "day_of_year($d)",
}
#: The last year PostgreSQL's timestamp holds, and Trino's (to 294247-01-10).
_PG_STAMP_YEAR = 294276
_TRINO_STAMP_DAY = (294247, 1, 10)


def _literal_text(node: exp.Expr) -> str | None:
    value = node.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    return value.this if isinstance(value, exp.Literal) and value.is_string else None


def _day_of(text: str) -> tuple[int, int, int] | None:
    parts = text.strip().split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):  # noqa: PLR2004
        return None
    year, month, day = (int(part) for part in parts)
    return year, month, day


def part_of_date(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """extract(part, date) of a part a date has, as Trino's EXTRACT of the date; date_part(part,
    date) of a date the statement writes past a timestamp's range, PostgreSQL's 22008 (date_part
    reads the date as a timestamp), or refused by name past Trino's. Any other keeps its form."""
    call = _call(node, frozenset({"extract", "date_part"}))
    if call is None or trino.said(call[1][1]) != _DATE:
        return None
    name, (part, day) = call
    if name == "date_part":
        text = _literal_text(day)
        read = None if text is None else _day_of(text)
        if read is None:
            return None
        if read[0] > _PG_STAMP_YEAR:
            raise Refused(Refusal("22008", "date out of range for timestamp"), ruling=True)
        if read > _TRINO_STAMP_DAY:
            raise Refused(
                Refusal(
                    "0A000", f"date_part() of a date past 294247-01-10 has no form on {dialect}"
                ),
                ruling=True,
            )
        return None
    unit = _literal_text(part)
    form = None if unit is None else _DATE_PARTS.get(unit.strip().lower())
    if form is None:
        return None
    return trino.written(form, "extract($p, $d)", {"p": part, "d": day})


_STATED = (
    "Of times the statement says (a cast, a literal), compared or added as Trino's TIME(6) and "
    "TIMESTAMP(6), to the microsecond as PostgreSQL's; any other keeps Trino's own, of "
    "milliseconds. Trino's TIME holds no 24:00:00."
)

FEATURES: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, trino.on_trino(Rewrite(form, mentions=mentions))),),
            limitations=dict.fromkeys(trino.TRINO, (Limitation(_STATED),)),
        ),
    )
    for name, summary, form, mentions in (
        (
            "comparisons of times on trino",
            "Two times of day compared, the larger or the smaller of two.",
            compared,
            r"\btime_(?:eq|ne|lt|le|gt|ge|cmp|larger|smaller)\b",
        ),
        (
            "a date's part on trino",
            "A part of a date, extract(part, date) or date_part(part, date).",
            part_of_date,
            r"\b(?:extract|date_part)\b",
        ),
        (
            "a date plus a time on trino",
            "A date plus a time of day, a timestamp.",
            summed,
            r"\b(?:datetime_pl|timedate_pl|timestamp)\b",
        ),
    )
)
