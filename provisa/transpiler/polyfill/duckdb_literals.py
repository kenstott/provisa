# Copyright (c) 2026 Kenneth Stott
# Canary: 5996cb14-f2d5-46ac-b7d4-6fbbd49e7c65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamp or timestamptz literal past what DuckDB holds (REQ-2236, amended 2026-11-23 and
2026-11-24: a value the engine cannot represent is a documented and enforced limitation).

DuckDB's TIMESTAMP and TIMESTAMPTZ hold microseconds since the epoch in 64 bits, to
294247-01-10 04:00:54.775806 (the next microsecond is its infinity); PostgreSQL's hold to
294276-12-31 23:59:59.999999. A text in between DuckDB's cast refuses with an error of its own
(``Conversion Error: timestamp field value out of range``), and a range's bound reader cannot read
it. Each is read where the statement is transpiled, before any form reads the literal: past
PostgreSQL's own range it is PostgreSQL's 22008, as timestamp_in and timestamptz_in raise; past
DuckDB's it is refused by name, the limitation the rows below state."""

# Requirements: REQ-2236

from __future__ import annotations

import calendar
import re

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import PostgresError, raising
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite
from provisa.transpiler.polyfill.trino_literals import days_from_civil

FAMILY = "literals"

_DUCKDB = "duckdb"
#: The last instant DuckDB's TIMESTAMP and TIMESTAMPTZ hold: 2^63 - 2 microseconds after the
#: epoch (2^63 - 1 is its infinity).
DUCKDB_STAMP_END = "294247-01-10 04:00:54.775806"
_DAY_US = 86_400_000_000
_DUCKDB_END_US = 2**63 - 2
#: The first instant PostgreSQL's timestamp does not hold (END_TIMESTAMP): 294277-01-01.
_PG_PAST_US = days_from_civil(294277, 1, 1) * _DAY_US
#: The last year timestamp_in reads as a date (its Julian day range); a later year is a text its
#: date reader refuses before the range is read, which this leaves to the engine's reading.
_PG_LAST_YEAR = 5874897
#: The widest offset of a zone from UTC: a timestamptz's text without one is past DuckDB's end
#: in every session zone only when it is past by more than this.
_WIDEST_OFFSET_US = 14 * 3_600_000_000

#: A timestamp's text in ISO form after Christ: a year of five digits or more (one of four is
#: within DuckDB's range), a month, a day, optionally a time of day with a fraction of a second,
#: then optionally an offset.
_FAR_STAMP = re.compile(
    r"\s*(\d{5,})-(\d{1,2})-(\d{1,2})"
    r"(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?"
    r"\s*(?:([+-])(\d{1,2})(?::?(\d{2})(?::?(\d{2}))?)?)?\s*",
    re.ASCII,
)
#: Whether a statement may hold a text ``_FAR_STAMP`` reads.
_MENTIONED = re.compile(r"'\s*\d{5,}-\d")

PAST_TIMESTAMP = f"timestamp_in() of a value past {DUCKDB_STAMP_END} has no exact form on duckdb"
PAST_TIMESTAMPTZ = (
    f"timestamptz_in() of a value past {DUCKDB_STAMP_END}+00 has no exact form on duckdb"
)


def _instants_us(read: re.Match[str], zoned: bool) -> tuple[int, int | None] | None:
    """The microseconds since the epoch of a text's own fields, and of its instant in UTC where it
    is a timestamptz that states its offset (a timestamp's offset is dropped, as timestamp_in
    drops it; one stated by none is in the session's zone); None for a day no calendar has."""
    year, month, day = int(read[1]), int(read[2]), int(read[3])
    if not 1 <= month <= 12 or not 1 <= day <= calendar.monthrange(year % 400 + 2000, month)[1]:  # noqa: PLR2004
        return None
    seconds = int(read[4] or 0) * 3600 + int(read[5] or 0) * 60 + int(read[6] or 0)
    micros = int((read[7] or "0")[:6].ljust(6, "0"))
    fields = days_from_civil(year, month, day) * _DAY_US + seconds * 1_000_000 + micros
    if not zoned or read[8] is None:
        return fields, None
    offset = int(read[9]) * 3600 + int(read[10] or 0) * 60 + int(read[11] or 0)
    return fields, fields - (offset if read[8] == "+" else -offset) * 1_000_000


def past_stamp(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to a timestamp or a timestamptz past what DuckDB holds: PostgreSQL's 22008 past
    its own range, refused by name past DuckDB's. None for any other node: DuckDB reads it."""
    if not isinstance(node, exp.Cast) or isinstance(node, exp.TryCast):
        return None
    zoned = node.to.is_type(exp.DataType.Type.TIMESTAMPTZ)
    if not zoned and not node.to.is_type(exp.DataType.Type.TIMESTAMP):
        return None
    if not isinstance(node.this, exp.Literal) or not node.this.is_string:
        return None
    text = node.this.this
    read = _FAR_STAMP.fullmatch(text)
    if read is None or int(read[1]) > _PG_LAST_YEAR:
        return None
    held = _instants_us(read, zoned)
    if held is None:
        return None
    fields, utc = held
    # PostgreSQL's range is of the instant: one in the session's zone is past it in every zone
    # only when it is past by the widest offset.
    pg_instant = fields if not zoned else (fields - _WIDEST_OFFSET_US if utc is None else utc)
    if pg_instant >= _PG_PAST_US:
        raise Refused(Refusal("22008", f'timestamp out of range: "{text}"'), ruling=True)
    # DuckDB reads the text's own fields into its type before it takes an offset off.
    if fields > _DUCKDB_END_US or (utc is not None and utc > _DUCKDB_END_US):
        raise Refused(Refusal("0A000", PAST_TIMESTAMPTZ if zoned else PAST_TIMESTAMP), ruling=True)
    return None


def refuse_past_stamps(pg_sql: str) -> None:
    """Raise, for the first text cast to a timestamp or a timestamptz in ``pg_sql`` past what
    DuckDB holds, its row's error (``past_stamp``): before any form reads the literal, as
    PostgreSQL reads a literal when it parses the statement (a range's bound reader among them)."""
    if _MENTIONED.search(pg_sql) is None:
        return
    try:
        tree = sqlglot.parse_one(pg_sql, read="postgres")
    except SqlglotError:
        return  # a text sqlglot cannot read: the transpile that follows reports it
    for node in tree.find_all(exp.Cast):
        try:
            past_stamp(node, _DUCKDB)
        except Refused as refused:
            raise PostgresError(refused.refusal.sqlstate, refused.refusal.message) from refused


def _row(name: str, summary: str, limitation: str) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, {_DUCKDB: Rewrite(past_stamp, mentions=_MENTIONED.pattern)}),),
            limitations={_DUCKDB: (Limitation(limitation),)},
        ),
    )


FEATURES: tuple[Feature, ...] = (
    _row(
        "a timestamp literal on duckdb",
        "A text cast to a timestamp.",
        f"Held to {DUCKDB_STAMP_END}, the last microsecond DuckDB's TIMESTAMP holds: a text past "
        f"it raises 0A000 ({PAST_TIMESTAMP}), one past PostgreSQL's range raises 22008 as "
        "timestamp_in does.",
    ),
    _row(
        "a timestamptz literal on duckdb",
        "A text cast to a timestamp with time zone.",
        f"Held to {DUCKDB_STAMP_END}+00, the last microsecond DuckDB's TIMESTAMPTZ holds: a text "
        f"past it raises 0A000 ({PAST_TIMESTAMPTZ}), one past PostgreSQL's range raises 22008 as "
        "timestamptz_in does.",
    ),
)


def _infinite_difference(name: str, sql_type: str) -> Feature:
    """``name``, the interval between two instants, on DuckDB: PostgreSQL 17 answers an infinite
    interval where either is infinite, which DuckDB holds none of (its subtraction raises its own
    error), so it is refused by name there, as its limitation states."""
    refused = INFINITE_DIFFERENCE.format(name=name)
    a, b = (f"CAST(${hole} AS {sql_type})" for hole in "ab")
    form = (
        f"CASE WHEN NOT isfinite({a}) OR NOT isfinite({b}) "
        f"THEN {raising('0A000', exp.Literal.string(refused).sql(dialect=_DUCKDB))} "
        f"ELSE {a} - {b} END"
    )
    return Feature(
        f"{name} on duckdb",
        "dates",
        "The interval between two instants.",
        read=Side(
            rules=(Rule(f"{name}($a, $b)", {_DUCKDB: form}, unlisted_keep=True),),
            limitations={
                _DUCKDB: (
                    Limitation(
                        "Of two finite values DuckDB's difference. Of an infinite one, whose "
                        f"difference PostgreSQL 17 answers as an infinite interval, it raises 0A000 "
                        f"({refused}): DuckDB holds no infinite interval."
                    ),
                )
            },
        ),
    )


#: The refusal of an instant's difference where either is infinite.
INFINITE_DIFFERENCE = "{name}() of an infinite value has no form on duckdb"

FEATURES += (
    _infinite_difference("timestamp_mi", "TIMESTAMP"),
    _infinite_difference("timestamptz_mi", "TIMESTAMPTZ"),
)

#: The family's place in the grid (polyfill.GRID): read before any row reads it as a value.
ORDER = 13
