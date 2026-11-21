# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f61-3c4a-4d97-a0e2-8f61c3d94b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The time zone polyfills (REQ-2236; found by the functions conformance run on DuckDB, REQ-2167).

PostgreSQL reads a zone from its tz database. On DuckDB, a zone is read from ICU, whose release is
older than the pinned one (provisa.transpiler.pg_zones) and which has no zone for PostgreSQL's POSIX
text of a fixed offset. A zone's offset, daylight flag and abbreviation are the pinned release's,
and a POSIX zone of a fixed offset is the ICU zone of that offset."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import functools

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import pg_zones
from provisa.transpiler.polyfill import instants
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Rewrite

FAMILY = "time zones"


def _no_form(function: str) -> dict[str, Refusal]:
    """Every dialect but DuckDB refusing ``function`` by its name."""
    return {
        dialect: Refusal("0A000", f"{function}() has no form on {dialect}")
        for dialect in DIALECTS
        if dialect not in {*NATIVE, "duckdb"}
    }


@functools.cache
def _zone_rows() -> str:
    """pg_timezone_names' rows on DuckDB, read once: each zone at its first use."""
    return f"unnest((SELECT list(_z) FROM ({pg_zones.NAMES}) _z))"


def zone_names(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """The pinned release's zones each read now: abbreviation, offset and daylight flag."""
    return sqlglot.parse_one(_zone_rows(), read="duckdb")


PG_TIMEZONE_NAMES = Feature(
    "pg_timezone_names",
    FAMILY,
    "The zones of the tz database, each with its abbreviation, offset and daylight flag now.",
    read=Side(
        rules=(
            Rule(
                "pg_timezone_names()",
                {**_no_form("pg_timezone_names"), "duckdb": Rewrite(zone_names)},
            ),
        )
    ),
)


def posix_zone(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A zone's text PostgreSQL reads as a POSIX zone of a fixed offset, as the ICU zone of it."""
    zone = node.expressions[0]
    written = zone
    while isinstance(written, exp.Cast):
        written = written.this
    if not (isinstance(written, exp.Literal) and written.is_string):
        return None
    icu = pg_zones.icu_fixed_zone(written.this)
    if icu is None:
        return None
    rewritten = node.copy()
    held = rewritten.expressions[0]
    literal = held
    while isinstance(literal, exp.Cast):
        literal = literal.this
    literal.replace(exp.Literal.string(icu))
    return rewritten


TIMEZONE = Feature(
    "timezone",
    FAMILY,
    "A value at a zone; a zone written as a POSIX offset ('+05:30') is that many hours west of UTC.",
    read=Side(
        rules=(
            # An interval given for the zone is no zone's text: the pattern's zone is text alone.
            Rule(
                "timezone($z::text, $v)",
                {"duckdb": Rewrite(posix_zone), "clickhouse": instants.AT_ZONE},
            ),
        ),
        limitations={"clickhouse": (instants.AT_ZONE_LIMITATION,)},
    ),
)

FEATURES: tuple[Feature, ...] = (PG_TIMEZONE_NAMES, TIMEZONE)
