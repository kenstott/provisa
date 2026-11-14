# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6e3a85-9b1f-4c27-a5e4-71f2b8c9e360
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The time zone rows of the polyfill grid (REQ-2236): each rule as the form the translator writes
on each dialect, and DuckDB's answer of it as PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import zoneinfo

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import MACROS, FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID, Rewrite, zones
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE
from provisa.transpiler.polyfill.notation import Refusal
from provisa.transpiler.transpile import _ZONE_ARGUMENTS, transpile, transpile_to_duckdb

_ROWS = [
    (feature, rule, dialect)
    for feature in zones.FEATURES
    for rule in feature.read.rules
    for dialect in sorted(set(DIALECTS) - NATIVE)
]


def test_the_rows_are_in_the_grid_and_not_in_the_old_tables():
    names = {f.name for f in zones.FEATURES}
    assert names <= {f.name for f in GRID.features}
    assert not {name for name, _ in MACROS} & names
    assert not set(_ZONE_ARGUMENTS) & names


@pytest.mark.parametrize(("feature", "rule", "dialect"), _ROWS)
def test_each_rule_is_the_form_the_translator_writes(feature, rule, dialect):
    form = rule.forms.get(dialect)
    if isinstance(form, Refusal):
        with pytest.raises(RowRefused) as refused:
            transpile(f"SELECT * FROM {feature.name}()", dialect)
        assert (str(refused.value), refused.value.sqlstate) == (form.message, form.sqlstate)
        assert isinstance(refused.value, FunctionRefused)
        return
    if form is None:
        # timezone() on a dialect of no form: refused before the grid (engine_refusals) or the
        # dialect's own function, as before the row.
        return
    # DuckDB's POSIX zone; ClickHouse's instant at a named zone (polyfill.instants).
    assert isinstance(form, Rewrite) and dialect in ("duckdb", "clickhouse")


#: (statement, DuckDB's SQL of it): timezone() of a POSIX zone of a fixed offset is the ICU zone
#: of that offset, west of UTC; an interval, a named zone and an abbreviation are as written.
TIMEZONE_FORMS = [
    (
        "SELECT timezone('+05:30'::text, ts) AS v FROM t",
        "SELECT TIMEZONE(CAST('GMT-05:30' AS TEXT), ts) AS v FROM t",
    ),
    ("SELECT timezone('12', ts) AS v FROM t", "SELECT TIMEZONE('GMT-12:00', ts) AS v FROM t"),
    ("SELECT timezone('UTC', ts) AS v FROM t", "SELECT TIMEZONE('UTC', ts) AS v FROM t"),
    ("SELECT timezone('est', ts) AS v FROM t", "SELECT TIMEZONE('est', ts) AS v FROM t"),
    (
        "SELECT timezone('-05:30'::interval, t::timetz) AS v FROM x",
        "SELECT TIMEZONE(CAST('-05:30' AS INTERVAL), CAST(t AS TIMETZ)) AS v FROM x",
    ),
    # Nested: the inner call's zone is read too, and once each.
    (
        "SELECT timezone('+05:30', timezone('12', ts)) AS v FROM t",
        "SELECT TIMEZONE('GMT-05:30', TIMEZONE('GMT-12:00', ts)) AS v FROM t",
    ),
]


@pytest.mark.parametrize(("sql", "duckdb_sql"), TIMEZONE_FORMS)
def test_timezone_of_a_posix_zone_on_duckdb(sql: str, duckdb_sql: str):
    assert transpile_to_duckdb(sql) == duckdb_sql


#: (statement, PostgreSQL 17's answer)
TIMEZONE_ANSWERS = [
    (
        "SELECT timezone('+05:30'::text, '2000-02-29 12:00:00+00'::timestamptz)",
        datetime.datetime(2000, 2, 29, 6, 30),
    ),
    (
        "SELECT timezone('-05:30'::interval, '00:00:00+00'::timetz)",
        datetime.time(18, 30, tzinfo=datetime.timezone(-datetime.timedelta(hours=5, minutes=30))),
    ),
]


@pytest.mark.parametrize(("sql", "answer"), TIMEZONE_ANSWERS)
def test_timezone_answers_as_postgresql_on_duckdb(sql: str, answer: object):
    assert duckdb.connect().sql(transpile_to_duckdb(sql)).fetchall() == [(answer,)]


def test_pg_timezone_names_on_duckdb_is_each_zone_read_from_the_pinned_release():
    out = transpile_to_duckdb("SELECT name, abbrev, utc_offset, is_dst FROM pg_timezone_names()")
    assert isinstance(sqlglot.parse_one(out, read="duckdb").find(exp.Subquery), exp.Subquery)
    rows = duckdb.connect().sql(out).fetchall()
    assert rows
    now = datetime.datetime.now(datetime.timezone.utc)
    wrong = []
    for name, abbrev, offset, dst in rows:
        local = now.astimezone(zoneinfo.ZoneInfo(name))
        if (abbrev, offset, dst) != (local.tzname(), local.utcoffset(), bool(local.dst())):
            wrong.append(name)
    assert wrong == []
