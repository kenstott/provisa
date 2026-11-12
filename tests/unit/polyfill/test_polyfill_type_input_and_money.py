# Copyright (c) 2026 Kenneth Stott
# Canary: 51e7a0c9-2d6b-4f84-8c3e-b09d7f6a2e14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""money's text and a NULL of interval_in, jsonb_in and xid8in, from the polyfill grid (REQ-2236):
rows the function support measure found wrong on Trino (run 43264139919), moved from
provisa.transpiler.pg_forms / function_macros and pg_forms_type_io into the grid."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import MACROS, RowRefused
from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.polyfill import GRID, graft, holes, match, money, type_io
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, written_in
from provisa.transpiler.polyfill.notation import Refusal, pattern, replacement
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

_FEATURES = (*money.FEATURES, *type_io.FEATURES)


def test_the_rows_are_in_the_grid_and_not_in_the_old_table():
    names = {f.name for f in _FEATURES}
    assert names == {"money_text", "interval_in", "jsonb_in", "xid8in"}
    assert names <= {f.name for f in GRID.features}
    assert not {name for name, _ in MACROS} & {"money_text"}


# --- money_text -------------------------------------------------------------------------------


def test_every_dialect_but_postgresql_has_money_texts_form_or_refusal():
    (rule,) = money.MONEY_TEXT.read.rules
    assert set(rule.forms) == set(DIALECTS) - NATIVE


@pytest.mark.parametrize("dialect", ["trino", "presto", "athena", "duckdb"])
def test_money_text_is_the_rows_form(dialect):
    """The call pg_money writes, as the grid's replacement and nothing else."""
    (rule,) = money.MONEY_TEXT.read.rules
    form = rule.forms[dialect]
    assert isinstance(form, str) and rule.pattern is not None
    written = transpile("SELECT money_text(v) AS m FROM t", dialect)
    got = sqlglot.parse_one(written, read=dialect).expressions[0].this
    expected = graft(replacement(form, written_in(dialect), rule.pattern), {"v": exp.column("v")})
    assert got.sql(dialect) == expected.sql(dialect)


@pytest.mark.parametrize(
    "dialect", sorted(set(DIALECTS) - NATIVE - {"duckdb", "trino", "presto", "athena"})
)
def test_money_text_is_refused_from_its_row_elsewhere(dialect):
    """pg_money raises the row's refusal as the subtype's, by the row's message."""
    (rule,) = money.MONEY_TEXT.read.rules
    form = rule.forms[dialect]
    assert isinstance(form, Refusal) and form.sqlstate == "XX000"
    with pytest.raises(SubtypeOperationRefused) as refused:
        transpile("SELECT CAST(NULL::money AS text) AS v", dialect)
    assert str(refused.value) == form.message
    assert refused.value.params == {
        "subtype": "money",
        "operation": "cast to text",
        "dialect": dialect,
    }


@pytest.mark.parametrize(
    ("pg_sql", "answer"),
    [
        ('SELECT pg_catalog."cash_out"(1234.5::money) AS v', "$1,234.50"),
        ('SELECT pg_catalog."cash_out"((-0.01)::money) AS v', "-$0.01"),
        ('SELECT pg_catalog."cash_out"(NULL::money) AS v', None),
        ("SELECT CAST(12::money AS text) AS v", "$12.00"),
    ],
)
def test_duckdb_answers_money_text_as_before(pg_sql, answer):
    sql = transpile_to_duckdb(pg_sql)
    assert duckdb.connect().execute(sql).fetchall() == [(answer,)]


# --- a NULL of a type only DuckDB holds ---------------------------------------------------------


@pytest.mark.parametrize("feature", type_io.FEATURES, ids=lambda f: f.name)
@pytest.mark.parametrize("dialect", sorted(set(DIALECTS) - NATIVE - {"duckdb"}))
def test_each_null_rule_is_refused_from_its_row(feature, dialect):
    for rule in feature.read.rules:
        form = rule.forms[dialect]
        assert form == Refusal("0A000", f"{feature.name} does not read this value")
        assert rule.pattern is not None
        call = pattern(rule.pattern).copy()
        call = graft(call, {h: exp.cast(exp.Literal.number(1), "integer") for h in holes(call)})
        with pytest.raises(RowRefused) as refused:
            transpile(f"SELECT pg_catalog.{call.sql('postgres')} AS v", dialect)
        assert str(refused.value) == form.message
        assert refused.value.sqlstate == form.sqlstate


@pytest.mark.parametrize(
    "pg_sql",
    [
        "SELECT pg_catalog.\"interval_in\"('1 day'::cstring, NULL::oid, (-1)::integer) AS v",
        "SELECT pg_catalog.\"interval_in\"('1 day'::cstring, 1186::oid, NULL::integer) AS v",
    ],
)
def test_any_null_argument_of_interval_in_is_refused_on_trino(pg_sql):
    with pytest.raises(RowRefused):
        transpile(pg_sql, "trino")


def test_a_value_reaches_the_type_io_form_not_the_row():
    """A call of no NULL matches no row: its form reads it (and refuses a text it does not)."""
    written = transpile('SELECT pg_catalog."jsonb_in"(c) AS v FROM t', "trino")
    said = {s.this for s in sqlglot.parse_one(written, read="trino").find_all(exp.Literal)}
    assert any("jsonb_in does not read this value" in str(s) for s in said)


def test_duckdb_answers_its_null_before_the_grid():
    sql = transpile_to_duckdb(
        'SELECT pg_catalog."interval_in"(NULL::cstring, NULL::oid, NULL::integer) AS v'
    )
    assert duckdb.connect().execute(sql).fetchall() == [(None,)]


# --- an untyped NULL of a pattern ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("argument", "matched"),
    [("NULL::cstring", True), ("NULL", True), ("NULL::integer", True), ("'x'::cstring", False)],
)
def test_a_null_a_pattern_types_not_matches_a_null_of_any_type(argument, matched):
    node = sqlglot.parse_one(f"jsonb_in({argument})", read="postgres")
    assert (match(pattern("jsonb_in(NULL)"), node) is not None) is matched
