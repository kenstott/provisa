# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f14-c7a3-4d90-8e61-0f3a9d2c7b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The last wrong answers the function support measure found on Trino (run 43288245572), each a row
of the polyfill grid (REQ-2236): array_eq, array_ne and array_position (polyfill.arrays),
regexp_replace's flags (polyfill.regex), pg_collation_for of a COLLATE (polyfill.collations),
ts_delete of a list (polyfill.textsearch) and interval_in's refusal of infinity
(polyfill.type_io). Each statement is the one the measure sent; each answer is postgres:17.11's."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import MACROS, RowRefused
from provisa.transpiler.pg_forms_textsearch import ROWS as TEXT_SEARCH_ROWS
from provisa.transpiler.pg_forms_types import FORMS_BY_NAME
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.polyfill.arrays import ARRAY_EQ, ARRAY_NE, ARRAY_POSITION
from provisa.transpiler.polyfill.collations import PG_COLLATION_FOR
from provisa.transpiler.polyfill.regex import REGEXP_REPLACE
from provisa.transpiler.polyfill.textsearch import TS_DELETE
from provisa.transpiler.polyfill.type_io import INTERVAL_IN
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_TRINO = ("trino", "presto", "athena")


def _trino(sql: str) -> exp.Expr:
    return sqlglot.parse_one(transpile_to_trino(sql), read="trino")


def _refused(sql: str, dialect: str = "trino") -> RowRefused:
    with pytest.raises(RowRefused) as refused:
        transpile_to_trino(sql) if dialect == "trino" else transpile(sql, dialect)
    return refused.value


def test_each_row_is_in_the_grid_and_not_in_an_old_table():
    rows = (ARRAY_EQ, ARRAY_NE, ARRAY_POSITION, REGEXP_REPLACE, PG_COLLATION_FOR, TS_DELETE)
    in_the_grid = {id(f) for f in GRID.features}
    assert all(id(row) in in_the_grid for row in (*rows, INTERVAL_IN))
    # The rules moved: no old table holds them.
    assert not {"array_eq", "array_ne"} & set(FORMS_BY_NAME)
    assert not {name for name, _ in MACROS} & {"array_eq", "array_ne", "ts_delete_array"}
    assert "ts_delete_array" not in {name for name, _, _ in TEXT_SEARCH_ROWS}


# --- 1, 2: array_eq and array_ne, a NULL element equal to a NULL element ------------------------

_ARRAYS = "'{-7,NULL,42}'::integer[], '{-7,NULL,42}'::integer[]"


@pytest.mark.parametrize(
    ("function", "test"), [("array_eq", exp.NullSafeEQ), ("array_ne", exp.NullSafeNEQ)]
)
def test_trino_compares_arrays_element_by_element_null_equal_to_null(function, test):
    """Trino's ``=`` of two arrays holding a NULL is NULL; PostgreSQL's array_eq is 't'."""
    tree = _trino(f'SELECT pg_catalog."{function}"({_ARRAYS}) AS v')
    case = tree.find(exp.Case)
    assert case is not None
    assert isinstance(case.args["default"], test)
    (strict,) = case.args["ifs"]  # a NULL array answers NULL: array_eq is strict
    assert isinstance(strict.args["true"], exp.Cast) and isinstance(
        strict.args["true"].this, exp.Null
    )
    assert tree.find(exp.EQ) is None and tree.find(exp.NEQ) is None


@pytest.mark.parametrize("dialect", _TRINO)
def test_presto_and_athena_take_trinos_array_comparison(dialect):
    tree = sqlglot.parse_one(transpile(f"SELECT array_eq({_ARRAYS}) AS v", dialect), read="trino")
    assert tree.find(exp.NullSafeEQ) is not None


@pytest.mark.parametrize(("function", "answer"), [("array_eq", True), ("array_ne", False)])
def test_duckdb_answers_array_comparison_as_before(function, answer):
    sql = transpile_to_duckdb(f'SELECT pg_catalog."{function}"({_ARRAYS}) AS v')
    assert duckdb.connect().execute(sql).fetchall() == [(answer,)]


# --- 3: array_position, NULL where nothing is found ---------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.\"array_position\"('{}'::integer[], (-7)::integer) AS v",
        "SELECT array_position('{1,2}'::integer[], 5) AS v",
        "SELECT array_position(ARRAY[1, NULL], NULL::integer) AS v",
    ],
)
def test_trinos_array_position_is_not_sent(sql):
    """Trino's array_position answers 0 where it finds nothing (PostgreSQL: NULL) and NULL for a
    NULL value (PostgreSQL finds it): the position is the first index whose element is not
    distinct from the value, of a non-empty array, as an integer."""
    tree = _trino(sql)
    assert tree.find(exp.ArrayPosition) is None
    assert not any(a.name.lower() == "array_position" for a in tree.find_all(exp.Anonymous))
    assert isinstance(tree, exp.Select)
    cast = tree.selects[0].unalias()
    assert isinstance(cast, exp.Cast) and cast.to.is_type(exp.DataType.Type.INT)
    assert cast.find(exp.NullSafeEQ) is not None
    assert cast.find(exp.ArrayFilter) is not None


def test_duckdb_keeps_its_own_array_position():
    """DuckDB's list_position answers NULL where it finds nothing: the row gives it no form."""
    assert ARRAY_POSITION.read.rules[0].forms.keys() == set(_TRINO)
    sql = transpile_to_duckdb("SELECT pg_catalog.\"array_position\"('{}'::integer[], 5) AS v")
    assert duckdb.connect().execute(sql).fetchall() == [(None,)]


# --- 8: regexp_replace's flags --------------------------------------------------------------------

_HELLO = (
    "SELECT pg_catalog.\"regexp_replace\"('Hello World'::text, '[a-z]+'::text, "
    "'Hello World'::text, {flags}) AS v"
)


def test_the_i_flag_replaces_the_first_match_case_insensitively_on_trino():
    """PostgreSQL: 'Hello World World'. Trino was sent the flag, and its reading of the statement
    dropped it: every match, case-sensitively ('HHello World WHello World')."""
    assert transpile_to_trino(_HELLO.format(flags="'i'::text")) == (
        "SELECT REGEXP_REPLACE(CAST('Hello World' AS VARCHAR), "
        "'(?s)^(?<before>.*?)(?:(?i:[a-z]+))(?<after>.*)$', '${before}Hello World${after}') AS v"
    )


@pytest.mark.parametrize(
    ("flags", "pattern"),
    [("'gi'", "'(?i:b)'"), ("'ig'", "'(?i:b)'"), ("'g'", "'b'"), ("'gic'", "'b'")],
)
def test_the_g_flag_replaces_every_match_and_c_wins_over_an_earlier_i(flags, pattern):
    written = transpile_to_trino(f"SELECT regexp_replace('aBb', 'b', 'c', {flags}) AS v")
    assert written == f"SELECT REGEXP_REPLACE('aBb', {pattern}, 'c') AS v"


def test_no_flags_is_the_first_match_as_before():
    assert transpile_to_trino("SELECT regexp_replace('abcb', 'b', 'X') AS v") == (
        "SELECT REGEXP_REPLACE('abcb', '(?s)^(?<before>.*?)(?:b)(?<after>.*)$', "
        "'${before}X${after}') AS v"
    )


def test_clickhouse_reads_the_flags_by_the_same_row():
    assert transpile(_HELLO.format(flags="'i'::text"), "clickhouse") == (
        "SELECT replaceRegexpOne(CAST('Hello World' AS Nullable(String)), '(?i:[a-z]+)', "
        "'Hello World') AS v"
    )


def test_a_null_flag_is_null():
    tree = _trino("SELECT regexp_replace('ab', 'b', 'c', NULL::text) AS v")
    assert isinstance(tree, exp.Select)
    cast = tree.selects[0].unalias()
    assert isinstance(cast, exp.Cast) and isinstance(cast.this, exp.Null)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT regexp_replace('ab', 'b', 'c', 'x') AS v",  # expanded syntax
        "SELECT regexp_replace('ab', 'b', 'c', 'n') AS v",  # newline-sensitive
        "SELECT regexp_replace(s, 'b', 'c', f::text) AS v FROM t",  # flags of no constant
        "SELECT regexp_replace('ab', '(b)\\1', 'c', 'i') AS v",
        "SELECT regexp_replace('ab', 'b', '\\0', 'i') AS v",
    ],
)
def test_what_no_form_writes_is_refused_by_the_rows_message(sql):
    refused = _refused(sql)
    assert (refused.sqlstate, str(refused)) == ("0A000", "regexp_replace() has no form on trino")


def test_a_regex_mask_still_replaces_every_match():
    """A rewrite's call of three arguments is every match: only a call the statement writes with
    no flags is the first match."""
    tree = _trino("SELECT regexp_replace(s, 'b', 'c', 'g') AS v FROM t")
    replaced = tree.find(exp.RegexpReplace)
    assert replaced is not None and replaced.expression == exp.Literal.string("b")


# --- 7: pg_collation_for of a COLLATE clause ------------------------------------------------------


@pytest.mark.parametrize(
    ("collate", "answer"),
    [
        ('"C"', '"C"'),
        ('"POSIX"', '"POSIX"'),
        ('"default"', '"default"'),
        ("ucs_basic", "ucs_basic"),
        ('"ucs_basic"', "ucs_basic"),
        ("pg_c_utf8", "pg_c_utf8"),
    ],
)
@pytest.mark.parametrize("dialect", ["trino", "duckdb", "clickhouse"])
def test_a_built_in_collation_is_its_name_as_quote_ident_writes_it(collate, answer, dialect):
    sql = f"SELECT pg_catalog.\"pg_collation_for\"('abc'::text COLLATE {collate}) AS v"
    written = transpile_to_trino(sql) if dialect == "trino" else transpile(sql, dialect)
    tree = sqlglot.parse_one(written)
    assert isinstance(tree, exp.Select)
    assert tree.selects[0].unalias() == exp.Literal.string(answer)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_collation_for('abc'::text COLLATE \"en_US\") AS v",
        'SELECT pg_collation_for(1 COLLATE "C") AS v',  # integer has no collation (42804)
    ],
)
@pytest.mark.parametrize("dialect", ["trino", "duckdb"])
def test_any_other_collate_is_refused_by_name(sql, dialect):
    refused = _refused(sql, dialect)
    assert (refused.sqlstate, str(refused)) == (
        "0A000",
        "pg_collation_for does not read this value",
    )


# --- 9: ts_delete of a list holding a NULL ---------------------------------------------------------

_TS_DELETE = "SELECT pg_catalog.\"ts_delete\"('rat:3A'::tsvector, '{x,NULL,z}'::text[]) AS v"


def test_trino_keeps_a_lexeme_the_list_does_not_hold_beside_a_null():
    """Trino's contains of a value a list holding a NULL lacks is NULL, so ``NOT contains`` dropped
    'rat' (''): PostgreSQL skips the NULL item and answers 'rat':3A."""
    tree = _trino(_TS_DELETE)
    kept = next(f for f in tree.find_all(exp.ArrayFilter) if f.find(exp.ArrayContains))
    test = kept.expression.this
    assert isinstance(test, exp.Not)
    assert isinstance(test.this, exp.Coalesce)
    assert isinstance(test.this.this, exp.ArrayContains)
    assert test.this.expressions == [exp.false()]


def test_duckdb_answers_ts_delete_of_a_list_holding_a_null():
    assert duckdb.connect().execute(transpile_to_duckdb(_TS_DELETE)).fetchall() == [("'rat':3A",)]


# --- 4: interval_in of infinity, which pgwire has no interval value of -----------------------------


@pytest.mark.parametrize("text", ["infinity", "-infinity"])
def test_interval_in_of_infinity_is_refused_on_trino(text):
    refused = _refused(f"SELECT pg_catalog.\"interval_in\"('{text}'::cstring, 0::oid, -1) AS v")
    assert (refused.sqlstate, str(refused)) == ("0A000", "interval_in does not read this value")


def test_interval_in_of_a_finite_constant_is_its_text_on_trino():
    """pgwire describes the text as interval (provisa.pgwire.cast_types)."""
    sql = (
        "SELECT pg_catalog.\"interval_in\"('1 day 02:03:04'::cstring, "
        "'interval'::regtype::oid, (-1)::integer) AS v"
    )
    assert transpile_to_trino(sql) == "SELECT '1 day 02:03:04' AS v"
