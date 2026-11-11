# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f1a1e-6a0b-4c0e-9f6f-0e2b8d3c7a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The array functions, the first family the translator takes from the polyfill grid (REQ-2236).
Their answers are replayed against postgres:17.11 by tests/unit/pgwire/test_pg_everyday_functions.py
(DuckDB) and tests/integration/test_everyday_functions_on_engines.py (Trino, ClickHouse)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import function_macros
from provisa.transpiler.function_macros import MACROS, FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID, arrays, graft, holes
from provisa.transpiler.polyfill.grid import ENGINES
from provisa.transpiler.polyfill.notation import Refusal, pattern, replacement
from provisa.transpiler.transpile import transpile

_DIALECTS = sorted(d for dialects in ENGINES.values() for d in dialects)
_RULES = [
    (feature, rule, engine)
    for feature in arrays.FEATURES
    for rule in feature.read.rules
    for engine in ENGINES
]


def _call(rule_pattern: str) -> str:
    """A call of the rule's function with a column for each hole."""
    tree = pattern(rule_pattern).copy()
    for cast in list(tree.find_all(exp.Cast)):
        cast.replace(exp.column(cast.this.this.name))
    return tree.sql("postgres")


def test_the_family_is_in_the_grid_and_not_in_the_old_table():
    names = {f.name for f in arrays.FEATURES}
    assert names <= {f.name for f in GRID.features}
    assert not {name for name, _ in MACROS} & names


@pytest.mark.parametrize(("feature", "rule", "engine"), _RULES)
def test_each_rule_is_the_form_the_translator_writes(feature, rule, engine):
    """The statement's call written with the grid's replacement and nothing else."""
    form = rule.forms[engine]
    call = _call(rule.pattern)
    sql = f"SELECT {call} AS v FROM t"
    if isinstance(form, Refusal):
        with pytest.raises(RowRefused) as refused:
            transpile(sql, engine)
        assert str(refused.value) == form.message
        assert refused.value.sqlstate == form.sqlstate
        return
    bindings = {h: exp.column(h) for h in holes(pattern(rule.pattern))}
    expected = graft(replacement(form, engine, rule.pattern), bindings)
    if engine == "clickhouse":
        expected = expected.transform(function_macros._thrown)  # noqa: SLF001
    written = sqlglot.parse_one(transpile(sql, engine), read=engine)
    assert written.expressions[0].this.sql(engine) == expected.sql(engine)


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_every_form_is_sql_its_dialect_reads_back(dialect: str):
    """Each hole bound, on every dialect of each engine: a lambda's own variable is the form's."""
    for feature in arrays.FEATURES:
        for rule in feature.read.rules:
            call = _call(rule.pattern)
            try:
                out = transpile(f"SELECT {call} AS v", dialect)
            except RowRefused:
                continue
            tree = sqlglot.parse_one(out, read=dialect)
            columns = {c.name for c in tree.find_all(exp.Column)}
            assert not [n for n in tree.walk() if isinstance(n, (exp.Parameter, exp.Placeholder))]
            assert {c for c in columns if not c.startswith("_")} <= set(
                holes(pattern(rule.pattern))
            ) | {
                "d"  # the lambda's own variable of array_dims on Trino and ClickHouse
            }, out


def test_a_refusal_is_raised_from_the_row_on_every_dialect_of_the_engine():
    for dialect in ENGINES["trino"]:
        with pytest.raises(RowRefused, match="generate_subscripts\\(\\) has no form on trino"):
            transpile("SELECT generate_subscripts(ARRAY[1], 1, true) AS v", dialect)


def test_a_dialect_no_engine_of_the_grid_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="array_ndims\\(\\) has no form on snowflake") as e:
        transpile("SELECT array_ndims(a) AS v FROM t", "snowflake")
    assert not isinstance(e.value, RowRefused)


def test_postgresql_is_native():
    assert transpile("SELECT array_ndims(a) AS v FROM t", "postgres") == (
        "SELECT ARRAY_NDIMS(a) AS v FROM t"
    )


def test_a_call_no_rule_matches_is_left_untouched():
    # An integer is no array: PostgreSQL refuses array_ndims(integer), and so does the engine.
    assert transpile("SELECT array_ndims(5) AS v", "duckdb") == "SELECT ARRAY_NDIMS(5) AS v"
    # No overload of that many arguments.
    assert transpile("SELECT array_ndims(a, 2) AS v FROM t", "trino") == (
        "SELECT ARRAY_NDIMS(a, 2) AS v FROM t"
    )


def test_a_call_inside_an_argument_is_rewritten_too():
    out = transpile("SELECT array_ndims(array_fill(1, ARRAY[2])) AS v", "trino")
    tree = sqlglot.parse_one(out, read="trino")
    names = {n.name.lower() for n in tree.find_all(exp.Anonymous)}
    assert not names & {"array_ndims", "array_fill"}, out


def test_a_call_by_pg_catalog_is_matched():
    out = transpile("SELECT pg_catalog.array_ndims(ARRAY[1]) AS v", "trino")
    assert "CARDINALITY" in out and "array_ndims" not in out.lower()


def test_a_call_outside_the_select_list_is_not_named():
    out = transpile("SELECT x FROM t WHERE array_ndims(a) = 1", "duckdb")
    assert " AS array_ndims" not in out and "TYPEOF(a)" in out


def test_a_call_in_the_select_list_is_named_for_the_function():
    tree = sqlglot.parse_one(transpile("SELECT array_shuffle(a) FROM t", "trino"), read="trino")
    assert tree.expressions[0].alias == "array_shuffle"


def test_an_argument_that_differs_each_time_is_evaluated_once():
    out = transpile("SELECT array_sample(a, (random() * 2)::integer) AS v FROM t", "duckdb")
    tree = sqlglot.parse_one(out, read="duckdb")
    assert tree.expressions[0].find(exp.Subquery) is not None
    assert len(list(tree.find_all(exp.Rand))) + out.upper().count("RANDOM()") >= 1


def test_a_differing_argument_naming_no_column_is_refused_where_rows_are_read():
    with pytest.raises(FunctionRefused):
        transpile("SELECT trim_array(ARRAY[1], (random() * 2)::integer) AS v FROM t", "duckdb")


def test_a_lambda_over_a_subquery_argument_reads_it_in_a_row():
    out = transpile("SELECT array_positions((SELECT ARRAY[1,2]), 1) AS v", "duckdb")
    assert "_args" in out


def test_the_translator_parses_only_statements_that_name_a_rule():
    assert function_macros.mentioned("SELECT trim_array(a, 1) FROM t", "duckdb")
    assert function_macros.mentioned("SELECT generate_subscripts(a, 1, true) FROM t", "trino")


@pytest.mark.parametrize("feature", arrays.FEATURES, ids=lambda f: f.name)
def test_a_documented_refusal_is_the_text_the_form_raises(feature):
    """REQ-2236, THE REFUSAL LIVES IN THE GRID: a limitation's refusal is what its engine raises."""
    for engine, limitations in feature.read.limitations.items():
        for limitation in limitations:
            refusal = limitation.refusal
            if refusal is None:
                continue
            forms = [rule.forms.get(engine) for rule in feature.read.rules]
            if refusal in forms:
                continue  # the row's own refusal: raised from it
            raised = [
                literal.this
                for rule, form in zip(feature.read.rules, forms)
                if isinstance(form, str)
                for literal in replacement(form, engine, rule.pattern).find_all(exp.Literal)
                if literal.is_string
            ]
            assert any(refusal.message in text for text in raised), (feature.name, engine)
            assert any(f"{refusal.sqlstate}:" in text for text in raised), (feature.name, engine)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT array_ndims(ARRAY[[1,2,3],[4,5,6]])", [(2,)]),
        ("SELECT array_dims(ARRAY[[1,2,3],[4,5,6]])", [("[1:2][1:3]",)]),
        ("SELECT array_upper(ARRAY[[1,2,3],[4,5,6]], 2)", [(3,)]),
        ("SELECT array_lower(ARRAY[1,2,3], 2)", [(None,)]),
        ("SELECT array_fill(7, ARRAY[3])", [([7, 7, 7],)]),
        ("SELECT array_positions(ARRAY[1,NULL,1], NULL::integer)", [([2],)]),
        ("SELECT array_replace(ARRAY[1,2,1], 1, 9)", [([9, 2, 9],)]),
        ("SELECT trim_array(ARRAY[1,2,3], 1)", [([1, 2],)]),
        ("SELECT array_shuffle(ARRAY[7])", [([7],)]),
        ("SELECT generate_subscripts(ARRAY[7,8,9], 1, true)", [(3,), (2,), (1,)]),
    ],
)
def test_the_duckdb_forms_answer_as_postgresql_does(sql: str, expected):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchall() == expected


def test_a_refusal_raised_on_duckdb_carries_postgresqls_sqlstate():
    from provisa.core.pg_error import from_engine

    with pytest.raises(duckdb.Error) as raised:
        duckdb.connect().execute(transpile("SELECT trim_array(ARRAY[1,2,3], 4)", "duckdb"))
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "2202E"
    assert "number of elements to trim must be between 0 and 3" in str(error)
