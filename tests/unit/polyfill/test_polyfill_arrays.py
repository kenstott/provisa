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

import chdb
import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy

from provisa.transpiler import function_macros
from provisa.transpiler.function_macros import MACROS, FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID, arrays, graft, holes
from provisa.transpiler.polyfill.grid import DIALECTS, written_in
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, pattern, replacement
from provisa.transpiler.transpile import transpile

_DIALECTS = sorted(DIALECTS)
_RULES = [
    (feature, rule, dialect)
    for feature in arrays.FEATURES
    for rule in feature.read.rules
    for dialect in DIALECTS
]


def _call(rule_pattern: str | None) -> str:
    """A call of the rule's function with a column for each hole, and a NULL of an array its
    pattern names as a NULL integer[]."""
    assert rule_pattern is not None  # every array rule has a pattern
    tree = pattern(rule_pattern).copy()
    for cast in list(tree.find_all(exp.Cast)):
        if isinstance(cast.this, exp.Null):
            cast.set("to", exp.DataType.build("integer[]", dialect="postgres"))
        else:
            cast.replace(exp.column(cast.this.this.name))
    return tree.sql("postgres")


def test_the_family_is_in_the_grid_and_not_in_the_old_table():
    names = {f.name for f in arrays.FEATURES}
    assert names <= {f.name for f in GRID.features}
    # btarraycmp's row writes Trino's form only: every other dialect keeps the old table's
    # (provisa.transpiler.pg_forms_index), which reads an array's elements as written first.
    assert not {name for name, _ in MACROS} & names - {"btarraycmp"}


@pytest.mark.parametrize(("feature", "rule", "dialect"), _RULES)
def test_each_rule_is_the_form_the_translator_writes(feature, rule, dialect):
    """The statement's call written with the grid's replacement and nothing else; a dialect the
    rule gives no form for keeps the call (its own function); a refusal is raised from the row."""
    form = rule.forms.get(dialect)
    sql = f"SELECT {_call(rule.pattern)} AS v FROM t"
    if isinstance(form, Refusal):
        with pytest.raises(RowRefused) as refused:
            transpile(sql, dialect)
        assert str(refused.value) == form.message
        assert refused.value.sqlstate == form.sqlstate
        assert isinstance(refused.value, FunctionRefused)  # the API's handler of a refusal
        return
    if isinstance(form, Rewrite):
        _rewritten_as_the_row_writes(form, sql, dialect)
        return
    written = sqlglot.parse_one(transpile(sql, dialect), read=dialect).expressions[0].this
    if form is None:  # sqlglot's own writing of the call, untouched by the grid
        plain = sqlglot.transpile(sql, read="postgres", write=dialect)[0]
        assert type(written) is type(sqlglot.parse_one(plain, read=dialect).expressions[0].this)
        return
    assert isinstance(form, str) and rule.pattern is not None
    bindings: dict[str, exp.Expr] = {h: exp.column(h) for h in holes(pattern(rule.pattern))}
    expected = graft(replacement(form, written_in(dialect), rule.pattern), bindings)
    if dialect == "clickhouse":
        expected = expected.transform(function_macros._thrown)  # noqa: SLF001
    # A dialect that quotes every identifier (Snowflake) writes the same column quoted, and one
    # that folds an unquoted name to upper case (Exasol) writes it so.
    assert _unquoted(written, dialect).sql(dialect) == _unquoted(expected, dialect).sql(dialect)


def _rewritten_as_the_row_writes(form: Rewrite, sql: str, dialect: str) -> None:
    """A rewrite's refusal raised from the row; what it writes, the call's form; and where it writes
    nothing (a dialect that keeps the old table's form), no refusal of the grid's."""
    call = sqlglot.parse_one(sql, read="postgres").expressions[0].this
    try:
        rewritten = form.function(call, dialect)
    except Refused as by_the_row:
        with pytest.raises(RowRefused) as refused:
            transpile(sql, dialect)
        assert str(refused.value) == by_the_row.refusal.message
        assert refused.value.sqlstate == by_the_row.refusal.sqlstate
        return
    try:
        written = transpile(sql, dialect)
    except RowRefused:
        pytest.fail("a row that keeps the old table's form refused the call")
    except FunctionRefused:
        return  # the old table's own refusal by name
    if rewritten is not None:
        expected = rewritten.sql(written_in(dialect))
        assert expected in sqlglot.parse_one(written, read=dialect).sql(written_in(dialect))


def _unquoted(tree: exp.Expr, dialect: str) -> exp.Expr:
    upper = Dialect.get_or_raise(dialect).normalization_strategy is NormalizationStrategy.UPPERCASE

    def plain(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Identifier) and node.quoted:
            return exp.to_identifier(node.name.lower() if upper else node.name)
        return node

    return tree.copy().transform(plain)


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_every_form_is_sql_its_dialect_reads_back(dialect: str):
    """Each hole bound, on every dialect: a lambda's own variable is the form's."""
    for feature in arrays.FEATURES:
        for rule in feature.read.rules:
            call = _call(rule.pattern)
            try:
                out = transpile(f"SELECT {call} AS v", dialect)
            except RowRefused:
                continue
            tree = sqlglot.parse_one(out, read=dialect)
            # A dialect that folds names to capitals (Exasol) writes them so.
            columns = {c.name.lower() for c in tree.find_all(exp.Column)}
            assert not [n for n in tree.walk() if isinstance(n, (exp.Parameter, exp.Placeholder))]
            assert {c for c in columns if not c.startswith("_")} <= set(
                holes(pattern(rule.pattern))
            ) | {
                "d"  # the lambda's own variable of array_dims on Trino and ClickHouse
            }, out


@pytest.mark.parametrize("dialect", ["trino", "presto", "athena", "clickhouse"])
def test_a_refusal_names_the_dialect_routed_to(dialect: str):
    """Athena and Presto take Trino's forms, and each refusal names the dialect itself."""
    with pytest.raises(RowRefused) as refused:
        transpile("SELECT generate_subscripts(ARRAY[1], 1, true) AS v", dialect)
    assert str(refused.value) == f"generate_subscripts() has no form on {dialect}"
    assert refused.value.sqlstate == "0A000"


@pytest.mark.parametrize("dialect", ["snowflake", "tsql", "mysql", "bigquery", "oracle"])
def test_a_source_a_statement_goes_to_directly_is_refused_by_its_own_row(dialect: str):
    with pytest.raises(RowRefused) as refused:
        transpile("SELECT array_ndims(a) AS v FROM t", dialect)
    assert str(refused.value) == f"array_ndims() has no form on {dialect}"
    assert refused.value.sqlstate == "0A000"


def test_a_dialect_provisa_sends_no_sql_to_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="array_ndims\\(\\) has no form on spark") as refused:
        transpile("SELECT array_ndims(a) AS v FROM t", "spark")
    assert not isinstance(refused.value, RowRefused)


def test_array_length_is_a_sources_own():
    assert "ARRAY_SIZE" in transpile("SELECT array_length(a, 1) AS v FROM t", "snowflake")


@pytest.mark.parametrize("dialect", ["trino", "presto", "athena", "clickhouse"])
def test_array_length_reads_the_dimension_it_is_given(dialect: str):
    """The engine's own array length takes no dimension and answers the first one's for any: the
    grid's form reads the dimension, as PostgreSQL's does."""
    out = transpile("SELECT array_length(a, d) AS v FROM t", dialect)
    columns = {c.name for c in sqlglot.parse_one(out, read=dialect).find_all(exp.Column)}
    assert "d" in columns, out


#: The sources a statement goes to directly that hold no array type.
_ARRAY_LESS = ("mysql", "sqlite", "oracle", "tsql", "singlestore", "exasol", "redshift")
#: The sources whose own array length takes no dimension (sqlglot drops it).
_FIRST_DIMENSION_ONLY = ("snowflake", "bigquery", "databricks", "hive", "druid")


@pytest.mark.parametrize("dialect", _ARRAY_LESS)
@pytest.mark.parametrize("call", ["array_length(a, 1)", "array_length(a, d)"])
def test_array_length_is_refused_by_name_where_there_is_no_array(dialect: str, call: str):
    with pytest.raises(RowRefused) as refused:
        transpile(f"SELECT {call} AS v FROM t", dialect)
    assert str(refused.value) == f"array_length() has no form on {dialect}"
    assert refused.value.sqlstate == "0A000"


@pytest.mark.parametrize("dialect", _FIRST_DIMENSION_ONLY)
@pytest.mark.parametrize("call", ["array_length(a, 2)", "array_length(a, d)", "array_length(a, 0)"])
def test_array_length_of_another_dimension_than_the_first_is_refused(dialect: str, call: str):
    """The source's own length answers the first dimension's for any: the dimension is not
    dropped, as sqlglot drops it, but refused."""
    with pytest.raises(RowRefused) as refused:
        transpile(f"SELECT {call} AS v FROM t", dialect)
    assert str(refused.value) == f"array_length() reads only dimension 1 on {dialect}"
    assert refused.value.sqlstate == "0A000"


@pytest.mark.parametrize(
    ("dialect", "length"),
    [
        ("snowflake", "ARRAY_SIZE"),
        ("bigquery", "ARRAY_LENGTH"),
        ("databricks", "SIZE"),
        ("hive", "SIZE"),
        ("druid", "ARRAY_LENGTH"),
    ],
)
def test_array_length_of_the_first_dimension_is_null_for_an_empty_array(dialect: str, length: str):
    """PostgreSQL answers NULL for an empty or NULL array, where the source's length answers 0
    (or -1): the form answers the length only where it is above 0."""
    out = transpile("SELECT array_length(a, 1) AS v FROM t", dialect)
    case = sqlglot.parse_one(out, read=dialect).expressions[0].find(exp.Case)
    assert case is not None, out
    assert f"{length}(" in out.upper() and "> 0" in out, out


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 1)", [(2,)]),
        ("SELECT array_length(CAST(ARRAY[] AS INTEGER[]), 1)", [(None,)]),
        ("SELECT array_length(CAST(NULL AS INTEGER[]), 1)", [(None,)]),
    ],
)
def test_the_duckdb_form_of_the_first_dimension_answers_as_postgresql_does(sql: str, expected):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchall() == expected


def _on_clickhouse(sql: str) -> list[str | None]:
    held = chdb.query(transpile(sql, "clickhouse"), "TabSeparated").bytes().decode()
    return [None if line == "\\N" else line for line in held.splitlines()]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 2)", "3"),
        ("SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 1)", "2"),
        ("SELECT array_length(ARRAY[1,2,3], 2)", None),
        ("SELECT array_length(ARRAY[1,2,3], 0)", None),
        ("SELECT array_length(CAST(ARRAY[] AS INTEGER[]), 1)", None),
    ],
)
def test_the_clickhouse_form_of_array_length_answers_as_postgresql_does(sql: str, expected):
    assert _on_clickhouse(sql) == [expected]


def test_a_quoted_or_qualified_call_is_read_as_sqlglots_node():
    out = transpile('SELECT pg_catalog."array_length"(ARRAY[[1, 2]], 2) AS v', "duckdb")
    assert "JSON_ARRAY_LENGTH" in out.upper(), out


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
    for dialect, limitations in feature.read.limitations.items():
        for limitation in limitations:
            refusal = limitation.refusal
            if refusal is None:
                continue
            forms = [rule.forms.get(dialect) for rule in feature.read.rules]
            if refusal in forms:
                continue  # the row's own refusal: raised from it
            raised = [
                literal.this
                for rule, form in zip(feature.read.rules, forms)
                if isinstance(form, str)
                for literal in replacement(form, written_in(dialect), rule.pattern).find_all(
                    exp.Literal
                )
                if literal.is_string
            ]
            assert any(refusal.message in text for text in raised), (feature.name, dialect)
            assert any(f"{refusal.sqlstate}:" in text for text in raised), (feature.name, dialect)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT array_ndims(ARRAY[[1,2,3],[4,5,6]])", [(2,)]),
        ("SELECT array_dims(ARRAY[[1,2,3],[4,5,6]])", [("[1:2][1:3]",)]),
        ("SELECT array_upper(ARRAY[[1,2,3],[4,5,6]], 2)", [(3,)]),
        ("SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 2)", [(3,)]),
        ("SELECT array_length(ARRAY[1,2,3], 2)", [(None,)]),
        ("SELECT array_lower(ARRAY[1,2,3], 2)", [(None,)]),
        ("SELECT array_fill(7, ARRAY[3])", [([7, 7, 7],)]),
        ("SELECT array_positions(ARRAY[1,NULL,1], NULL::integer)", [([2],)]),
        ("SELECT array_replace(ARRAY[1,2,1], 1, 9)", [([9, 2, 9],)]),
        ("SELECT trim_array(ARRAY[1,2,3], 1)", [([1, 2],)]),
        ("SELECT array_shuffle(ARRAY[7])", [([7],)]),
        ("SELECT generate_subscripts(ARRAY[7,8,9], 1, true)", [(3,), (2,), (1,)]),
        ("SELECT array_cat(ARRAY[1], ARRAY[2])", [([1, 2],)]),
        ("SELECT array_cat(NULL::integer[], ARRAY[1, 2])", [([1, 2],)]),
        ("SELECT array_cat(ARRAY[1, 2], NULL::integer[])", [([1, 2],)]),
        ("SELECT array_cat(NULL::integer[], NULL::integer[])", [(None,)]),
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


#: array_cat of two arrays on Trino, as release v0.1.0-alpha.509 (R509-K) wrote it: a column can be
#: NULL at run time, and Trino's concat of a NULL array is NULL, so the other array is answered.
_ARRAY_CAT_ON_TRINO = "CASE WHEN a IS NULL THEN b WHEN b IS NULL THEN a ELSE CONCAT(a, b) END"


@pytest.mark.parametrize("dialect", ["trino", "presto", "athena"])
@pytest.mark.parametrize(
    ("call", "rule", "written"),
    [
        ("array_cat(a, b)", 2, _ARRAY_CAT_ON_TRINO),
        ("array_cat(NULL, b)", 0, "b"),
        ("array_cat(a, NULL)", 1, "a"),
    ],
)
def test_array_cat_on_trino_is_written_by_its_grid_row(dialect: str, call: str, rule: int, written):
    row = arrays.ARRAY_CAT.read.rules[rule]
    assert row.matched(sqlglot.parse_one(call, read="postgres")) is not None
    assert isinstance(row.forms[dialect], str)
    assert transpile(f"SELECT {call} AS v FROM t", dialect) == f"SELECT {written} AS v FROM t"


@pytest.mark.parametrize("dialect", ["trino", "presto", "athena"])
def test_array_cat_of_two_null_arrays_on_trino_is_null(dialect: str):
    assert (
        transpile("SELECT array_cat(NULL, NULL) AS v FROM t", dialect) == "SELECT NULL AS v FROM t"
    )
