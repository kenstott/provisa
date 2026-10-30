# Copyright (c) 2026 Kenneth Stott
# Canary: e6a04f92-7b31-4d58-a2c7-1d95f8036be7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Ranges and multiranges are emulated on every engine (REQ-2171, RANGES ARE EMULATED ON EVERY
ENGINE).

The statements of tests/unit/range_corpus.py run here as a registered column runs: the compiler
marks the column, the transpiler writes the emulation, and DuckDB runs the portable SQL over the
column's text. Each answer is PostgreSQL 17's, recorded (tests/unit/pg17_range_answers.py). The
same expressions are every other dialect's, so what is checked for them here is the SQL emitted."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

from typing import Any

import duckdb
import pytest
import sqlglot

from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.core import pg_range
from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.duckdb_pg_functions import register
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino
from tests.unit import range_corpus as corpus
from tests.unit.pg17_range_answers import RECORDED

ENGINE_DIALECTS = ("trino", "clickhouse", "bigquery", "snowflake", "databricks", "tsql", "mysql")
ALL_DIALECTS = (*ENGINE_DIALECTS, "duckdb", "spark", "oracle", "redshift", "exasol", "athena")


@pytest.fixture(scope="module")
def engine() -> Any:
    con = duckdb.connect()
    register(con)
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA s")
    return con


def _load(con: Any, samples: list[str | None]) -> None:
    con.execute("CREATE OR REPLACE TABLE s.t (i INTEGER, v VARCHAR)")
    con.executemany("INSERT INTO s.t VALUES (?, ?)", list(enumerate(samples)))


def _answer(con: Any, statement: str, subtype: str) -> list | str:
    sql = transpile_to_duckdb(mark_subtype_operations(statement, {("s", "t"): {"v": subtype}}))
    try:
        rows = con.execute(sql).fetchall()
    except duckdb.Error as error:
        known = from_engine(error)
        if known is None:
            raise
        return f"ERROR {known.sqlstate}"
    return [corpus.text_of(row[-1]) for row in rows]


def _statements(type_name: str, selected: Any) -> list[str]:
    return [s for s in RECORDED[type_name] if selected(s)]


@pytest.mark.parametrize("type_name", list(corpus.SAMPLES))
def test_a_range_columns_operators_answer_as_postgresql_does(engine, type_name):
    _load(engine, corpus.SAMPLES[type_name])
    for statement in _statements(type_name, lambda s: " AS a CROSS JOIN " in s):
        assert _answer(engine, statement, type_name) == RECORDED[type_name][statement], statement


@pytest.mark.parametrize("type_name", list(corpus.SAMPLES))
def test_a_range_columns_parts_and_order_answer_as_postgresql_does(engine, type_name):
    _load(engine, corpus.SAMPLES[type_name])
    for statement in _statements(type_name, lambda s: "CROSS JOIN" not in s and "value" not in s):
        got = _answer(engine, statement, type_name)
        assert got == RECORDED[type_name][statement], statement


def _value_cases(type_name: str, op: str) -> tuple[str, list, list[tuple[str, str]]]:
    """Every pair PostgreSQL answers, in one read, with its answers; then a read of each pair it
    refuses, a value operator raising for the whole read otherwise, up to three of them."""
    count = range(len(corpus.SAMPLES[type_name]))
    pairs = [(i, j) for i in count for j in count]
    answers = RECORDED[type_name][f"value {op}"]
    refused = [(p, a) for p, a in zip(pairs, answers, strict=True) if isinstance(a, str)]
    kept = [a[0] for a in answers if not isinstance(a, str)]
    skipped = ", ".join(str(100 * i + j) for (i, j), _ in refused) or "-1"
    statement = (
        f"SELECT a.i, b.i, (a.v {op} b.v)::text AS r FROM s.t AS a, s.t AS b "
        f"WHERE a.i * 100 + b.i NOT IN ({skipped}) ORDER BY a.i, b.i"
    )
    return (
        statement,
        kept,
        [(corpus.value_statement(op, i, j), want) for (i, j), want in refused[:3]],
    )


@pytest.mark.parametrize("type_name", list(corpus.SAMPLES))
@pytest.mark.parametrize("op", corpus.VALUE_OPERATORS)
def test_union_intersection_and_difference_answer_as_postgresql_does(engine, type_name, op):
    _load(engine, corpus.SAMPLES[type_name])
    statement, want, refused = _value_cases(type_name, op)
    assert _answer(engine, statement, type_name) == want
    for one, sqlstate in refused:
        assert _answer(engine, one, type_name) == sqlstate, one


@pytest.mark.parametrize("type_name", list(corpus.MULTIRANGES))
def test_a_multirange_columns_answers_are_postgresqls(engine, type_name):
    _load(engine, corpus.MULTIRANGES[type_name])
    for statement, want in RECORDED[type_name].items():
        assert _answer(engine, statement, type_name) == want, statement


def test_a_constructor_over_columns_answers_as_postgresql_does(engine):
    columns = ", ".join(
        f"{name} {'DECIMAL(10,2)' if kind.startswith('numeric') else kind}"
        for name, (kind, _) in corpus.BOUND_COLUMNS.items()
    )
    engine.execute(f"CREATE OR REPLACE TABLE s.b ({columns})")
    for row in zip(*(values for _, values in corpus.BOUND_COLUMNS.values()), strict=True):
        engine.execute(f"INSERT INTO s.b VALUES ({', '.join(row)})")
    for statement, want in RECORDED["constructors"].items():
        assert _answer(engine, statement, "int4range") == want, statement


# --- what every dialect is sent ------------------------------------------------------------------


COLUMNS = {("s", "t"): {"v": "int4range", "n": "numrange", "m": "int4multirange"}}


def _written(sql: str, dialect: str) -> str:
    return transpile(mark_subtype_operations(sql, COLUMNS), dialect)


OPERATIONS = [
    "SELECT a.i FROM s.t AS a WHERE a.v && '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v @> 3",
    "SELECT a.i FROM s.t AS a WHERE a.v <@ '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v -|- '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v << '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v >> '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v &< '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v &> '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.v < '[1,5)'",
    "SELECT a.i FROM s.t AS a WHERE a.n && '[1.5,2)'",
    "SELECT a.i FROM s.t AS a ORDER BY a.v",
    "SELECT a.v + '[1,5)'::int4range FROM s.t AS a",
    "SELECT a.v * '[1,5)'::int4range FROM s.t AS a",
    "SELECT a.v - '[1,5)'::int4range FROM s.t AS a",
    "SELECT lower(a.v), upper(a.v), isempty(a.v), lower_inc(a.v), upper_inf(a.v) FROM s.t AS a",
    "SELECT range_merge(a.v, '[1,5)') FROM s.t AS a",
    "SELECT int4range(a.i, a.i + 1, '[]') FROM s.t AS a",
]


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
@pytest.mark.parametrize("sql", OPERATIONS)
def test_every_dialect_is_sent_sql_it_reads_for_every_operation(dialect, sql):
    written = _written(sql, dialect)
    assert "PROVISA_SUBTYPE" not in written.upper()
    assert "INT4RANGE" not in written.upper()
    # Whatever the target's grammar, what is sent parses as it (an unknown function would, so the
    # checks of each dialect's own forms are below).
    sqlglot.parse_one(written, read=dialect)


@pytest.mark.parametrize("sql", OPERATIONS)
def test_trinos_own_pipeline_reads_what_is_written_for_it(sql):
    written = transpile_to_trino(mark_subtype_operations(sql, COLUMNS))
    assert "PROVISA_SUBTYPE" not in written.upper()


@pytest.mark.parametrize(
    ("dialect", "fragment"),
    [
        ("trino", "NULLIF(a.v, 'empty')"),
        ("trino", "STRPOS(NULLIF(a.v, 'empty'), ',')"),
        ("mysql", "LOCATE(',', NULLIF(a.v, 'empty'))"),
        ("mysql", "CHAR_LENGTH(a.v)"),
        ("tsql", "CHARINDEX(',', NULLIF(a.v, 'empty'))"),
        ("tsql", "LEN(a.v)"),
        ("bigquery", "INSTR(NULLIF(a.v, 'empty'), ',')"),
        ("clickhouse", "POSITION(nullIf(a.v, 'empty'), ',')"),
        ("snowflake", "CHARINDEX(',', NULLIF(\"a\".\"v\", 'empty'))"),
        ("databricks", "LOCATE(',', NULLIF(a.v, 'empty'))"),
    ],
)
def test_a_dialect_reads_a_bound_with_its_own_functions(dialect, fragment):
    assert fragment in _written("SELECT a.i FROM s.t AS a WHERE a.v && '[1,5)'", dialect)


@pytest.mark.parametrize(
    ("dialect", "numeric", "timestamp"),
    [
        ("trino", "DECIMAL(38, 10)", "TIMESTAMP(6)"),
        ("mysql", "DECIMAL(38, 10)", "DATETIME(6)"),
        ("tsql", "NUMERIC(38, 10)", "DATETIME2(6)"),
        ("snowflake", "DECIMAL(38, 10)", "TIMESTAMP_NTZ(6)"),
        ("bigquery", "BIGNUMERIC", "DATETIME"),
        ("clickhouse", "Decimal(38, 10)", "DateTime64(6)"),
        ("databricks", "DECIMAL(38, 10)", "TIMESTAMP_NTZ"),
    ],
)
def test_bounds_compare_as_the_element_type_on_each_dialect(dialect, numeric, timestamp):
    columns = {("s", "t"): {"n": "numrange", "w": "tsrange"}}
    numeric_sql = "SELECT a.i FROM s.t AS a WHERE a.n && '[1,2)'"
    time_sql = "SELECT a.i FROM s.t AS a WHERE a.w && '[2020-01-01,2020-02-01)'"
    assert numeric in transpile(mark_subtype_operations(numeric_sql, columns), dialect)
    assert timestamp in transpile(mark_subtype_operations(time_sql, columns), dialect)


def test_a_predicate_is_a_value_in_a_select_list_only_where_the_dialect_has_booleans():
    written = _written("SELECT a.v && '[1,5)' AS o FROM s.t AS a", "tsql")
    assert written.startswith("SELECT CASE WHEN")
    assert " THEN 1 WHEN NOT " in written


@pytest.mark.parametrize(
    ("dialect", "function"),
    [
        ("duckdb", "error("),
        ("trino", "FAIL("),
        ("bigquery", "ERROR("),
        ("clickhouse", "throwIf("),
        ("databricks", "RAISE_ERROR("),
    ],
)
def test_a_result_postgresql_refuses_is_refused_by_the_engine(dialect, function):
    written = _written("SELECT a.v + '[1,5)'::int4range FROM s.t AS a", dialect)
    assert function.lower() in written.lower()
    assert "result of range union would not be contiguous" in written


def test_postgresql_runs_every_operator_natively():
    assert "CAST(a.v AS INT4RANGE) -|- '[1,5)'" in _written(
        "SELECT a.i FROM s.t AS a WHERE a.v -|- '[1,5)'", "postgres"
    )
    assert "CAST(a.v AS INT4RANGE) * " in _written(
        "SELECT a.v * '[1,5)'::int4range FROM s.t AS a", "postgres"
    )
    assert "LOWER(CAST(a.v AS INT4RANGE))" in _written(
        "SELECT lower(a.v) FROM s.t AS a", "postgres"
    )
    assert "CAST(a.m AS INT4MULTIRANGE) @> 3" in _written(
        "SELECT a.i FROM s.t AS a WHERE a.m @> 3", "postgres"
    )


@pytest.mark.parametrize(
    ("dialect", "fragment"),
    [
        ("duckdb", "LIST_FILTER(REGEXP_EXTRACT_ALL("),
        ("trino", "ANY_MATCH(REGEXP_EXTRACT_ALL("),
        ("athena", "ANY_MATCH(REGEXP_EXTRACT_ALL("),
        ("clickhouse", "arrayExists("),
        ("databricks", "EXISTS(REGEXP_EXTRACT_ALL("),
        ("spark", "EXISTS(REGEXP_EXTRACT_ALL("),
        ("snowflake", "ARRAY_SIZE(FILTER(REGEXP_SUBSTR_ALL("),
        ("bigquery", "UNNEST(REGEXP_EXTRACT_ALL("),
        ("tsql", "STRING_SPLIT("),
        ("mysql", "JSON_TABLE("),
    ],
)
def test_a_multirange_predicate_uses_each_dialects_list_form(dialect, fragment):
    written = _written("SELECT a.i FROM s.t AS a WHERE a.m && '[1,5)'", dialect)
    assert fragment.upper() in written.upper()


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_a_multirange_of_literals_is_answered_where_the_statement_is_read(dialect):
    written = transpile("SELECT '{[1,3)}'::int4multirange + '{[3,5)}'::int4multirange", dialect)
    assert "'{[1,5)}'" in written


@pytest.mark.parametrize(
    ("dialect", "sort", "fold"),
    [
        ("duckdb", "LIST_SORT(", "LIST_REDUCE("),
        ("trino", "ARRAY_SORT(", "REDUCE("),
        ("athena", "ARRAY_SORT(", "REDUCE("),
        ("clickhouse", "arraySort(", "arrayFold("),
        ("databricks", "ARRAY_SORT(", "AGGREGATE("),
        ("spark", "ARRAY_SORT(", "AGGREGATE("),
    ],
)
def test_a_multirange_built_from_columns_is_sql_where_the_dialect_folds(dialect, sort, fold):
    written = _written("SELECT a.m + a.m FROM s.t AS a", dialect).upper()
    assert sort.upper() in written and fold.upper() in written
    assert "PROVISA_" not in written  # no function of Provisa's: the engine computes it


def test_snowflake_folds_a_multirange_but_cannot_sort_one():
    assert "REDUCE(" in _written("SELECT a.m * a.m FROM s.t AS a", "snowflake")
    assert "REDUCE(" in _written("SELECT a.m - a.m FROM s.t AS a", "snowflake")
    with pytest.raises(SubtypeOperationRefused):
        _written("SELECT a.m + a.m FROM s.t AS a", "snowflake")


@pytest.mark.parametrize("dialect", ("bigquery", "tsql", "mysql"))
@pytest.mark.parametrize("op", corpus.VALUE_OPERATORS)
def test_a_multirange_built_from_columns_is_refused_by_name_without_a_fold(dialect, op):
    with pytest.raises(SubtypeOperationRefused) as refused:
        _written(f"SELECT a.m {op} a.m FROM s.t AS a", dialect)
    assert refused.value.params == {
        "subtype": "int4multirange",
        "operation": op,
        "dialect": dialect,
    }


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a.v && b.v AS r FROM s.t AS a, s.t AS b",
        "SELECT a.v -|- b.v AS r FROM s.t AS a, s.t AS b",
        "SELECT a.v = b.v AS r FROM s.t AS a, s.t AS b",
        "SELECT '[1,2)'::int4range << '[3,4)'::int4range AS r",
        "SELECT isempty('empty'::int4range) AS r",
    ],
)
def test_pgwire_describes_a_predicate_as_a_boolean_whatever_integer_the_engine_gives(sql):
    (described,) = result_casts(sql)
    assert described.oid == 16
    assert (described.coerce(1), described.coerce(0), described.coerce(None)) == (True, False, None)


def test_a_shift_of_integers_is_no_predicate():
    (described,) = result_casts("SELECT 1 << 2 AS r")
    assert described is None


@pytest.mark.parametrize("dialect", ("trino", "athena", "snowflake"))
def test_an_offset_is_named_once_where_the_dialect_has_a_regular_expression(dialect):
    columns = {("s", "t"): {"z": "tstzrange"}}
    written = transpile(
        mark_subtype_operations("SELECT a.i FROM s.t AS a WHERE a.z && a.z", columns), dialect
    )
    assert "REGEXP_REPLACE" in written.upper()
    assert ":00' ELSE" not in written and "':00'" not in written


def test_a_trino_zoned_statement_stays_near_the_size_of_a_plain_one():
    def size(subtype: str) -> int:
        marked = mark_subtype_operations(
            "SELECT a.i, range_merge(a.v, b.v)::text AS r FROM s.t AS a, s.t AS b",
            {("s", "t"): {"v": subtype}},
        )
        return len(transpile(marked, "trino"))

    assert size("tstzrange") < size("tsrange") * 1.5


# What each engine itself refused, or answered differently, when the corpus ran on it: Trino 481,
# ClickHouse 24.3, MariaDB 11, SQL Server 2022.


def test_a_multirange_built_from_columns_stays_small_enough_for_trinos_compiler():
    """Trino refused these (QUERY_EXCEEDED_COMPILER_LIMIT) when each bound's fields were chosen
    apart and the piece was written twice: a side is chosen once, from the text held."""
    for sql in ("SELECT a.m * a.m FROM s.t AS a", "SELECT a.m - a.m FROM s.t AS a"):
        assert len(_written(sql, "trino")) < 8000


def test_clickhouses_list_of_a_nullable_text_is_of_its_text():
    # Nested type Array(String) cannot be inside Nullable type.
    assert "extractAll(ifNull(a.m, '')" in _written(
        "SELECT a.i FROM s.t AS a WHERE a.m && '[1,5)'", "clickhouse"
    )


@pytest.mark.parametrize(
    ("dialect", "fragment"),
    [
        ("clickhouse", "arrayStringConcat(groupArray("),  # no groupConcat on 24.3
        ("trino", "ARRAY_JOIN(ARRAY_AGG("),  # its LISTAGG asks a WITHIN GROUP
    ],
)
def test_a_group_of_ranges_is_joined_in_the_dialects_own_form(dialect, fragment):
    assert fragment in _written("SELECT range_agg(a.v) FROM s.t AS a", dialect)


def test_clickhouse_reads_an_offset_with_its_own_reader():
    columns = {("s", "t"): {"z": "tstzrange"}}
    written = transpile(
        mark_subtype_operations("SELECT a.i FROM s.t AS a WHERE a.z && a.z", columns), "clickhouse"
    )
    assert "parseDateTime64BestEffortOrNull(" in written


@pytest.mark.parametrize(
    ("dialect", "split"), [("tsql", "STRING_SPLIT("), ("mysql", "JSON_TABLE(")]
)
def test_the_empty_item_of_a_multirange_of_no_range_is_no_range(dialect, split):
    """Splitting '{}' gives one empty item: on SQL Server a substring of it failed, and read as a
    range it held everything. It is kept from both."""
    written = _written("SELECT a.i FROM s.t AS a WHERE a.m && '[1,5)'", dialect)
    assert split in written
    assert "IS NULL" in written and "NULLIF(" in written


def test_a_literal_cast_beside_a_range_is_never_cast_again_on_the_engine():
    """'...+00'::timestamptz is no value of SQL Server's DATETIMEOFFSET or ClickHouse's DateTime."""
    columns = {("s", "t"): {"z": "tstzrange"}}
    marked = mark_subtype_operations(
        "SELECT a.z @> '2020-01-10 00:00:00+00'::timestamptz AS r FROM s.t AS a", columns
    )
    assert "'2020-01-10 00:00:00+00' AS DATETIMEOFFSET" not in transpile(marked, "tsql")


@pytest.mark.parametrize("dialect", ("mysql", "clickhouse"))
def test_a_timestamp_bound_read_as_text_is_the_text_held(dialect):
    columns = {("s", "t"): {"w": "tsrange"}}
    marked = mark_subtype_operations("SELECT lower(a.w)::text AS r FROM s.t AS a", columns)
    # Neither engine's cast writes PostgreSQL's form: no timestamp type at all.
    assert "DATETIME" not in transpile(marked, dialect).upper()


# --- literals, as PostgreSQL reads them ----------------------------------------------------------


@pytest.mark.parametrize(
    ("statement", "sqlstate"),
    [
        ("SELECT '[5,1)'::int4range", "22000"),
        ("SELECT 'x'::int4range", "22P02"),
        ("SELECT '[1,2'::int4range", "22P02"),
        ("SELECT '[a,2)'::int4range", "22P02"),
        ("SELECT '[1,2)'::int4range + '[5,6)'::int4range", "22000"),
        ("SELECT '[1,10)'::int4range - '[3,4)'::int4range", "22000"),
        ("SELECT '{[1,2)'::int4multirange", "22P02"),
        ("SELECT int4range(1, 2, 'x')", "42601"),
    ],
)
def test_a_literal_postgresql_refuses_is_refused_with_its_sqlstate(statement, sqlstate):
    with pytest.raises(PostgresError) as refused:
        transpile_to_duckdb(statement)
    assert refused.value.sqlstate == sqlstate


def test_a_multirange_literal_is_canonical():
    rtype = pg_range.MULTIRANGE_TYPES["int4multirange"]
    value = pg_range.parse_multirange(rtype, "{[1,3), [2,7), empty ,(9,10]}")
    assert pg_range.multirange_text(value) == "{[1,7),[10,11)}"
