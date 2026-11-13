# Copyright (c) 2026 Kenneth Stott
# Canary: 668eeabf-f65e-41ee-b0aa-6a7a3dedf5d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FUNCTIONS ARE COMPILER PUSHDOWN (REQ-2167): a function a dialect does not have is its form
there, written at transpile time from the one table, on any argument a statement can name; a
dialect with no form refuses it by name; nothing is left on the engine for one engine alone."""

# Requirements: REQ-2167

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler import function_macros
from provisa.transpiler.pg_forms_system import OF_A_STORED_VALUE
from provisa.transpiler.function_macros import (
    MACROS,
    REFUSED_ROWS,
    FunctionRefused,
    inline,
    outcome,
)
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_DIALECTS = (
    "duckdb",
    "trino",
    "presto",
    "athena",
    "spark",
    "databricks",
    "snowflake",
    "mysql",
    "bigquery",
    "clickhouse",
    "tsql",
    "redshift",
    "sqlite",
    "oracle",
)


@pytest.fixture
def con():
    connection = duckdb.connect()
    register(connection)
    connection.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "('a', 'b', 2.5, 5, -1::BIGINT, '\\x01\\x02'::BLOB), "
        "('c', NULL, 3.5, 255, 7::BIGINT, '\\xff'::BLOB)) v(a, b, x, n, big, bytes)"
    )
    return connection


def _one(con, sql: str):
    return con.execute(transpile_to_duckdb(sql)).fetchall()


def test_no_function_is_left_a_macro_on_the_engine(con):
    macros = {
        (row[0], len(row[1]))
        for row in con.execute(
            "SELECT DISTINCT function_name, parameters FROM duckdb_functions() "
            "WHERE function_type = 'macro' AND schema_name = 'main'"
        ).fetchall()
    }
    assert not macros & set(MACROS)


@pytest.mark.parametrize(("name", "arity"), sorted(MACROS))
def test_every_overload_is_native_on_postgresql_and_has_a_form_on_duckdb(name, arity):
    assert outcome(name, arity, "postgres") == "native"
    # REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED: replay found no form answering as PostgreSQL does.
    assert outcome(name, arity, "duckdb") == (
        "refused" if (name, arity) in REFUSED_ROWS else "emulated"
    )


def _routine_bound(column: sqlglot.exp.Column) -> bool:
    """A Trino routine's parameter read in its own body: the statement defines it (WITH FUNCTION)."""
    routine = column.find_ancestor(sqlglot.exp.FunctionSpecification)
    return routine is not None and column.name in {
        p.name for p in routine.this.expressions if isinstance(p, sqlglot.exp.ColumnDef)
    }


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_every_form_is_sql_its_dialect_reads_back(dialect):
    for name, arity in MACROS:
        if outcome(name, arity, dialect) != "emulated":
            continue
        args = [f"p{i}" for i in range(arity)]
        # A column of no said type is refused by name: only the statement tells inet from cidr.
        said = ["p0::inet", *args[1:]] if name == "set_masklen" else args
        said = [args[0], "2"] if name in ("round_numeric", "trunc_numeric") else said
        # A column's storage and collation are its source's: only a value is told.
        said = ["'x'"] if name in OF_A_STORED_VALUE else said
        # ClickHouse reads the Nth match's pattern as a constant, and refuses a column's by name.
        constant = dialect == "clickhouse" and name == "regexp_replace"
        said = [args[0], "'x'", *args[2:]] if constant else said
        try:
            out = transpile(f"SELECT {name}({', '.join(said)}) AS v", dialect)
        except FunctionRefused:
            # A row of the polyfill grid refuses it on this dialect (REQ-2236), or this dialect is
            # no column of the grid, which refuses a call a row matches by name: no form is written.
            assert name in {f.name for f in GRID.features}
            continue
        tree = sqlglot.parse_one(out, read=dialect)
        # A ClickHouse statement's SETTINGS name a setting, not a column (REQ-2171: its old analyzer).
        tree.set("settings", None)
        columns = {c.name for c in tree.find_all(sqlglot.exp.Column) if not _routine_bound(c)}
        # Every parameter is bound; a lambda's own variable (_x) is the form's.
        assert {c for c in columns if not c.startswith("_")} <= set(args), out


@pytest.mark.parametrize(
    ("sql", "dialect", "expected"),
    [
        ("SELECT time(a) AS v FROM t", "duckdb", "SELECT CAST(a AS TIME) AS v FROM t"),
        (
            "SELECT textcat(a, b) AS v FROM t",
            "mysql",
            "SELECT CAST(CONCAT(a, b) AS CHAR) AS v FROM t",
        ),
        (
            "SELECT textcat(a, b) AS v FROM t",
            "snowflake",
            'SELECT CAST("a" || "b" AS VARCHAR) AS "v" FROM "t"',
        ),
        ("SELECT textlike(a, b) AS v FROM t", "trino", "SELECT (a LIKE b ESCAPE '\\') AS v FROM t"),
        (
            "SELECT texticlike(a, b) AS v FROM t",
            "trino",
            "SELECT (LOWER(a) LIKE LOWER(b) ESCAPE '\\') AS v FROM t",
        ),
        ("SELECT textregexeq(a, b) AS v FROM t", "trino", "SELECT (REGEXP_LIKE(a, b)) AS v FROM t"),
        (
            "SELECT textregexeq(a, b) AS v FROM t",
            "snowflake",
            'SELECT (REGEXP_INSTR("a", "b") > 0) AS "v" FROM "t"',
        ),
        ("SELECT textregexne(a, b) AS v FROM t", "spark", "SELECT NOT (a RLIKE b) AS v FROM t"),
        ("SELECT every(a) AS v FROM t", "snowflake", 'SELECT BOOLAND_AGG("a") AS "v" FROM "t"'),
        ("SELECT every(a) AS v FROM t", "bigquery", "SELECT LOGICAL_AND(a) AS v FROM t"),
        ("SELECT dcbrt(x) AS v FROM t", "trino", "SELECT CBRT(x) AS v FROM t"),
        ("SELECT dtrunc(x) AS v FROM t", "trino", "SELECT TRUNCATE(CAST(x AS DOUBLE)) AS v FROM t"),
        ("SELECT unicode_version() AS v", "mysql", "SELECT '15.1' AS v"),
        # array_cat (its grid row, provisa.transpiler.polyfill.arrays): PostgreSQL's NULL with an
        # array is the array, NULL with NULL is NULL.
        (
            "SELECT array_cat(a, b) AS v FROM t",
            "duckdb",
            "SELECT CASE WHEN NOT a IS NULL OR NOT b IS NULL THEN LIST_CONCAT(a, b) END AS v FROM t",
        ),
        (
            "SELECT array_cat(a, b) AS v FROM t",
            "trino",
            "SELECT CASE WHEN a IS NULL THEN b WHEN b IS NULL THEN a ELSE CONCAT(a, b) END AS v "
            "FROM t",
        ),
        (
            "SELECT array_cat(NULL, b) AS v FROM t",
            "trino",
            "SELECT b AS v FROM t",
        ),
        (
            "SELECT array_cat(a, NULL) AS v FROM t",
            "trino",
            "SELECT a AS v FROM t",
        ),
        (
            "SELECT array_cat(NULL, NULL) AS v FROM t",
            "trino",
            "SELECT NULL AS v FROM t",
        ),
    ],
)
def test_a_form_is_written_for_its_dialect(sql, dialect, expected):
    assert transpile(sql, dialect) == expected


def test_a_form_survives_the_engines_own_rewrites():
    assert "BOOL_AND(a)" in transpile_to_trino("SELECT every(a) AS v FROM t")


#: arraycontains on Trino, as PostgreSQL answers it: NULL of a NULL array, false where the second
#: holds a NULL (equal to nothing), and otherwise whether the first holds each of its elements.
_ARRAYCONTAINS_ON_TRINO = (
    "CASE WHEN {a} IS NULL OR {b} IS NULL THEN NULL ELSE ALL_MATCH({b}, _x -> NOT _x IS NULL "
    "AND COALESCE(CONTAINS({a}, _x), FALSE)) END"
)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT arraycontains(a, b) AS v FROM t",
        "SELECT a @> b AS v FROM t",
    ],
)
def test_an_arrays_containment_has_postgresqls_answers_on_trino(sql):
    assert transpile(sql, "trino") == (
        f"SELECT {_ARRAYCONTAINS_ON_TRINO.format(a='a', b='b')} AS v FROM t"
    )


def test_an_arrays_containment_reaches_trino_as_trinos_own_sql():
    """REQ-2176: a session hop's label filter (``__labels @> ARRAY[...]``) is PostgreSQL's operator
    in the semantic statement; the statement Trino is sent carries no ``@>``, and Trino's own
    rewrites, which read it back as Trino's SQL, read it."""
    written = transpile_to_trino("SELECT k FROM t AS _hs WHERE _hs.\"__labels\" @> ARRAY['Big']")
    assert "@>" not in written
    tree = sqlglot.parse_one(written, read="trino")
    assert tree.find(sqlglot.exp.ArrayContainsAll) is None
    assert written == "SELECT k FROM t AS _hs WHERE " + _ARRAYCONTAINS_ON_TRINO.format(
        a='_hs."__labels"', b="ARRAY['Big']"
    )


def test_an_arrays_containment_stays_postgresqls_operator_on_postgresql():
    assert transpile("SELECT a @> b AS v FROM t", "postgres") == "SELECT a @> b AS v FROM t"


@pytest.mark.parametrize(
    ("sql", "dialect", "function"),
    [
        ("SELECT bytealike(a, b) FROM t", "mysql", "bytealike"),
        ("SELECT bytealike(a, b) FROM t", "trino", "bytealike"),
        ("SELECT regexp_match(a, b) FROM t", "snowflake", "regexp_match"),
        ("SELECT center(a) FROM t", "bigquery", "center"),
        ("SELECT a FROM t WHERE jsonb_pretty(a) = 'object'", "clickhouse", "jsonb_pretty"),
        ("SELECT textlike(a, b) FROM t", "mysql", "textlike"),
        ("SELECT regexp_substr(a, b) FROM t", "clickhouse", "regexp_substr"),
        ("SELECT time(a) FROM t", "trino", "time"),
    ],
)
def test_a_function_with_no_form_is_refused_by_name(sql, dialect, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, dialect)
    assert refused.value.params == {"function": function, "dialect": dialect}
    assert refused.value.code == "query.function_unsupported"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT sind(x), textcat(a, b), bytealike(a, b) FROM t",
        'SELECT pg_catalog."textcat"(a, b) FROM t',
    ],
)
def test_postgresql_runs_its_own_function(sql):
    written = transpile(sql, "postgres")
    assert "CASE" not in written and "CAST" not in written


def test_a_function_of_a_name_sqlglot_writes_stays_sqlglots():
    assert transpile("SELECT regexp_substr(a, b) AS v FROM t", "mysql") == (
        "SELECT REGEXP_SUBSTR(a, b) AS v FROM t"
    )


def test_trino_has_no_regexp_substr_of_its_own():
    assert transpile("SELECT regexp_substr(a, b) AS v FROM t", "trino") == (
        "SELECT REGEXP_EXTRACT(a, b) AS v FROM t"
    )
    with pytest.raises(FunctionRefused):
        transpile("SELECT regexp_substr(a, b, 2) AS v FROM t", "trino")


def test_an_overload_a_dialect_does_not_take_is_refused():
    assert outcome("regexp_substr", 4, "bigquery") == "mapped"
    assert outcome("regexp_substr", 5, "bigquery") == "refused"
    assert outcome("regexp_substr", 6, "mysql") == "refused"
    assert outcome("regexp_substr", 6, "snowflake") == "mapped"


def test_a_function_named_in_pg_catalog_is_that_function():
    assert transpile('SELECT pg_catalog."textcat"(a, b) AS v FROM t', "spark") == (
        "SELECT CAST(a || b AS STRING) AS v FROM t"
    )


def test_a_call_in_an_argument_is_written_too():
    written = transpile("SELECT textcat(textcat(a, b), a) AS v FROM t", "spark")
    assert "textcat" not in written.lower()


def test_an_expression_argument_keeps_its_place():
    written = transpile("SELECT textcat(x + 1, y) AS v FROM t", "spark")
    assert "(x + 1) || y" in written


def test_the_column_of_a_call_is_named_for_the_function(con):
    relation = con.execute(transpile_to_duckdb("SELECT textcat(a, b) FROM t"))
    assert [d[0] for d in relation.description] == ["textcat"]


def test_a_call_with_no_overload_of_its_argument_count_is_the_engines_to_refuse():
    tree = sqlglot.parse_one("SELECT textcat(a) FROM t", read="postgres")
    assert inline(tree, "mysql").sql(dialect="mysql") == "SELECT TEXTCAT(a) FROM t"


def test_a_statement_that_calls_none_of_them_is_not_parsed():
    assert not function_macros.mentioned("SELECT a FROM t", "duckdb")
    assert not function_macros.mentioned("SELECT textcat(a, b) FROM t", "postgres")
    assert function_macros.mentioned('SELECT "textcat"(a, b) FROM t', "duckdb")


@pytest.mark.parametrize(
    ("sql", "rows"),
    [
        ("SELECT textcat(a, b) FROM t ORDER BY a", [("ab",), (None,)]),
        ("SELECT textlike(a, 'a%') FROM t ORDER BY a", [(True,), (False,)]),
        ("SELECT quote_nullable(b) FROM t ORDER BY a", [("'b'",), ("NULL",)]),
        ("SELECT quote_ident(a) FROM t ORDER BY a", [("a",), ("c",)]),
        ("SELECT bpchar(a) FROM t ORDER BY a", [("a",), ("c",)]),
        ("SELECT dround(x) FROM t ORDER BY a", [(2.0,), (4.0,)]),
        ("SELECT to_bin(n) FROM t ORDER BY a", [("101",), ("11111111",)]),
        ("SELECT to_oct(n) FROM t ORDER BY a", [("5",), ("377",)]),
        ("SELECT to_bin(big) FROM t ORDER BY a", [("1" * 64,), ("111",)]),
        ("SELECT get_byte(bytes, 0) FROM t ORDER BY a", [(1,), (255,)]),
        ("SELECT scale(x) FROM t ORDER BY a", [(1,), (1,)]),
        ("SELECT every(a = 'a') FROM t", [(False,)]),
        ("SELECT string_to_table(a || ',' || a, ',') FROM t WHERE a = 'a'", [("a",), ("a",)]),
    ],
)
def test_a_function_computes_over_a_column(con, sql, rows):
    assert _one(con, sql) == rows


@pytest.mark.parametrize(
    ("sql", "value"),
    [
        ("SELECT to_bin(-1)", "1" * 32),
        ("SELECT to_oct(-1)", "37777777777"),
        ("SELECT to_oct(-1::bigint)", "1777777777777777777777"),
        ("SELECT to_bin(0)", "0"),
        ("SELECT set_byte('\\x00'::bytea, 0, 300)", b"\x2c"),
        ("SELECT height('(2,3),(0,0)')", 3.0),
        ("SELECT width('(2,3),(0,0)')", 2.0),
        ("SELECT radius('<(1,2),3>')", 3.0),
        ("SELECT isclosed('((0,0),(1,1))')", True),
        ("SELECT isopen('[(0,0),(1,1)]')", True),
        ("SELECT pg_wal_lsn_diff('0/10', '0/8')", 8),
        ("SELECT pg_snapshot_xmin('10:20:12,15')", 10),
        ("SELECT pg_visible_in_snapshot(12, '10:20:12')", False),
        ("SELECT pg_encoding_max_length(6)", 4),
        ("SELECT pg_char_to_encoding('UTF8')", 6),
        ("SELECT mxid_age(5)", -4),
    ],
)
def test_a_function_rewritten_from_a_udf_answers_as_it_did(con, sql, value):
    assert _one(con, sql) == [(value,)]


def test_a_byte_out_of_range_is_postgresqls_error(con):
    with pytest.raises(duckdb.Error, match="2202E: index 5 out of valid range, 0..1"):
        _one(con, "SELECT get_byte(bytes, 5) FROM t WHERE a = 'a'")


def test_a_bit_string_of_an_integer_is_its_low_bits(con):
    assert _one(con, 'SELECT "bit"(5, 8)::text') == [("00000101",)]
    assert _one(con, 'SELECT "bit"(-1, 4)::text') == [("1111",)]


@pytest.mark.parametrize(
    ("dialect", "want"),
    [
        ("trino", "SELECT ABS(CAST(NULL AS DOUBLE)) AS v"),
        ("clickhouse", "SELECT ABS(CAST(NULL AS Nullable(Float64))) AS v"),
    ],
)
def test_a_call_by_pg_catalog_is_the_call_of_the_function(dialect: str, want: str):
    assert transpile('SELECT pg_catalog."abs"(NULL::double precision) AS v', dialect) == want


def test_a_qualified_call_inside_a_qualified_call_is_read_too():
    written = transpile('SELECT pg_catalog."abs"(pg_catalog."sign"(x)) FROM t', "trino")
    assert "pg_catalog" not in written.lower()


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_a_numeric_literal_keeps_its_digits(dialect: str):
    assert "DECIMAL(21, 2)" in transpile("SELECT (-2.25)::numeric AS v", dialect).upper()


def test_a_numeric_of_a_column_stays_as_said():
    assert transpile("SELECT x::numeric AS v FROM t", "trino") == (
        "SELECT CAST(x AS DECIMAL) AS v FROM t"
    )


@pytest.mark.parametrize(
    ("dialect", "want"),
    [
        ("trino", "REGEXP_LIKE(a, '(?i)' || b)"),
        ("clickhouse", "match(a, CONCAT('(?i)', b))"),
        ("duckdb", "REGEXP_MATCHES(a, b, 'i')"),
    ],
)
def test_the_case_insensitive_match_is_one_form_on_any_engine(dialect: str, want: str):
    assert want in transpile("SELECT texticregexne(a, b) AS v FROM t", dialect)


def test_clickhouses_like_has_no_escape_clause():
    assert "ESCAPE" not in transpile("SELECT textlike(a, b) AS v FROM t", "clickhouse")


@pytest.mark.parametrize(
    ("sql", "dialect", "function"),
    [
        ("SELECT exp(2.5::numeric) AS v", "trino", "exp"),
        ("SELECT avg(c) AS v FROM (VALUES (1.5::numeric)) AS t(c)", "clickhouse", "avg"),
        ("SELECT overlay(a PLACING b FROM 1) AS v FROM t", "trino", "overlay"),
        ("SELECT regexp_count(a, b, 2) AS v FROM t", "trino", "regexp_count"),
        ("SELECT substr(c, 1) AS v FROM (VALUES ('\\xbeef'::bytea)) AS t(c)", "trino", "substring"),
        ("SELECT date_add('2000-01-01'::date, '1 day'::interval) AS v", "clickhouse", "date_add"),
        ('SELECT pg_catalog."gcd"(1, 2) AS v', "trino", "gcd"),
        ("SELECT timestamp('2024-06-15'::date, '12:30:45'::time) AS v", "clickhouse", "timestamp"),
        (
            "SELECT pg_catalog.\"datetime_pl\"('2024-06-15'::date, '12:30:45'::time) AS v",
            "clickhouse",
            "datetime_pl",
        ),
        (
            'SELECT pg_catalog."overlaps"(NULL::time, NULL::time, NULL::time, NULL::time) AS v',
            "clickhouse",
            "overlaps",
        ),
    ],
)
def test_what_a_replay_refused_is_refused_by_name(sql: str, dialect: str, function: str):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, dialect)
    assert refused.value.params == {"function": function, "dialect": dialect}


def test_a_column_of_no_said_type_is_the_engines_to_read():
    assert transpile("SELECT avg(x) AS v FROM t", "trino") == "SELECT AVG(x) AS v FROM t"


@pytest.mark.parametrize(
    ("dialect", "want"),
    [
        ("trino", "(?s)^(?<before>.*?)(?:o)(?<after>.*)$"),
        ("clickhouse", "replaceRegexpOne("),
    ],
)
def test_regexp_replace_with_no_flag_is_the_first_match(dialect: str, want: str):
    assert want in transpile("SELECT regexp_replace(a, 'o', 'X') AS v FROM t", dialect)


def test_regexp_replace_with_the_g_flag_is_every_match():
    written = transpile("SELECT regexp_replace(a, 'o', 'X', 'g') AS v FROM t", "trino")
    assert written == "SELECT REGEXP_REPLACE(a, 'o', 'X') AS v FROM t"


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("SELECT '\\xdead'::bytea AS v", "FROM_HEX('dead')"),
        ("SELECT 'abc'::character AS v", "CAST('a' AS CHAR)"),
        ("SELECT (-2.25)::numeric AS v", "DECIMAL(21, 2)"),
    ],
)
def test_a_literal_keeps_what_its_cast_says(sql: str, want: str):
    assert want in transpile(sql, "trino")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT date_part('century'::text, x::timestamp) AS v FROM t",
        "SELECT ln(x) AS v FROM t",
        'SELECT pg_catalog."in_range"(a::time, b::time, c::interval, true, false) AS v FROM t',
    ],
)
def test_clickhouse_refuses_what_it_cannot_evaluate_or_answers_differently(sql: str):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


def test_a_call_named_for_the_engines_own_spelling_is_written_so():
    assert transpile("SELECT cbrt(x) AS v FROM t", "clickhouse") == "SELECT cbrt(x) AS v FROM t"


@pytest.mark.parametrize("call", ["substr(a, 1 + 1, 3)", "substr(a, 2)", "left(a, 2)"])
def test_a_safe_position_stays_the_engines_own(call: str):
    assert "GREATEST" not in transpile(f"SELECT {call} AS v FROM t", "trino")
    assert "CASE" not in transpile(f"SELECT {call} AS v FROM t", "trino")


@pytest.mark.parametrize(
    ("call", "want"),
    [
        ("substr('Hello World', -7)", "Hello World"),
        ("substr('Hello', -1, 3)", "H"),
        ("substr('Hello', 2, 2)", "el"),
        ("left('Hello', -1)", "Hell"),
        ("right('Hello', -1)", "ello"),
        ("right('Hello', 42)", "Hello"),
    ],
)
def test_a_position_before_the_value_reads_as_postgresql_reads_it(call: str, want: str):
    written = sqlglot.transpile(
        transpile(f"SELECT {call} AS v", "trino"), read="trino", write="duckdb"
    )[0]
    assert duckdb.connect().execute(written).fetchone()[0] == want


def test_a_replacement_of_nothing_is_clickhouses_own():
    """ClickHouse refuses a pattern of nothing; a replacement of nothing is a deletion it writes."""
    assert "REPLACE(a, '\"', '')" in transpile(
        "SELECT pg_catalog.replace(a, '\"', '') AS v FROM t", "clickhouse"
    )


@pytest.mark.parametrize("pattern", ["''", "NULL::text"])
def test_a_pattern_of_nothing_is_refused_on_clickhouse(pattern: str):
    with pytest.raises(FunctionRefused):
        transpile(f"SELECT pg_catalog.replace(a, {pattern}, 'x') AS v FROM t", "clickhouse")


def test_a_timestamp_range_is_compared_on_clickhouse():
    """The emulation strips a bound's quotes with replace(…, '"', '')."""
    marked = mark_subtype_operations(
        "SELECT a.v @> b.v AS r FROM s.t AS a CROSS JOIN s.t AS b", {("s", "t"): {"v": "tsrange"}}
    )
    assert "'\"', '')" in transpile(marked, "clickhouse")
