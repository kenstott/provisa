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

from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler import function_macros
from provisa.transpiler.function_macros import MACROS, FunctionRefused, inline, outcome
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
        row[0]
        for row in con.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() "
            "WHERE function_type = 'macro' AND schema_name = 'main'"
        ).fetchall()
    }
    assert not macros & {name for name, _ in MACROS}


@pytest.mark.parametrize(("name", "arity"), sorted(MACROS))
def test_every_overload_is_native_on_postgresql_and_has_a_form_on_duckdb(name, arity):
    assert outcome(name, arity, "postgres") == "native"
    assert outcome(name, arity, "duckdb") == "emulated"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_every_form_is_sql_its_dialect_reads_back(dialect):
    for name, arity in MACROS:
        if outcome(name, arity, dialect) != "emulated":
            continue
        args = [f"p{i}" for i in range(arity)]
        out = transpile(f"SELECT {name}({', '.join(args)}) AS v", dialect)
        columns = {
            c.name for c in sqlglot.parse_one(out, read=dialect).find_all(sqlglot.exp.Column)
        }
        # Every parameter is bound; a lambda's own variable (_x) is the form's.
        assert {c for c in columns if not c.startswith("_")} <= set(args), out


@pytest.mark.parametrize(
    ("sql", "dialect", "expected"),
    [
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
    ],
)
def test_a_form_is_written_for_its_dialect(sql, dialect, expected):
    assert transpile(sql, dialect) == expected


def test_a_form_survives_the_engines_own_rewrites():
    assert "BOOL_AND(a)" in transpile_to_trino("SELECT every(a) AS v FROM t")


@pytest.mark.parametrize(
    ("sql", "dialect", "function"),
    [
        ("SELECT bytealike(a, b) FROM t", "mysql", "bytealike"),
        ("SELECT sind(x) FROM t", "trino", "sind"),
        ("SELECT regexp_match(a, b) FROM t", "snowflake", "regexp_match"),
        ("SELECT center(a) FROM t", "bigquery", "center"),
        ("SELECT a FROM t WHERE jsonb_typeof(a) = 'object'", "clickhouse", "jsonb_typeof"),
        ("SELECT textlike(a, b) FROM t", "mysql", "textlike"),
        ("SELECT regexp_substr(a, b) FROM t", "clickhouse", "regexp_substr"),
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
    assert _one(con, "SELECT bit(5, 8)::text") == [("101",)]
    assert _one(con, "SELECT bit(-1, 4)::text") == [("1111",)]
