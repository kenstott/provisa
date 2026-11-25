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
    return con.execute(transpile_to_duckdb(sql, generated=False)).fetchall()


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
            out = transpile(f"SELECT {name}({', '.join(said)}) AS v", dialect, generated=False)
        except FunctionRefused:
            # A row of the polyfill grid refuses it on this dialect (REQ-2236), or this dialect is
            # no column of the grid, which refuses a call a row matches by name: no form is written.
            # (a row is named by its call, or by its signature: interval_eq(interval, interval))
            assert name in {f.name.split("(")[0] for f in GRID.features}
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
    assert transpile(sql, dialect, generated=False) == expected


def test_a_form_survives_the_engines_own_rewrites():
    assert "BOOL_AND(a)" in transpile_to_trino("SELECT every(a) AS v FROM t", generated=False)


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
    assert transpile(sql, "trino", generated=False) == (
        f"SELECT {_ARRAYCONTAINS_ON_TRINO.format(a='a', b='b')} AS v FROM t"
    )


def test_an_arrays_containment_reaches_trino_as_trinos_own_sql():
    """REQ-2176: a session hop's label filter (``__labels @> ARRAY[...]``) is PostgreSQL's operator
    in the semantic statement; the statement Trino is sent carries no ``@>``, and Trino's own
    rewrites, which read it back as Trino's SQL, read it."""
    written = transpile_to_trino(
        "SELECT k FROM t AS _hs WHERE _hs.\"__labels\" @> ARRAY['Big']", generated=False
    )
    assert "@>" not in written
    tree = sqlglot.parse_one(written, read="trino")
    assert tree.find(sqlglot.exp.ArrayContainsAll) is None
    assert written == "SELECT k FROM t AS _hs WHERE " + _ARRAYCONTAINS_ON_TRINO.format(
        a='_hs."__labels"', b="ARRAY['Big']"
    )


def test_an_arrays_containment_stays_postgresqls_operator_on_postgresql():
    assert (
        transpile("SELECT a @> b AS v FROM t", "postgres", generated=False)
        == "SELECT a @> b AS v FROM t"
    )


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
        transpile(sql, dialect, generated=False)
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
    written = transpile(sql, "postgres", generated=False)
    assert "CASE" not in written and "CAST" not in written


def test_a_function_of_a_name_sqlglot_writes_stays_sqlglots():
    assert transpile("SELECT regexp_substr(a, b) AS v FROM t", "mysql", generated=False) == (
        "SELECT REGEXP_SUBSTR(a, b) AS v FROM t"
    )


def test_trino_has_no_regexp_substr_of_its_own():
    assert transpile("SELECT regexp_substr(a, b) AS v FROM t", "trino", generated=False) == (
        "SELECT REGEXP_EXTRACT(a, b) AS v FROM t"
    )
    with pytest.raises(FunctionRefused):
        transpile("SELECT regexp_substr(a, b, 2) AS v FROM t", "trino", generated=False)


def test_an_overload_a_dialect_does_not_take_is_refused():
    assert outcome("regexp_substr", 4, "bigquery") == "mapped"
    assert outcome("regexp_substr", 5, "bigquery") == "refused"
    assert outcome("regexp_substr", 6, "mysql") == "refused"
    assert outcome("regexp_substr", 6, "snowflake") == "mapped"


def test_a_function_named_in_pg_catalog_is_that_function():
    assert transpile('SELECT pg_catalog."textcat"(a, b) AS v FROM t', "spark", generated=False) == (
        "SELECT CAST(a || b AS STRING) AS v FROM t"
    )


def test_a_call_in_an_argument_is_written_too():
    written = transpile("SELECT textcat(textcat(a, b), a) AS v FROM t", "spark", generated=False)
    assert "textcat" not in written.lower()


def test_an_expression_argument_keeps_its_place():
    written = transpile("SELECT textcat(x + 1, y) AS v FROM t", "spark", generated=False)
    assert "(x + 1) || y" in written


def test_the_column_of_a_call_is_named_for_the_function(con):
    relation = con.execute(transpile_to_duckdb("SELECT textcat(a, b) FROM t", generated=False))
    assert [d[0] for d in relation.description] == ["textcat"]


def test_a_call_with_no_overload_of_its_argument_count_is_the_engines_to_refuse():
    tree = sqlglot.parse_one("SELECT textcat(a) FROM t", read="postgres")
    assert inline(tree, "mysql", generated=False).sql(dialect="mysql") == "SELECT TEXTCAT(a) FROM t"


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
    assert (
        transpile('SELECT pg_catalog."abs"(NULL::double precision) AS v', dialect, generated=False)
        == want
    )


def test_a_qualified_call_inside_a_qualified_call_is_read_too():
    written = transpile(
        'SELECT pg_catalog."abs"(pg_catalog."sign"(x)) FROM t', "trino", generated=False
    )
    assert "pg_catalog" not in written.lower()


@pytest.mark.parametrize(
    ("dialect", "decimal"), [("trino", "DECIMAL(3, 2)"), ("clickhouse", "DECIMAL(21, 2)")]
)
def test_a_numeric_literal_keeps_its_digits(dialect: str, decimal: str):
    """On Trino a literal's decimal is of its digits alone (pg_literals.TIGHT)."""
    assert decimal in transpile("SELECT (-2.25)::numeric AS v", dialect, generated=False).upper()


def test_a_numeric_of_a_column_stays_as_said():
    assert transpile("SELECT x::numeric AS v FROM t", "trino", generated=False) == (
        "SELECT CAST(x AS DECIMAL) AS v FROM t"
    )


@pytest.mark.parametrize(
    ("dialect", "want"),
    [
        ("trino", "REGEXP_LIKE(a, '(?i)' || b)"),
        # The pattern as RE2 reads it as PostgreSQL does, its run-time checks beside (REQ-2236).
        ("clickhouse", "match(a, concat('(?si)', "),
        ("duckdb", "REGEXP_MATCHES(a, b, 'i')"),
    ],
)
def test_the_case_insensitive_match_is_one_form_on_any_engine(dialect: str, want: str):
    assert want in transpile("SELECT texticregexne(a, b) AS v FROM t", dialect, generated=False)


def test_clickhouses_like_has_no_escape_clause():
    assert "ESCAPE" not in transpile(
        "SELECT textlike(a, b) AS v FROM t", "clickhouse", generated=False
    )


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
    ],
)
def test_what_a_replay_refused_is_refused_by_name(sql: str, dialect: str, function: str):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, dialect, generated=False)
    assert refused.value.params == {"function": function, "dialect": dialect}


def test_a_column_of_no_said_type_is_the_engines_to_read():
    assert (
        transpile("SELECT avg(x) AS v FROM t", "trino", generated=False)
        == "SELECT AVG(x) AS v FROM t"
    )


@pytest.mark.parametrize(
    ("dialect", "want"),
    [
        ("trino", "(?s)^(?<before>.*?)(?:o)(?<after>.*)$"),
        ("clickhouse", "replaceRegexpOne("),
    ],
)
def test_regexp_replace_with_no_flag_is_the_first_match(dialect: str, want: str):
    assert want in transpile(
        "SELECT regexp_replace(a, 'o', 'X') AS v FROM t", dialect, generated=False
    )


def test_regexp_replace_with_the_g_flag_is_every_match():
    written = transpile(
        "SELECT regexp_replace(a, 'o', 'X', 'g') AS v FROM t", "trino", generated=False
    )
    assert written == "SELECT REGEXP_REPLACE(a, 'o', 'X') AS v FROM t"


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("SELECT '\\xdead'::bytea AS v", "FROM_HEX('dead')"),
        ("SELECT 'abc'::character AS v", "CAST('a' AS CHAR)"),
        # of its digits alone on Trino (pg_literals.TIGHT)
        ("SELECT (-2.25)::numeric AS v", "DECIMAL(3, 2)"),
    ],
)
def test_a_literal_keeps_what_its_cast_says(sql: str, want: str):
    assert want in transpile(sql, "trino", generated=False)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT date_part('century'::text, x::timestamp) AS v FROM t",
        # ln of a numeric: its exact digits, which ClickHouse's double does not hold (ln of a
        # double is polyfill.floats' row)
        "SELECT ln(x::numeric) AS v FROM t",
        'SELECT pg_catalog."in_range"(a::time, b::time, c::interval, true, false) AS v FROM t',
    ],
)
def test_clickhouse_refuses_what_it_cannot_evaluate_or_answers_differently(sql: str):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse", generated=False)


def test_a_call_named_for_the_engines_own_spelling_is_written_so():
    assert (
        transpile("SELECT cbrt(x) AS v FROM t", "clickhouse", generated=False)
        == "SELECT cbrt(x) AS v FROM t"
    )


@pytest.mark.parametrize("call", ["substr(a, 1 + 1, 3)", "substr(a, 2)", "left(a, 2)"])
def test_a_safe_position_stays_the_engines_own(call: str):
    assert "GREATEST" not in transpile(f"SELECT {call} AS v FROM t", "trino", generated=False)
    assert "CASE" not in transpile(f"SELECT {call} AS v FROM t", "trino", generated=False)


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
        transpile(f"SELECT {call} AS v", "trino", generated=False), read="trino", write="duckdb"
    )[0]
    assert duckdb.connect().execute(written).fetchone()[0] == want


def _on_chdb(sql: str) -> object:
    import chdb

    ((value,),) = [
        tuple(line.split("\t")) for line in chdb.query(sql, "TSV").bytes().decode().splitlines()
    ]
    return value


def test_a_replacement_of_nothing_is_a_deletion_on_clickhouse():
    """A replacement of nothing deletes each occurrence (polyfill.texts' replace row)."""
    written = transpile(
        "SELECT pg_catalog.replace('a\"b\"', '\"', '') AS v", "clickhouse", generated=False
    )
    assert _on_chdb(written) == "ab"


@pytest.mark.parametrize(("pattern", "want"), [("''", "abc"), ("NULL::text", "\\N")])
def test_a_pattern_of_nothing_is_postgresql_s_answer_on_clickhouse(pattern: str, want: str):
    """An empty pattern replaces nothing and a NULL one answers NULL, as PostgreSQL's replace:
    neither reaches the server's replaceAll, which refuses both."""
    written = transpile(
        f"SELECT pg_catalog.replace('abc', {pattern}, 'x') AS v", "clickhouse", generated=False
    )
    assert _on_chdb(written) == want


def test_a_timestamp_range_is_compared_on_clickhouse():
    """The emulation strips a bound's quotes with replace(…, '"', '')."""
    marked = mark_subtype_operations(
        "SELECT a.v @> b.v AS r FROM s.t AS a CROSS JOIN s.t AS b", {("s", "t"): {"v": "tsrange"}}
    )
    assert "'\"', '')" in transpile(marked, "clickhouse", generated=False)


def _column_named(sql: str, dialect: str) -> str:
    """The name the transpiled statement gives its one result column."""
    written = sqlglot.parse_one(transpile(sql, dialect, generated=False), read=dialect)
    (item,) = written.selects
    assert isinstance(item, sqlglot.exp.Alias), item.sql(dialect=dialect)
    return item.alias


@pytest.mark.parametrize(
    ("sql", "dialect", "name"),
    [
        # REQ-2236: a polyfilled function call is named for its function, as PostgreSQL names it.
        ("SELECT log10(x) FROM t", "duckdb", "log10"),
        ("SELECT regexp_count(a, 'x') FROM t", "clickhouse", "regexp_count"),
        ("SELECT array_cat(a, b) FROM t", "clickhouse", "array_cat"),
        ("SELECT width_bucket(x, 0, 10, 5) FROM t", "trino", "width_bucket"),
        ("SELECT sha256(a) FROM t", "trino", "sha256"),
    ],
)
def test_an_unaliased_polyfilled_function_call_is_named_for_its_function(
    sql: str, dialect: str, name: str
):
    assert _column_named(sql, dialect) == name


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        # REQ-2236: PostgreSQL names an unaliased operator expression ?column?, never for the grid
        # row or the function its form calls.
        ("SELECT j #> '{a}' FROM t", "duckdb"),
        ("SELECT a ~ b FROM t", "clickhouse"),
        ("SELECT a ~* 'x' FROM t", "clickhouse"),
        ("SELECT 'abc' SIMILAR TO 'a%'", "clickhouse"),
        ("SELECT interval '1 day' * 2", "clickhouse"),
        ("SELECT a @> b FROM t", "trino"),
        ("SELECT ARRAY[1, 2] @> ARRAY[2]", "trino"),
        ("SELECT j #> '{a}' FROM t", "trino"),
    ],
)
def test_an_unaliased_polyfilled_operator_is_named_question_column(sql: str, dialect: str):
    assert _column_named(sql, dialect) == "?column?"


@pytest.mark.parametrize(
    ("sql", "dialect", "name"),
    [
        # REQ-2236: an unaliased cast of a constant is named for its type, as PostgreSQL spells it.
        ("SELECT '1'::int4", "clickhouse", "int4"),
        ("SELECT 1.5::int", "clickhouse", "int4"),
        ("SELECT '12'::int2", "clickhouse", "int2"),
        ("SELECT '1 day'::interval", "clickhouse", "interval"),
        ("SELECT '1'::int4", "trino", "int4"),
        ("SELECT '12'::int2", "trino", "int2"),
    ],
)
def test_an_unaliased_polyfilled_cast_is_named_for_its_type(sql: str, dialect: str, name: str):
    assert _column_named(sql, dialect) == name


@pytest.mark.parametrize(
    ("expression", "name"),
    [
        # The rule itself, on every engine (DuckDB writes most of these natively): FigureColname.
        ("a ~ b", "?column?"),
        ("a || b", "?column?"),
        ("ARRAY[1] @> ARRAY[2]", "?column?"),
        ("lower(a) ~ b", "?column?"),
        ("j -> 'k'", "?column?"),
        ("'1'::int4", "int4"),
        ("x::text", "x"),
        ("CAST(a || b AS TEXT)", "text"),
        ("lower(a)::text", "lower"),
        ("substr(a, 2)", "substring"),
        ("log10(x)", "log10"),
        ("div(a, b)", "div"),
        ("pg_catalog.cbrt(x)", "cbrt"),
        ("string_agg(a, ',')", "string_agg"),
        ("regexp_count(a, b)", "regexp_count"),
    ],
)
def test_the_column_name_rule(expression: str, name: str):
    (item,) = sqlglot.parse_one(f"SELECT {expression}", read="postgres").selects
    assert function_macros.column_name(item) == name
