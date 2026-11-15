# Copyright (c) 2026 Kenneth Stott
# Canary: 56351c01-dfd7-476e-b736-3d274eab554e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every rewrite of a client's statement is made on SQLGlot's tree, never on its text (REQ-2167):
what sqlglot's own parser misread is read by its parser (provisa.transpiler.pg_syntax), and each
rewrite that was a regular expression over the statement is a transform of the tree. Each test is
the exact statement before and after, on each dialect it is written for."""

# Requirements: REQ-2167

from __future__ import annotations

import pytest
import sqlglot

import provisa.transpiler  # noqa: F401 -- pg_syntax's reading of PostgreSQL
from provisa.pgwire._pipeline import _resolve_session_settings
from provisa.pgwire.catalog import _unbound
from provisa.pgwire.catalog_rewrite import _literal_held, _oid_then_star
from provisa.pgwire.catalog_route import _read
from provisa.pgwire.ext_surfaces import _ENV_VAR, rewrite_surface_operators
from provisa.pgwire.server import _current_query_said, _substitute_params
from provisa.transpiler.pg_functions import readable
from provisa.transpiler.transpile import transpile

DIALECTS = ("postgres", "duckdb", "trino", "clickhouse")


# --- what sqlglot's parser misread, read by it -------------------------------------------------


@pytest.mark.parametrize(
    ("dialect", "written"),
    [
        ("postgres", "SELECT CAST(x AS varbit(5)) FROM t"),
        ("duckdb", "SELECT CAST(x AS varbit(5)) FROM t"),
        ("trino", "SELECT CAST(x AS varbit(5)) FROM t"),
        ("clickhouse", "SELECT CAST(x AS Nullable(varbit(5))) FROM t"),
    ],
)
def test_bit_varying_is_a_type_not_a_bit_and_an_alias(dialect: str, written: str) -> None:
    assert transpile("SELECT CAST(x AS bit varying(5)) FROM t", dialect) == written


def test_bit_varying_cast_reads_with_and_without_a_length() -> None:
    tree = sqlglot.parse_one("SELECT x::bit varying, y::bit varying(3), z::bit(2)", read="postgres")
    assert tree.sql(dialect="postgres") == (
        "SELECT CAST(x AS varbit), CAST(y AS varbit(3)), CAST(z AS BIT(2))"
    )


@pytest.mark.parametrize(
    ("dialect", "written"),
    [
        ("postgres", "SELECT CAST(NULL AS _cstring) FROM t"),
        ("duckdb", "SELECT CAST(NULL AS TEXT[]) FROM t"),
        ("trino", "SELECT CAST(NULL AS ARRAY(VARCHAR)) FROM t"),
        # ClickHouse holds no NULL of an array (an Array is never Nullable; CAST(NULL AS
        # Array(...)) is its error): a bare NULL, which pgwire describes as the cast's type.
        ("clickhouse", "SELECT NULL FROM t"),
    ],
)
def test_cstring_array_is_its_array_type_not_a_subscript(dialect: str, written: str) -> None:
    assert transpile("SELECT NULL::cstring[] FROM t", dialect) == written


@pytest.mark.parametrize(
    ("sql", "oid"),
    [
        ("SELECT NULL::cstring[] FROM t", 1263),
        ("SELECT NULL::text[] AS v", 1009),
        ("SELECT NULL::integer[] AS v", 1007),
    ],
)
def test_a_null_array_clickhouse_serves_untyped_is_described_as_its_cast(sql: str, oid: int):
    """ClickHouse serves the bare NULL with no type (BLOB): the cast's array type describes it."""
    from provisa.pgwire.server import _describe_casts, _sql_type_to_bvtype, _type_oid

    types = [_sql_type_to_bvtype("BLOB")]
    oids: list[int | None] = [_type_oid("BLOB")]
    _describe_casts(sql, types, oids, [-1], [], ["BLOB"])
    assert oids == [oid]


def test_object_identifier_arrays_are_named_as_postgresql_names_them() -> None:
    tree = sqlglot.parse_one("SELECT x::oid[], y::regtype[][], z::regproc[]", read="postgres")
    assert tree.sql(dialect="postgres") == (
        "SELECT CAST(x AS _oid), CAST(y AS _regtype), CAST(z AS _regproc)"
    )


@pytest.mark.parametrize(
    "spelled",
    [
        "SELECT overlaps(a, b, c, d) FROM t",
        "SELECT pg_catalog.overlaps(a, b, c, d) FROM t",
        'SELECT pg_catalog."overlaps"(a, b, c, d) FROM t',
    ],
)
def test_a_call_of_overlaps_is_postgresql_s_on_postgres(spelled: str) -> None:
    assert transpile(spelled, "postgres") == 'SELECT pg_catalog."overlaps"(a, b, c, d) FROM t'


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "clickhouse"])
def test_a_call_of_overlaps_is_its_form_elsewhere_however_spelled(dialect: str) -> None:
    bare = transpile("SELECT overlaps(a, b, c, d) FROM t", dialect)
    assert bare.startswith("SELECT CASE WHEN (CASE WHEN a IS NULL THEN b WHEN b IS NOT NULL")
    assert bare.endswith(" END AS overlaps_timestamp FROM t")
    assert transpile('SELECT pg_catalog."overlaps"(a, b, c, d) FROM t', dialect) == bare


def test_the_overlaps_operator_is_left_as_the_operator() -> None:
    for dialect in DIALECTS:
        assert transpile("SELECT (a, b) OVERLAPS (c, d) FROM t", dialect) == (
            "SELECT (a, b) OVERLAPS (c, d) FROM t"
        )


@pytest.mark.parametrize(
    ("dialect", "written"),
    [
        ("postgres", "SELECT CONVERT(b, 'UTF8', 'LATIN1') FROM t"),
        ("duckdb", "SELECT CONVERT(b, 'UTF8', 'LATIN1') FROM t"),
    ],
)
def test_convert_is_postgresql_s_call(dialect: str, written: str) -> None:
    assert transpile("SELECT convert(b, 'UTF8', 'LATIN1') FROM t", dialect) == written


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_convert_is_refused_by_name_where_it_has_no_form(dialect: str) -> None:
    from provisa.transpiler.function_macros import FunctionRefused

    with pytest.raises(FunctionRefused, match="convert"):
        transpile("SELECT convert(b, 'UTF8', 'LATIN1') FROM t", dialect)


@pytest.mark.parametrize(
    ("dialect", "written"),
    [
        ("postgres", "SELECT JSON_OBJECT('{a,1}') FROM t"),
        ("duckdb", "SELECT JSON_OBJECT('{a,1}') FROM t"),
        ("trino", "SELECT JSON_OBJECT('{a,1}') FROM t"),
        ("clickhouse", "SELECT json_object('{a,1}') FROM t"),
    ],
)
def test_json_object_of_an_array_is_postgresql_s_call(dialect: str, written: str) -> None:
    assert transpile("SELECT json_object('{a,1}') FROM t", dialect) == written


def test_json_object_of_pairs_is_read_as_sql_s() -> None:
    tree = sqlglot.parse_one("SELECT json_object('a' VALUE 1)", read="postgres")
    assert tree.sql(dialect="postgres") == "SELECT JSON_OBJECT('a': 1)"
    assert tree.sql(dialect="duckdb") == "SELECT JSON_OBJECT('a', 1)"


def test_negative_inner_product_reads_and_writes_back() -> None:
    tree = sqlglot.parse_one("SELECT v <#> w FROM t ORDER BY v <#> '[1,2]'", read="postgres")
    assert tree.sql(dialect="postgres") == "SELECT v <#> w FROM t ORDER BY v <#> '[1,2]'"
    assert sqlglot.parse_one("SELECT a #> '{x}', a < b", read="postgres").sql("postgres") == (
        "SELECT a #> '{x}', a < b"
    )


# --- the rewrites, on the tree -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("dialect", "written"),
    [
        ("postgres", "SELECT REGEXP_REPLACE(s, 'a(b)', '\\1', 'g') FROM t"),
        ("duckdb", "SELECT pg_catalog.regexp_replace(s, 'a(b)', '\\1', 'g') FROM t"),
        ("trino", "SELECT REGEXP_REPLACE(s, 'a(b)', '\\1') AS regexp_replace FROM t"),
        ("clickhouse", "SELECT REGEXP_REPLACE(s, 'a(b)', '\\\\1') AS regexp_replace FROM t"),
    ],
)
def test_pg_catalog_regexp_replace_is_the_bare_call(dialect: str, written: str) -> None:
    assert transpile("SELECT pg_catalog.regexp_replace(s, 'a(b)', '\\1', 'g') FROM t", dialect) == (
        written
    )


def test_duckdb_reads_the_engine_s_form_of_each_syntax_call() -> None:
    assert readable(
        "SELECT x::bit varying(3), to_hex(5), \"to_hex\"(6), json_object('{a,1}'), "
        "json_object(ARRAY['a'], ARRAY['1'])"
    ) == (
        "SELECT CAST(x AS BIT(3)), PG_TO_HEX(5), PG_TO_HEX(6), PG_JSON_OBJECT('{a,1}'), "
        "PG_JSON_OBJECT(ARRAY['a'], ARRAY['1'])"
    )
    assert readable("SELECT convert(b, 'UTF8', 'LATIN1')") == (
        "SELECT PG_CONVERT(b, 'UTF8', 'LATIN1')"
    )


def test_duckdb_leaves_a_statement_with_none_as_written() -> None:
    assert readable("SELECT json_object('a' VALUE 1)") == "SELECT json_object('a' VALUE 1)"
    assert readable("SELECT 'to_hex(1)'") == "SELECT 'to_hex(1)'"


def test_catalog_route_names_arrays_and_syntax_calls_as_postgresql_does() -> None:
    tree = _read(
        'SELECT NULL::oid[], NULL::name[], NULL::"char"[], NULL::pg_catalog.oid[], '
        "convert(a, b, c), pg_catalog.json_object('{a,1}')"
    )
    assert tree.sql(dialect="postgres") == (
        "SELECT CAST(NULL AS _oid), CAST(NULL AS _name), CAST(NULL AS _char), "
        'CAST(NULL AS _oid), pg_catalog."convert"(a, b, c), pg_catalog."json_object"(\'{a,1}\')'
    )


def test_catalog_literals_are_held_as_the_catalog_db_holds_them() -> None:
    tree = sqlglot.parse_one(
        "SELECT '{16395,16396}'::oid[], '{a,\"b c\",NULL}'::text[], '1 2'::oidvector, "
        "'(0,1)'::tid, ARRAY[1, 2]::int[], c.oid, c.* FROM pg_class AS c",
        read="postgres",
    )
    held = tree.transform(lambda node: _literal_held(node, False))
    assert held.sql(dialect="postgres") == (
        "SELECT ARRAY[16395, 16396], CAST(ARRAY['a', 'b c', NULL] AS TEXT[]), "
        "CAST(ARRAY[1, 2] AS oidvector), CAST(0 AS tid), ARRAY[1, 2], c.oid, c.* "
        "FROM pg_class AS c"
    )
    for select in held.find_all(sqlglot.exp.Select):
        _oid_then_star(select.expressions)
    assert held.sql(dialect="duckdb") == (
        "SELECT [16395, 16396], CAST(['a', 'b c', NULL] AS TEXT[]), CAST([1, 2] AS oidvector), "
        "CAST(0 AS tid), [1, 2], c.oid, c.* EXCLUDE (oid) FROM pg_class AS c"
    )


def test_a_tid_literal_a_tid_function_reads_is_kept() -> None:
    tree = sqlglot.parse_one("SELECT '(0,1)'::tid", read="postgres")
    held = tree.transform(lambda node: _literal_held(node, True))
    assert held.sql(dialect="postgres") == "SELECT CAST('(0,1)' AS tid)"


def test_pgvector_negative_inner_product_is_the_engine_s_function(monkeypatch) -> None:
    monkeypatch.setenv(_ENV_VAR, "pgvector")
    assert rewrite_surface_operators(
        "SELECT a <#> b, c <#> '[1,2]'::vector FROM t ORDER BY (a + 1) <#> $1"
    ) == (
        "SELECT ARRAY_NEGATIVE_INNER_PRODUCT(a, b), "
        "ARRAY_NEGATIVE_INNER_PRODUCT(c, CAST('[1,2]' AS VECTOR)) FROM t "
        "ORDER BY ARRAY_NEGATIVE_INNER_PRODUCT((a + 1), $1)"
    )


def test_parameters_are_bound_on_the_tree_not_in_strings() -> None:
    bound = _substitute_params("SELECT $1, '$1', $2 FROM t WHERE a = $10", list(range(1, 11)))
    assert bound == "SELECT 1, '$1', 2 FROM t WHERE a = 10"
    assert _substitute_params("SELECT $1, $2", [None, "it's"]) == "SELECT NULL, 'it''s'"


def test_current_query_is_the_client_s_statement() -> None:
    said = _current_query_said(
        "SELECT current_query(), pg_catalog.current_query(), 'current_query()'", "SELECT 'x'"
    )
    assert said == (
        "SELECT CAST('SELECT ''x''' AS TEXT), CAST('SELECT ''x''' AS TEXT), 'current_query()'"
    )


@pytest.mark.parametrize("dialect", DIALECTS)
def test_session_settings_are_their_literals_on_each_engine(dialect: str) -> None:
    sql = (
        "SELECT * FROM t WHERE r = current_setting('provisa.region') "
        "AND s = current_setting('provisa.gone', true) "
        "AND u = 'current_setting(''provisa.region'')'"
    )
    assert _resolve_session_settings(sql, {"region": "e'u"}, dialect) == (
        "SELECT * FROM t WHERE r = 'e''u' AND s = NULL "
        "AND u = 'current_setting(''provisa.region'')'"
    )


def test_unbound_parameters_are_null_outside_strings() -> None:
    assert _unbound("SELECT $1, '$2' FROM t WHERE a = $2") == (
        "SELECT NULL, '$2' FROM t WHERE a = NULL"
    )


@pytest.mark.parametrize(
    ("call", "written"),
    [
        ('pg_catalog."interval_in"(NULL::cstring, NULL::oid, NULL::integer)', "INTERVAL"),
        ('pg_catalog."jsonb_in"(NULL::cstring)', "JSONB"),
        ('pg_catalog."xid8in"(NULL::cstring)', "xid8"),
    ],
)
def test_a_null_input_is_postgresqls_null_of_its_type_on_postgres(call: str, written: str) -> None:
    """REQ-2167: PostgreSQL holds interval, jsonb and xid8, so a strict input's NULL is of that
    type there, as PostgreSQL describes it; only an engine without the type answers text."""
    assert transpile(f"SELECT {call} AS v", "postgres") == f"SELECT CAST(NULL AS {written}) AS v"


# --- a keyword as a column label after AS ------------------------------------------------------

# Metabase 0.5x's table-privileges statement (bi_tools/metabase.yaml, id 10), as it sends it.
_METABASE_PRIVILEGES = """with table_privileges as (
 select
   NULL as role,
   t.schemaname as schema,
   t.objectname as table,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'update') as update,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'select') as select,
   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'insert') as insert,
   pg_catalog.has_table_privilege(     current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'delete') as delete
 from (
   select schemaname, tablename as objectname from pg_catalog.pg_tables
   union
   select schemaname, viewname as objectname from pg_catalog.pg_views
   union
   select schemaname, matviewname as objectname from pg_catalog.pg_matviews
 ) t
 where t.schemaname !~ '^pg_'
   and t.schemaname <> 'information_schema'
   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
)
select t.*
from table_privileges t"""


def test_metabase_privileges_statement_reads_select_as_a_label() -> None:
    written = sqlglot.parse_one(_METABASE_PRIVILEGES, read="postgres").sql("postgres")
    assert 'AS "select"' in written
    assert " AS select" not in written.lower()
    assert sqlglot.parse_one(written, read="postgres").sql("postgres") == written


@pytest.mark.parametrize("keyword", ["select", "not", "SELECT", "Not"])
def test_any_keyword_after_as_is_a_quoted_label(keyword: str) -> None:
    tree = sqlglot.parse_one(f"SELECT 1 AS {keyword}, 2 AS x", read="postgres")
    assert tree.sql("postgres") == f'SELECT 1 AS "{keyword.lower()}", 2 AS x'


def test_a_label_without_as_is_untouched() -> None:
    assert sqlglot.parse_one("SELECT 1 x", read="postgres").sql("postgres") == "SELECT 1 AS x"
