# Copyright (c) 2026 Kenneth Stott
# Canary: 0f5c73cb-e214-4061-9aca-fde866f4b218
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE'S CATALOG FUNCTIONS (REQ-2167, amended 2026-10-27): a pg_catalog function is answered
with PostgreSQL's result type and semantics through the serving engine's equivalent. Each statement
runs as the conformance probe runs it (scripts/conformance/pgwire_catalog_types.py), through a real
pgwire server whose engine is a real DuckDB runtime; the type is the OID of PostgreSQL 17.11's
pg_proc, the text PostgreSQL's documented answer."""

# Requirements: REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.federation.duckdb_pg_functions import size_pretty
from provisa.pgwire.function_types import call_result_type
from provisa.transpiler.pg_functions import capture_groups
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC = 21, 23, 20, 700, 701, 1700
TEXT, BYTEA, OID, BPCHAR, CHAR = 25, 17, 26, 1042, 18
TIMESTAMP, TIMESTAMPTZ, TEXT_ARRAY, NAME_ARRAY, INT4_ARRAY = 1114, 1184, 1009, 1003, 1007
INET, CIDR, JSONB, VOID, RECORD = 869, 650, 3802, 2278, 2249


def _exec(conn: Any, sql: str) -> Any:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    return res


def _rows(res: Any) -> list[str | None]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


#: statement -> (type OID, the rows' text).
ANSWERS: dict[str, tuple[int, list[str | None]]] = {
    # DuckDB's integer widths: PostgreSQL's are int4.
    "SELECT length('abc') AS v": (INT4, ["3"]),
    'SELECT pg_catalog."length"(NULL::text) AS v': (INT4, [None]),
    "SELECT octet_length('abc') AS v": (INT4, ["3"]),
    "SELECT strpos('abc', 'c') AS v": (INT4, ["3"]),
    "SELECT gcd(12, 18) AS v": (INT4, ["6"]),
    "SELECT lcm(4, 6) AS v": (INT4, ["12"]),
    "SELECT generate_series(1, 2) AS v": (INT4, ["1", "2"]),
    "SELECT family('1.2.3.4'::inet) AS v": (INT4, ["4"]),
    "SELECT regexp_count('abab', 'a') AS v": (INT4, ["2"]),
    # A numeric argument answers numeric: DuckDB's NULL constant folds to an integer, its
    # functions of a numeric to a double.
    'SELECT pg_catalog."abs"(NULL::numeric) AS v': (NUMERIC, [None]),
    "SELECT abs(-1.5) AS v": (NUMERIC, ["1.5"]),
    "SELECT round(1.26, 1) AS v": (NUMERIC, ["1.3"]),
    "SELECT sqrt(4::numeric) AS v": (NUMERIC, ["2.0"]),
    "SELECT sign(-2.5) AS v": (NUMERIC, ["-1"]),
    "SELECT sign(NULL::float8) AS v": (FLOAT8, [None]),
    "SELECT avg(2::integer) AS v": (NUMERIC, ["2.0"]),
    "SELECT sum(2::bigint) AS v": (NUMERIC, ["2"]),
    "SELECT sum(1.5::real) AS v": (FLOAT4, ["1.5"]),
    "SELECT stddev(NULL::integer) AS v": (NUMERIC, [None]),
    # min and max of a type the engine holds in another.
    "SELECT max(5::oid) AS v": (OID, ["5"]),
    "SELECT min('a'::character) AS v": (BPCHAR, ["a"]),
    # Types of their own.
    "SELECT current_schemas(false) AS v": (NAME_ARRAY, ["{public}"]),
    'SELECT pg_catalog."char"(65) AS v': (CHAR, ["A"]),
    "SELECT network('10.1.2.3/8'::inet) AS v": (CIDR, ["10.0.0.0/8"]),
    "SELECT jsonb_build_object() AS v": (JSONB, ["{}"]),
    # The digests, as bytes.
    "SELECT sha256('\\x01'::bytea) AS v": (
        BYTEA,
        ["\\x4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a"],
    ),
    "SELECT sha224('\\x'::bytea) AS v": (
        BYTEA,
        ["\\xd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"],
    ),
    'SELECT pg_catalog."sha512"(NULL::bytea) AS v': (BYTEA, [None]),
    # A bytea trimmed by bytes.
    "SELECT btrim('\\x000100'::bytea, '\\x00'::bytea) AS v": (BYTEA, ["\\x01"]),
    "SELECT ltrim('\\x000100'::bytea, '\\x00'::bytea) AS v": (BYTEA, ["\\x0100"]),
    "SELECT rtrim('\\x000100'::bytea, '\\x00'::bytea) AS v": (BYTEA, ["\\x0001"]),
    # Set-returning: a row per match; none for no match.
    "SELECT regexp_matches('abc', '(b)(c)') AS v": (TEXT_ARRAY, ["{b,c}"]),
    "SELECT regexp_matches('abcbd', 'b(.)', 'g') AS v": (TEXT_ARRAY, ["{c}", "{d}"]),
    "SELECT regexp_matches('abc', 'x') AS v": (TEXT_ARRAY, []),
    "SELECT regexp_matches(NULL::text, NULL::text) AS v": (TEXT_ARRAY, []),
    # A time zone's conversions, the session's: UTC here.
    "SELECT timezone('2024-01-02 03:04:05+00'::timestamptz) AS v": (
        TIMESTAMP,
        ["2024-01-02 03:04:05"],
    ),
    "SELECT timezone('2024-01-02 03:04:05'::timestamp) AS v": (
        TIMESTAMPTZ,
        ["2024-01-02 03:04:05+00"],
    ),
    "SELECT to_timestamp('2024-01-02', 'YYYY-MM-DD') AS v": (
        TIMESTAMPTZ,
        ["2024-01-02 00:00:00+00"],
    ),
    "SELECT random(NULL::integer, NULL::integer) AS v": (INT4, [None]),
    "SELECT random(5, 5) AS v": (INT4, ["5"]),
    "SELECT random(5::bigint, 5::bigint) AS v": (INT8, ["5"]),
    "SELECT random(1.50, 1.50) AS v": (NUMERIC, ["1.50"]),
    "SELECT setseed(0.5) AS v": (VOID, [""]),
    "SELECT setseed(NULL::float8) AS v": (VOID, [None]),
    # Called by clients: pgAdmin's table properties, DBeaver's binary literals, pgjdbc's escapes,
    # psql's \l+ and \sv.
    "SELECT substring('fillfactor=70' FROM 'fillfactor=([0-9]*)') AS v": (TEXT, ["70"]),
    "SELECT substring('abc' FROM 'b.') AS v": (TEXT, ["bc"]),
    "SELECT substring('abc' FROM 'x') AS v": (TEXT, [None]),
    "SELECT substring('abc' FROM 2) AS v": (TEXT, ["bc"]),
    "SELECT decode('41', 'hex') AS v": (BYTEA, ["\\x41"]),
    "SELECT decode('QQ==', 'base64') AS v": (BYTEA, ["\\x41"]),
    "SELECT encode('\\x41'::bytea, 'hex') AS v": (TEXT, ["41"]),
    "SELECT encode('\\x41'::bytea, 'base64') AS v": (TEXT, ["QQ=="]),
    "SELECT encode('\\x41005c80'::bytea, 'escape') AS v": (TEXT, ["A\\000\\\\\\200"]),
    "SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4) AS v": (TEXT, ["Thomas"]),
    "SELECT overlay('abc' PLACING 'Z' FROM 2) AS v": (TEXT, ["aZc"]),
    'SELECT pg_catalog."extract"(NULL::text, NULL::date) AS v': (NUMERIC, [None]),
    "SELECT to_char(1, NULL::text) AS v": (TEXT, [None]),
    "SELECT pg_size_pretty(1024::numeric) AS v": (TEXT, ["1024 bytes"]),
    "SELECT pg_size_pretty(20971520::numeric) AS v": (TEXT, ["20 MB"]),
    'SELECT pg_catalog."bpchar"(NULL::"char") AS v': (BPCHAR, [None]),
    "SELECT has_type_privilege(23, 'USAGE') AS v": (16, ["t"]),
    "SELECT pg_blocking_pids(1) AS v": (INT4_ARRAY, ["{}"]),
    "SELECT pg_current_logfile() AS v": (TEXT, [None]),
    "SELECT pg_current_logfile('csvlog') AS v": (TEXT, [None]),
    "SELECT pg_stat_reset_single_table_counters(1) AS v": (VOID, [""]),
    "SELECT pg_get_viewdef(1, true) AS v": (TEXT, [None]),
    "SELECT oidvectortypes(NULL::oidvector) AS v": (TEXT, [None]),
    'SELECT pg_catalog."pg_get_replica_identity_index"(NULL::regclass) AS v': (2205, [None]),
    'SELECT pg_catalog."pg_get_statisticsobjdef_expressions"(1) AS v': (TEXT_ARRAY, [None]),
}


@pytest.mark.parametrize("sql", list(ANSWERS))
def test_a_function_is_answered_as_postgresql_answers_it(conn: Any, sql: str):
    res = _exec(conn, sql)
    assert (res.ftype(0), _rows(res)) == ANSWERS[sql]


def test_random_stays_between_its_bounds(conn: Any):
    for _ in range(50):
        res = _exec(conn, "SELECT random(1, 3) AS v")
        assert res.ftype(0) == INT4
        assert _rows(res)[0] in ("1", "2", "3")
    res = _exec(conn, "SELECT random(1.25, 1.75) AS v")
    assert res.ftype(0) == NUMERIC
    assert 1.25 <= float(str(_rows(res)[0])) <= 1.75


def test_inverted_random_bounds_are_refused_as_postgresql_refuses_them(conn: Any):
    res = conn.conn.pgconn.exec_params(b"SELECT random(3, 1) AS v", [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert b"lower bound must be less than or equal to upper bound" in res.error_message


def test_a_connection_is_told_its_own_ends(conn: Any):
    addr = _exec(conn, "SELECT inet_client_addr() AS v")
    assert (addr.ftype(0), _rows(addr)) == (INET, ["127.0.0.1"])
    server = _exec(conn, "SELECT inet_server_addr() AS v")
    assert (server.ftype(0), _rows(server)) == (INET, ["127.0.0.1"])
    port = _exec(conn, "SELECT inet_client_port() AS v")
    assert port.ftype(0) == INT4 and int(str(_rows(port)[0])) > 0


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_get_keywords() AS v",
        "SELECT ts_token_type('default') AS v",
        "SELECT pg_show_all_settings() AS v",
    ],
)
def test_a_set_returning_catalog_function_answers_records_in_a_select_list(conn: Any, sql: str):
    res = _exec(conn, sql)
    assert res.ftype(0) == RECORD and res.ntuples > 0


def test_the_catalog_lists_no_extension_as_records_of_none(conn: Any):
    # pgAdmin's extension list: Provisa's catalog holds none.
    res = _exec(conn, "SELECT pg_available_extensions() AS v")
    assert (res.ftype(0), res.ntuples) == (RECORD, 0)


def test_a_record_is_sent_in_postgresqls_text_form(conn: Any):
    res = _exec(conn, "SELECT ts_token_type('default') AS v")
    assert _rows(res)[0] == '(1,asciiword,"Word, all ASCII")'


def test_the_keywords_are_postgresqls(conn: Any):
    # pgjdbc's getSQLKeywords.
    res = _exec(
        conn,
        "SELECT word FROM pg_catalog.pg_get_keywords() WHERE catcode = 'R' AND word = 'select'",
    )
    assert _rows(res) == ["select"]


def test_the_default_parser_has_postgresqls_token_types(conn: Any):
    # psql \dFp+ and pgAdmin's text search configurations.
    res = _exec(conn, "SELECT alias FROM pg_catalog.ts_token_type(3722::oid) WHERE tokid = 1")
    assert _rows(res) == ["asciiword"]
    res = _exec(conn, "SELECT count(*) FROM pg_catalog.ts_token_type('default')")
    assert _rows(res) == ["23"]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT substring('a' FROM 'b' || 'c') AS v",
        "SELECT regexp_matches('a', 'b' || 'c') AS v",
        "SELECT encode('\\x41'::bytea, 'x' || 'y') AS v",
    ],
)
def test_a_pattern_or_format_only_known_at_run_time_is_refused_by_name(conn: Any, sql: str):
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert b"not a literal is not supported" in res.error_message


@pytest.mark.parametrize(
    ("size", "pretty"),
    [
        (0, "0 bytes"),
        (10239, "10239 bytes"),
        (10240, "10 kB"),
        (1048576, "1024 kB"),
        (-20480, "-20 kB"),
    ],
)
def test_a_size_is_written_as_dbsize_writes_it(size: int, pretty: str):
    assert size_pretty(size) == pretty


@pytest.mark.parametrize(
    ("pattern", "groups"),
    [("a", 0), ("(a)(b)", 2), ("(?:a)", 0), (r"\(a", 0), ("[(]a", 0), ("[]()]", 0)],
)
def test_capture_groups_are_counted(pattern: str, groups: int):
    assert capture_groups(pattern) == groups


def test_a_call_of_a_column_has_no_overload_to_read():
    import sqlglot

    tree = sqlglot.parse_one("SELECT avg(x), count(*) FROM t", read="postgres")
    assert isinstance(tree, sqlglot.exp.Select)
    avg, count = tree.selects
    assert call_result_type(avg) is None  # numeric or double precision, by x's type
    assert call_result_type(count) == "bigint"
