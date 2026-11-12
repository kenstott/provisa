# Copyright (c) 2026 Kenneth Stott
# Canary: a5c2c89a-8cc0-4e7a-8d67-2435cc0960fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions conformance run's wrong answers and engine errors on DuckDB (run 42935119044),
each sent as the probe sends it through a real pgwire server on the DuckDB engine: answered as
PostgreSQL 17.11 answers the same statement, or, where DuckDB cannot give that answer, refused by
name (REQ-2167)."""

# Requirements: REQ-2167

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from scripts.conformance.pgwire_function_probe import refused_by_name
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

INT4, NUMERIC, BOOL, BIT, TIMESTAMPTZ, DATE, TIMETZ = 23, 1700, 16, 1560, 1184, 1082, 1266
INT4RANGE, INT8RANGE, NUMRANGE = 3904, 3926, 3906

#: (statement, PostgreSQL 17's result type, its rows' text)
ANSWERS: list[tuple[str, int, list[Any]]] = [
    # A NULL range's parts are NULLs of the function's own result type, not untyped (text).
    *(
        (f'SELECT pg_catalog."{name}"(NULL::int4range) AS v', BOOL, [None])
        for name in ("isempty", "lower_inc", "lower_inf", "upper_inc", "upper_inf")
    ),
    ('SELECT pg_catalog."upper_inc"(NULL::int4multirange) AS v', BOOL, [None]),
    ('SELECT pg_catalog."lower"(NULL::int4range) AS v', INT4, [None]),
    ('SELECT pg_catalog."upper"(NULL::int4range) AS v', INT4, [None]),
    ('SELECT pg_catalog."lower"(NULL::int4multirange) AS v', INT4, [None]),
    ('SELECT pg_catalog."upper"(NULL::int4multirange) AS v', INT4, [None]),
    ('SELECT pg_catalog."range_adjacent"(NULL::int4range, NULL::int4range) AS v', BOOL, [None]),
    ('SELECT pg_catalog."range_cmp"(NULL::int4range, NULL::int4range) AS v', INT4, [None]),
    # A bound written as a cast negative number is that number.
    ('SELECT pg_catalog."int4range"((-7)::integer, (-7)::integer) AS v', INT4RANGE, ["empty"]),
    ('SELECT pg_catalog."int8range"((-7)::bigint, (-7)::bigint) AS v', INT8RANGE, ["empty"]),
    ('SELECT pg_catalog."int4range"((-7)::integer, 3::integer) AS v', INT4RANGE, ["[-7,3)"]),
    (
        'SELECT pg_catalog."numrange"((-7.125)::numeric, (-7.125)::numeric) AS v',
        NUMRANGE,
        ["empty"],
    ),
    # ceil and its kin of a numeric literal named by pg_catalog: a DECIMAL, not its digits' text.
    ('SELECT pg_catalog."ceil"(2.5::numeric) AS v', NUMERIC, ["3"]),
    ('SELECT pg_catalog."floor"(2.5::numeric) AS v', NUMERIC, ["2"]),
    ('SELECT pg_catalog."round"(2.5::numeric) AS v', NUMERIC, ["3"]),
    ('SELECT pg_catalog."trunc"(2.5::numeric) AS v', NUMERIC, ["2"]),
    # bit(integer, typmod): a typmod of 0 or below is one bit (bitfromint4).
    ('SELECT pg_catalog."bit"((-7)::integer, (-7)::integer) AS v', BIT, ["1"]),
    ('SELECT pg_catalog."bit"((-7)::bigint, (-7)::integer) AS v', BIT, ["1"]),
    ('SELECT pg_catalog."bit"(5::integer, 4::integer) AS v', BIT, ["0101"]),
    # bit(bit, length, explicit): an explicit cast pads with zeros on the right, or truncates.
    ("SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, true::boolean) AS v", BIT, ["100"]),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 2::integer, true::boolean) AS v", BIT, ["10"]),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 0::integer, false::boolean) AS v", BIT, ["101"]),
    # numeric(numeric, typmod): a typmod below the header leaves the value as it is.
    ('SELECT pg_catalog."numeric"(2.5::numeric, 3::integer) AS v', NUMERIC, ["2.5"]),
    # in_range of a time reads its interval offset's time alone: '-1 year 2 days' is none.
    (
        "SELECT pg_catalog.\"in_range\"('23:59:59.5'::time without time zone, '23:59:59.5'::time "
        "without time zone, '-1 year 2 days'::interval, true::boolean, true::boolean) AS v",
        BOOL,
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"in_range\"('23:59:59.5-05'::time with time zone, '23:59:59.5-05'::time "
        "with time zone, '-1 year 2 days'::interval, true::boolean, true::boolean) AS v",
        BOOL,
        ["t"],
    ),
    (
        "SELECT pg_catalog.\"in_range\"('10:00'::time, '11:00'::time, '1 day 00:30'::interval, "
        "true, true) AS v",
        BOOL,
        ["t"],
    ),
    # A dimension the array does not have: NULL, and no subscripts. An empty array has none.
    ("SELECT pg_catalog.\"array_length\"('{1,2,3}'::integer[], 3::integer) AS v", INT4, [None]),
    ("SELECT pg_catalog.\"array_length\"('{1,2,3}'::integer[], 1::integer) AS v", INT4, ["3"]),
    ("SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 2) AS v", INT4, ["3"]),
    ("SELECT array_length('{}'::integer[], 1) AS v", INT4, [None]),
    ("SELECT pg_catalog.\"generate_subscripts\"('{1,2,3}'::integer[], 3::integer) AS v", INT4, []),
    ("SELECT generate_subscripts(ARRAY[[1,2,3],[4,5,6]], 2) AS v", INT4, ["1", "2", "3"]),
    # string_agg of a delimiter of each row: the row's delimiter before its value, the first's
    # dropped, a NULL value's row skipped, a NULL delimiter none; text ordered as en_US orders it.
    (
        'SELECT pg_catalog."string_agg"(c1, c2 ORDER BY c1) AS v FROM (VALUES '
        "('\\xdeadbeef'::bytea, '\\xdeadbeef'::bytea), ('abc'::bytea, 'abc'::bytea), "
        "('\\x00ff'::bytea, '\\x00ff'::bytea)) AS t(c1, c2)",
        17,
        ["\\x00ff616263616263deadbeefdeadbeef"],
    ),
    (
        "SELECT pg_catalog.\"string_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('abc'::text, "
        "'-'::text), ('Hello'::text, NULL::text), ('12'::text, '+'::text), (NULL::text, '*'::text)) "
        "AS t(c1, c2)",
        25,
        ["12-abcHello"],
    ),
    # substring(string SIMILAR pattern ESCAPE escape) of a pattern that does not match: NULL.
    ("SELECT pg_catalog.\"substring\"('abc'::text, 1::text, 2::text) AS v", 25, [None]),
    (
        "SELECT pg_catalog.\"substring\"('abcd'::text, 'a#\"bc#\"d'::text, '#'::text) AS v",
        25,
        ["bc"],
    ),
    ("SELECT pg_catalog.\"substring\"('101'::bit(3), 2::integer) AS v", BIT, ["01"]),
    # sum of an interval column a VALUES list types: field by field.
    (
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('1 day 02:03:04'::interval), "
        "('3 mons'::interval), ('-1 year 2 days'::interval)) AS t(c1)",
        1186,
        ["-9 mons +3 days 02:03:04"],
    ),
    # position of a bytea in a bytea: from 1, 0 where it is not found.
    (
        "SELECT pg_catalog.\"position\"('\\xdeadbeef'::bytea, '\\xdeadbeef'::bytea) AS v",
        INT4,
        ["1"],
    ),
    ("SELECT pg_catalog.\"position\"('\\xdeadbeef'::bytea, '\\xbeef'::bytea) AS v", INT4, ["3"]),
    ("SELECT pg_catalog.\"position\"('\\xdeadbeef'::bytea, '\\x00'::bytea) AS v", INT4, ["0"]),
    # "char" of an integer: its byte, the high bit's as an octal escape, 0's as nothing.
    ('SELECT pg_catalog."char"((-7)::integer) AS v', 18, ["\\371"]),
    ('SELECT pg_catalog."char"(65::integer) AS v', 18, ["A"]),
    ('SELECT pg_catalog."char"(0::integer) AS v', 18, [""]),
    # An encoding's name as PostgreSQL reads it: 'abc' is WIN1258's.
    ("SELECT pg_catalog.\"length\"('\\xdeadbeef'::bytea, 'abc'::name) AS v", INT4, ["4"]),
    ("SELECT pg_catalog.\"length\"('abc'::bytea, 'SQL-ASCII'::name) AS v", INT4, ["3"]),
    # uuid_extract_timestamp: version 1 and 7 (RFC 9562's examples), NULL for any other.
    (
        "SELECT pg_catalog.\"uuid_extract_timestamp\"('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid) "
        "AS v",
        TIMESTAMPTZ,
        [None],
    ),
    (
        "SELECT pg_catalog.\"uuid_extract_timestamp\"('c232ab00-9414-11ec-b3c8-9f6bdeced846'::uuid) "
        "AS v",
        TIMESTAMPTZ,
        ["2022-02-22 19:22:22+00"],
    ),
    (
        "SELECT pg_catalog.\"uuid_extract_timestamp\"('019535d9-3df7-79fb-b466-fa907fa17f9e'::uuid) "
        "AS v",
        TIMESTAMPTZ,
        ["2025-02-24 02:46:24.503+00"],
    ),
    # broadcast of an IPv4 address of the high bit (DuckDB holds it as a negative integer).
    ("SELECT pg_catalog.\"broadcast\"('192.168.0.1/24'::inet) AS v", 869, ["192.168.0.255/24"]),
    (
        "SELECT pg_catalog.\"broadcast\"('2001:db8::1/64'::inet) AS v",
        869,
        ["2001:db8::ffff:ffff:ffff:ffff/64"],
    ),
    # A template the statement casts to text is the template.
    (
        "SELECT pg_catalog.\"to_date\"('2024-06-15'::text, 'YYYY-MM-DD'::text) AS v",
        1082,
        ["2024-06-15"],
    ),
    (
        "SELECT pg_catalog.\"to_timestamp\"('2024-06-15 12:30:45'::text, "
        "'YYYY-MM-DD HH24:MI:SS'::text) AS v",
        TIMESTAMPTZ,
        ["2024-06-15 12:30:45+00"],
    ),
    # An array literal of two dimensions is a list of lists: xpath's namespace mapping.
    (
        "SELECT pg_catalog.\"xpath\"('/b/@x'::text, '<b x=\"y\"/>'::xml, "
        "'{{x,http://example.com}}'::text[]) AS v",
        143,
        ["{y}"],
    ),
    (
        "SELECT pg_catalog.\"xpath_exists\"('/b/@x'::text, '<b x=\"y\"/>'::xml, "
        "'{{x,http://example.com}}'::text[]) AS v",
        BOOL,
        ["t"],
    ),
    ("SELECT '{{1,2},{3,4}}'::int[] AS v", 1007, ["{{1,2},{3,4}}"]),
    # range_agg and range_intersect_agg of a column a VALUES list types, their ORDER BY no matter.
    *(
        (
            f'SELECT pg_catalog."{name}"(c1 ORDER BY c1) AS v FROM (VALUES {rows}) AS t(c1)',
            oid,
            [answer],
        )
        for name, rows, oid, answer in (
            (
                "range_agg",
                "('{[1,5),[7,9)}'::int4multirange), ('{}'::int4multirange), "
                "('{(,3]}'::int4multirange)",
                4451,
                "{(,5),[7,9)}",
            ),
            (
                "range_agg",
                "('[1,5)'::int4range), ('empty'::int4range), ('(,3]'::int4range)",
                4451,
                "{(,5)}",
            ),
            (
                "range_intersect_agg",
                "('{[1,5),[7,9)}'::int4multirange), ('{}'::int4multirange), "
                "('{(,3]}'::int4multirange)",
                4451,
                "{}",
            ),
            (
                "range_intersect_agg",
                "('[1,5)'::int4range), ('empty'::int4range), ('(,3]'::int4range)",
                INT4RANGE,
                "empty",
            ),
            (
                "range_intersect_agg",
                "('[1,5)'::int4range), ('[2,8)'::int4range)",
                INT4RANGE,
                "[2,5)",
            ),
        )
    ),
    # A zone's text PostgreSQL reads as POSIX: '12' is twelve hours WEST of UTC.
    (
        "SELECT pg_catalog.\"date_add\"('2000-02-29 00:00:00-05'::timestamp with time zone, "
        "'-1 year 2 days'::interval, '12'::text) AS v",
        TIMESTAMPTZ,
        ["1999-03-03 05:00:00+00"],
    ),
    (
        "SELECT pg_catalog.\"date_subtract\"('2000-02-29 00:00:00-05'::timestamp with time zone, "
        "'-1 year 2 days'::interval, '12'::text) AS v",
        TIMESTAMPTZ,
        ["2001-02-27 05:00:00+00"],
    ),
    (
        "SELECT pg_catalog.\"date_trunc\"('year'::text, '2000-02-29 00:00:00-05'::timestamp with "
        "time zone, '12'::text) AS v",
        TIMESTAMPTZ,
        ["2000-01-01 12:00:00+00"],
    ),
    (
        'SELECT pg_catalog."make_timestamptz"(2000::integer, 2::integer, 29::integer, '
        "23::integer, 59::integer, 59.5::double precision, '+05:30'::text) AS v",
        TIMESTAMPTZ,
        ["2000-02-29 18:29:59.5+00"],
    ),
    # A numeric zone, as make_timestamptz reads one: ISO 8601's sign, east of UTC.
    (
        'SELECT pg_catalog."make_timestamptz"(2000::integer, 2::integer, 29::integer, '
        "12::integer, 0::integer, 0::double precision, '-3'::text) AS v",
        TIMESTAMPTZ,
        ["2000-02-29 15:00:00+00"],
    ),
    # timezone() of a POSIX zone: west of UTC.
    (
        "SELECT pg_catalog.\"timezone\"('+05:30'::text, '2000-02-29 12:00:00+00'::timestamp with "
        "time zone) AS v",
        1114,
        ["2000-02-29 06:30:00"],
    ),
    # Run 43237880934's engine errors.
    # timezone() of an interval: the interval itself, never read as a zone's text.
    (
        "SELECT pg_catalog.\"timezone\"('-05:30'::interval, '00:00:00+00'::time with time zone) "
        "AS v",
        TIMETZ,
        ["18:30:00-05:30"],
    ),
    # to_date and to_timestamp of a month's abbreviation (Mon): PostgreSQL's template, read whole.
    (
        "SELECT pg_catalog.\"to_date\"('15 Jun 2024'::text, 'DD Mon YYYY'::text) AS v",
        DATE,
        ["2024-06-15"],
    ),
    (
        "SELECT pg_catalog.\"to_timestamp\"('15 Jun 2024'::text, 'DD Mon YYYY'::text) AS v",
        TIMESTAMPTZ,
        ["2024-06-15 00:00:00+00"],
    ),
    ("SELECT to_date('15 JUNE 2024', 'DD Month YYYY') AS v", DATE, ["2024-06-15"]),
    (
        "SELECT to_timestamp('Sat 15 jun 2024 10 pm', 'Dy DD mon YYYY HH am') AS v",
        TIMESTAMPTZ,
        ["2024-06-15 22:00:00+00"],
    ),
]


def _answer(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


@pytest.mark.parametrize(("sql", "oid", "rows"), ANSWERS, ids=[a[0] for a in ANSWERS])
def test_duckdb_answers_as_postgresql_17(conn: Any, sql: str, oid: int, rows: list[Any]):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    got = [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]
    assert (res.ftype(0), got) == (oid, rows)


#: (statement, PostgreSQL's SQLSTATE, its message): PostgreSQL's own error, said as it says it.
ERRORS: list[tuple[str, str, str]] = [
    (
        "SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, false::boolean) AS v",
        "22026",
        "bit string length 1 does not match type bit(3)",
    ),
    ('SELECT pg_catalog."char"(300::integer) AS v', "22003", '"char" out of range'),
    # record_in of the anonymous record type: PostgreSQL's own refusal.
    (
        "SELECT pg_catalog.\"record_in\"('(1,x)'::cstring, 2249::oid, (-1)::integer) AS v",
        "0A000",
        "input of anonymous composite types is not implemented",
    ),
    # Its time negative, whatever its days.
    (
        "SELECT pg_catalog.\"in_range\"('10:00'::time, '11:00'::time, '1 day -00:30'::interval, "
        "true, true) AS v",
        "22013",
        "invalid preceding or following size in window function",
    ),
]


@pytest.mark.parametrize(("sql", "sqlstate", "message"), ERRORS, ids=[e[0] for e in ERRORS])
def test_duckdb_errs_as_postgresql_17(conn: Any, sql: str, sqlstate: str, message: str):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    said = (
        res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
        res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
    )
    assert said == (sqlstate, message)


#: Statements whose PostgreSQL answer DuckDB cannot give: each refused naming its function.
REFUSED: list[tuple[str, str]] = [
    # A scale DuckDB's DECIMAL (38 digits at most) has no room for.
    ('SELECT pg_catalog."round"(1000::numeric, 42::integer) AS v', "round"),
    ('SELECT pg_catalog."trunc"(1000::numeric, 42::integer) AS v', "trunc"),
    # A step of months, days and time of different signs, which DuckDB's range takes none of.
    *(
        (
            f"SELECT pg_catalog.\"generate_series\"('2000-02-29 00:00:00'::{kind}, "
            f"'2000-02-29 00:00:00'::{kind}, '-1 year 2 days'::interval{zone}) AS v",
            "generate_series",
        )
        for kind, zone in (
            ("timestamp without time zone", ""),
            ("timestamp with time zone", ""),
            ("timestamp with time zone", ", '12'::text"),
        )
    ),
    # A text min or max of a value en_US orders by rules the engine does not have (a space).
    *(
        (
            f"SELECT pg_catalog.\"{name}\"(c1) AS v FROM (VALUES ('abc'::text), "
            "('Hello World'::text), ('12'::text)) AS t(c1)",
            name,
        )
        for name in ("min", "max")
    ),
    (
        "SELECT pg_catalog.\"string_agg\"(c1, c2 ORDER BY c1) AS v FROM (VALUES ('abc'::text, "
        "'abc'::text), ('Hello World'::text, 'Hello World'::text), ('12'::text, '12'::text)) "
        "AS t(c1, c2)",
        "string_agg",
    ),
    # record_in of a named composite type, whose columns the engine does not hold.
    (
        "SELECT pg_catalog.\"record_in\"('(1,x,10,)'::cstring, "
        "'pg_catalog.pg_namespace'::regtype::oid, (-1)::integer) AS v",
        "record_in",
    ),
    # A bit string of no bits, which DuckDB's BIT cannot hold.
    ("SELECT pg_catalog.\"substring\"('0'::bit, 2::integer) AS v", "substring"),
    ("SELECT pg_catalog.\"substring\"('0'::bit, 2::integer, 3::integer) AS v", "substring"),
    # The send of an object identifier type the engine has no catalog of names to look up.
    ("SELECT pg_catalog.\"regprocsend\"('now'::regproc) AS v", "regprocsend"),
    ("SELECT pg_catalog.\"regproceduresend\"('now()'::regprocedure) AS v", "regproceduresend"),
    ("SELECT pg_catalog.\"regopersend\"('||/'::regoper) AS v", "regopersend"),
    (
        "SELECT pg_catalog.\"regoperatorsend\"('+(integer,integer)'::regoperator) AS v",
        "regoperatorsend",
    ),
    ("SELECT pg_catalog.\"regconfigsend\"('english'::regconfig) AS v", "regconfigsend"),
    ("SELECT pg_catalog.\"regdictionarysend\"('simple'::regdictionary) AS v", "regdictionarysend"),
    ('SELECT pg_catalog."regcollationsend"(\'"C"\'::regcollation) AS v', "regcollationsend"),
]


@pytest.mark.parametrize(("sql", "function"), REFUSED, ids=[r[0] for r in REFUSED])
def test_duckdb_refuses_by_name(conn: Any, sql: str, function: str):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    error = (res.error_message or b"").decode()
    assert refused_by_name(error), error
    assert error.split(":", 1)[1].strip().startswith(function), error


@pytest.fixture
def conn_without_crypto(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> Any:
    """A connection to an engine whose install has not staged the crypto extension, as CI's."""
    import provisa.federation.duckdb_extensions as extensions

    monkeypatch.setattr(extensions, "load_staged", lambda con, name: False)
    return request.getfixturevalue("conn")


@pytest.mark.parametrize("bits", ["224", "384", "512"])
def test_a_digest_without_the_crypto_extension_is_refused_by_name(
    conn_without_crypto: Any, bits: str
):
    res = _answer(
        conn_without_crypto, f"SELECT pg_catalog.\"sha{bits}\"('\\xdeadbeef'::bytea) AS v"
    )
    assert res.status == pq.ExecStatus.FATAL_ERROR
    error = (res.error_message or b"").decode()
    assert refused_by_name(error), error
    assert error.split(":", 1)[1].strip().startswith(f"sha{bits}()"), error


def test_an_oid_spelled_in_digits_is_sent_as_its_four_bytes(conn: Any):
    res = _answer(conn, "SELECT pg_catalog.\"regprocsend\"('1299'::regproc) AS v")
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert (res.ftype(0), res.get_value(0, 0)) == (17, b"\\x00000513")


#: overlaps of an interval period, as a statement written again through sqlglot names it: its
#: routine's name, overlaps_timestamp (run 42935119044: the engine route of every overlaps call).
OVERLAPS_WRITTEN_AGAIN = [
    (
        "SELECT OVERLAPS_TIMESTAMP(CAST(NULL AS TIMESTAMP), CAST(NULL AS INTERVAL), "
        "CAST(NULL AS TIMESTAMP), CAST(NULL AS INTERVAL)) AS v",
        None,
    ),
    (
        "SELECT OVERLAPS_TIMESTAMP(CAST('2024-06-15 12:30:45' AS TIMESTAMP), "
        "CAST('1 day 02:03:04' AS INTERVAL), CAST('2024-06-16 12:30:45' AS TIMESTAMP), "
        "CAST('2024-06-15 12:30:45' AS TIMESTAMP)) AS v",
        True,
    ),
    (
        "SELECT OVERLAPS_TIMESTAMP(CAST('12:00' AS TIME), CAST('1 hour' AS INTERVAL), "
        "CAST('13:30' AS TIME), CAST('1 hour' AS INTERVAL)) AS v",
        False,
    ),
]


@pytest.mark.parametrize(("sql", "answer"), OVERLAPS_WRITTEN_AGAIN)
def test_overlaps_by_its_routines_name_ends_an_interval_period(sql: str, answer: bool | None):
    import duckdb

    from provisa.transpiler.transpile import transpile_to_duckdb

    assert duckdb.connect().sql(transpile_to_duckdb(sql)).fetchall() == [(answer,)]


@pytest.mark.parametrize(
    ("written", "icu"),
    [
        ("12", "GMT-12:00"),
        ("+05:30", "GMT-05:30"),
        ("-3", "GMT+03:00"),
        ("XYZ-2", "GMT+02:00"),
        ("UTC", None),
        ("America/New_York", None),
        ("est", None),
        ("24", None),
        ("5:30:15", None),
    ],
)
def test_a_posix_zone_of_a_fixed_offset_is_the_icu_zone_of_it(written: str, icu: str | None):
    from provisa.transpiler.pg_zones import icu_fixed_zone

    assert icu_fixed_zone(written) == icu


@pytest.mark.parametrize(
    ("written", "icu"),
    [("+05:30", "GMT+05:30"), ("-3", "GMT-03:00"), ("+0530", "GMT+05:30"), ("12", None)],
)
def test_a_numeric_zone_is_the_icu_zone_east_of_utc(written: str, icu: str | None):
    from provisa.transpiler.pg_zones import icu_numeric_zone

    assert icu_numeric_zone(written) == icu


_PAIRS = "FROM (VALUES (3::integer, 3::integer), ((-7)::integer, (-7)::integer), (42::integer, 42::integer)) AS t(c1, c2)"

#: (statement, PostgreSQL 17's result type, its JSON): compared parsed, its members in order, as
#: the check compares json and jsonb.
JSON_ANSWERS: list[tuple[str, int, str]] = [
    # value ORDER BY ...: DuckDB's json_group_object is a macro, which takes no ORDER BY.
    *(
        (
            f'SELECT pg_catalog."{name}"(c1, c2 ORDER BY c1) AS v {_PAIRS}',
            114,
            '{ "-7" : -7, "3" : 3, "42" : 42 }',
        )
        for name in (
            "json_object_agg",
            "json_object_agg_strict",
            "json_object_agg_unique",
            "json_object_agg_unique_strict",
        )
    ),
    (
        f"SELECT json_object_agg(c1, c2 ORDER BY c1 DESC) AS v {_PAIRS}",
        114,
        '{ "42" : 42, "3" : 3, "-7" : -7 }',
    ),
    *(
        (
            f'SELECT pg_catalog."{name}"(c1, c2 ORDER BY c1) AS v {_PAIRS}',
            3802,
            '{"3": 3, "-7": -7, "42": 42}',
        )
        for name in (
            "jsonb_object_agg",
            "jsonb_object_agg_strict",
            "jsonb_object_agg_unique",
            "jsonb_object_agg_unique_strict",
        )
    ),
    # A json value keeps a repeated key, in order.
    (
        "SELECT json_object_agg_strict(k, v) FROM (VALUES (1, 2), (1, 3), (2, NULL::integer)) "
        "AS t(k, v)",
        114,
        '{ "1" : 2, "1" : 3 }',
    ),
    # A key of any type is its text.
    ('SELECT pg_catalog."json_build_object"(1::integer, 2::integer) AS v', 114, '{"1" : 2}'),
    ('SELECT pg_catalog."jsonb_build_object"(1::integer, 2::integer) AS v', 3802, '{"1": 2}'),
]


def _members(text: str) -> object:  # object-ok: parsed JSON of any shape
    import json

    return json.loads(text, object_pairs_hook=list)


@pytest.mark.parametrize(("sql", "oid", "answer"), JSON_ANSWERS, ids=[a[0] for a in JSON_ANSWERS])
def test_duckdb_json_answers_as_postgresql_17(conn: Any, sql: str, oid: int, answer: str):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.ftype(0) == oid
    assert _members(res.get_value(0, 0).decode()) == _members(answer)


def test_pg_timezone_names_reads_each_zone_now_as_the_pinned_release_does():
    """Run 43237880934's wrong answer: America/Edmonton was CST, -06:00 and daylight time, as
    DuckDB's ICU release has it, where PostgreSQL, of a release as recent as the pinned one, said
    standard time. Each zone's offset and daylight flag are the pinned release's, as its
    abbreviation is."""
    import datetime
    import zoneinfo

    import duckdb

    from provisa.transpiler import pg_zones

    rows = duckdb.connect().sql(f"SELECT name, abbrev, utc_offset, is_dst FROM ({pg_zones.NAMES})")
    now = datetime.datetime.now(datetime.timezone.utc)
    wrong = []
    for name, abbrev, offset, dst in rows.fetchall():
        local = now.astimezone(zoneinfo.ZoneInfo(name))
        pinned = (local.tzname(), local.utcoffset(), bool(local.dst()))
        if (abbrev, offset, dst) != pinned:
            wrong.append((name, (abbrev, offset, dst), pinned))
    assert wrong == []
