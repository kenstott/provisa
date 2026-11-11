# Copyright (c) 2026 Kenneth Stott
# Canary: 575b9aa7-3594-43ac-8f20-95bbb67dfffc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine's answer carries its columns' types, on every read path (REQ-846).

The event loop learns a materialized view's output columns from a LIMIT 0 read of its build
statement (events/app_wiring.wire_event_loop): a view whose read reports no types is left off the
scheduler, so it is never built. The engine's description of a result names each column's type
whether or not a row comes back."""

# Requirements: REQ-846

from __future__ import annotations

import pytest

from provisa.federation.pg_runtime import PgFederationRuntime

pytestmark = pytest.mark.unit


@pytest.fixture
def runtime(embedded_postgres):
    rt = PgFederationRuntime(engine_dsn=embedded_postgres.dsn())
    yield rt
    rt.close()


_READ = "SELECT 'us'::text AS region, SUM(n) AS total, COUNT(*) AS n FROM (VALUES (1), (2)) AS t(n)"


async def test_a_read_reports_its_columns_types_with_no_row_returned(runtime):
    result = await runtime.run(f"SELECT * FROM ({_READ}) AS probe LIMIT 0")
    assert result.rows == []
    assert result.column_names == ["region", "total", "n"]
    assert result.column_types == ["text", "int8", "int8"]


async def test_a_read_reports_the_same_types_as_the_streamed_read(runtime):
    streamed = runtime.run_sync(_READ)
    try:
        expected = streamed.column_types
    finally:
        streamed.close()
    assert (await runtime.run(_READ)).column_types == expected


async def test_a_range_is_postgresqls_own_text(runtime):
    result = await runtime.run("SELECT int4range(NULL, NULL) AS a, '[1,5)'::int4range AS b")
    assert result.rows == [("(,)", "[1,5)")]


def test_a_streamed_range_is_postgresqls_own_text(runtime):
    assert runtime.run_sync("SELECT daterange(NULL, NULL) AS a").rows == [("(,)",)]


def test_an_empty_xml_array_is_no_elements(runtime):
    assert runtime.run_sync("SELECT '{}'::xml[] AS a").rows == [([],)]


async def test_a_read_names_each_columns_own_type(runtime):
    read = "SELECT 'a'::char(1) AS c FROM (VALUES (1)) AS t(n)"
    assert (await runtime.run(read)).column_type_oids[0] == 1042
    assert runtime.run_sync(read).column_type_oids[0] == 1042


#: REQ-2167: what PostgreSQL wrote of a json value is its answer, spacing and key order kept; read
#: into a dict, it came back as Python's repr ("{'a': 'a'}").
_JSON_BUILDERS = [
    (
        "SELECT json_object('{a,b,c}'::text[], '{a,b,c}'::text[]) AS v",
        '{"a" : "a", "b" : "b", "c" : "c"}',
    ),
    ("SELECT json_object('{a,1,b,2}'::text[]) AS v", '{"a" : "1", "b" : "2"}'),
    ("SELECT json_build_object('a', 1, 'b', 'x') AS v", '{"a" : 1, "b" : "x"}'),
    ("SELECT json_build_array(1, 'x', NULL) AS v", '[1, "x", null]'),
    ("SELECT to_json(ARRAY[1, 2]) AS v", "[1,2]"),
    ("SELECT row_to_json(ROW(1, 'x')) AS v", '{"f1":1,"f2":"x"}'),
    ("SELECT array_to_json(ARRAY[1, 2]) AS v", "[1,2]"),
    ('SELECT \'{"b":1,  "a":2}\'::json AS v', '{"b":1,  "a":2}'),
]


@pytest.mark.parametrize(("sql", "want"), _JSON_BUILDERS)
async def test_a_json_value_is_postgresqls_own_text(runtime, sql: str, want: str):
    assert (await runtime.run(sql)).rows == [(want,)]
    assert runtime.run_sync(sql).rows == [(want,)]


def test_a_json_array_keeps_each_elements_text(runtime):
    assert runtime.run_sync("SELECT ARRAY['{\"a\" : 1}'::json] AS v").rows == [(['{"a" : 1}'],)]


#: An array whose lower bounds are not 1 is written with its bounds, which psycopg2's array reader
#: refused ("array does not start with '{'"): its text is PostgreSQL's answer.
_BOUNDED = [
    (
        "SELECT array_fill(3::integer, '{1,2,3}'::integer[], '{1,2,3}'::integer[]) AS v",
        "[1:1][2:3][3:5]={{{3,3,3},{3,3,3}}}",
    ),
    ("SELECT '[0:1]={a,b}'::text[] AS v", "[0:1]={a,b}"),
]


@pytest.mark.parametrize(("sql", "want"), _BOUNDED)
async def test_an_array_with_its_own_bounds_is_postgresqls_own_text(runtime, sql: str, want: str):
    assert (await runtime.run(sql)).rows == [(want,)]
    assert runtime.run_sync(sql).rows == [(want,)]


def test_an_array_from_one_is_still_a_list(runtime):
    assert runtime.run_sync("SELECT array_fill(3, ARRAY[2]) AS v").rows == [([3, 3],)]


#: REQ-2167 (function check run 42527458223): interval_in('3 mons') answered '90 days' on the pg
#: engine, where PostgreSQL keeps the months. psycopg2 reads an interval as a timedelta, a month
#: thirty days; the engine's read keeps PostgreSQL's months, days and microseconds.
_INTERVALS = [
    "3 mons",
    "1 year 2 mons 3 days 04:05:06.789",
    "-1 years +2 days",
    "1 day -01:00:00",
    "100:00:00",
    "-00:00:01",
]


@pytest.mark.parametrize("text", _INTERVALS)
async def test_an_interval_keeps_postgresqls_fields(runtime, text: str):
    from provisa.core.ir_types import interval_parts
    from provisa.transpiler.pg_type_literals import interval_fields

    sql = f"SELECT '{text}'::interval AS v"
    want = interval_fields(text)
    assert interval_parts((await runtime.run(sql)).rows[0][0]) == want
    assert interval_parts(runtime.run_sync(sql).rows[0][0]) == want


@pytest.mark.parametrize("text", ["infinity", "-infinity"])
def test_an_infinite_interval_keeps_its_fields(text: str):
    """PostgreSQL 17's interval infinities, which the embedded 16 cannot write, read by the
    engine's reader of interval_out's text."""
    from provisa.core.ir_types import interval_parts
    from provisa.federation.pg_runtime import _interval
    from provisa.transpiler.pg_type_literals import interval_fields, interval_text

    fields = interval_parts(_interval(text, None))
    assert (fields, interval_text(*fields)) == (interval_fields(text), text)


def test_an_interval_in_another_style_is_refused():
    import psycopg2

    from provisa.federation.pg_runtime import _interval

    with pytest.raises(psycopg2.DataError, match="can't read interval '\\+0-3'"):
        _interval("+0-3", None)


def test_an_interval_array_keeps_each_elements_fields(runtime):
    from provisa.core.ir_types import interval_parts

    (value,) = runtime.run_sync("SELECT ARRAY['3 mons', '1 day']::interval[] AS v").rows[0]
    assert [interval_parts(v) for v in value] == [(3, 0, 0), (0, 1, 0)]


def test_a_null_interval_is_null(runtime):
    assert runtime.run_sync("SELECT NULL::interval AS v").rows == [(None,)]
