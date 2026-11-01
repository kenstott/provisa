# Copyright (c) 2026 Kenneth Stott
# Canary: e6d40b73-1c28-4f95-a07e-3b94c1d7f026
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The type operators family answers as PostgreSQL 17 does (REQ-2171): each statement of
type_io_corpus, written for DuckDB, against what postgres:17.11 answered. Trino and ClickHouse replay
the same statements in tests/integration/test_type_io_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.pgwire.copy_handler import CopyHandler
from provisa.pgwire.server import ProvisaSession, _sqlstate_of, startup_time_zone
from provisa.pgwire.session_zone import UNKNOWN_ZONE, SessionZone, refuse_other_zone
from provisa.transpiler.function_macros import MACROS, outcome
from provisa.transpiler.pg_forms_type_io import ROWS, TRINO, ZONED, readable, zoned_call
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_type_io_answers import RECORDED
from tests.unit.pgwire.type_io_corpus import (
    NAME_OF_THE_REFUSAL,
    REFUSALS,
    REFUSED,
    by_function,
    statements,
    verdict,
)

_FUNCTIONS = by_function(RECORDED)
_ENGINES = ("duckdb", "trino", "clickhouse")


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


@pytest.fixture(scope="module")
def connection() -> Any:
    connected = duckdb.connect()
    connected.execute("SET TimeZone='UTC'")
    return connected


def _difference(connection: Any, sql: str) -> str | None:
    try:
        got = connection.execute(transpile_to_duckdb(sql)).fetchone()[0]
    except duckdb.Error as raised:
        refusal = from_engine(raised)
        if refusal is None:
            return f"not raised as PostgreSQL's: {raised}"
        return verdict(sql, RECORDED[sql], None, f"{refusal.sqlstate}: {refusal}")
    return verdict(sql, RECORDED[sql], got, None)


@pytest.mark.parametrize("function", sorted(_FUNCTIONS))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, function: str):
    found = {sql: _difference(connection, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


@pytest.mark.parametrize("name, arity", [(row[0], len(row[1])) for row in ROWS])
@pytest.mark.parametrize("dialect", _ENGINES)
def test_every_overload_has_a_form_on_the_engine(name: str, arity: int, dialect: str):
    assert outcome(name, arity, dialect) != "refused"
    assert MACROS[(name, arity)].forms


def test_a_function_is_read_row_by_row():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (-7), (NULL)) v(i)")
    rows = connection.execute(transpile_to_duckdb("SELECT int4send(i) AS v FROM t")).fetchall()
    assert [row[0] for row in rows] == [b"\x00\x00\x00\x01", b"\xff\xff\xff\xf9", None]


def test_a_statement_that_calls_none_of_them_is_not_parsed():
    assert readable("SELECT ¿ FROM t") == "SELECT ¿ FROM t"


def test_a_call_keeps_the_name_of_its_column():
    written = transpile_to_duckdb("SELECT pg_catalog.int4out(NULL::integer)")
    assert duckdb.connect().execute(written).description[0][0] == "int4out"


@pytest.mark.parametrize(
    "sql", [sql for sql, (oid, _) in RECORDED.items() if oid is not None and "ROW(" not in sql]
)
def test_the_wire_says_the_type_postgresql_says(sql: str):
    casts = result_casts(sql)
    assert casts is not None and casts[0] is not None and casts[0].oid == RECORDED[sql][0]


_ZONED_CALLS = {
    "timestamptz_in": "SELECT timestamptz_in('2024-06-15 12:30:45+00', 1184, -1)",
    "timestamptz_out": "SELECT pg_catalog.\"timestamptz_out\"('2024-06-15 12:30:45+00'::timestamptz)",
    "timestamptz_send": "SELECT timestamptz_send(t) FROM events",
}


@pytest.mark.parametrize("function", sorted(ZONED))
@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Kolkata", "-5", UNKNOWN_ZONE])
def test_a_form_bound_to_utc_is_refused_by_name_in_another_zone(function: str, zone: str):
    session = ProvisaSession()
    session.role_id = "analyst"
    session.zone = SessionZone(zone)
    with pytest.raises(PostgresError) as raised:
        session._execute_sql_bound(_ZONED_CALLS[function])
    assert _sqlstate_of(raised.value) == "0A000"
    assert str(raised.value).startswith(f"{function} does not read this value")
    assert zone in str(raised.value)


@pytest.mark.parametrize("function", sorted(ZONED))
@pytest.mark.parametrize("zone", [None, "UTC", "utc", "Etc/UTC", "GMT"])
def test_a_session_on_utc_is_answered(function: str, zone: str | None):
    refuse_other_zone(_ZONED_CALLS[function], zone)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT 'timestamptz_out(' AS said",
        "SELECT timestamp_out('2024-06-15 12:30:45'::timestamp)",
    ],
)
def test_a_statement_that_calls_no_such_form_is_not_refused(sql: str):
    assert zoned_call(sql) is None
    refuse_other_zone(sql, "America/New_York")


def test_a_call_in_a_text_sqlglot_cannot_read_is_taken_for_a_call():
    assert zoned_call("SELECT ¿ timestamptz_out(x) FROM (") == "timestamptz_out"


def _zone_after(*statements: str, started: str | None = None) -> str | None:
    zone = SessionZone(started)
    in_block = False
    for sql in statements:
        word = sql.split()[0].upper()
        opens = word == "BEGIN" or (in_block and word not in ("COMMIT", "ROLLBACK", "ABORT"))
        to_savepoint = sql.upper().startswith("ROLLBACK TO")
        zone.note(sql, in_block, opens or to_savepoint)
        in_block = opens or to_savepoint
    return zone.current


@pytest.mark.parametrize(
    "statements, want",
    [
        (["SET TimeZone TO 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET timezone = 'Asia/Tokyo';"], "Asia/Tokyo"),
        (["SET TIME ZONE 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET SESSION TIME ZONE 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET TIME ZONE -5"], "-5"),
        (["SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE"], "INTERVAL '-08:00' HOUR TO MINUTE"),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TimeZone TO DEFAULT"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TIME ZONE LOCAL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "RESET TimeZone"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "RESET ALL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "DISCARD ALL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TimeZone TO ''"], "Asia/Tokyo"),
        (["SET LOCAL TimeZone TO 'Asia/Tokyo'"], None),
        (["BEGIN", "SET LOCAL TimeZone TO 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["BEGIN", "SET LOCAL TimeZone TO 'Asia/Tokyo'", "COMMIT"], None),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "COMMIT"], "Asia/Tokyo"),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "ROLLBACK"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "BEGIN", "SET TimeZone TO 'UTC'", "ABORT"], "Asia/Tokyo"),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "ROLLBACK TO s"], UNKNOWN_ZONE),
        (["SELECT set_config('TimeZone', 'Asia/Tokyo', false)"], "Asia/Tokyo"),
        (["SELECT pg_catalog.set_config('timezone', 'Asia/Tokyo'::text, false)"], "Asia/Tokyo"),
        (["SELECT set_config('TimeZone', 'Asia/Tokyo', true)"], None),
        (["SELECT set_config('search_path', 'a', false)"], None),
        (["SELECT set_config($1, $2, false)"], UNKNOWN_ZONE),
        (["SELECT set_config('TimeZone', $1, false)"], UNKNOWN_ZONE),
    ],
)
def test_the_session_knows_its_zone(statements: list[str], want: str | None):
    assert _zone_after(*statements) == want


def test_copy_of_such_a_form_is_refused_in_another_zone():
    session = SimpleNamespace(role_id="analyst", zone=SessionZone("Asia/Tokyo"))
    sql = "COPY (SELECT timestamptz_out(t) FROM events) TO STDOUT"
    with pytest.raises(PostgresError, match="^timestamptz_out does not read this value"):
        CopyHandler(SimpleNamespace()).handle(SimpleNamespace(session=session), sql)


@pytest.mark.parametrize("set_zone", ["SET TimeZone TO 'Asia/Tokyo'", "SET TIME ZONE 'Asia/Tokyo'"])
def test_a_set_of_the_session_reaches_the_refusal(set_zone: str):
    session = ProvisaSession()
    session.role_id = "analyst"
    session.user_id = "analyst"
    session._execute_sql_bound(set_zone)
    assert session.zone.current == "Asia/Tokyo"
    with pytest.raises(PostgresError, match="^timestamptz_out does not read this value"):
        session._execute_sql_bound(_ZONED_CALLS["timestamptz_out"])


def test_reset_returns_to_the_zone_the_connection_started_on():
    assert (
        _zone_after("SET TimeZone TO 'UTC'", "RESET TimeZone", started="Asia/Tokyo") == "Asia/Tokyo"
    )
    assert _zone_after("SET TimeZone TO DEFAULT", started="Asia/Tokyo") == "Asia/Tokyo"


@pytest.mark.parametrize(
    "params, want",
    [
        ({}, None),
        ({"TimeZone": "America/New_York"}, "America/New_York"),  # pgjdbc
        ({"timezone": "America/New_York"}, "America/New_York"),  # libpq, for PGTZ
        ({"options": "-c TimeZone=America/New_York"}, "America/New_York"),
        ({"options": "--timezone=America/New_York"}, "America/New_York"),
        ({"TimeZone": "Asia/Tokyo", "options": "-c TimeZone=UTC"}, "Asia/Tokyo"),
        ({"options": "-c TimeZone=UTC -c TimeZone=Asia/Tokyo"}, UNKNOWN_ZONE),
    ],
)
def test_a_connection_can_start_on_a_zone(params: dict[str, str], want: str | None):
    assert startup_time_zone(params) == want


@pytest.mark.parametrize("function", ["timestamptz_out", "timestamptz_send"])
def test_the_engines_own_zone_does_not_change_an_answer(function: str):
    connection = duckdb.connect()
    connection.execute("SET TimeZone='America/New_York'")
    found = {sql: _difference(connection, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


@pytest.mark.parametrize(("name", "form"), [(r[0], r[2][TRINO.dialects]) for r in ROWS])
def test_trino_is_handed_nothing_it_refuses_to_analyze(name: str, form: str):
    assert re.search(r"AS INTEGER\) AS (DATE|TIME|VARBINARY|UUID)", form) is None
    assert "[[]" not in form and "[]]" not in form


@pytest.mark.parametrize(
    "sql",
    [
        """SELECT pg_catalog."float4out"('1.4e-45'::real) AS v""",
        """SELECT pg_catalog."float8out"('5e-324'::double precision) AS v""",
    ],
)
def test_trino_writes_the_one_digit_java_does_not(sql: str):
    assert "'%.0e'" in transpile(sql, "trino")


def test_a_null_time_is_text_on_clickhouse():
    sql = """SELECT pg_catalog."time_in"(NULL::cstring, NULL::oid, NULL::integer) AS v"""
    assert "toTime64" not in transpile(sql, "clickhouse")
    assert "TIME" in transpile(sql, "trino")


@pytest.mark.parametrize(
    ("value", "refuses"),
    [
        ("'(1,2)'::point", False),
        ("'1,2'::point", True),
        ("'[(0,0),(3,4)]'::lseg", False),
        ("'((0,0),(1,1))'::lseg", True),
        ("'[(0,0),(1,1)]'::path", False),
        ("'((0,0),(1,1))'::polygon", False),
        ("'(0,0),(1,1)'::polygon", True),
        ("'<(1,2),3>'::circle", False),
    ],
)
def test_a_canonical_geometric_literal_cannot_reach_a_refusal(value: str, refuses: bool):
    stem = value.split("::")[1].replace("polygon", "poly")
    sql = f'SELECT pg_catalog."{stem}_send"({value}) AS v'
    assert (NAME_OF_THE_REFUSAL in transpile(sql, "clickhouse")) is refuses
