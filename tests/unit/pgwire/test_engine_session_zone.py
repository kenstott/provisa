# Copyright (c) 2026 Kenneth Stott
# Canary: 8d0f3a57-c1e2-4b96-8f24-3ad96e7b10c2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What runs a statement runs it in the session's zone (REQ-2171): a session on a zone reads what
PostgreSQL answers through a served socket and a real DuckDB engine; the zone stays on the statement,
so the next client on the same engine connection is on its own; and an answer in one zone is never
another zone's cached answer. tests/integration/test_engine_session_zone.py holds Trino, ClickHouse
and PostgreSQL itself to the same table."""

# Requirements: REQ-2171

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from provisa.cache.key import raw_sql_cache_key
from provisa.core.session_time_zone import (
    StatementZone,
    bind_zone,
    carrying_zone,
    current_zone,
    reset_zone,
    told_as,
    zone_cache_key,
)
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.pgwire import _pipeline
from provisa.pgwire.server import ProvisaConnection, ProvisaServer
from provisa.pgwire.session_zone import SessionZone
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.port_lease import lease_port
from tests.unit.pgwire.session_zone_answers import CASES, INSTANTS, statement
from tests.unit.pgwire.test_timestamptz_session_zone import (
    _Admits,
    _connect,
    _refused,
    _run,
    _simple,
)

_NEW_YORK = StatementZone("America/New_York", name="America/New_York", posix="America/New_York")
_TOKYO = StatementZone("Asia/Tokyo", name="Asia/Tokyo", posix="Asia/Tokyo")
_TEXT = "SELECT c::text AS v FROM t ORDER BY i"


@pytest.fixture(scope="module")
def engine() -> Iterator[DuckDBFederationRuntime]:
    runtime = DuckDBFederationRuntime()
    runtime._con.execute("CREATE TABLE t (i INTEGER, c TIMESTAMPTZ)")
    for i, instant in enumerate(INSTANTS):
        runtime._con.execute("INSERT INTO t VALUES (?, ?::TIMESTAMPTZ)", [i, instant])
    yield runtime
    runtime.close()


@pytest.fixture(scope="module")
def port() -> Iterator[int]:
    leased = lease_port()
    server = ProvisaServer(("127.0.0.1", leased), ProvisaConnection(), ssl_ctx=None)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.1)
    yield leased
    server.shutdown()


@pytest.fixture
def served(engine: DuckDBFederationRuntime) -> Iterator[None]:
    state = MagicMock()
    state.contexts = {"alice": MagicMock(tables={})}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple", "default_role": "alice", "role_mapping": []}
    state.auth_middleware_active = True
    state.multitenancy = False
    state.admin_db = None

    async def pipeline(sql, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        return await engine.run(transpile_to_duckdb(sql))

    with (
        patch("provisa.auth.wiring.build_auth_provider", return_value=_Admits()),
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", pipeline),
    ):
        yield


def _texts(rows: list[bytes | None]) -> tuple[str, ...]:
    return tuple((row or b"").decode() for row in rows)


def _rows(stream: Any) -> list[str]:
    return [row[0] for row in stream.rows]


@pytest.mark.parametrize("set_zone, name, want", CASES)
def test_a_client_reads_what_postgresql_answers(port, served, set_zone, name, want):
    with _connect(port) as connection:
        _run(connection, set_zone)
        assert _texts(_simple(connection, statement(name))) == want


def test_the_engines_stream_runs_in_the_zone_too(engine):
    token = bind_zone(_NEW_YORK)
    try:
        assert _rows(engine.run_sync(transpile_to_duckdb(_TEXT)))[2] == "2026-06-30 22:30:00-04"
    finally:
        reset_zone(token)


def test_a_session_that_set_nothing_is_told_nothing(port, served, engine):
    with _connect(port) as connection:
        assert _simple(connection, _TEXT)[0] == b"2026-07-01 12:00:00.5+00"


def test_set_local_runs_in_its_zone_and_ends_with_the_block(port, served):
    with _connect(port) as connection:
        _run(connection, "BEGIN")
        _run(connection, "SET LOCAL TimeZone = 'Asia/Kolkata'")
        assert _simple(connection, _TEXT)[1] == b"2026-01-01 17:30:00+05:30"
        _run(connection, "COMMIT")
        assert _simple(connection, _TEXT)[1] == b"2026-01-01 12:00:00+00"


def test_the_engine_connection_holds_no_clients_zone(engine):
    """A zone set on the connection, or globally, would be every sibling and later cursor's."""
    token = bind_zone(_NEW_YORK)
    try:
        engine.run_sync(_TEXT.replace("::text", "")).rows
    finally:
        reset_zone(token)
    assert engine._con.execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)
    assert engine._con.cursor().execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)


def test_a_second_client_on_the_same_connection_gets_its_own_zone(port, served):
    with _connect(port) as first, _connect(port) as second, _connect(port) as third:
        _run(first, "SET TimeZone = 'America/New_York'")
        _run(second, "SET TimeZone = 'Asia/Kolkata'")
        assert _simple(first, _TEXT)[0] == b"2026-07-01 08:00:00.5-04"
        assert _simple(second, _TEXT)[0] == b"2026-07-01 17:30:00.5+05:30"
        assert _simple(third, _TEXT)[0] == b"2026-07-01 12:00:00.5+00"
        assert _simple(first, _TEXT)[0] == b"2026-07-01 08:00:00.5-04"


def test_clients_at_once_on_one_engine_never_read_each_others_zone(engine):
    sql = transpile_to_duckdb(_TEXT)
    wrong: list[tuple[str, str]] = []
    start = threading.Barrier(3)

    def client(zone: StatementZone | None, want: str) -> None:
        token = bind_zone(zone)
        try:
            start.wait()
            for _ in range(50):
                got = _rows(engine.run_sync(sql))[0]
                if got != want:
                    wrong.append((want, got))
        finally:
            reset_zone(token)

    clients = [
        threading.Thread(target=client, args=args)
        for args in (
            (_NEW_YORK, "2026-07-01 08:00:00.5-04"),
            (_TOKYO, "2026-07-01 21:00:00.5+09"),
            (None, "2026-07-01 12:00:00.5+00"),
        )
    ]
    for each in clients:
        each.start()
    for each in clients:
        each.join()
    assert wrong == []


def test_a_stream_keeps_its_zone_while_another_client_runs(engine):
    sql = transpile_to_duckdb(_TEXT)
    token = bind_zone(_NEW_YORK)
    try:
        held = engine.run_sync(sql)
    finally:
        reset_zone(token)
    token = bind_zone(_TOKYO)
    try:
        assert _rows(engine.run_sync(sql))[0] == "2026-07-01 21:00:00.5+09"
    finally:
        reset_zone(token)
    assert _rows(held)[0] == "2026-07-01 08:00:00.5-04"


def test_a_zone_the_engine_cannot_read_is_refused_where_the_answer_depends_on_it(port, served):
    with _connect(port) as connection:
        _run(connection, "SET TIME ZONE 5.5")
        state, message = _refused(connection, _TEXT)
        assert state == b"0A000"
        assert b"<+05:30>-05:30" in (message or b"")
        assert _simple(connection, "SELECT 7 AS v") == [b"7"]


def test_a_refusal_is_not_a_wrong_zone_for_the_next_statement(port, served):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'Foo5Bar'")
        assert _refused(connection, _TEXT)[0] == b"0A000"
        _run(connection, "SET TimeZone = 'Asia/Kolkata'")
        assert _simple(connection, _TEXT)[1] == b"2026-01-01 17:30:00+05:30"


@pytest.mark.parametrize(
    "word, shown, name, offset",
    [
        ("America/New_York", "America/New_York", "America/New_York", None),
        ("UTC+5", "UTC+5", "Etc/GMT+5", "-05:00"),
        ("-5", "<-05>+05", "Etc/GMT+5", "-05:00"),
        ("5", "<+05>-05", "Etc/GMT-5", "+05:00"),
        ("5.5", "<+05:30>-05:30", None, "+05:30"),
        ("XYZ-3:30", "XYZ-3:30", None, "+03:30"),
        ("0", "<+00>-00", "Etc/UTC", "+00:00"),
        ("Foo5Bar", "FOO5BAR", None, None),
    ],
)
def test_a_zone_in_the_spellings_engines_read(word, shown, name, offset):
    assert SessionZone(word).statement_zone() == StatementZone(shown, name, offset, shown)


@pytest.mark.parametrize("word", [None, "UTC", "utc", "Etc/UTC", "GMT"])
def test_a_session_on_utc_tells_nothing(word):
    assert SessionZone(word).statement_zone() is None


def test_a_zone_no_statement_names_can_be_told_to_nothing():
    zone = SessionZone()
    zone.note("SELECT set_config('TimeZone', current_user, false)", False, False)
    told = zone.statement_zone()
    assert told is not None and (told.name, told.offset, told.posix) == (None, None, None)


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize(
    "word, value, offset",
    [
        ("UTC", "2000-1-1", 0),
        ("UTC", "2000-7-1", 0),
        ("Europe/Rome", "2000-1-1", 3600),
        ("Europe/Rome", "2000-7-1", 7200),
        ("Europe/Rome", "1000-1-1", 2996),
        ("NOSUCH0", "2000-1-1", 0),
        ("0", "2000-1-1", 0),
    ],
)
def test_a_zone_given_to_set_config_as_a_bound_value_is_the_zone(
    port, served, word, value, offset, binary
):
    """As psycopg's own test_load_datetimetz_tz: the zone is a bound value, so it can be told."""
    with _connect(port) as connection, connection.transaction():
        connection.execute("select set_config('TimeZone', %s, true)", [word])
        cursor = connection.cursor(binary=binary)
        got = cursor.execute(f"select '{value}'::timestamptz").fetchone()[0]
        assert got.utcoffset().total_seconds() == offset


def test_a_thread_is_given_the_zone_and_only_the_zone():
    seen: list[StatementZone | None] = []
    token = bind_zone(_TOKYO)
    try:
        carried = carrying_zone(lambda: seen.append(current_zone.get()))
    finally:
        reset_zone(token)
    worker = threading.Thread(target=carried)
    worker.start()
    worker.join()
    assert seen == [_TOKYO]
    assert current_zone.get() is None


def test_told_as_refuses_only_what_could_depend_on_the_zone():
    token = bind_zone(StatementZone("FOO5BAR"))
    try:
        assert told_as("SELECT a FROM t", lambda z: z.name, "X") is None
        with pytest.raises(Exception, match="X cannot run this statement"):
            told_as("SELECT CAST(a AS TEXT) FROM t", lambda z: z.name, "X")
    finally:
        reset_zone(token)


# -- the caches ------------------------------------------------------------------------------------


def _key(zone: str | None) -> str:
    return raw_sql_cache_key(_TEXT, [], "alice", wire_formats=None, zone=zone)


def test_an_answer_in_one_zone_is_not_another_zones():
    assert len({_key(None), _key("Asia/Tokyo"), _key("America/New_York")}) == 3


def test_a_session_on_utc_keeps_the_entry_every_other_surface_writes():
    assert _key(None) == raw_sql_cache_key(_TEXT, [], "alice", wire_formats=None)
    assert zone_cache_key() is None


def _plan(zone: str | None) -> _pipeline._Plan:
    return _pipeline._Plan(
        route=None,
        sql=_TEXT,
        source_id=None,
        dialect="duckdb",
        cache_opt_in=True,
        response_cacheable=True,
        role_id="alice",
        cache_sql=_TEXT,
        cache_params=[],
        cache_zone=zone,
    )


def test_a_plan_keeps_the_zone_it_was_planned_in_for_the_entry_written_after_its_drain():
    """A stream's entry is written once the client has read it, after execute_sql has unbound the
    zone: the key is the plan's, not the moment's."""
    with patch("provisa.compiler.temp_tables.slot_key", return_value=None):
        tokyo = _pipeline._response_cache_key(_plan("Asia/Tokyo"), wire_formats=None)
        utc = _pipeline._response_cache_key(_plan(None), wire_formats=None)
    assert tokyo == _key("Asia/Tokyo") != utc


def test_the_statement_as_compiled_does_not_depend_on_the_zone(engine):
    """What the plan and engine-form caches keep is the physical text: DuckDB is told the zone
    beside it, so one kept form serves every zone."""
    physical = transpile_to_duckdb(_TEXT)
    for zone, want in ((_NEW_YORK, "2026-07-01 08"), (_TOKYO, "2026-07-01 21")):
        token = bind_zone(zone)
        try:
            assert transpile_to_duckdb(_TEXT) == physical
            assert _rows(engine.run_sync(physical))[0].startswith(want)
        finally:
            reset_zone(token)
