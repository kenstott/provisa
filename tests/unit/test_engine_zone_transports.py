# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7c9e21-b84d-46a0-97e3-d1c05a6f2b78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How each engine is told a statement's zone, and that nothing it shares keeps it (REQ-2171): Trino
in the statement's own session through the proxy (its call header), ClickHouse in the request's settings, PostgreSQL in a SET
LOCAL that ends with the transaction. chdb is ClickHouse, so its answers here are ClickHouse's. What
these engines then answer of a stored instant is tests/integration/test_engine_session_zone.py's."""

# Requirements: REQ-2171

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest

from provisa.core.pg_error import PostgresError
from provisa.core.session_time_zone import StatementZone, bind_zone, reset_zone
from provisa.executor import trino as trino_exec
from provisa.executor.trino_flight import (
    EngineUnreachable,
    TrinoContext,
    execute_trino_flight,
    execute_trino_flight_sync,
)
from provisa.executor.drivers import postgresql
from provisa.federation import clickhouse_runtime, pg_runtime
from tests.zaychik_double import ProxyConnection, backend_state, trino_backend

_TOKYO = StatementZone("Asia/Tokyo", name="Asia/Tokyo", posix="Asia/Tokyo")
_HALF = StatementZone("<+05:30>-05:30", offset="+05:30", posix="<+05:30>-05:30")
_RULED = StatementZone("FOO5BAR", posix="FOO5BAR")


@pytest.fixture
def on_zone(request: pytest.FixtureRequest) -> Iterator[None]:
    token = bind_zone(request.param)
    yield
    reset_zone(token)


def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trino_exec, "_retry_budget", lambda: 30.0)
    monkeypatch.setattr(trino_exec, "_backoff_secs", lambda attempt, cap=30.0: 0.0)


_ORG = TrinoContext(source="provisa/org-1", catalog="system", schema="org_1")


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_trino_gets_the_zone_in_the_statements_own_session(on_zone):
    proxy = ProxyConnection()
    execute_trino_flight(proxy, "SELECT 1", context=_ORG)
    (session,) = proxy.sessions
    assert session["timeZone"] == "Asia/Tokyo"
    assert session["source"] == "provisa/org-1"
    assert "timeZone" not in _ORG.body()  # the context another statement is given is unchanged


@pytest.mark.parametrize("on_zone", [_HALF], indirect=True)
def test_trino_reads_an_offset_where_the_tz_database_names_none(on_zone):
    proxy = ProxyConnection()
    execute_trino_flight(proxy, "SELECT 1", context=_ORG)
    assert proxy.sessions[0]["timeZone"] == "+05:30"


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_trino_gets_the_zone_on_a_statement_sent_with_no_context(on_zone):
    proxy = ProxyConnection()
    execute_trino_flight(proxy, "SELECT 1")
    assert proxy.sessions == [{"timeZone": "Asia/Tokyo"}]


def test_trino_on_utc_names_no_zone():
    proxy = ProxyConnection()
    execute_trino_flight(proxy, "SELECT 1", context=_ORG)
    assert "timeZone" not in proxy.sessions[0]
    bare = ProxyConnection()
    execute_trino_flight(bare, "SELECT 1")
    assert bare.sessions == []  # nothing to send: the proxy's own session


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_a_trino_retry_keeps_the_zone(on_zone, monkeypatch):
    _no_wait(monkeypatch)
    failures = [EngineUnreachable("gone")]

    def fail_once(sql: str, session: dict | None) -> None:
        if failures:
            raise failures.pop()

    proxy = ProxyConnection(on_execute=fail_once)
    execute_trino_flight(proxy, "SELECT 1", context=_ORG)
    assert [session["timeZone"] for session in proxy.sessions] == ["Asia/Tokyo"] * 2


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_the_streamed_read_gets_the_zone(on_zone):
    proxy = ProxyConnection()
    execute_trino_flight_sync(proxy, "SELECT 1", context=_ORG)
    assert proxy.sessions[0]["timeZone"] == "Asia/Tokyo"


@pytest.mark.asyncio
@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
async def test_the_backend_carries_the_zone_to_the_executor_thread(on_zone):
    proxy = ProxyConnection()
    await trino_backend().execute(backend_state(proxy), "SELECT 1")
    assert proxy.sessions[0]["timeZone"] == "Asia/Tokyo"


@pytest.mark.parametrize("on_zone", [_RULED], indirect=True)
def test_trino_refuses_what_could_depend_on_a_zone_it_cannot_read(on_zone):
    proxy = ProxyConnection()
    with pytest.raises(PostgresError, match="Trino cannot run this statement"):
        execute_trino_flight(proxy, "SELECT CAST(c AS VARCHAR) FROM t", context=_ORG)
    assert proxy.statements == []


# -- ClickHouse ------------------------------------------------------------------------------------


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_clickhouse_is_told_in_the_requests_own_settings(on_zone):
    assert clickhouse_runtime._settings("q", "SELECT 1") == {
        "query_id": "q",
        "max_query_size": str(clickhouse_runtime.MAX_QUERY_SIZE),
        "session_timezone": "Asia/Tokyo",
    }


def test_clickhouse_on_utc_has_no_zone_setting():
    assert clickhouse_runtime._settings("q", "SELECT 1") == {
        "query_id": "q",
        "max_query_size": str(clickhouse_runtime.MAX_QUERY_SIZE),
    }


@pytest.mark.parametrize("on_zone", [_HALF], indirect=True)
def test_clickhouse_reads_a_fixed_offset_in_cctzs_spelling(on_zone):
    assert clickhouse_runtime._settings("q", "SELECT 1")["session_timezone"] == (
        "Fixed/UTC+05:30:00"
    )


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
@pytest.mark.parametrize(
    "sql, want",
    [
        ("SELECT 1", "SELECT 1 SETTINGS session_timezone = 'Asia/Tokyo'"),
        ("SELECT 1;", "SELECT 1 SETTINGS session_timezone = 'Asia/Tokyo'"),
        ("SELECT 1 SETTINGS a = 1", "SELECT 1 SETTINGS a = 1, session_timezone = 'Asia/Tokyo'"),
        ("SELECT 'SETTINGS'", "SELECT 'SETTINGS' SETTINGS session_timezone = 'Asia/Tokyo'"),
    ],
)
def test_chdb_carries_the_zone_in_the_statement(on_zone, sql, want):
    assert clickhouse_runtime._in_session_zone(sql) == want


@pytest.fixture(scope="module")
def chdb() -> Iterator[Any]:
    backend = clickhouse_runtime._EmbeddedBackend()
    yield backend
    backend.close()


def _first(backend: Any, sql: str) -> Any:
    table = backend.query_arrow(sql)
    return table.column(0)[0].as_py()


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_chdb_runs_the_statement_in_the_zone_told(on_zone, chdb):
    assert _first(chdb, "SELECT timezone()") == "Asia/Tokyo"


def test_nothing_of_the_last_statements_zone_stays_on_the_session(chdb):
    own = _first(chdb, "SELECT timezone()")
    token = bind_zone(_TOKYO)
    try:
        assert _first(chdb, "SELECT timezone()") == "Asia/Tokyo"
    finally:
        reset_zone(token)
    assert _first(chdb, "SELECT timezone()") == own


@pytest.mark.parametrize("on_zone", [_HALF], indirect=True)
def test_chdb_reads_a_fixed_half_hour_offset(on_zone, chdb):
    assert _first(chdb, "SELECT timeZoneOffset(now())") == 19800


# -- PostgreSQL ------------------------------------------------------------------------------------


@pytest.mark.parametrize("on_zone", [_RULED], indirect=True)
def test_postgresql_engine_is_told_as_set_local(on_zone):
    """PostgreSQL alone reads a POSIX daylight rule, and ``true`` ends it with the transaction."""
    connection = MagicMock()
    pg_runtime._tell_zone(connection, "SELECT 1")
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.assert_called_once_with("SELECT set_config('TimeZone', %s, true)", ("FOO5BAR",))


@pytest.mark.parametrize("on_zone", [_TOKYO], indirect=True)
def test_a_postgresql_source_is_told_inside_the_statements_own_transaction(on_zone):
    connection = MagicMock()
    with postgresql._in_session_zone(connection, "SELECT 1"):
        connection.transaction.return_value.__enter__.assert_called_once()
        connection.transaction.return_value.__exit__.assert_not_called()
    assert connection.execute.call_args.args == (
        "SELECT set_config('TimeZone', %s, true)",
        ("Asia/Tokyo",),
    )
    connection.transaction.return_value.__exit__.assert_called_once()


def test_a_postgresql_source_on_utc_stays_autocommit():
    connection = MagicMock()
    with postgresql._in_session_zone(connection, "SELECT 1"):
        pass
    connection.transaction.assert_not_called()
    connection.execute.assert_not_called()


def test_no_source_or_engine_tells_a_zone_with_bare_set():
    """SET without LOCAL would stay on a pooled connection for its next client."""
    for module in (postgresql, pg_runtime):
        source = open(module.__file__).read()
        assert "SET TimeZone" not in source and "SET TIME ZONE" not in source
