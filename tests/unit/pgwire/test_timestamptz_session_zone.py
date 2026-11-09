# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b7f12-6a4d-4c08-b1d5-92f0c7a4e816
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamptz over pgwire in a session not on UTC, against what PostgreSQL 17.11 answered
(REQ-2171): text is written in the session's zone, binary is the instant in UTC whatever the zone,
and SHOW and ParameterStatus name the zone the session is on. Each statement runs through a served
socket and libpq, so the bytes compared are the ones a client reads."""

# Requirements: REQ-2171

from __future__ import annotations

import datetime
import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.executor.result import QueryResult as EngineResult
from provisa.pgwire.server import ProvisaConnection, ProvisaServer
from tests.port_lease import lease_port

UTC = datetime.timezone.utc
_SUMMER = datetime.datetime(2026, 7, 1, 12, 0, 0, 500000, tzinfo=UTC)
_WINTER = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
_OLD = datetime.datetime(1800, 1, 1, 0, 0, 0, tzinfo=UTC)

#: timestamptz_send of each instant, as PostgreSQL 17.11 answered under Etc/UTC and Asia/Kolkata.
_SENT = {
    _SUMMER: "0002f88a33229120",
    _WINTER: "0002ea5119d41000",
    _OLD: "ffe993dcf8170000",
}

#: (the statement that puts the session on the zone, SHOW TimeZone, the summer instant's text, the
#: winter instant's text), each as PostgreSQL 17.11 answered.
_ZONES = [
    (
        "SET TimeZone = 'America/New_York'",
        "America/New_York",
        "2026-07-01 08:00:00.5-04",
        "2026-01-01 07:00:00-05",
    ),
    (
        "SET TIME ZONE 'america/new_york'",
        "America/New_York",
        "2026-07-01 08:00:00.5-04",
        "2026-01-01 07:00:00-05",
    ),
    (
        "SET timezone TO 'asia/KOLKATA'",
        "Asia/Kolkata",
        "2026-07-01 17:30:00.5+05:30",
        "2026-01-01 17:30:00+05:30",
    ),
    ("SET \"TimeZone\" = 'Asia/Tokyo'", "Asia/Tokyo", "2026-07-01 21:00:00.5+09", None),
    ("SET TimeZone = 'Etc/GMT+5'", "Etc/GMT+5", "2026-07-01 07:00:00.5-05", None),
    ("SET TimeZone = 'UTC+5'", "UTC+5", "2026-07-01 07:00:00.5-05", None),
    ("SET TimeZone = 'utc+5'", "UTC+5", "2026-07-01 07:00:00.5-05", None),
    ("SET TimeZone = 'XYZ-3:30'", "XYZ-3:30", "2026-07-01 15:30:00.5+03:30", None),
    ("SET TIME ZONE -5", "<-05>+05", "2026-07-01 07:00:00.5-05", None),
    ("SET TIME ZONE 5.5", "<+05:30>-05:30", "2026-07-01 17:30:00.5+05:30", None),
    ("SET TimeZone = '5'", "<+05>-05", "2026-07-01 17:00:00.5+05", None),
    ("SET TimeZone = '0'", "<+00>-00", "2026-07-01 12:00:00.5+00", None),
    (
        "SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE",
        "<-08>+08",
        "2026-07-01 04:00:00.5-08",
        None,
    ),
    ("SET TimeZone = 'utc'", "UTC", "2026-07-01 12:00:00.5+00", None),
    ("SET TimeZone = 'gmt'", "GMT", "2026-07-01 12:00:00.5+00", None),
]


class _Infinite(datetime.datetime):
    """The value an engine's ``infinity`` arrives as: datetime's end, marked with its sign."""

    pg_infinity = 1


_ROWS = [
    (_SUMMER,),
    (_WINTER,),
    (_OLD,),
    (_Infinite(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),),
    (None,),
]


def _micros(value: datetime.datetime) -> bytes:
    delta = value - datetime.datetime(2000, 1, 1, tzinfo=UTC)
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds).to_bytes(
        8, "big", signed=True
    )


@pytest.fixture(scope="module")
def port() -> Iterator[int]:
    leased = lease_port()
    server = ProvisaServer(("127.0.0.1", leased), ProvisaConnection(), ssl_ctx=None)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.1)
    yield leased
    server.shutdown()


class _Admits:
    """A provider that admits alice: pgwire hands it the startup credential as a basic token."""

    auth_scheme = "basic"

    @property
    def token_validators(self) -> dict[str, Any]:
        return {"basic": self._validate}

    async def _validate(self, token: str) -> Any:
        from provisa.auth.models import AuthIdentity

        return AuthIdentity(
            user_id="alice",
            email=None,
            display_name="alice",
            roles=["alice"],
            raw_claims={"sub": "alice"},
        )


@pytest.fixture
def seen_formats() -> list[Any]:
    return []


@pytest.fixture(autouse=True)
def served(seen_formats: list[Any]) -> Iterator[None]:
    state = MagicMock()
    state.contexts = {"alice": MagicMock(tables={})}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple", "default_role": "alice", "role_mapping": []}
    state.auth_middleware_active = True
    state.multitenancy = False
    state.admin_db = None

    async def pipeline(sql, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        seen_formats.append(wire_formats)
        return EngineResult(rows=list(_ROWS), column_names=["c"], column_types=["timestamptz"])

    async def describe(sql, role_id, *, as_of=None):
        from provisa.pgwire._pipeline import _Described

        return _Described([("c", "timestamptz")], None)

    with (
        patch("provisa.auth.wiring.build_auth_provider", return_value=_Admits()),
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", pipeline),
        patch("provisa.pgwire._pipeline.describe_pgwire_statement", describe),
    ):
        yield


def _connect(port: int, **startup: str) -> psycopg.Connection:
    return psycopg.connect(
        host="127.0.0.1",
        port=port,
        user="alice",
        password="secret",
        dbname="provisa",
        sslmode="disable",
        autocommit=True,
        **startup,
    )


def _read(connection: psycopg.Connection, sql: str, binary: bool = False) -> list[bytes | None]:
    fmt = pq.Format.BINARY if binary else pq.Format.TEXT
    result = connection.pgconn.exec_params(sql.encode(), [], result_format=fmt)
    assert result.status == pq.ExecStatus.TUPLES_OK, result.error_message
    return [result.get_value(i, 0) for i in range(result.ntuples)]


def _simple(connection: psycopg.Connection, sql: str) -> list[bytes | None]:
    result = connection.pgconn.exec_(sql.encode())
    assert result.status == pq.ExecStatus.TUPLES_OK, result.error_message
    return [result.get_value(i, 0) for i in range(result.ntuples)]


def _run(connection: psycopg.Connection, sql: str) -> None:
    connection.pgconn.exec_(sql.encode())


def _refused(connection: psycopg.Connection, sql: str) -> tuple[bytes | None, bytes | None]:
    result = connection.pgconn.exec_(sql.encode())
    assert result.status == pq.ExecStatus.FATAL_ERROR
    return (
        result.error_field(pq.DiagnosticField.SQLSTATE),
        result.error_field(pq.DiagnosticField.MESSAGE_PRIMARY),
    )


def _status(connection: psycopg.Connection) -> str | None:
    return connection.info.parameter_status("TimeZone")


@pytest.mark.parametrize("set_zone, shown, summer, winter", _ZONES)
def test_a_timestamptz_is_written_in_the_sessions_zone(port, set_zone, shown, summer, winter):
    with _connect(port) as connection:
        _run(connection, set_zone)
        for got in (_simple(connection, "SELECT c FROM t"), _read(connection, "SELECT c FROM t")):
            assert got[0] == summer.encode()
            if winter is not None:
                assert got[1] == winter.encode()


def test_now_is_written_in_the_sessions_zone(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'America/New_York'")
        assert _simple(connection, "SELECT now()")[0] == b"2026-07-01 08:00:00.5-04"


def test_a_zone_of_long_ago_writes_its_seconds_and_infinity_is_infinity(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'America/New_York'")
        rows = _simple(connection, "SELECT c FROM t")
    assert rows[2:] == [b"1799-12-31 19:03:58-04:56:02", b"infinity", None]


@pytest.mark.parametrize("set_zone", [zone[0] for zone in _ZONES])
def test_binary_is_the_instant_whatever_the_zone(port, set_zone):
    with _connect(port) as connection:
        before = _read(connection, "SELECT c FROM t", binary=True)
        _run(connection, set_zone)
        after = _read(connection, "SELECT c FROM t", binary=True)
    assert after == before
    assert [value.hex() for value in after[:3]] == list(_SENT.values())
    assert after[:3] == [_micros(_SUMMER), _micros(_WINTER), _micros(_OLD)]


def test_a_session_on_no_zone_of_its_own_writes_utc(port):
    with _connect(port) as connection:
        assert _simple(connection, "SELECT c FROM t")[0] == b"2026-07-01 12:00:00.5+00"


@pytest.mark.parametrize("set_zone, shown, _summer, _winter", _ZONES)
@pytest.mark.parametrize(
    "ask", ["SHOW TimeZone", "SHOW timezone", 'SHOW "TimeZone"', "SHOW TIME ZONE"]
)
def test_show_names_the_zone_as_postgresql_writes_it(port, set_zone, shown, _summer, _winter, ask):
    with _connect(port) as connection:
        _run(connection, set_zone)
        result = connection.pgconn.exec_(ask.encode())
    assert result.fname(0) == b"TimeZone"
    assert result.get_value(0, 0) == shown.encode()


@pytest.mark.parametrize("set_zone, shown, _summer, _winter", _ZONES)
def test_current_setting_names_the_zone(port, set_zone, shown, _summer, _winter):
    with _connect(port) as connection:
        _run(connection, set_zone)
        assert _simple(connection, "SELECT current_setting('TimeZone')") == [shown.encode()]


def test_a_session_that_set_nothing_shows_the_servers_zone(port):
    with _connect(port) as connection:
        assert _simple(connection, "SHOW TimeZone") == [b"UTC"]
        assert _status(connection) == "UTC"


@pytest.mark.parametrize(
    "startup, want",
    [
        ({"options": "-c timezone=asia/tokyo"}, "Asia/Tokyo"),
        ({"options": "-c TimeZone=America/New_York"}, "America/New_York"),
    ],
)
def test_startup_reports_the_zone_the_connection_named(port, startup, want):
    with _connect(port, **startup) as connection:
        assert _status(connection) == want
        assert _simple(connection, "SHOW TimeZone") == [want.encode()]
        got = _simple(connection, "SELECT c FROM t")[0]
    tz = "2026-07-01 21:00:00.5+09" if want == "Asia/Tokyo" else "2026-07-01 08:00:00.5-04"
    assert got == tz.encode()


def test_a_startup_zone_postgresql_does_not_know_ends_the_connection(port):
    with pytest.raises(psycopg.OperationalError, match='invalid value for parameter "TimeZone"'):
        _connect(port, options="-c timezone=Bad/One")


def test_a_set_reports_the_zone_as_postgresql_does(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'america/new_york'")
        assert _status(connection) == "America/New_York"
        _run(connection, "RESET TimeZone")
        assert _status(connection) == "UTC"


def test_set_local_is_reported_and_ends_with_the_block(port):
    with _connect(port) as connection:
        _run(connection, "BEGIN")
        _run(connection, "SET LOCAL TimeZone = 'Asia/Tokyo'")
        assert _status(connection) == "Asia/Tokyo"
        assert _simple(connection, "SHOW TimeZone") == [b"Asia/Tokyo"]
        _run(connection, "COMMIT")
        assert _status(connection) == "UTC"


def test_rollback_undoes_the_zone_and_what_is_reported(port):
    with _connect(port) as connection:
        _run(connection, "BEGIN")
        _run(connection, "SET TimeZone = 'Asia/Tokyo'")
        assert _status(connection) == "Asia/Tokyo"
        _run(connection, "ROLLBACK")
        assert _status(connection) == "UTC"
        assert _simple(connection, "SELECT c FROM t")[0] == b"2026-07-01 12:00:00.5+00"


@pytest.mark.parametrize("word", ["Not/AZone", "abc"])
def test_a_zone_postgresql_does_not_know_is_refused_and_changes_nothing(port, word):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'Asia/Tokyo'")
        state, message = _refused(connection, f"SET TimeZone = '{word}'")
        assert state == b"22023"
        assert message == f'invalid value for parameter "TimeZone": "{word}"'.encode()
        assert _simple(connection, "SHOW TimeZone") == [b"Asia/Tokyo"]
        assert _status(connection) == "Asia/Tokyo"


def test_a_zone_no_statement_names_is_refused_in_text_and_sent_in_binary(port):
    with _connect(port) as connection:
        _run(connection, "SELECT set_config('TimeZone', current_user, false)")
        assert _read(connection, "SELECT c FROM t", binary=True)[0] == _micros(_SUMMER)
        state, message = _refused(connection, "SELECT c FROM t")
        assert state == b"0A000"
        assert b"TimeZone" in (message or b"")


def test_a_posix_zone_with_a_daylight_rule_is_shown_and_refused_in_text(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = 'Foo5Bar'")
        assert _simple(connection, "SHOW TimeZone") == [b"FOO5BAR"]
        assert _read(connection, "SELECT c FROM t", binary=True)[0] == _micros(_SUMMER)
        assert _refused(connection, "SELECT c FROM t")[0] == b"0A000"


@pytest.mark.parametrize(
    "word, shown, written",
    [
        ("+2:00", b"+2:00", b"2026-07-01 10:00:00.5-02"),
        ("+02:00", b"+02:00", b"2026-07-01 10:00:00.5-02"),
        ("-02:00", b"-02:00", b"2026-07-01 14:00:00.5+02"),
        ("12:00", b"12:00", b"2026-07-01 00:00:00.5-12"),
        ("-07:08", b"-07:08", b"2026-07-01 19:08:00.5+07:08"),
    ],
)
def test_a_posix_zone_with_no_abbreviation_is_hours_west_of_utc(port, word, shown, written):
    """PostgreSQL allows an empty abbreviation in a POSIX zone (tzparse), and POSIX's offset is
    west, the opposite of a bare number of hours."""
    with _connect(port) as connection:
        _run(connection, f"SET TimeZone = '{word}'")
        assert _simple(connection, "SHOW TimeZone") == [shown]
        assert _status(connection) == shown.decode()
        assert _simple(connection, "SELECT c FROM t")[0] == written


def test_a_posix_zone_with_no_abbreviation_and_a_daylight_rule_is_shown_and_refused_in_text(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = '+2:00Foo'")
        assert _simple(connection, "SHOW TimeZone") == [b"+2:00FOO"]
        assert _refused(connection, "SELECT c FROM t")[0] == b"0A000"


def test_a_bare_number_of_hours_is_still_east_of_utc(port):
    with _connect(port) as connection:
        _run(connection, "SET TimeZone = '+2'")
        assert _simple(connection, "SHOW TimeZone") == [b"<+02>-02"]
        assert _simple(connection, "SELECT c FROM t")[0] == b"2026-07-01 14:00:00.5+02"


def test_a_session_leaves_a_posix_zone_for_utc_as_a_pool_reset_does(port):
    """psycopg's pool tests set '+2:00', commit, and reset to utc in a block of their own."""
    with _connect(port) as connection:
        _run(connection, "BEGIN")
        _run(connection, "SET TimeZone TO '+2:00'")
        _run(connection, "COMMIT")
        _run(connection, "BEGIN")
        _run(connection, "SET TimeZone TO utc")
        _run(connection, "COMMIT")
        assert _simple(connection, "SHOW TimeZone") == [b"UTC"]


def test_a_source_s_own_text_is_not_forwarded_to_a_session_on_another_zone(port, seen_formats):
    """A Postgres source wrote its rows' text in its own zone, and a cached copy of them is shared
    with every session: only a session on the server's zone may be handed those bytes."""
    with _connect(port) as connection:
        _read(connection, "SELECT c FROM t")
        on_utc = seen_formats[-1]
        _run(connection, "SET TimeZone = 'Asia/Tokyo'")
        _read(connection, "SELECT c FROM t")
        on_tokyo = seen_formats[-1]
        _read(connection, "SELECT c FROM t", binary=True)
        binary_on_tokyo = seen_formats[-1]
    assert on_utc == [0]
    assert on_tokyo is None
    assert binary_on_tokyo == [1]


def test_a_cursor_writes_its_rows_in_the_zone_in_force_when_fetched(port):
    with _connect(port) as connection:
        _run(connection, "BEGIN")
        _run(connection, "DECLARE c CURSOR FOR SELECT c FROM t")
        assert _simple(connection, "FETCH 1 FROM c") == [b"2026-07-01 12:00:00.5+00"]
        _run(connection, "SET TimeZone = 'America/New_York'")
        assert _simple(connection, "FETCH 1 FROM c") == [b"2026-01-01 07:00:00-05"]
        _run(connection, "COMMIT")


def test_set_config_answers_the_name_it_read_the_zone_as(port):
    with _connect(port) as connection:
        got = _simple(connection, "SELECT set_config('timezone', 'asia/tokyo', false)")
        assert got == [b"Asia/Tokyo"]
        assert _status(connection) == "Asia/Tokyo"
