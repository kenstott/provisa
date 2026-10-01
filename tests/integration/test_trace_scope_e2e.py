# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c6b18-7d43-4a5e-b0c1-8e3a5d7f9b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1910): debug tracing is scoped and temporary.

Two real servers — two app instances, each serving its own org (DuckDB engine over a SQLite
source, own tenant store, embedded Redis, pgwire and Arrow Flight listening) — share ONE platform
control plane and each export OTLP to a receiver inside this process. Every assertion is on the
spans a request actually put on the wire:

- a debug-trace window opened through one instance puts the OTHER instance's org in debug detail
  (the window is the control plane's, not a process's), while the org the window does not name
  still exports one span per request;
- stopping the window, and the window running out on its own, each return the org to one span;
- the per-request hint is rejected naming the operator setting until the operator permits it for
  the role — over GraphQL (directive and header), pgwire and Arrow Flight — and gives that one
  request a debug trace once permitted.
"""

# Requirements: REQ-1910, REQ-030

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import yaml

from tests.otlp_receiver import OtlpReceiver, ReceivedSpan

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_REPO = Path(__file__).resolve().parents[2]
_ROLE = "org_admin"
_ORG_A = "trace_scope_a_e2e"
_ORG_B = "trace_scope_b_e2e"
_GQL = "query { ts__events(where: {id: {eq: 7}}) { id amount } }"
_GQL_HINTED = "query @debugTrace { ts__events(where: {id: {eq: 7}}) { id amount } }"
_SQL = "SELECT id, amount FROM trace_scope.events WHERE id = 7"
_SQL_HINTED = f"-- @provisa trace=debug\n{_SQL}"
_SETTING = "debug_trace_hint_roles"
# Attributes that carry statement text; normal detail records none of them.
_SQL_TEXT_ATTRS = (
    "db.statement",
    "provisa.query_text",
    "flight.sql",
    "flight.gql_query",
    "db.query.text",
)
# The request path's snapshot of the operator's settings is at most this stale (trace_scope).
_SNAPSHOT_TTL = 5.0


@dataclass
class _Instance:
    srv: object
    receiver: OtlpReceiver
    org_id: str

    @property
    def base_url(self) -> str:
        return self.srv.base_url  # type: ignore[attr-defined]

    @property
    def pgwire_port(self) -> int:
        return self.srv.pgwire_port  # type: ignore[attr-defined]

    @property
    def flight_port(self) -> int:
        return self.srv.flight_port  # type: ignore[attr-defined]


def _config(work: Path) -> Path:
    db = work / "events.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount TEXT)")
        con.executemany("INSERT INTO events VALUES (?, ?)", ((i, f"{i}.50") for i in range(1, 51)))
        con.commit()
    finally:
        con.close()
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg = {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [],
        "sources": [{"id": "ts-sqlite", "type": "sqlite", "path": str(db)}],
        "domains": [{"id": "trace-scope", "description": "debug-trace scope e2e"}],
        "tables": [
            {
                "source_id": "ts-sqlite",
                "domain_id": "trace-scope",
                "schema": "default",
                "table": "events",
                "columns": [
                    {"name": n, "data_type": t, "visible_to": [_ROLE]}
                    for n, t in (("id", "integer"), ("amount", "varchar"))
                ],
            }
        ],
    }
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _start(config: Path, org: str, platform_url: str) -> _Instance:
    """An isolated ``test``-instance server (own ports, own tenant store) on the SHARED platform
    control plane at ``platform_url``, exporting OTLP to a receiver in this process. The trace
    detail is left at the process default: any debug detail seen is the scope's doing."""
    from tests.integration.isolated_server import IsolatedServer
    from tests.port_lease import lease_port

    receiver = OtlpReceiver(port=lease_port())
    receiver.start()
    srv = IsolatedServer(
        org,
        engine="duckdb",
        enable_pgwire=True,
        await_flight=True,
        config=str(config),
        control_plane="sqlite",
        env={
            "PLATFORM_DATABASE_URL": platform_url,
            "OTEL_SDK_DISABLED": "false",
            "OTEL_EXPORTER_OTLP_ENDPOINT": receiver.endpoint,
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_SPAN_EXPORT_DELAY_MILLIS": "200",
        },
    )
    try:
        srv.start(timeout=240)
    except BaseException:
        receiver.stop()
        raise
    return _Instance(srv, receiver, org)


def _stop(instance: _Instance) -> None:
    instance.srv.stop_process()  # type: ignore[attr-defined]
    instance.receiver.stop()


@dataclass
class _Stack:
    a: _Instance
    b: _Instance
    platform_url: str


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    work = tmp_path_factory.mktemp("trace_scope")
    config = _config(work)
    platform_url = f"sqlite+pysqlite:///{work / 'platform.db'}"
    a = _start(config, _ORG_A, platform_url)
    try:
        b = _start(config, _ORG_B, platform_url)
    except BaseException:
        _stop(a)
        raise
    try:
        yield _Stack(a, b, platform_url)
    finally:
        _stop(b)
        _stop(a)


@pytest.fixture(autouse=True)
def _no_settings_left_behind(stack):
    """Each test starts and ends with no window open and no role permitted the hint."""
    _clear(stack)
    yield
    _clear(stack)


def _clear(stack: _Stack) -> None:
    for instance in (stack.a, stack.b):
        state = _admin(instance, "GET", "")
        for window in state["windows"]:
            _admin(instance, "DELETE", f"/windows/{window['id']}")
        for role in state["hint_roles"]:
            _admin(instance, "PUT", "/hint-roles", {**role, "permitted": False})
    # A change made through one instance reaches the other within the snapshot TTL.
    time.sleep(_SNAPSHOT_TTL + 0.5)


# -- the operator's admin API ----------------------------------------------------------------------


def _admin(instance: _Instance, method: str, path: str, body: dict | None = None) -> dict:
    resp = httpx.request(
        method, f"{instance.base_url}/admin/platform/debug-trace{path}", json=body, timeout=30
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# -- one request, and the spans it exported --------------------------------------------------------


def _graphql(instance: _Instance, query: str = _GQL, headers: dict | None = None) -> httpx.Response:
    return httpx.post(
        f"{instance.base_url}/data/graphql",
        json={"query": query, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE, **(headers or {})},
        timeout=120,
    )


def _graphql_ok(instance: _Instance, query: str = _GQL, headers: dict | None = None) -> None:
    resp = _graphql(instance, query, headers)
    assert resp.status_code == 200 and not resp.json().get("errors"), resp.text
    assert resp.json()["data"]["ts__events"] == [{"id": 7, "amount": "7.50"}], resp.text


def _pgwire(instance: _Instance, sql: str = _SQL) -> list:
    import psycopg

    conn = psycopg.connect(
        host="127.0.0.1",
        port=instance.pgwire_port,
        dbname="provisa",
        user=_ROLE,
        password="provisa",
        autocommit=True,
    )
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _flight(instance: _Instance, query: str) -> list:
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{instance.flight_port}")
    try:
        ticket = flight.Ticket(json.dumps({"query": query, "role": _ROLE}).encode())
        return client.do_get(ticket).read_all().column(0).to_pylist()
    finally:
        client.close()


def _spans_of(instance: _Instance, send) -> list[ReceivedSpan]:
    """Every span of every trace that began while ``send`` ran — one request's spans, since
    nothing else is sent to the instance meanwhile."""
    before = {s.trace_id for s in instance.receiver.settle()}
    t0 = time.time_ns()
    send()
    t1 = time.time_ns()
    by_trace: dict[str, list[ReceivedSpan]] = {}
    for s in instance.receiver.settle():
        by_trace.setdefault(s.trace_id, []).append(s)
    return [
        s
        for tid, group in by_trace.items()
        if tid not in before and t0 <= min(g.start_ns for g in group) <= t1
        for s in group
    ]


def _names(spans: list[ReceivedSpan]) -> list[str]:
    return sorted(f"{s.name} [{s.scope}]" for s in spans)


def _sql_text(spans: list[ReceivedSpan]) -> list[tuple[str, str]]:
    return [(s.name, k) for s in spans for k in _SQL_TEXT_ATTRS if k in s.attrs]


def _is_debug(spans: list[ReceivedSpan]) -> bool:
    """Debug detail records the statement text; normal detail records none."""
    return bool(_sql_text(spans))


def _assert_normal(spans: list[ReceivedSpan]) -> None:
    assert len(spans) == 1, f"{len(spans)} spans: {_names(spans)}"
    assert _sql_text(spans) == []


def _assert_debug(spans: list[ReceivedSpan], *, child_spans: bool = False) -> None:
    """The request was traced in debug detail: its statement text is recorded, and — for a
    request whose stages are spans (``child_spans``) — the request span is not the only one."""
    assert _sql_text(spans), f"no statement text on: {[(s.name, s.attrs) for s in spans]}"
    if child_spans:
        assert len(spans) > 1, f"{len(spans)} spans: {_names(spans)}"


def _graphql_spans(instance: _Instance) -> list[ReceivedSpan]:
    return _spans_of(instance, lambda: _graphql_ok(instance))


def _pgwire_spans(instance: _Instance) -> list[ReceivedSpan]:
    def _send():
        assert _pgwire(instance) == [(7, "7.50")]

    return _spans_of(instance, _send)


def _until_debug(instance: _Instance, timeout: float = 4 * _SNAPSHOT_TTL) -> list[ReceivedSpan]:
    """Send GraphQL requests until one is traced in debug detail — a window opened elsewhere
    reaches this instance within the snapshot TTL."""
    deadline = time.monotonic() + timeout
    while True:
        spans = _graphql_spans(instance)
        if _is_debug(spans):
            return spans
        assert time.monotonic() < deadline, f"never traced in debug detail: {_names(spans)}"
        time.sleep(0.5)


# -- windows ---------------------------------------------------------------------------------------


def test_with_no_window_each_org_exports_one_span_per_request(stack):
    _assert_normal(_graphql_spans(stack.a))
    _assert_normal(_pgwire_spans(stack.a))
    _assert_normal(_graphql_spans(stack.b))


def test_a_window_opened_through_one_instance_covers_its_org_on_another_instance(stack):
    # Opened through instance B, for org A, which instance A serves.
    opened = _admin(stack.b, "POST", "/windows", {"scope": "org", "org_id": _ORG_A, "minutes": 15})
    (window,) = opened["windows"]
    assert (window["scope"], window["org_id"]) == ("org", _ORG_A)
    assert 0 < window["remaining_seconds"] <= 15 * 60

    _assert_debug(_until_debug(stack.a), child_spans=True)
    # The window is resolved in middleware, before the request body is read, so the ASGI receive
    # span — opened by that read, ahead of the endpoint — is part of the debug trace too.
    under_window = _names(_graphql_spans(stack.a))
    assert any("http receive" in n for n in under_window), under_window
    _assert_debug(_pgwire_spans(stack.a))
    # Both instances list the one window; the org it does not name is still in normal detail.
    assert [w["id"] for w in _admin(stack.a, "GET", "")["windows"]] == [window["id"]]
    _assert_normal(_graphql_spans(stack.b))
    _assert_normal(_pgwire_spans(stack.b))


def test_stopping_a_window_returns_the_org_to_one_span(stack):
    opened = _admin(stack.a, "POST", "/windows", {"scope": "org", "org_id": _ORG_A, "minutes": 15})
    _assert_debug(
        _graphql_spans(stack.a), child_spans=True
    )  # the instance that made the change sees it at once
    stopped = _admin(stack.a, "DELETE", f"/windows/{opened['windows'][0]['id']}")
    assert stopped["windows"] == []
    _assert_normal(_graphql_spans(stack.a))
    _assert_normal(_pgwire_spans(stack.a))


def test_a_role_window_and_a_source_window_cover_their_requests(stack):
    _admin(
        stack.a,
        "POST",
        "/windows",
        {"scope": "role", "org_id": _ORG_A, "target": "analyst", "minutes": 15},
    )
    _assert_normal(_graphql_spans(stack.a))  # the request's role is org_admin, not analyst
    _admin(
        stack.a,
        "POST",
        "/windows",
        {"scope": "source", "org_id": _ORG_A, "target": "ts-sqlite", "minutes": 15},
    )
    _assert_debug(_graphql_spans(stack.a), child_spans=True)
    _assert_debug(_pgwire_spans(stack.a))


def test_a_window_ends_on_its_own(stack):
    """A window with 20 seconds left, written straight to the shared control plane: instance A
    picks it up, and once its time has passed A is back to one span with nobody stopping it."""
    from provisa.core import trace_scope
    from provisa.core.database import Database, create_engine_from_url

    control_plane = Database(create_engine_from_url(stack.platform_url), name="trace-scope-e2e")
    started = datetime.now(timezone.utc) - timedelta(seconds=40)
    window = asyncio.run(
        trace_scope.start_window(
            control_plane,
            scope="org",
            org_id=_ORG_A,
            target=None,
            minutes=1,
            created_by="e2e",
            now=started,
        )
    )
    _until_debug(stack.a)
    remaining = (window.expires_at - datetime.now(timezone.utc)).total_seconds()
    assert remaining > 0, "the window ran out before it was observed; the stack is too slow"
    time.sleep(remaining + 1)
    _assert_normal(_graphql_spans(stack.a))
    _assert_normal(_pgwire_spans(stack.a))
    assert _admin(stack.a, "GET", "")["windows"] == []


# -- the per-request hint --------------------------------------------------------------------------


def test_a_hint_from_a_role_without_permission_is_rejected_on_every_transport(stack):
    import psycopg
    import pyarrow.flight as flight

    for resp in (
        _graphql(stack.a, _GQL_HINTED),
        _graphql(stack.a, _GQL, {"X-Provisa-Trace": "debug"}),
    ):
        assert resp.status_code == 403, resp.text
        assert resp.json()["code"] == "query.operator_floor", resp.text
        assert _SETTING in resp.json()["detail"], resp.text

    with pytest.raises(psycopg.errors.InsufficientPrivilege) as pg_exc:
        _pgwire(stack.a, _SQL_HINTED)
    assert pg_exc.value.sqlstate == "42501"
    assert _SETTING in str(pg_exc.value)

    with pytest.raises(flight.FlightError) as flight_exc:
        _flight(stack.a, _GQL_HINTED)
    assert _SETTING in str(flight_exc.value)

    # The same requests without the hint are served.
    _graphql_ok(stack.a)
    assert _pgwire(stack.a) == [(7, "7.50")]
    assert _flight(stack.a, _GQL) == [7]


def test_a_permitted_hint_gives_that_request_a_debug_trace_and_no_other(stack):
    state = _admin(
        stack.a, "PUT", "/hint-roles", {"org_id": _ORG_A, "role_id": _ROLE, "permitted": True}
    )
    assert state["hint_roles"] == [{"org_id": _ORG_A, "role_id": _ROLE}]

    _assert_debug(_spans_of(stack.a, lambda: _graphql_ok(stack.a, _GQL_HINTED)), child_spans=True)
    _assert_debug(
        _spans_of(stack.a, lambda: _graphql_ok(stack.a, _GQL, {"X-Provisa-Trace": "debug"}))
    )

    def _hinted_pgwire():
        assert _pgwire(stack.a, _SQL_HINTED) == [(7, "7.50")]

    _assert_debug(_spans_of(stack.a, _hinted_pgwire))

    def _hinted_flight():
        assert _flight(stack.a, _GQL_HINTED) == [7]

    _assert_debug(_spans_of(stack.a, _hinted_flight))

    # The role's requests without the hint stay in normal detail.
    _assert_normal(_graphql_spans(stack.a))
    _assert_normal(_pgwire_spans(stack.a))


def test_the_permission_is_the_orgs_not_the_role_names(stack):
    # org_admin is permitted in org A only; the same role name in org B is still rejected.
    _admin(stack.a, "PUT", "/hint-roles", {"org_id": _ORG_A, "role_id": _ROLE, "permitted": True})
    time.sleep(_SNAPSHOT_TTL + 0.5)
    resp = _graphql(stack.b, _GQL_HINTED)
    assert resp.status_code == 403, resp.text
    assert _SETTING in resp.json()["detail"], resp.text
