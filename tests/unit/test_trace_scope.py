# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3e7a51-2d84-4b6f-a1c9-5e0f7d3b8a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Debug tracing is scoped and temporary (REQ-1910).

The operator turns debug tracing on for an org, a role or a source for a stated window, after which
it turns itself off; a single request may ask for a debug trace with a hint the operator permits
per role. What is pinned here: who a window covers, that it ends on its own, that an unpermitted
hint is refused naming the setting, that every hint syntax parses, and that the request path reads
a cached snapshot rather than the control plane.
"""

# Requirements: REQ-1910, REQ-030

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from provisa.compiler.directives import (
    cache_hint_for,
    cache_hint_from_grpc_metadata,
)
from provisa.core import trace_scope as ts
from provisa.core.database import Database, create_engine_from_url
from provisa.core.operator_floor import OperatorFloorError
from provisa.core.schema_admin import debug_trace_hint_roles, debug_trace_windows, metadata

_NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _window(scope: str, org_id: str, target: str | None = None, minutes: int = 15):
    return ts.DebugWindow(
        id=f"{scope}-{org_id}-{target}",
        scope=scope,
        org_id=org_id,
        target=target,
        started_at=_NOW,
        expires_at=_NOW + timedelta(minutes=minutes),
        created_by="ops@example.com",
    )


def _snapshot(*windows, hint_roles=()):
    return ts.TraceScopeSnapshot(windows=tuple(windows), hint_roles=frozenset(hint_roles))


def _debug(snapshot, *, org="acme", role="analyst", sources=(), hint=False, now=_NOW):
    return snapshot.debug_for(org_id=org, role_id=role, source_ids=sources, hint=hint, now=now)


# --- who a window covers ---------------------------------------------------------------------


def test_an_org_window_covers_that_org_and_no_other():
    snap = _snapshot(_window("org", "acme"))
    assert _debug(snap, org="acme") is True
    assert _debug(snap, org="globex") is False


def test_a_role_window_covers_that_role_in_that_org_only():
    snap = _snapshot(_window("role", "acme", "analyst"))
    assert _debug(snap, org="acme", role="analyst") is True
    assert _debug(snap, org="acme", role="steward") is False
    assert _debug(snap, org="globex", role="analyst") is False


def test_a_source_window_covers_a_request_that_reads_that_source():
    snap = _snapshot(_window("source", "acme", "sales_pg"))
    assert _debug(snap, sources=("sales_pg", "crm")) is True
    assert _debug(snap, sources=("crm",)) is False
    assert _debug(snap, sources=()) is False
    assert _debug(snap, org="globex", sources=("sales_pg",)) is False


def test_no_window_and_no_hint_is_normal_mode():
    assert _debug(_snapshot()) is False


# --- a window ends on its own ----------------------------------------------------------------


def test_a_window_stops_covering_requests_once_its_time_has_passed():
    snap = _snapshot(_window("org", "acme", minutes=15))
    assert _debug(snap, now=_NOW + timedelta(minutes=14, seconds=59)) is True
    assert _debug(snap, now=_NOW + timedelta(minutes=15)) is False
    assert _debug(snap, now=_NOW + timedelta(hours=3)) is False


# --- the per-request hint is permitted per role ----------------------------------------------


def test_a_hint_from_a_permitted_role_is_a_debug_request():
    snap = _snapshot(hint_roles=[("acme", "analyst")])
    assert _debug(snap, hint=True) is True


def test_a_hint_from_a_role_without_permission_is_rejected_naming_the_setting():
    snap = _snapshot(hint_roles=[("acme", "steward"), ("globex", "analyst")])
    with pytest.raises(ts.DebugTraceHintNotPermitted) as exc:
        _debug(snap, org="acme", role="analyst", hint=True)
    assert ts.HINT_SETTING in str(exc.value)
    assert "analyst" in str(exc.value)


def test_an_unpermitted_hint_is_rejected_even_inside_an_open_window():
    # The window already makes the request a debug one, but the hint is still a request the
    # operator did not permit — rejected, never silently absorbed.
    snap = _snapshot(_window("org", "acme"))
    with pytest.raises(ts.DebugTraceHintNotPermitted):
        _debug(snap, hint=True)


def test_the_rejection_is_an_operator_floor_error():
    # PermissionError subclass: 403 over HTTP, SQLSTATE 42501 on pgwire, PERMISSION_DENIED on
    # Flight and gRPC, through each transport's existing mapping.
    assert issubclass(ts.DebugTraceHintNotPermitted, OperatorFloorError)


# --- every hint syntax parses ----------------------------------------------------------------


def test_the_graphql_directive_is_the_hint():
    assert cache_hint_for("graphql", "query Q @debugTrace { orders { id } }").debug_trace is True
    assert cache_hint_for("graphql", "query Q { orders { id } }").debug_trace is False


def test_the_provisa_comment_is_the_hint_in_sql_graphql_and_cypher():
    assert cache_hint_for("sql", "-- @provisa trace=debug\nSELECT 1").debug_trace is True
    assert cache_hint_for("sql", "SELECT 1").debug_trace is False
    assert cache_hint_for("graphql", "# -- @provisa trace=debug\n{ orders { id } }").debug_trace
    assert cache_hint_for("cypher", "// @provisa trace=debug\nMATCH (n) RETURN n").debug_trace
    assert cache_hint_for("cypher", "MATCH (n) RETURN n").debug_trace is False


def test_the_hint_rides_beside_the_cache_hint_without_changing_it():
    hint = cache_hint_for("sql", "-- @provisa trace=debug cache_ttl=30\nSELECT 1")
    assert (hint.opt_in, hint.ttl, hint.debug_trace) == (True, 30, True)


def test_an_unknown_trace_level_fails_the_request():
    with pytest.raises(ValueError, match="trace"):
        cache_hint_for("sql", "-- @provisa trace=verbose\nSELECT 1")


def test_grpc_metadata_and_http_header_carry_the_hint():
    assert cache_hint_from_grpc_metadata([("x-provisa-trace", "debug")]).debug_trace is True
    assert cache_hint_from_grpc_metadata([("X-Provisa-Trace", "DEBUG")]).debug_trace is True
    assert cache_hint_from_grpc_metadata([("x-provisa-role", "analyst")]).debug_trace is False
    with pytest.raises(ValueError, match="x-provisa-trace"):
        cache_hint_from_grpc_metadata([("x-provisa-trace", "verbose")])


# --- the control-plane store, and the cached read on the request path ------------------------


def _store(path) -> Database:
    db = Database(create_engine_from_url(f"sqlite+pysqlite:///{path}"), name="trace-scope-test")
    with db.engine.begin() as conn:
        metadata.create_all(conn, tables=[debug_trace_windows, debug_trace_hint_roles])
    return db


@pytest.fixture(autouse=True)
def _fresh_cache():
    ts.invalidate()
    yield
    ts.invalidate()


@pytest.mark.asyncio
async def test_a_started_window_is_listed_and_stopping_it_removes_it(tmp_path):
    db = _store(tmp_path / "cp.db")
    window = await ts.start_window(
        db, scope="org", org_id="acme", target=None, minutes=15, created_by="ops@example.com"
    )
    assert window.expires_at - window.started_at == timedelta(minutes=15)
    assert [w.id for w in await ts.list_windows(db)] == [window.id]
    assert await ts.stop_window(db, window.id) is True
    assert await ts.list_windows(db) == []
    assert await ts.stop_window(db, window.id) is False


@pytest.mark.asyncio
async def test_a_window_needs_a_stated_bounded_duration_and_a_target_for_role_and_source(tmp_path):
    db = _store(tmp_path / "cp.db")
    for minutes in (0, -5, ts.MAX_WINDOW_MINUTES + 1):
        with pytest.raises(ValueError, match="minutes"):
            await ts.start_window(
                db, scope="org", org_id="acme", target=None, minutes=minutes, created_by=None
            )
    for scope in ("role", "source"):
        with pytest.raises(ValueError, match="target"):
            await ts.start_window(
                db, scope=scope, org_id="acme", target=None, minutes=5, created_by=None
            )
    with pytest.raises(ValueError, match="scope"):
        await ts.start_window(
            db, scope="table", org_id="acme", target="t", minutes=5, created_by=None
        )


@pytest.mark.asyncio
async def test_an_expired_window_is_not_listed(tmp_path):
    db = _store(tmp_path / "cp.db")
    await ts.start_window(
        db,
        scope="org",
        org_id="acme",
        target=None,
        minutes=15,
        created_by=None,
        now=_NOW - timedelta(hours=1),
    )
    assert await ts.list_windows(db, now=_NOW) == []


@pytest.mark.asyncio
async def test_hint_permission_is_set_and_cleared_per_role(tmp_path):
    db = _store(tmp_path / "cp.db")
    await ts.set_hint_permission(db, "acme", "analyst", True, updated_by="ops@example.com")
    await ts.set_hint_permission(db, "acme", "analyst", True, updated_by="ops@example.com")
    assert await ts.list_hint_roles(db) == [("acme", "analyst")]
    await ts.set_hint_permission(db, "acme", "analyst", False, updated_by="ops@example.com")
    assert await ts.list_hint_roles(db) == []


@pytest.mark.asyncio
async def test_two_instances_on_one_control_plane_share_a_window(tmp_path):
    # Two Database handles on one store stand for two app instances: the window written through
    # one is what the other resolves — the state is the control plane's, not a process's.
    instance_a = _store(tmp_path / "cp.db")
    instance_b = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"), name="instance-b"
    )
    await ts.start_window(
        instance_a, scope="org", org_id="acme", target=None, minutes=15, created_by=None
    )
    snap = await ts.load_snapshot(instance_b)
    now = datetime.now(timezone.utc)
    assert snap.debug_for(org_id="acme", role_id="r", source_ids=(), hint=False, now=now) is True
    assert snap.debug_for(org_id="globex", role_id="r", source_ids=(), hint=False, now=now) is False


@pytest.mark.asyncio
async def test_the_request_path_reads_a_cached_snapshot_not_the_control_plane(
    tmp_path, monkeypatch
):
    db = _store(tmp_path / "cp.db")
    loads = 0
    real_load = ts.load_snapshot

    async def _counting_load(database):
        nonlocal loads
        loads += 1
        return await real_load(database)

    monkeypatch.setattr(ts, "load_snapshot", _counting_load)
    clock = [1000.0]
    monkeypatch.setattr(ts, "_monotonic", lambda: clock[0])

    for _ in range(50):
        await ts.current_snapshot(db)
    assert loads == 1

    # Another instance's change is picked up once the short TTL has passed, with one more read.
    clock[0] += ts.SNAPSHOT_TTL_SECONDS + 0.1
    for _ in range(50):
        await ts.current_snapshot(db)
    assert loads == 2


@pytest.mark.asyncio
async def test_a_change_made_on_this_instance_is_visible_to_its_next_request(tmp_path):
    db = _store(tmp_path / "cp.db")
    now = datetime.now(timezone.utc)
    first = await ts.current_snapshot(db)
    assert first.debug_for(org_id="acme", role_id="r", source_ids=(), hint=False, now=now) is False
    await ts.start_window(db, scope="org", org_id="acme", target=None, minutes=5, created_by=None)
    second = await ts.current_snapshot(db)
    assert second.debug_for(org_id="acme", role_id="r", source_ids=(), hint=False, now=now) is True


@pytest.mark.asyncio
async def test_a_process_with_no_control_plane_has_no_window_and_permits_no_hint():
    """``state.admin_db is None`` is a server with no operator settings (tooling, a harness built
    around a stand-in state): its requests are normal detail and a hint is refused. A stand-in
    whose admin_db is anything else is read as a control plane."""
    from types import SimpleNamespace

    state = SimpleNamespace(admin_db=None, org_id="default")
    assert await ts.request_is_debug(state, "analyst", hint=False) is False
    with pytest.raises(ts.DebugTraceHintNotPermitted):
        await ts.request_is_debug(state, "analyst", hint=True)
