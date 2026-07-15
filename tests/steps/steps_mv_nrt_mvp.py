# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD steps for the NRT materialized-view lifecycle (MVP) — real event-loop code over a SQLite
control plane. Covers REQ-964 (determinism guard), REQ-963 (debounce), REQ-960 (crash-safety),
REQ-959 (ownership CAS).

Steps are synchronous (pytest-bdd does not await coroutine steps); async work runs via
``asyncio.run`` against the control-plane SQLite FILE, which carries state across steps. The
processor's ``process_pending(conn)`` takes its connection as an argument, so a fresh per-step
Database on the same file is equivalent to a long-lived one.
"""

from __future__ import annotations

import asyncio
import types
from datetime import datetime, timedelta, timezone

import pytest
from pytest_bdd import given, scenarios, then, when
from sqlalchemy.ext.asyncio import create_async_engine

import provisa.events.processor as processor_mod
from provisa.api.admin import schema_common
from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.processor import TableProcessor

scenarios("../features/mv_nrt_mvp.feature")

_T0 = datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def ctx(tmp_path) -> dict:
    path = str(tmp_path / "cp.db")

    async def _init():
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        async with engine.begin() as c:
            await c.run_sync(
                lambda s: events.metadata.create_all(
                    s, tables=[events, event_status, node_freshness_state]
                )
            )
        await engine.dispose()

    asyncio.run(_init())
    return {"cp_path": path}


def _run(ctx, coro_fn):
    """Open a fresh control-plane Database on the shared file, run coro_fn(conn), dispose."""

    async def _():
        engine = create_async_engine(f"sqlite+aiosqlite:///{ctx['cp_path']}")
        try:
            async with Database(engine, name="cp").acquire() as conn:
                return await coro_fn(conn)
        finally:
            await engine.dispose()

    return asyncio.run(_())


class _Proc(TableProcessor):
    def __init__(self, *a, result, **k):
        super().__init__(*a, **k)
        self._result = result
        self.calls = 0
        self.seen = None

    async def handle(self, pending, *, prior_hash, ctx=None):
        self.calls += 1
        self.seen = pending
        return self._result


def _make_proc(*, result, quiet=0.0, max_delay=None, deps=None, node="mv.live", name="box-1"):
    return _Proc(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=deps or (lambda n: ["down.x"]),
        db=None,
        name=name,
        result=result,
        debounce_quiet=quiet,
        debounce_max_delay=max_delay,
    )


# --- determinism guard (REQ-964) -------------------------------------------


@given("a materialized view whose SQL calls now()")
def _mv_volatile(ctx):
    ctx["sql"] = "SELECT now() AS ts, region FROM orders"


@given('a materialized view "SELECT region, sum(amt) AS t FROM orders GROUP BY region"')
def _mv_ok(ctx):
    ctx["sql"] = "SELECT region, sum(amt) AS t FROM orders GROUP BY region"


@when("the view is registered")
def _register(ctx, monkeypatch):
    registered: list = []
    state = types.SimpleNamespace(
        mv_registry=types.SimpleNamespace(
            get=lambda _: None, register=lambda mv: registered.append(mv), unregister=lambda _: None
        ),
        org_id="t",
        engine=None,
        # _sync_view_mv resolves the MV's materialize target from the bound engine (REQ: never a
        # hardcoded catalog). Stub the (catalog, schema) the store would expose.
        federation_engine=types.SimpleNamespace(
            materialize_store_target=lambda _org: ("mat_store", "public")
        ),
    )
    mod = types.ModuleType("provisa.api.app")
    mod.state = state  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "provisa.api.app", mod)
    ctx["registered"] = registered
    try:
        schema_common._sync_view_mv("v", ctx["sql"], 300)
        ctx["error"] = None
    except ValueError as e:
        ctx["error"] = str(e)


@then("registration is rejected as non-deterministic")
def _rejected(ctx):
    assert ctx["error"] is not None and "non-deterministic" in ctx["error"]


@then("the view is not added to the MV registry")
def _not_registered(ctx):
    assert ctx["registered"] == []


@then("the view is added to the MV registry")
def _registered_ok(ctx):
    assert ctx["error"] is None and len(ctx["registered"]) == 1


# --- debounce (REQ-963) -----------------------------------------------------


@given("a live MV with a debounce quiet window of 100 seconds")
def _live_mv(ctx):
    ctx["proc"] = _make_proc(result=("replace", {"n": 3}, "h1"), quiet=100, max_delay=300)


@given("three upstream changes have fanned in")
def _three_changes(ctx):
    async def _do(conn):
        for _ in range(3):
            e = await queue.post_event(conn, source_table="up", event_type="replace")
            await queue.fan_out(conn, e, ["mv.live"])

    _run(ctx, _do)


@when("the loop ticks before the quiet window elapses")
def _tick_early(ctx):
    ctx["early"] = _run(ctx, lambda conn: ctx["proc"].process_pending(conn))


@then("the MV does not recompute and the changes stay pending")
def _deferred(ctx):
    assert ctx["early"] is None and ctx["proc"].calls == 0
    pending = _run(ctx, lambda conn: queue.peek_pending(conn, dependent_table="mv.live"))
    assert len(pending) == 3


@when("the debounce deadline has passed")
def _advance_clock(ctx, monkeypatch):
    monkeypatch.setattr(
        processor_mod, "_now", lambda: datetime.now(timezone.utc) + timedelta(seconds=400)
    )


@then("the MV recomputes exactly once over all three changes")
def _fires_once(ctx):
    fired = _run(ctx, lambda conn: ctx["proc"].process_pending(conn))
    assert fired is not None and ctx["proc"].calls == 1
    assert ctx["proc"].seen is not None and len(ctx["proc"].seen) == 3


# --- crash-safety (REQ-960) -------------------------------------------------


@given("a source node whose fan-out crashes after landing")
def _crash_node(ctx):
    crashed = {"n": 0}

    def deps(_node):
        if crashed["n"] == 0:
            crashed["n"] = 1
            raise RuntimeError("simulated crash during fan_out")
        return ["down.x"]

    ctx["proc"] = _make_proc(result=("replace", {"rows": 1}, "h1"), deps=deps, node="mv.a")


@when("the node processes its pending work and crashes")
def _process_crash(ctx):
    async def _seed(conn):
        e = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, e, ["mv.a"])

    _run(ctx, _seed)
    with pytest.raises(RuntimeError, match="simulated crash"):
        _run(ctx, lambda conn: ctx["proc"].process_pending(conn))


@then("the landed data is preserved but no downstream ripple is committed")
def _no_ripple(ctx):
    assert ctx["proc"].calls == 1  # handle (land) ran

    async def _check(conn):
        state = await queue.get_node_state(conn, "mv.a")
        ripple = [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"]
        return state, ripple

    state, ripple = _run(ctx, _check)
    assert state is None and ripple == []


@then("the claim is still owned by the processor")
def _still_owned(ctx):
    owned = _run(
        ctx, lambda conn: queue.resume_claims(conn, dependent_table="mv.a", processor_name="box-1")
    )
    assert owned


@when("the processor re-runs")
def _rerun(ctx):
    ctx["recovered"] = _run(ctx, lambda conn: ctx["proc"].process_pending(conn))


@then("the land is idempotent and the downstream ripple commits exactly once")
def _recovered(ctx):
    assert ctx["recovered"] is not None and ctx["proc"].calls == 2

    async def _check(conn):
        state = await queue.get_node_state(conn, "mv.a")
        posted = [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"]
        return state, posted

    state, posted = _run(ctx, _check)
    assert state is not None and state["content_hash"] == "h1"
    assert len(posted) == 1


# --- ownership CAS (REQ-959) ------------------------------------------------


@given("a claim owned by one processor that a peer reclaims and takes over")
def _peer_takeover(ctx):
    async def _do(conn):
        eid = await queue.post_event(conn, source_table="s.o", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.x"])
        await queue.claim(conn, dependent_table="mv.x", processor_name="box-1", now=_T0)
        await queue.reclaim(
            conn, now=_T0 + timedelta(minutes=5), heartbeat_cutoff=_T0 + timedelta(minutes=5)
        )
        await queue.claim(
            conn, dependent_table="mv.x", processor_name="box-2", now=_T0 + timedelta(minutes=6)
        )
        return eid

    ctx["eid"] = _run(ctx, _do)


@when("the stale owner tries to complete the claim")
def _stale_complete(ctx):
    ctx["stale_ok"] = _run(
        ctx,
        lambda conn: queue.complete(
            conn, event_id=ctx["eid"], dependent_table="mv.x", processor_name="box-1", now=_T0
        ),
    )


@then("its ownership CAS fails")
def _cas_fails(ctx):
    assert ctx["stale_ok"] is False


@then("the new owner can complete the claim")
def _new_owner_ok(ctx):
    ok = _run(
        ctx,
        lambda conn: queue.complete(
            conn, event_id=ctx["eid"], dependent_table="mv.x", processor_name="box-2", now=_T0
        ),
    )
    assert ok is True
