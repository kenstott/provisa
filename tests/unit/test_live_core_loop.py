# Copyright (c) 2026 Kenneth Stott
# Canary: c6fc96b8-d428-4484-8f51-1548dcfe5932
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Live-data core loop: REQ-960 (crash-safe commit ordering + idempotent land), REQ-959 (claim/
failover via the ownership CAS), REQ-957 (the preprocess hook contract).

Two processors + a crash are simulated with the real queue + store on sqlite (control-plane DB
separate from the store DB, as in production)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.handlers import make_source_land
from provisa.events.processor import (
    NodeContext,
    PreprocessError,
    SourceTableProcessor,
    TableProcessor,
)
from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        yield Database(engine, name="q")
    finally:
        await engine.dispose()


def _store_dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


class _Proc(TableProcessor):
    def __init__(self, *a, result=None, **k):
        super().__init__(*a, **k)
        self._result = result
        self.seen = None

    async def handle(self, pending, *, prior_hash, ctx=None):
        self.seen = pending
        return self._result


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _fan_to(conn, node: str, *, event_type: str = "append", source: str = "s.up") -> int:
    up = await queue.post_event(conn, source_table=source, event_type=event_type)
    await queue.fan_out(conn, up, [node])
    return up


# ============================ REQ-960: crash-safe commit ============================


@pytest.mark.asyncio
async def test_req960_post_before_complete_ordering(tmp_path, monkeypatch):
    """The one control-plane commit runs post → fan_out → complete in that order (post-before-
    complete): a crash after complete-before-post could lose the ripple; the invariant forbids it."""
    order: list[str] = []
    real_post, real_fan, real_complete = queue.post_event, queue.fan_out, queue.complete

    async def post(*a, **k):
        order.append("post")
        return await real_post(*a, **k)

    async def fan(*a, **k):
        order.append("fan_out")
        return await real_fan(*a, **k)

    async def complete(*a, **k):
        order.append("complete")
        return await real_complete(*a, **k)

    monkeypatch.setattr(queue, "post_event", post)
    monkeypatch.setattr(queue, "fan_out", fan)
    monkeypatch.setattr(queue, "complete", complete)

    async with _db(tmp_path) as db, db.acquire() as conn:
        await _fan_to(conn, "mv.a")
        proc = _Proc(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            result=("replace", {"rows": 1}, "h1"),
        )
        order.clear()  # drop the upstream fan_to's calls; record only the loop's commit sequence
        await proc.process_pending(conn)
    assert order == ["post", "fan_out", "complete"]


@pytest.mark.asyncio
async def test_req960_idempotent_append_land_converges(tmp_path):
    """A keyed append land is an upsert-by-key: a re-run / double-land after a mid-commit crash
    converges to one row per key instead of doubling (the store-side half of REQ-960)."""
    dsn = _store_dsn(tmp_path)
    batch = [{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}]
    for _ in range(3):  # three at-least-once attempts of the SAME landed delta
        await store_writer.land(
            dsn,
            schema="",
            table="orders",
            columns=_COLS,
            rows=batch,
            change_signal="ttl_probe",
            watermark_column="updated_at",
            pk_columns=["id"],
        )
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id, status FROM orders ORDER BY id")
    assert [(r[0], r[1]) for r in rows] == [(1, "new"), (2, "sold")]  # converged, not 6 rows


@pytest.mark.asyncio
async def test_req960_crash_between_land_and_commit_reruns_no_lost_ripple(tmp_path):
    """A crash after land but before the commit rolls back atomically (no ripple, claim outstanding);
    the re-run re-lands (idempotent) and commits the ripple — nothing lost."""
    crashed = {"n": 0}

    def deps(_node):
        if crashed["n"] == 0:
            crashed["n"] = 1
            raise RuntimeError("crash during fan_out")
        return ["down.x"]

    class _CrashProc(_Proc):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self.calls = 0

        async def handle(self, pending, *, prior_hash, ctx=None):
            self.calls += 1
            return await super().handle(pending, prior_hash=prior_hash, ctx=ctx)

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "mv.a")
        proc = _CrashProc(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=deps,
            db=db,
            name="box-1",
            result=("replace", {"rows": 1}, "h1"),
        )
        async with db.acquire() as conn:
            with pytest.raises(RuntimeError, match="crash"):
                await proc.process_pending(conn)
        async with db.acquire() as conn:
            assert proc.calls == 1
            assert [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"
            ] == []
            assert await queue.resume_claims(conn, dependent_table="mv.a", processor_name="box-1")
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None and proc.calls == 2
            now = _now()
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=now
            ) == [ev]


# ============================ REQ-959: claim / failover ============================


@pytest.mark.asyncio
async def test_req959_reassert_on_restart_matches_and_resumes(tmp_path):
    """A returning owner CASes its remembered claim — a match means resume (resume_claims returns
    the still-owned ids)."""
    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await _fan_to(conn, "mv.a")
        claimed = await queue.claim(
            conn, dependent_table="mv.a", processor_name="box-1", now=_now()
        )
        assert claimed == [up]
        # box-1 restarts and reasserts → the claim is still its own → resume.
        assert await queue.resume_claims(conn, dependent_table="mv.a", processor_name="box-1") == [
            up
        ]


@pytest.mark.asyncio
async def test_req959_reassert_drops_when_peer_took_over(tmp_path):
    """If a peer reclaimed the work while the owner was down, the returning owner's reassert matches
    zero rows → it drops; the peer now owns it."""
    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await _fan_to(conn, "mv.a")
        await queue.claim(conn, dependent_table="mv.a", processor_name="box-1", now=_now())
        # peer steals: reclaim (owner presumed gone) then re-claim under its own name.
        future = _now() + timedelta(days=1)
        await queue.reclaim(conn, now=future, heartbeat_cutoff=future)
        await queue.claim(conn, dependent_table="mv.a", processor_name="peer", now=future)
        assert await queue.resume_claims(conn, dependent_table="mv.a", processor_name="box-1") == []
        assert await queue.resume_claims(conn, dependent_table="mv.a", processor_name="peer") == [
            up
        ]


@pytest.mark.asyncio
async def test_req959_steal_on_deadline_reclaims_stuck_but_alive(tmp_path):
    """A stuck-but-alive owner (fresh heartbeat, deadline+grace passed, not completed) is reclaimable
    on the deadline — the failure a heartbeat structurally cannot catch."""
    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await _fan_to(conn, "mv.a")
        now = _now()
        await queue.claim(
            conn,
            dependent_table="mv.a",
            processor_name="box-1",
            now=now,
            deadline=now - timedelta(seconds=1),  # already past
        )
        # heartbeat is fresh (cutoff in the far past → not lapsed); only the deadline makes it
        # reclaimable → proves the deadline path, not the heartbeat path.
        reclaimed = await queue.reclaim(
            conn, now=now, heartbeat_cutoff=now - timedelta(days=1), grace_seconds=0.0
        )
        assert reclaimed == 1
        assert await queue.claim(conn, dependent_table="mv.a", processor_name="peer", now=now) == [
            up
        ]


@pytest.mark.asyncio
async def test_req959_superseded_owner_late_commit_fails_cas_applies_nothing(tmp_path):
    """A superseded owner's late commit fails the ownership CAS at complete and applies nothing —
    the loop returns a no-op, no ripple, baseline unset (the peer owns the work)."""

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            up = await _fan_to(conn, "mv.a")

        class _StolenProc(_Proc):
            async def handle(self, pending, *, prior_hash, ctx=None):
                async with db.acquire() as c2:  # a peer steals mid-handle (deadline passed)
                    future = _now() + timedelta(days=1)
                    await queue.reclaim(c2, now=future, heartbeat_cutoff=future)
                    await queue.claim(c2, dependent_table="mv.a", processor_name="peer", now=future)
                return await super().handle(pending, prior_hash=prior_hash, ctx=ctx)

        proc = _StolenProc(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            result=("replace", {"rows": 1}, "h1"),
        )
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None  # OwnershipLost → no-op
        async with db.acquire() as conn:
            assert [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"
            ] == []
            assert await queue.get_node_state(conn, "mv.a") is None
            assert await queue.resume_claims(
                conn, dependent_table="mv.a", processor_name="peer"
            ) == [up]


# ============================ REQ-957: preprocess hook ============================


def _bound_gate(fn, node):
    """Wrap a raw ``(streams, ctx) -> verdict`` hook into the bound evaluator the processor now
    receives (REQ-1165), mirroring ``make_rows_evaluator``: the source's fetched rows are the single
    ``{node: rows}`` input. ``None`` fn → no gate."""
    if fn is None:
        return None
    from provisa.mv.preflight import run_preflight

    async def _eval(rows, ctx, _fn=fn, _node=node):
        return await run_preflight(_fn, {_node: rows}, ctx)

    return _eval


def _src_proc(db, dsn, *, preprocess, node="s.orders", deps=None):
    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",  # replace shape → deterministic content hash
        watermark_column=None,
        pk_columns=["id"],
        fetch=lambda _p: _fetch_rows(),
    )
    return SourceTableProcessor(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: deps or ["down.x"],
        db=db,
        name="box-1",
        land=land,
        preprocess=_bound_gate(preprocess, node),
    )


_ROWS = [{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}]


async def _fetch_rows() -> list[dict]:
    return [dict(r) for r in _ROWS]


async def _events_for(conn, node: str) -> list[dict]:
    return [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == node]


@pytest.mark.asyncio
async def test_req957_absent_hook_is_identity(tmp_path):
    dsn = _store_dsn(tmp_path)
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=None)
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is not None
        async with store_writer.store_connection(dsn) as sc:
            rows = await sc.fetch("SELECT id FROM orders ORDER BY id")
        assert [r[0] for r in rows] == [1, 2]  # all rows landed unchanged


@pytest.mark.asyncio
async def test_req1165_quarantine_holds_without_land_or_poison(tmp_path):
    """REQ-1165: a QUARANTINE verdict holds — no land, a non-fanned ``quarantine`` event, and the
    claimed events are completed. Distinct from ABORT (which poisons dependents)."""
    dsn = _store_dsn(tmp_path)

    def hold(rows, ctx):
        return ctx.quarantine("held for review")

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=hold, deps=["down.x"])
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None  # the quarantine event id
            evs = await _events_for(conn, "s.orders")
            assert [e["event_type"] for e in evs] == ["quarantine"]  # held, no landed change
            assert evs[0]["payload"]["reason"] == "held for review"
            # the claimed upstream event is completed (not left to re-fire)
            assert await queue.peek_pending(conn, dependent_table="s.orders") == []
            # NOT fanned to dependents — a hold does not poison the DAG
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=_now()
            ) == []
        async with store_writer.store_connection(dsn) as sc:
            from sqlalchemy.exc import OperationalError

            try:
                rows = await sc.fetch("SELECT id FROM orders")
            except OperationalError:
                rows = []  # table never created → nothing landed, as required
        assert rows == []


@pytest.mark.asyncio
async def test_req957_warn_emits_advisory_and_still_lands(tmp_path):
    dsn = _store_dsn(tmp_path)

    def warn_then_pass(rows, ctx):
        ctx.warn(["late row", "coerced status"])
        return ctx.ok()

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=warn_then_pass)
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is not None  # still lands + re-posts
        async with db.acquire() as conn:
            evs = await _events_for(conn, "s.orders")
            kinds = [e["event_type"] for e in evs]
            assert "warn" in kinds and "replace" in kinds  # advisory + the landed change
            warn = next(e for e in evs if e["event_type"] == "warn")
            assert warn["payload"]["reasons"] == ["late row", "coerced status"]
        async with store_writer.store_connection(dsn) as sc:
            rows = await sc.fetch("SELECT id FROM orders")
        assert [r[0] for r in rows] == [1, 2]  # warn is non-fatal → rows landed


@pytest.mark.asyncio
async def test_req957_raise_emits_error_short_circuits_land_and_fans(tmp_path):
    dsn = _store_dsn(tmp_path)

    def blow_up(rows, ctx):
        raise ValueError("bad batch")

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=blow_up, deps=["down.x"])
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None  # the error event id (propagates in a drain)
        async with db.acquire() as conn:
            evs = await _events_for(conn, "s.orders")
            assert [e["event_type"] for e in evs] == ["error"]  # error, no landed change
            assert "bad batch" in str(evs[0]["payload"])
            # fanned the error forward to dependents (poison propagation)
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=_now()
            ) == [ev]
        async with store_writer.store_connection(dsn) as sc:
            from sqlalchemy.exc import OperationalError

            try:
                landed = await sc.fetch("SELECT id FROM orders")
            except OperationalError:
                landed = []
        assert landed == []  # land short-circuited


@pytest.mark.asyncio
async def test_req957_claimed_upstream_error_short_circuits_before_produce(tmp_path):
    """A claimed upstream error skips produce entirely (a built-in, not the hook): this node emits
    its own error and fans it forward — produce (fetch) never runs."""
    dsn = _store_dsn(tmp_path)
    fetched = {"n": 0}

    async def fetch(_p):
        fetched["n"] += 1
        return [dict(r) for r in _ROWS]

    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
    )
    proc = None
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            # an upstream node emitted an error, fanned to s.orders
            await _fan_to(conn, "s.orders", event_type="error", source="s.up")
        proc = SourceTableProcessor(
            "s.orders",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            land=land,
        )
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None
        assert fetched["n"] == 0  # produce never ran
        async with db.acquire() as conn:
            evs = await _events_for(conn, "s.orders")
            assert [e["event_type"] for e in evs] == ["error"]
            assert evs[0]["payload"]["poison"] is True
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=_now()
            ) == [ev]


@pytest.mark.asyncio
async def test_req957_ctx_carries_readonly_envelope(tmp_path):
    """ctx exposes node/kind/claimed/prior_hash/columns to the hook (read-only envelope)."""
    dsn = _store_dsn(tmp_path)
    seen: dict = {}

    def capture(rows, ctx: NodeContext):
        seen["node"] = ctx.node
        seen["kind"] = ctx.kind
        seen["claimed"] = list(ctx.claimed)
        seen["columns"] = ctx.columns
        return ctx.ok()

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=capture)
        async with db.acquire() as conn:
            await proc.process_pending(conn)
    assert seen["node"] == "s.orders" and seen["kind"] == "source"
    assert seen["columns"] == _COLS
    assert seen["claimed"] and seen["claimed"][0]["event_type"] == "append"


@pytest.mark.asyncio
async def test_req957_async_hook_and_preprocess_error_type(tmp_path):
    """The hook may be async; a PreprocessError raised directly is honored as the fatal outcome."""
    dsn = _store_dsn(tmp_path)

    async def async_reject(rows, ctx):
        raise PreprocessError("rejected async")

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await _fan_to(conn, "s.orders")
        proc = _src_proc(db, dsn, preprocess=async_reject)
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None
            evs = await _events_for(conn, "s.orders")
            assert [e["event_type"] for e in evs] == ["error"]
            assert "rejected async" in str(evs[0]["payload"])
