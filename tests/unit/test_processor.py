# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: TableProcessor — the claim→handle→complete→re-post loop, kafka pattern, and variants."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.processor import MVTableProcessor, SourceTableProcessor, TableProcessor


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


class _Proc(TableProcessor):
    """A test processor with a fixed handle result."""

    def __init__(self, *a, result, **k):
        super().__init__(*a, **k)
        self._result = result
        self.seen = None
        self.seen_prior_hash = "<unset>"

    async def handle(self, pending, *, prior_hash):
        self.seen = pending
        self.seen_prior_hash = prior_hash
        return self._result


def _proc(db, *, result, node="mv.a", deps=None):
    return _Proc(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: deps or ["down.x"],
        db=db,
        name="box-1",
        result=result,
    )


@pytest.mark.asyncio
async def test_process_pending_claims_handles_reposts(tmp_path):
    async with _db(tmp_path) as db, db.acquire() as conn:
        # upstream change fanned out to our node → pending work
        up = await queue.post_event(conn, source_table="s.orders", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])

        proc = _proc(
            db, result=("replace", {"rows": 3}, "h1"), node="mv.a", deps=["down.x", "down.y"]
        )
        my_event = await proc.process_pending(conn)

        assert proc.seen_prior_hash is None  # first land: no prior baseline
        assert proc.seen and proc.seen[0]["id"] == up  # handle saw the claimed event
        # persisted the returned content hash as the new baseline
        _st = await queue.get_node_state(conn, "mv.a")
        assert _st is not None and _st["content_hash"] == "h1"
        # re-posted the node's OWN change event (replace) for its dependents
        posted = [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"]
        assert (
            len(posted) == 1
            and posted[0]["event_type"] == "replace"
            and posted[0]["id"] == my_event
        )
        # fanned out to both downstream dependents (claimable)
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        assert await queue.claim(conn, dependent_table="down.x", processor_name="p", now=now) == [
            my_event
        ]
        assert await queue.claim(conn, dependent_table="down.y", processor_name="p", now=now) == [
            my_event
        ]


@pytest.mark.asyncio
async def test_nothing_pending_is_noop(tmp_path):
    async with _db(tmp_path) as db, db.acquire() as conn:
        assert await _proc(db, result=("replace", {}, None)).process_pending(conn) is None


@pytest.mark.asyncio
async def test_unchanged_handle_does_not_repost(tmp_path):
    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])
        proc = _proc(db, result=None)  # handle says unchanged
        assert await proc.process_pending(conn) is None
        # no new mv.a event posted (token-gate: no downstream ripple)
        assert [
            r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"
        ] == []


@pytest.mark.asyncio
async def test_consume_kafka_posts_delta_per_message(tmp_path):
    async with _db(tmp_path) as db:

        async def consumer():
            yield {"op": "u", "id": 1}
            yield {"op": "d", "id": 2}

        proc = SourceTableProcessor(
            "s.cdc",
            change_signal="kafka",
            watermark_column=None,
            dependents_of=lambda n: ["mv.a"],
            db=db,
            name="box-1",
            land=None,
        )
        await proc.consume_kafka(consumer())
        async with db.acquire() as conn:
            rows = await queue.read_since(conn, cursor=0)
        assert [r["event_type"] for r in rows] == ["delta", "delta"]  # each message → a delta event
        assert rows[0]["payload"] == {"op": "u", "id": 1}


@pytest.mark.asyncio
async def test_variants_delegate_handle(tmp_path):
    async with _db(tmp_path) as db:

        async def land(pending, *, prior_hash):
            return ("append", {"n": len(pending)}, None)

        async def generate(pending, *, prior_hash):
            return ("replace", {"g": 1}, "mvhash")

        src = SourceTableProcessor(
            "s",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="b",
            land=land,
        )
        mv = MVTableProcessor(
            "m",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="b",
            generate=generate,
        )
        assert await src.handle([{"x": 1}], prior_hash=None) == ("append", {"n": 1}, None)
        assert await mv.handle([], prior_hash=None) == ("replace", {"g": 1}, "mvhash")


class _CrashProc(_Proc):
    """Processor whose fan-out raises on the first attempt, then succeeds — models a
    crash between land and the control-plane commit (REQ-960)."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.handle_calls = 0

    async def handle(self, pending, *, prior_hash):
        self.handle_calls += 1
        return await super().handle(pending, prior_hash=prior_hash)


@pytest.mark.asyncio
async def test_req960_crash_between_land_and_commit_loses_nothing(tmp_path):
    """A crash after land but before the post+fan_out+complete commit must roll back
    atomically (no downstream ripple, claim not completed), and a re-run must recover."""
    from datetime import datetime, timezone

    crashed = {"n": 0}

    def deps(_node):
        # Raise on the first fan-out (inside the transaction, after land) → crash.
        if crashed["n"] == 0:
            crashed["n"] = 1
            raise RuntimeError("simulated crash during fan_out")
        return ["down.x"]

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            up = await queue.post_event(conn, source_table="s.o", event_type="append")
            await queue.fan_out(conn, up, ["mv.a"])

        proc = _CrashProc(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=deps,
            db=db,
            name="box-1",
            result=("replace", {"rows": 3}, "h1"),
        )

        # 1) Crash run: fan_out raises → transaction rolls back.
        async with db.acquire() as conn:
            with pytest.raises(RuntimeError, match="simulated crash"):
                await proc.process_pending(conn)

        async with db.acquire() as conn:
            # Land happened (handle ran), but NOTHING downstream committed.
            assert proc.handle_calls == 1
            assert await queue.get_node_state(conn, "mv.a") is None  # baseline not set
            assert [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"
            ] == []  # no ripple
            # The claim is still outstanding and owned by box-1 → resumable via reassert (REQ-959).
            assert await queue.resume_claims(conn, dependent_table="mv.a", processor_name="box-1")

        # 2) Recovery run: re-claim + re-run → idempotent land, ripple now committed.
        async with db.acquire() as conn:
            my_event = await proc.process_pending(conn)
            assert my_event is not None
            assert proc.handle_calls == 2  # land re-ran (idempotent)
            _st = await queue.get_node_state(conn, "mv.a")
            assert _st is not None and _st["content_hash"] == "h1"
            now = datetime.now(timezone.utc)
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=now
            ) == [my_event]


@pytest.mark.asyncio
async def test_req959_ownership_cas_aborts_ripple_on_peer_takeover(tmp_path):
    """If a peer reclaims this node's work while it is mid-handle, the ownership CAS at complete fails
    and the whole commit rolls back — no ripple, no partial completion (REQ-959)."""
    from datetime import datetime, timedelta, timezone

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            up = await queue.post_event(conn, source_table="s.o", event_type="append")
            await queue.fan_out(conn, up, ["mv.a"])

        # A handle that, DURING its run (after land), lets a peer reclaim + re-own the work —
        # models a stuck-but-alive owner whose deadline passed mid-compute.
        class _StolenProc(_Proc):
            async def handle(self, pending, *, prior_hash):
                async with db.acquire() as c2:
                    future = datetime.now(timezone.utc) + timedelta(days=1)
                    await queue.reclaim(c2, now=future, heartbeat_cutoff=future)
                    await queue.claim(c2, dependent_table="mv.a", processor_name="peer", now=future)
                return await super().handle(pending, prior_hash=prior_hash)

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
            # No ripple committed, baseline not set — the peer now owns the work.
            assert [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.a"
            ] == []
            assert await queue.get_node_state(conn, "mv.a") is None
            assert await queue.resume_claims(
                conn, dependent_table="mv.a", processor_name="peer"
            ) == [up]


# --- REQ-963 debounce ------------------------------------------------------


def _debounce_proc(db, *, quiet, max_delay, result, deps=None):
    return _Proc(
        "mv.live",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: deps or ["down.x"],
        db=db,
        name="box-1",
        result=result,
        debounce_quiet=quiet,
        debounce_max_delay=max_delay,
    )


def test_debounce_deadline_math():
    """min(last_change + quiet, first_change + max_delay); quiet<=0 → None (fire immediately)."""
    from datetime import datetime, timedelta, timezone

    t0 = datetime(2026, 7, 8, tzinfo=timezone.utc)
    p = _debounce_proc(None, quiet=10, max_delay=None, result=None)
    # no debounce
    p0 = _debounce_proc(None, quiet=0, max_delay=5, result=None)
    assert p0._debounce_deadline([{"created_at": t0}]) is None
    # trailing-edge: last + quiet
    peeked = [{"created_at": t0}, {"created_at": t0 + timedelta(seconds=2)}]
    assert p._debounce_deadline(peeked) == t0 + timedelta(seconds=12)
    # max_delay cap wins under a long quiet tail
    pc = _debounce_proc(None, quiet=10, max_delay=5, result=None)
    peeked2 = [{"created_at": t0}, {"created_at": t0 + timedelta(seconds=8)}]
    assert pc._debounce_deadline(peeked2) == t0 + timedelta(seconds=5)  # first + max_delay


@pytest.mark.asyncio
async def test_debounce_collapses_burst_into_one_recompute(tmp_path, monkeypatch):
    """A burst of fan-ins is deferred until the debounce deadline, then fires ONCE coalescing all of
    them into a single recompute (REQ-963)."""
    from datetime import timedelta

    import provisa.events.processor as processor_mod

    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            # three rapid upstream changes fan into the live MV
            ids = []
            for _ in range(3):
                e = await queue.post_event(conn, source_table="s.o", event_type="append")
                await queue.fan_out(conn, e, ["mv.live"])
                ids.append(e)

        proc = _debounce_proc(db, quiet=100, max_delay=300, result=("replace", {"n": 3}, "h1"))

        # 1) Before the quiet window elapses → defer (no claim, no recompute).
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            assert proc.seen is None  # handle never ran
            # work is still unclaimed (peekable), not orphaned
            assert len(await queue.peek_pending(conn, dependent_table="mv.live")) == 3

        # 2) Advance the clock past the deadline → fire ONCE, coalescing all three events.
        fire_at = processor_mod._now() + timedelta(seconds=400)
        monkeypatch.setattr(processor_mod, "_now", lambda: fire_at)
        async with db.acquire() as conn:
            my_event = await proc.process_pending(conn)
            assert my_event is not None
            assert proc.seen is not None and len(proc.seen) == 3  # one recompute over all 3
            posted = [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "mv.live"
            ]
            assert len(posted) == 1  # a single downstream ripple, not three
