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
        assert (await queue.get_node_state(conn, "mv.a"))["content_hash"] == "h1"
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
            # The claim is still outstanding (not completed) → recoverable.
            reclaimed = await queue.reclaim_stale(
                conn, older_than=datetime.now(timezone.utc).replace(year=2099)
            )
            assert reclaimed  # the orphaned claim is reclaimable

        # 2) Recovery run: re-claim + re-run → idempotent land, ripple now committed.
        async with db.acquire() as conn:
            my_event = await proc.process_pending(conn)
            assert my_event is not None
            assert proc.handle_calls == 2  # land re-ran (idempotent)
            assert (await queue.get_node_state(conn, "mv.a"))["content_hash"] == "h1"
            now = datetime.now(timezone.utc)
            assert await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=now
            ) == [my_event]
