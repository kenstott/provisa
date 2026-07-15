# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-968 (forced regen / replay — source / node / window scope, gate bypass), REQ-983 (preserved
snapshots — declared + why-tagged, sealed + immutable), and REQ-969 (incremental maintenance)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state, preserved_snapshots
from provisa.events import injector, queue, snapshots, supervisor
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.handlers import make_mv_generate, make_mv_incremental, make_source_land
from provisa.events.processor import MVTableProcessor, NodeContext, SourceTableProcessor
from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


def _store(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


async def _rows(dsn, table):
    async with store_writer.store_connection(dsn) as conn:
        return await conn.fetch(f"SELECT id, status FROM {table} ORDER BY id")


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state, preserved_snapshots]
            )
        )
    try:
        yield Database(engine, name="q")
    finally:
        await engine.dispose()


class _CapMV(MVTableProcessor):
    """An MV processor that captures the NodeContext handed to each fire (forced flag, window)."""

    last_ctx: NodeContext | None = None

    async def handle(self, pending, *, prior_hash, ctx=None):
        self.last_ctx = ctx
        return await super().handle(pending, prior_hash=prior_hash, ctx=ctx)


class _CapSource(SourceTableProcessor):
    """A source processor that captures the NodeContext handed to each land (forced flag)."""

    last_ctx: NodeContext | None = None

    async def handle(self, pending, *, prior_hash, ctx=None):
        self.last_ctx = ctx
        return await super().handle(pending, prior_hash=prior_hash, ctx=ctx)


async def _mv_events(conn, node):
    return [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == node]


# ---------------------------------------------------------------------------
# REQ-968: forced regen bypasses the no-op/output gate
# ---------------------------------------------------------------------------


def _const_mv(db, dsn, *, node="mv.a", deps=("down.x",)):
    async def run_query():
        return [
            {"id": 1, "status": "a"}
        ]  # CONSTANT output → the hash gate would suppress a re-fire

    gen = make_mv_generate(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        run_query=run_query,
        persist="replace",
        pk_columns=["id"],
    )
    return _CapMV(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: list(deps),
        db=db,
        name="b",
        generate=gen,
    )


@pytest.mark.asyncio
async def test_regen_by_node_bypasses_output_gate_and_cascades(tmp_path):
    dsn = _store(tmp_path)
    async with _db(tmp_path) as db, db.acquire() as conn:
        proc = _const_mv(db, dsn, deps=["down.x"])
        # fire 1: an organic upstream change lands + sets the baseline hash + ripples
        up = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])
        assert await proc.process_pending(conn) is not None

        # fire 2: another organic change, but the recompute is byte-identical → gate suppresses
        up2 = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up2, ["mv.a"])
        assert await proc.process_pending(conn) is None
        assert proc.last_ctx is not None and proc.last_ctx.forced is False

        # FORCED node regen: recomputes DESPITE the unchanged hash (gate bypass) + cascades to down.x
        fid = await injector.force_regen(conn, scope="node", node="mv.a", reason="changed SQL def")
        forced_evt = next(e for e in await _mv_events(conn, "mv.a") if e["id"] == fid)
        assert forced_evt["payload"]["forced"] is True  # marked for audit (REQ-967)
        assert forced_evt["payload"]["reason"] == "changed SQL def"

        posted = await proc.process_pending(conn)
        assert posted is not None  # re-posted even though nothing changed
        assert proc.last_ctx is not None and proc.last_ctx.forced is True
        now = datetime.now(UTC)
        # the forced re-post reached down.x (the normal forward cascade)
        assert posted in await queue.claim(
            conn, dependent_table="down.x", processor_name="p", now=now
        )


@pytest.mark.asyncio
async def test_regen_by_source_relands_and_cascades_forward(tmp_path):
    dsn = _store(tmp_path)

    async def fetch(_pending):
        return [{"id": 1, "status": "a"}]  # constant → forced land bypasses the output gate

    land = make_source_land(
        dsn,
        schema="",
        table="src",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
        probe_type="none",
    )
    async with _db(tmp_path) as db:
        # lineage: the MV depends on the source
        deps = supervisor.dependents_of({"mv.b": "SELECT id, status FROM src"})

        async def mv_run():
            return [{"id": 1, "status": "a"}]

        mv_gen = make_mv_generate(
            dsn,
            schema="",
            table="mvb",
            columns=_COLS,
            run_query=mv_run,
            persist="replace",
            pk_columns=["id"],
        )
        src = _CapSource(
            "src",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=deps,
            db=db,
            name="s",
            land=land,
        )
        mv = _CapMV(
            "mv.b",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=deps,
            db=db,
            name="m",
            generate=mv_gen,
        )
        # seed once so both baselines are set (an organic drain)
        async with db.acquire() as conn:
            e = await queue.post_event(conn, source_table="src", event_type="replace")
            await queue.fan_out(conn, e, ["src"])
        await supervisor.drain(db, [src, mv])

        # FORCED source regen: re-land the root (bypassing ITS output gate) and cascade forward.
        # The source content is constant, so the source's gate WOULD have suppressed a re-land — the
        # forced flag is what makes it re-land + re-post.
        async with db.acquire() as conn:
            before = len(await _mv_events(conn, "src"))
            await injector.force_regen(conn, scope="source", node="src", reason="bad load recovery")
        await supervisor.drain(db, [src, mv])
        assert src.last_ctx is not None and src.last_ctx.forced is True  # source re-landed forced
        async with db.acquire() as conn:
            after = len(await _mv_events(conn, "src"))
        assert after > before  # the source re-posted despite unchanged content (gate bypassed)
        assert [(r[0], r[1]) for r in await _rows(dsn, "mvb")] == [(1, "a")]


@pytest.mark.asyncio
async def test_regen_by_window_pegs_the_requested_period(tmp_path):
    dsn = _store(tmp_path)
    cal = Calendar(name="g", version="v1")
    src = PeriodicCalendar(cal, "daily")

    async def run_query():
        return [{"id": 1, "status": "a"}]

    gen = make_mv_generate(
        dsn,
        schema="",
        table="mvw",
        columns=_COLS,
        run_query=run_query,
        persist="replace",
        pk_columns=["id"],
    )
    async with _db(tmp_path) as db, db.acquire() as conn:
        proc = _CapMV(
            "mv.w",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="b",
            generate=gen,
            deadline_source=src,
        )
        # force regen the sealed period 2026-07-08, addressed by window-id + an as-of inside it
        await injector.force_regen(
            conn,
            scope="window",
            node="mv.w",
            reason="restate period",
            window_id="2026-07-08",
            as_of=datetime(2026, 7, 8, 12, tzinfo=UTC),
        )
        assert await proc.process_pending(conn) is not None  # recomputes, gate bypassed
        assert proc.last_ctx is not None
        assert proc.last_ctx.forced is True
        assert proc.last_ctx.window_id == "2026-07-08"  # pegged to the requested period, not "now"


@pytest.mark.asyncio
async def test_regen_window_id_mismatch_fails_loud(tmp_path):
    dsn = _store(tmp_path)
    cal = Calendar(name="g", version="v1")

    async def run_query():
        return [{"id": 1, "status": "a"}]

    gen = make_mv_generate(
        dsn,
        schema="",
        table="mvw",
        columns=_COLS,
        run_query=run_query,
        persist="replace",
        pk_columns=["id"],
    )
    async with _db(tmp_path) as db, db.acquire() as conn:
        proc = _CapMV(
            "mv.w",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="b",
            generate=gen,
            deadline_source=PeriodicCalendar(cal, "daily"),
        )
        # as_of resolves to 2026-07-09 but the caller addressed 2026-07-08 → loud mismatch
        await injector.force_regen(
            conn,
            scope="window",
            node="mv.w",
            reason="oops",
            window_id="2026-07-08",
            as_of=datetime(2026, 7, 9, 12, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="not the requested"):
            await proc.process_pending(conn)


def test_force_regen_rejects_unknown_scope_and_missing_reason():
    async def _run():
        async with _db_ctx() as conn:
            with pytest.raises(ValueError, match="unknown regen scope"):
                await injector.force_regen(conn, scope="galaxy", node="mv.a", reason="x")
            with pytest.raises(ValueError, match="must carry a reason|reason"):
                await injector.force_regen(conn, scope="node", node="mv.a", reason="  ")
            with pytest.raises(ValueError, match="window_id"):
                await injector.force_regen(conn, scope="window", node="mv.a", reason="x")

    import asyncio

    asyncio.run(_run())


@asynccontextmanager
async def _db_ctx():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        engine = create_async_engine(f"sqlite+aiosqlite:///{Path(d) / 'q.db'}")
        async with engine.begin() as c:
            await c.run_sync(
                lambda s: events.metadata.create_all(
                    s, tables=[events, event_status, node_freshness_state]
                )
            )
        try:
            async with Database(engine, name="q").acquire() as conn:
                yield conn
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# REQ-983: preserved snapshots — declared + why-tagged, sealed + immutable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_seals_and_is_immutable(tmp_path):
    dsn = _store(tmp_path)
    async with _db(tmp_path) as db, db.acquire() as conn:
        sealed = await snapshots.seal_snapshot(
            conn,
            dsn,
            name="q1_close",
            reason="upstream ledger purged; not reconstructible",
            schema="",
            table="snap_q1",
            columns=_COLS,
            rows=[{"id": 1, "status": "final"}],
            pk_columns=["id"],
            window_id="2026-Q1",
        )
        assert sealed.reason and sealed.window_id == "2026-Q1"
        assert [(r[0], r[1]) for r in await _rows(dsn, "snap_q1")] == [(1, "final")]
        # the seal record makes it IMMUTABLE — a re-seal is refused (no silent overwrite)
        with pytest.raises(ValueError, match="already sealed and immutable"):
            await snapshots.seal_snapshot(
                conn,
                dsn,
                name="q1_close",
                reason="try again",
                schema="",
                table="snap_q1",
                columns=_COLS,
                rows=[{"id": 2, "status": "tampered"}],
                pk_columns=["id"],
            )
        assert [(r[0], r[1]) for r in await _rows(dsn, "snap_q1")] == [(1, "final")]  # unchanged
        got = await snapshots.get_snapshot(conn, "q1_close")
        assert got is not None and got.content_hash == sealed.content_hash


@pytest.mark.asyncio
async def test_snapshot_without_why_tag_fails_loud(tmp_path):
    dsn = _store(tmp_path)
    async with _db(tmp_path) as db, db.acquire() as conn:
        for reason in ("", "   "):
            with pytest.raises(ValueError, match="MUST be declared with a why-tag"):
                await snapshots.seal_snapshot(
                    conn,
                    dsn,
                    name="nope",
                    reason=reason,
                    schema="",
                    table="snap_x",
                    columns=_COLS,
                    rows=[{"id": 1, "status": "a"}],
                    pk_columns=["id"],
                )
        # nothing was recorded (declaration rejected before any seal)
        assert await snapshots.get_snapshot(conn, "nope") is None


# ---------------------------------------------------------------------------
# REQ-969 (MAY): incremental maintenance — delta-in → incremental apply + delta-out
# ---------------------------------------------------------------------------

_INCR_SQL = (
    "SELECT id, status FROM orders"  # single-input bare-column projection → incrementalizable
)


@pytest.mark.asyncio
async def test_incremental_applies_only_the_delta(tmp_path):
    dsn = _store(tmp_path)
    # prior landed state
    await store_writer.persist_land(
        dsn,
        schema="",
        table="mvi",
        columns=_COLS,
        rows=[{"id": 1, "status": "a"}],
        persist="upsert",
        pk_columns=["id"],
    )

    recompute_calls = {"n": 0}

    async def run_query():
        recompute_calls["n"] += 1
        return [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]

    gen = make_mv_incremental(
        dsn,
        schema="",
        table="mvi",
        columns=_COLS,
        sql=_INCR_SQL,
        run_query=run_query,
        pk_columns=["id"],
    )
    # an upstream delta carrying only the changed row
    pending = [{"event_type": "delta", "payload": {"delta": [{"id": 2, "status": "b"}]}}]
    et, payload, digest = await gen(pending, prior_hash=None)
    assert et == "delta" and payload["delta"] == [{"id": 2, "status": "b"}] and digest is None
    assert recompute_calls["n"] == 0  # NO full recompute — only the delta was applied
    assert [(r[0], r[1]) for r in await _rows(dsn, "mvi")] == [(1, "a"), (2, "b")]


@pytest.mark.asyncio
async def test_incremental_full_recompute_when_no_delta(tmp_path):
    dsn = _store(tmp_path)

    async def run_query():
        return [{"id": 1, "status": "a"}]

    gen = make_mv_incremental(
        dsn,
        schema="",
        table="mvi",
        columns=_COLS,
        sql=_INCR_SQL,
        run_query=run_query,
        pk_columns=["id"],
    )
    # a full replace input carries no delta rows → documented full recompute (explicit, not silent)
    pending = [{"event_type": "replace", "payload": {"rows": 5}}]
    et, _payload, digest = await gen(pending, prior_hash=None)
    assert et == "delta" and digest is not None  # recomputed → a content hash was produced
    assert [(r[0], r[1]) for r in await _rows(dsn, "mvi")] == [(1, "a")]


def test_incremental_without_pk_fails_loud(tmp_path):
    with pytest.raises(ValueError, match="requires a primary key"):
        make_mv_incremental(
            _store(tmp_path),
            schema="",
            table="mvi",
            columns=_COLS,
            sql=_INCR_SQL,
            run_query=None,
            pk_columns=[],
        )


def test_incremental_on_infeasible_sql_fails_loud(tmp_path):
    # a GROUP BY aggregation has no safe row-wise incremental form here → explicit error, not a
    # silent full-recompute downgrade
    with pytest.raises(ValueError, match="no safe incremental form"):
        make_mv_incremental(
            _store(tmp_path),
            schema="",
            table="mvi",
            columns=_COLS,
            sql="SELECT id, count(*) AS n FROM orders GROUP BY id",
            run_query=None,
            pk_columns=["id"],
        )
