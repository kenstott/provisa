# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9f0c72-6b41-4e58-8a2d-7c1e4f9b6a03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Freshness-trigger variations driven end-to-end through the REAL loop + two-database store.

The prior loop e2e all use ``change_signal="ttl"`` with a full-replace landing on every fire. These
exercise the trigger variations that are actually observable at the loop/store boundary (as opposed
to the scheduler-fire decision, which is unit/integration territory):

- WATERMARK-append landing (REQ-982): a watermark probe lands an APPEND delta that ACCUMULATES across
  fires, in contrast to a ttl replace that overwrites.
- PERIODIC calendar boundary (REQ-961/962): a daily-grain MV seals and lands into the store once its
  listed input is fresh-through ``window.end`` — and HOLDS (no land, a ``warn``) when the input is a
  stale outage. Promotes the freshness-contract integration case to a real materialized land.
- FORCED regen (REQ-968): a manual regen recomputes and re-lands REGARDLESS of the content-hash gate,
  rippling to dependents even when the recomputed content is byte-identical.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from provisa.events import injector, queue, supervisor
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.freshness_reader import make_db_freshness_of
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor
from provisa.federation import store_writer

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

UTC = timezone.utc
_COLS = [("id", "bigint"), ("status", "text")]
# The just-closed daily window when _now sits at 2026-07-10 01:30 is 2026-07-09 → end 2026-07-10 00:00.
WIN_END = datetime(2026, 7, 10, tzinfo=UTC)
AFTER_BOUNDARY = datetime(2026, 7, 10, 1, 30, tzinfo=UTC)


@asynccontextmanager
async def _at(monkeypatch, instant):
    """Pin the loop's wall clock so a periodic boundary is deterministic."""
    import provisa.events.processor as pm

    monkeypatch.setattr(pm, "_now", lambda: instant)
    yield


async def _fan_in(cp, node, source, *, event_type="append", n=1):
    async with cp.acquire() as conn:
        for _ in range(n):
            eid = await queue.post_event(conn, source_table=source, event_type=event_type)
            await queue.fan_out(conn, eid, [node])


async def _ids(store, table):
    async with store_writer.store_connection(store) as sconn:
        return [r[0] for r in await sconn.fetch(f'SELECT id FROM "{table}" ORDER BY id')]


async def _table_absent(store, table):
    async with store_writer.store_connection(store) as sconn:
        return await sconn.fetchval("SELECT to_regclass($1)", f"public.{table}") is None


# ---------------------------------------------------------------------------


async def test_watermark_append_accumulates_across_fires(control_plane):
    """REQ-982: a watermark probe lands an APPEND. Across two fires the store table ACCUMULATES the
    union of the batches (upsert-by-key), rather than the second batch overwriting the first."""
    cp, store = control_plane["db"], control_plane["store"]
    batches = [[{"id": 1, "status": "a"}, {"id": 2, "status": "b"}], [{"id": 3, "status": "c"}]]
    fire = {"n": 0}

    async def fetch(_p):
        b = batches[fire["n"]]
        fire["n"] += 1
        return b

    src = SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column="id",
        dependents_of=lambda _n: [],
        name="src",
        land=make_source_land(
            store,
            schema="",
            table="orders",
            columns=_COLS,
            change_signal="ttl",
            watermark_column="id",
            pk_columns=["id"],
            fetch=fetch,
            probe_type="watermark",  # REQ-982: watermark → append shape
        ),
        db=cp,
    )

    await _fan_in(cp, "s.orders", "s.orders")
    await supervisor.drain(cp, [src])
    assert await _ids(store, "orders") == [1, 2]

    await _fan_in(cp, "s.orders", "s.orders")
    await supervisor.drain(cp, [src])
    assert await _ids(store, "orders") == [1, 2, 3]  # accumulated, not overwritten


def _periodic_mv(cp, store, *, run, expected):
    cal = Calendar(name="g", version="v1")
    return MVTableProcessor(
        "mat.daily",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda _n: [],
        name="mv-box",
        generate=make_mv_generate(
            store, schema="", table="mv_a", columns=_COLS, run_query=run, pk_columns=["id"]
        ),
        deadline_source=PeriodicCalendar(cal, "daily"),
        expected_events=expected,
        freshness_of=make_db_freshness_of(cp),  # the REAL DB-backed reader
        db=cp,
    )


async def test_periodic_calendar_seal_lands_into_store(control_plane, monkeypatch):
    """REQ-961/962: at the daily boundary, with the listed input fresh-through window.end, the periodic
    MV's contract passes → it seals and materializes into the real store."""
    cp, store = control_plane["db"], control_plane["store"]
    await _fan_in(cp, "mat.daily", "s.tx")
    async with cp.acquire() as conn:
        await queue.record_refresh(conn, "s.tx", at=WIN_END, ok=True)  # input fresh through boundary

    async def run():
        return [{"id": 7, "status": "sealed"}]

    mv = _periodic_mv(cp, store, run=run, expected=["s.tx"])
    async with _at(monkeypatch, AFTER_BOUNDARY):
        await supervisor.drain(cp, [mv])

    assert await _ids(store, "mv_a") == [7]  # the window sealed and materialized


async def test_periodic_stale_input_holds_no_land(control_plane, monkeypatch):
    """REQ-961: an input last refreshed BEFORE window.end is an outage → the periodic MV HOLDS: no
    seal, no materialized land, a ``warn`` emitted (never a silent skip that lands stale data)."""
    cp, store = control_plane["db"], control_plane["store"]
    await _fan_in(cp, "mat.daily", "s.tx")
    async with cp.acquire() as conn:
        await queue.record_refresh(conn, "s.tx", at=WIN_END - timedelta(hours=2), ok=True)  # stale

    async def run():
        return [{"id": 7, "status": "sealed"}]

    mv = _periodic_mv(cp, store, run=run, expected=["s.tx"])
    async with _at(monkeypatch, AFTER_BOUNDARY):
        await supervisor.drain(cp, [mv])

    assert await _table_absent(store, "mv_a")  # held — nothing materialized
    async with cp.acquire() as conn:
        kinds = {
            r["source_table"]: r["event_type"]
            for r in await queue.read_since(conn, cursor=0)
            if r["source_table"] == "mat.daily"
        }
    assert kinds.get("mat.daily") == "warn"  # outage surfaced, not swallowed


async def test_forced_regen_bypasses_hash_gate_and_ripples(control_plane):
    """REQ-968: a forced regen recomputes + re-lands REGARDLESS of the content-hash gate, and ripples
    to a dependent even though the recomputed content is byte-identical to the prior land."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {"mv.a": "SELECT id, status FROM s.orders", "mv.b": "SELECT id, status FROM mv.a"}
    )
    calls = {"a": 0, "b": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        calls["a"] += 1
        return [{"id": 10, "status": "constant"}]  # identical every recompute

    async def run_b():
        calls["b"] += 1
        return [{"id": 20, "status": "b"}]

    src = SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name="src",
        land=make_source_land(
            store,
            schema="",
            table="orders",
            columns=_COLS,
            change_signal="ttl",
            watermark_column=None,
            pk_columns=["id"],
            fetch=fetch,
        ),
        db=cp,
    )
    mv_a = MVTableProcessor(
        "mv.a", change_signal="ttl", watermark_column=None, dependents_of=dep, name="mv.a",
        generate=make_mv_generate(store, schema="", table="mv_a", columns=_COLS, run_query=run_a,
                                  pk_columns=["id"]), db=cp,
    )
    mv_b = MVTableProcessor(
        "mv.b", change_signal="ttl", watermark_column=None, dependents_of=dep, name="mv.b",
        generate=make_mv_generate(store, schema="", table="mv_b", columns=_COLS, run_query=run_b,
                                  pk_columns=["id"]), db=cp,
    )
    procs = [src, mv_a, mv_b]

    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
        await queue.fan_out(conn, eid, ["s.orders"])
    await supervisor.drain(cp, procs)
    assert calls == {"a": 1, "b": 1}

    # a plain re-fire of mv.a would be gated (identical content); a FORCED regen bypasses the gate
    async with cp.acquire() as conn:
        await injector.force_regen(conn, scope="node", node="mv.a", reason="manual replay")
    await supervisor.drain(cp, procs)

    assert calls["a"] == 2  # recomputed despite unchanged content (gate bypassed)
    assert calls["b"] == 2  # ...and the forced re-land rippled to the dependent
