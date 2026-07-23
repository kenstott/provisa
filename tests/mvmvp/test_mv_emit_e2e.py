# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c7e51-2f83-4d06-b1e7-6c0a9d3f85b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Subscription EMISSION end-to-end against the REAL isolated two-database stack — the two halves of
the pipe the operator's declared outcomes flow through:

PART A — the INTERNAL DAG emit (REQ-965 demand-driven, pay-per-consumer). An MV declares a SET of
emit shapes ({replace, append, delta}); per fire it produces ONLY the declared shapes some dependent
SUBSCRIBES to, and routes each shape to its shape-matched dependents:
- a declared shape with no subscriber is NOT produced (delta's diff cost is paid only when consumed);
- distinct shapes route to DISTINCT dependents, each reconstructing exactly once;
- emit-NONE persists to the MV's own store table but posts nothing — it tells no one, no ripple.

PART B — the EXTERNAL bridge (REQ-258/565). A row landing in the materialize store fires a Postgres
LISTEN/NOTIFY trigger that the real ``PgNotificationProvider`` turns into a ``ChangeEvent`` for an
external subscriber — the genuine MV-land → subscriber-frame contract, end to end over real Postgres.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
from sqlalchemy import select

from provisa.core.schema_org import event_status, events
from provisa.events import queue, supervisor
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor
from provisa.federation import store_writer
from provisa.subscriptions.pg_provider import PgNotificationProvider
from provisa.subscriptions.pg_triggers import _trigger_sql

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_COLS = [("id", "bigint"), ("status", "text")]
_TABLE = {"mv.a": "mv_a", "mv.b": "mv_b", "mv.c": "mv_c", "mv.d": "mv_d"}


def _src(db, store, *, dep, fetch):
    return SourceTableProcessor(
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
        db=db,
    )


def _emit_mv(db, store, *, run, emit, router):
    """An MV that declares a demand-driven emit SET (``emit``) routed by ``router(node, shape)``."""
    return MVTableProcessor(
        "mv.a",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda _n: [],  # unused when emit_outcomes is set — routing is per-shape
        name="mv.a",
        generate=make_mv_generate(
            store, schema="", table="mv_a", columns=_COLS, run_query=run, pk_columns=["id"]
        ),
        emit_outcomes=frozenset(emit),
        subscribers_of=router,
        db=db,
    )


def _plain_mv(node, db, store, *, run):
    return MVTableProcessor(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda _n: [],
        name=node,
        generate=make_mv_generate(
            store, schema="", table=_TABLE[node], columns=_COLS, run_query=run, pk_columns=["id"]
        ),
        db=db,
    )


async def _inject(cp):
    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
        await queue.fan_out(conn, eid, ["s.orders"])


async def _routes(cp, node):
    """The (event_type, dependent_table) fan-out pairs posted BY ``node`` — the demand-driven routing
    resolved this run, read straight off the events × event_status join."""
    async with cp.acquire() as conn:
        result = await conn.execute_core(
            select(events.c.event_type, event_status.c.dependent_table)
            .select_from(events.join(event_status, events.c.id == event_status.c.event_id))
            .where(events.c.source_table == node)
        )
        return {(r[0], r[1]) for r in result.fetchall()}


async def _ids(store, table):
    async with store_writer.store_connection(store) as sconn:
        return [r[0] for r in await sconn.fetch(f'SELECT id FROM "{table}" ORDER BY id')]


async def _table_absent(store, table):
    async with store_writer.store_connection(store) as sconn:
        return await sconn.fetchval("SELECT to_regclass($1)", f"public.{table}") is None


# --- PART A: internal demand-driven DAG emit -------------------------------


async def test_demand_driven_emit_drops_unsubscribed_shape(control_plane):
    """REQ-965: mv.a DECLARES {replace, append, delta} but only replace + append have a subscriber.
    Per fire it produces exactly those two, routed to their distinct dependents; delta — declared but
    unsubscribed — is NOT produced (pay-per-consumer: its diff cost is never paid)."""
    cp, store = control_plane["db"], control_plane["store"]
    subs = {"replace": ["sub.r"], "append": ["sub.p"], "delta": []}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        return [{"id": 10, "status": "a"}]

    procs = [
        _src(cp, store, dep=lambda _n: ["mv.a"], fetch=fetch),
        _emit_mv(
            cp, store, run=run_a,
            emit={"replace", "append", "delta"},
            router=lambda _node, shape: subs[shape],
        ),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)

    assert await _routes(cp, "mv.a") == {("replace", "sub.r"), ("append", "sub.p")}
    assert await _ids(store, "mv_a") == [10]  # persisted regardless of what emitted


async def test_per_shape_routing_reaches_distinct_dependents(control_plane):
    """REQ-965: distinct emit shapes route to DISTINCT downstream MVs — replace → mv.b, append → mv.c
    — each reconstructing exactly once; the unsubscribed delta arm (mv.d) never fires."""
    cp, store = control_plane["db"], control_plane["store"]
    calls = {"b": 0, "c": 0, "d": 0}
    subs = {"replace": ["mv.b"], "append": ["mv.c"], "delta": []}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        return [{"id": 10, "status": "a"}]

    def _run(key, rid):
        async def run():
            calls[key] += 1
            return [{"id": rid, "status": key}]

        return run

    procs = [
        _src(cp, store, dep=lambda _n: ["mv.a"], fetch=fetch),
        _emit_mv(
            cp, store, run=run_a,
            emit={"replace", "append", "delta"},
            router=lambda _node, shape: subs[shape],
        ),
        _plain_mv("mv.b", cp, store, run=_run("b", 20)),
        _plain_mv("mv.c", cp, store, run=_run("c", 30)),
        _plain_mv("mv.d", cp, store, run=_run("d", 40)),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)

    assert await _ids(store, "mv_b") == [20]  # replace arm reconstructed
    assert await _ids(store, "mv_c") == [30]  # append arm reconstructed
    assert await _table_absent(store, "mv_d")  # delta arm had no subscriber → never fired
    assert calls == {"b": 1, "c": 1, "d": 0}


async def test_emit_none_persists_but_no_ripple(control_plane):
    """REQ-965: an MV whose declared shapes have NO subscriber this fire persists to its own store
    table but posts NOTHING — a terminal MV that materializes yet tells no one, so a would-be
    dependent never reconstructs."""
    cp, store = control_plane["db"], control_plane["store"]
    calls = {"b": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        return [{"id": 10, "status": "a"}]

    async def run_b():
        calls["b"] += 1
        return [{"id": 20, "status": "b"}]

    procs = [
        _src(cp, store, dep=lambda _n: ["mv.a"], fetch=fetch),
        _emit_mv(cp, store, run=run_a, emit={"replace"}, router=lambda _n, _s: []),
        _plain_mv("mv.b", cp, store, run=run_b),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)

    assert await _ids(store, "mv_a") == [10]  # persisted
    assert await _routes(cp, "mv.a") == set()  # ...but emitted nothing
    assert calls["b"] == 0  # so the would-be dependent never reconstructed


# --- PART B: the external bridge (store land → subscriber ChangeEvent) ------


async def _first_change_event(provider, table, *, install, timeout=10.0):
    """Start ``provider.watch(table)``, run ``install`` once listening, and return the first
    ChangeEvent the store's LISTEN/NOTIFY trigger produces (or fail on timeout)."""
    gen = provider.watch(table)
    task = asyncio.ensure_future(gen.__anext__())
    try:
        await asyncio.sleep(0.5)  # let add_listener register before the notifying write
        await install()
        return await asyncio.wait_for(task, timeout=timeout)
    finally:
        if not task.done():
            task.cancel()
        await gen.aclose()


async def test_store_land_notifies_pg_subscriber(control_plane):
    """REQ-258/565 — the bridge: a row landing in the materialize store fires the pg_notify trigger,
    and the real PgNotificationProvider surfaces it to an external subscriber as an INSERT
    ChangeEvent carrying the landed row."""
    store_dsn = control_plane["store"]
    pool = await asyncpg.create_pool(store_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute('CREATE TABLE orders (id bigint PRIMARY KEY, status text)')
            await conn.execute(_trigger_sql("public", "orders"))

        provider = PgNotificationProvider(pool)

        async def land():
            async with pool.acquire() as c:
                await c.execute("INSERT INTO orders (id, status) VALUES (1, 'new')")

        ev = await _first_change_event(provider, "orders", install=land)
        assert ev.operation == "insert"
        assert ev.table == "orders"
        assert ev.row["id"] == 1 and ev.row["status"] == "new"
    finally:
        await pool.close()


async def test_store_delete_notifies_delete_change_event(control_plane):
    """REQ-258 — the trigger maps a DELETE to a ``delete`` ChangeEvent carrying the removed (OLD) row,
    so an external subscriber sees the retraction, not a silent drop."""
    store_dsn = control_plane["store"]
    pool = await asyncpg.create_pool(store_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute('CREATE TABLE orders (id bigint PRIMARY KEY, status text)')
            await conn.execute(_trigger_sql("public", "orders"))
            await conn.execute("INSERT INTO orders (id, status) VALUES (7, 'gone')")

        provider = PgNotificationProvider(pool)

        async def retract():
            async with pool.acquire() as c:
                await c.execute("DELETE FROM orders WHERE id = 7")

        ev = await _first_change_event(provider, "orders", install=retract)
        assert ev.operation == "delete"
        assert ev.row["id"] == 7 and ev.row["status"] == "gone"
    finally:
        await pool.close()
