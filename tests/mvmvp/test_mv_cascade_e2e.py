# Copyright (c) 2026 Kenneth Stott
# Canary: b2e7d1a4-5c36-4f80-9a1b-3e6c8d2f7051
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Multi-level MV reconstruction end-to-end against the REAL isolated two-database stack.

Every prior MV loop e2e is depth-1 (a source feeds one MV whose dependent is a stub node). These
exercise the actual composition the MV engine exists for: one upstream change ripples through a
chain of REAL MV recomputes, ordered by the SQLGlot-derived lineage DAG (REQ-966), collapsing bursts
and fan-ins into a single downstream recompute, halting at the content-hash gate (REQ-981), and
carrying poison forward (REQ-957).

- ``source -> mv.a -> mv.b`` : a change reconstructs two levels; a burst collapses to one recompute
  per level.
- diamond (``mv.a`` + ``mv.b`` -> ``mv.c``) : the fan-in coalesces into ONE ``mv.c`` recompute.
- a cyclic lineage is rejected at registration (fan-out would never terminate).
- the content-hash gate stops the ripple: an unchanged ``mv.a`` recompute does not reconstruct ``mv.b``.
- an ``error`` event poisons the whole chain: no level below the fault lands.
"""

from __future__ import annotations

import pytest

from provisa.events import queue, supervisor
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor
from provisa.federation import store_writer

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_COLS = [("id", "bigint"), ("status", "text")]
# node name -> its store table (schemaless "public"); node "mv.a" lands into "mv_a".
_TABLE = {"s.orders": "orders", "mv.a": "mv_a", "mv.b": "mv_b", "mv.c": "mv_c"}


def _src(db, store, *, dep, fetch, name="src"):
    return SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name=name,
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


def _mv(node, run, db, store, *, dep):
    return MVTableProcessor(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name=node,
        generate=make_mv_generate(
            store, schema="", table=_TABLE[node], columns=_COLS, run_query=run, pk_columns=["id"]
        ),
        db=db,
    )


async def _inject(cp, *, n=1):
    """Post ``n`` source changes onto the source node's own queue (the loop coalesces them)."""
    async with cp.acquire() as conn:
        for _ in range(n):
            eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
            await queue.fan_out(conn, eid, ["s.orders"])


async def _ids(store, table):
    async with store_writer.store_connection(store) as sconn:
        return [r[0] for r in await sconn.fetch(f'SELECT id FROM "{table}" ORDER BY id')]


async def _vintage(cp, node):
    async with cp.acquire() as conn:
        return await queue.get_node_state(conn, node)


# ---------------------------------------------------------------------------


async def test_depth_two_chain_reconstructs_both_levels(control_plane):
    """source -> mv.a -> mv.b : one source change lands the source AND reconstructs two real MV levels
    in a single drain, each stamped with its own vintage (REQ-966/967)."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {"mv.a": "SELECT id, status FROM s.orders", "mv.b": "SELECT id, status FROM mv.a"}
    )
    calls = {"a": 0, "b": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        calls["a"] += 1
        return [{"id": 10, "status": "a"}]

    async def run_b():
        calls["b"] += 1
        return [{"id": 20, "status": "b"}]

    procs = [
        _src(cp, store, dep=dep, fetch=fetch),
        _mv("mv.a", run_a, cp, store, dep=dep),
        _mv("mv.b", run_b, cp, store, dep=dep),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)

    assert await _ids(store, "orders") == [1]
    assert await _ids(store, "mv_a") == [10]
    assert await _ids(store, "mv_b") == [20]  # the change reached level 2
    assert calls == {"a": 1, "b": 1}
    for node in ("s.orders", "mv.a", "mv.b"):
        state = await _vintage(cp, node)
        assert state is not None and state["content_hash"] is not None


async def test_burst_collapses_to_one_recompute_per_level(control_plane):
    """A burst of source changes fans in and collapses to exactly ONE recompute at each level of the
    chain — the debounce/coalesce gate applies through the whole DAG, not just level 1 (REQ-963)."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {"mv.a": "SELECT id, status FROM s.orders", "mv.b": "SELECT id, status FROM mv.a"}
    )
    calls = {"a": 0, "b": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    async def run_a():
        calls["a"] += 1
        return [{"id": 10, "status": "a"}]

    async def run_b():
        calls["b"] += 1
        return [{"id": 20, "status": "b"}]

    procs = [
        _src(cp, store, dep=dep, fetch=fetch),
        _mv("mv.a", run_a, cp, store, dep=dep),
        _mv("mv.b", run_b, cp, store, dep=dep),
    ]
    await _inject(cp, n=5)  # five upstream changes
    await supervisor.drain(cp, procs)

    assert calls == {"a": 1, "b": 1}  # collapsed, not five recomputes per level


async def test_diamond_fan_in_coalesces_to_one_downstream_recompute(control_plane):
    """mv.a and mv.b both depend on the source; mv.c depends on BOTH. One source change ripples down
    both arms, and the two arrivals at mv.c coalesce into ONE mv.c reconstruction (fan-in collapse)."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {
            "mv.a": "SELECT id, status FROM s.orders",
            "mv.b": "SELECT id, status FROM s.orders",
            "mv.c": "SELECT a.id, a.status FROM mv.a a JOIN mv.b b ON a.id = b.id",
        }
    )
    calls = {"a": 0, "b": 0, "c": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    def _run(key, row):
        async def run():
            calls[key] += 1
            return [row]

        return run

    procs = [
        _src(cp, store, dep=dep, fetch=fetch),
        _mv("mv.a", _run("a", {"id": 10, "status": "a"}), cp, store, dep=dep),
        _mv("mv.b", _run("b", {"id": 11, "status": "b"}), cp, store, dep=dep),
        _mv("mv.c", _run("c", {"id": 12, "status": "c"}), cp, store, dep=dep),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)

    assert await _ids(store, "mv_a") == [10]
    assert await _ids(store, "mv_b") == [11]
    assert await _ids(store, "mv_c") == [12]
    assert calls["c"] == 1  # both arms fanned in, but mv.c reconstructed exactly once


async def test_cyclic_lineage_rejected_at_registration(control_plane):
    """A cyclic MV lineage (mv.a <- mv.b <- mv.a) is rejected when the dependents graph is built —
    fan-out would never terminate, so the DAG is refused, not silently run to the max-rounds backstop."""
    with pytest.raises(ValueError, match="cycle"):
        supervisor.dependents_of(
            {"mv.a": "SELECT id FROM mv.b", "mv.b": "SELECT id FROM mv.a"}
        )


async def test_hash_gate_stops_the_ripple(control_plane):
    """The content-hash gate (REQ-981) halts reconstruction: when mv.a recomputes to byte-identical
    content on a second change, it does not re-post, so mv.b is NOT reconstructed a second time."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {"mv.a": "SELECT id, status FROM s.orders", "mv.b": "SELECT id, status FROM mv.a"}
    )
    calls = {"a": 0, "b": 0}
    src_rev = {"n": 0}

    async def fetch(_p):
        # the SOURCE genuinely changes each fire (distinct rows) so it re-lands and ripples to mv.a;
        # the gate under test is mv.a's, not the source's.
        src_rev["n"] += 1
        return [{"id": src_rev["n"], "status": "new"}]

    async def run_a():
        calls["a"] += 1
        return [{"id": 10, "status": "constant"}]  # collapses to identical content every recompute

    async def run_b():
        calls["b"] += 1
        return [{"id": 20, "status": "b"}]

    procs = [
        _src(cp, store, dep=dep, fetch=fetch),
        _mv("mv.a", run_a, cp, store, dep=dep),
        _mv("mv.b", run_b, cp, store, dep=dep),
    ]
    await _inject(cp)
    await supervisor.drain(cp, procs)
    assert calls == {"a": 1, "b": 1}

    # a second source change: mv.a recomputes (calls a==2) but to the SAME content → gate → no ripple
    await _inject(cp)
    await supervisor.drain(cp, procs)
    assert calls["a"] == 2  # mv.a did recompute
    assert calls["b"] == 1  # ...but mv.b was NOT reconstructed again — the gate stopped the ripple


async def test_error_event_poisons_the_whole_chain(control_plane):
    """An ``error`` event on the head of the chain poisons every level below it: no MV lands, and the
    error fans forward to each dependent (REQ-957 poison propagation) rather than a silent halt."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of(
        {"mv.a": "SELECT id, status FROM s.orders", "mv.b": "SELECT id, status FROM mv.a"}
    )
    calls = {"a": 0, "b": 0}

    async def fetch(_p):
        return [{"id": 1, "status": "new"}]

    def _run(key):
        async def run():
            calls[key] += 1
            return [{"id": 1, "status": key}]

        return run

    procs = [
        _src(cp, store, dep=dep, fetch=fetch),
        _mv("mv.a", _run("a"), cp, store, dep=dep),
        _mv("mv.b", _run("b"), cp, store, dep=dep),
    ]
    # inject an ERROR at the source head rather than a normal append
    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="error")
        await queue.fan_out(conn, eid, ["s.orders"])
    await supervisor.drain(cp, procs)

    # neither MV produced — poison skips produce at every level
    assert calls == {"a": 0, "b": 0}
    async with store_writer.store_connection(store) as sconn:
        for table in ("mv_a", "mv_b"):
            exists = await sconn.fetchval(
                "SELECT to_regclass($1)", f"public.{table}"
            )
            assert exists is None  # nothing landed below the fault
    # the error fanned forward to both dependents, not swallowed
    async with cp.acquire() as conn:
        kinds = [
            r["event_type"]
            for r in await queue.read_since(conn, cursor=0)
            if r["source_table"] in ("mv.a", "mv.b")
        ]
    assert kinds and set(kinds) == {"error"}
