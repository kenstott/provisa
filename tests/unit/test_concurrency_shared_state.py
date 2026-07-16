# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Concurrency stress tests for RACES ON SHARED MUTABLE STATE.

Target class: a bad interleaving that corrupts shared state and throws NO exception — invisible to
error-surfacing checks. Because the corruption is silent, each harness pins an INVARIANT at the
boundary (single-instance lazy init, exact counter total, entry coherence) so a losing interleaving
becomes a LOUD assertion failure instead of a leaked engine / lost update / torn read.

Method: a threading.Barrier releases every worker onto the check-then-mutate window at once, and each
scenario runs many trials with a fresh state — timing-dependent bugs need repetition + simultaneity
to reproduce. Hermetic (in-process, fakeredis); no docker, no Trino.

Covers the shared-state points enumerated for the wedge path:
  - ingest.engine.get_engine      lazy per-source engine cache      (was found-race → hardened)
  - warm_tables.QueryCounter      per-table frequency counter       (lock — verify no lost updates)
  - source_adapters.get_adapter   lazy adapter-module cache         (verify single module)
  - cache.hot_tables.HotTableMgr  hot-cache entry map               (verify entry coherence)
"""

from __future__ import annotations

import asyncio
import threading

import pytest


def _hammer(worker, n_threads: int, trials: int, setup=None):
    """Run *worker* on *n_threads* released simultaneously by a barrier, for *trials* rounds.

    worker(barrier, sink) appends its observation to the shared list `sink`. `setup()` runs before
    each round to reset the state under test. Returns the list of per-round sinks.
    """
    rounds = []
    for _ in range(trials):
        if setup is not None:
            setup()
        barrier = threading.Barrier(n_threads)
        sink: list = []
        threads = [threading.Thread(target=worker, args=(barrier, sink)) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        rounds.append(sink)
    return rounds


# ── 1. ingest.engine.get_engine — one AsyncEngine per source_id ─────────────────


class TestIngestEngineCacheRace:
    """Concurrent first-hits for one source_id must all receive the SAME engine, and exactly one
    engine must be created. A losing interleaving builds N engines and leaks N-1 connection pools.

    Power: the instrumented constructor sleeps, WIDENING the check-then-create window so a losing
    interleaving reproduces every round instead of ~1-in-120. The correct (locked) implementation
    serialises on the lock and still builds exactly one; a check-then-set with no lock builds many.
    """

    def _run(self, n_threads: int, trials: int):
        import time

        import provisa.ingest.engine as E

        built = {"n": 0}
        lock = threading.Lock()
        orig = E.create_async_engine

        def slow_counting(*a, **k):
            with lock:
                built["n"] += 1
            time.sleep(0.003)  # hold the window open so racers pile in
            return orig(*a, **k)

        E.create_async_engine = slow_counting  # type: ignore[assignment]
        try:
            for _ in range(trials):
                E._engines.clear()
                built["n"] = 0
                barrier = threading.Barrier(n_threads)
                sink: list = []

                def w():
                    barrier.wait()
                    sink.append(E.get_engine("s1", "postgresql+asyncpg", "h", 5432, "db", "u", "p"))

                ts = [threading.Thread(target=w) for _ in range(n_threads)]
                for t in ts:
                    t.start()
                for t in ts:
                    t.join()
                yield built["n"], sink
        finally:
            E.create_async_engine = orig  # type: ignore[assignment]
            loop = asyncio.new_event_loop()
            for e in list(E._engines.values()):
                loop.run_until_complete(e.dispose())
            loop.close()
            E._engines.clear()

    def test_exactly_one_engine_constructed(self):
        for n_built, _ in self._run(n_threads=32, trials=20):
            assert n_built == 1, f"constructed {n_built} engines for one source (double-init race)"

    def test_all_callers_get_the_same_engine(self):
        for _, sink in self._run(n_threads=32, trials=20):
            assert len({id(e) for e in sink}) == 1, "get_engine handed out >1 engine for one source"


# ── 2. warm_tables.QueryCounter — no lost updates ───────────────────────────────


class TestQueryCounterNoLostUpdates:
    """The lock must make increment atomic: N threads each incrementing M times must total N*M with
    no lost read-modify-writes."""

    def test_count_is_exact_under_contention(self):
        from provisa.cache.warm_tables import QueryCounter

        n_threads, per_thread = 32, 500
        for _ in range(10):
            counter = QueryCounter()
            barrier = threading.Barrier(n_threads)

            def w():
                barrier.wait()
                for _ in range(per_thread):
                    counter.increment("t")

            ts = [threading.Thread(target=w) for _ in range(n_threads)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            assert counter.get_count("t") == n_threads * per_thread, "lost update in QueryCounter"


# ── 3. source_adapters.get_adapter — single module per type ─────────────────────


class TestAdapterCacheRace:
    """Concurrent first-hits for one source type must all resolve to the SAME module object."""

    def test_single_module_under_thread_stress(self):
        import provisa.source_adapters.registry as R

        # Register a lightweight stdlib module as a fake adapter so the import is cheap and free of
        # side effects; the cache-race path is identical regardless of the target module.
        R.register_adapter("_race_probe", "types")
        try:

            def worker(barrier, sink):
                barrier.wait()
                sink.append(R.get_adapter("_race_probe"))

            def reset():
                R._loaded.pop("_race_probe", None)

            rounds = _hammer(worker, n_threads=48, trials=120, setup=reset)
            for sink in rounds:
                assert len({id(m) for m in sink}) == 1, (
                    "get_adapter returned >1 module for one type"
                )
        finally:
            R._ADAPTER_MAP.pop("_race_probe", None)
            R._loaded.pop("_race_probe", None)


# ── 4. cache.hot_tables.HotTableManager — entry coherence ───────────────────────


@pytest.mark.asyncio
async def test_hot_table_entry_coherence_under_concurrent_promotion():
    """Under concurrent promotions and reads of the same hot table, any entry observed via get_entry
    must be internally COHERENT — its column_names match the keys of its rows, and is_hot() agrees
    with the presence of rows. A torn read (rows set before column_names, or vice-versa) is a silent
    corruption; this makes it loud.
    """
    from provisa.cache.hot_tables import HotTableCandidate, HotTableManager

    mgr = HotTableManager(redis_url=None, auto_threshold=1000, max_rows=1000)  # embedded fakeredis
    mgr.register_candidate(
        HotTableCandidate(table_name="orders", pk_column="id", catalog="pg", schema="public")
    )

    stop = False
    violations: list[str] = []

    def _check_entry(entry) -> None:
        if entry is None:
            return
        if entry.rows:
            expected_cols = set(entry.rows[0].keys())
            if set(entry.column_names) != expected_cols:
                violations.append(
                    f"torn entry: column_names={entry.column_names} rows[0]={list(expected_cols)}"
                )
        # is_hot() must agree with the entry's own row presence.
        if bool(entry.rows) != mgr.is_hot("orders"):
            violations.append("is_hot disagrees with entry.rows")

    async def promoter(seed: int):
        # Each promoter offers a differently-shaped (but internally consistent) row batch.
        for i in range(50):
            cols = {f"c{j}": seed * 100 + i for j in range(1 + (seed % 3))}
            cols["id"] = seed * 1000 + i
            await mgr.maybe_promote_dicts("orders", [dict(cols)])
            await asyncio.sleep(0)  # yield to interleave with readers/other promoters

    async def reader():
        while not stop:
            _check_entry(mgr.get_entry("orders"))
            await asyncio.sleep(0)

    readers = [asyncio.create_task(reader()) for _ in range(8)]
    await asyncio.gather(*(promoter(s) for s in range(6)))
    stop = True
    await asyncio.gather(*readers)

    assert not violations, "\n".join(violations[:10])
