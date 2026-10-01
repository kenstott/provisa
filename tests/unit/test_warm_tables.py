# Copyright (c) 2026 Kenneth Stott
# Canary: 36ddbb6f-093a-4cf9-9a7a-c365eb31a0cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for warm tables — query counter, promotion, demotion (REQ-AD5)."""

from __future__ import annotations


import pytest

from provisa.cache.warm_tables import QueryCounter, WarmTableManager


# --- QueryCounter ---


class TestQueryCounter:
    def test_increment_and_get(self):
        c = QueryCounter()
        c.increment("orders")
        c.increment("orders")
        c.increment("users")
        assert c.get_count("orders") == 2
        assert c.get_count("users") == 1
        assert c.get_count("missing") == 0

    def test_get_counts_returns_copy(self):
        c = QueryCounter()
        c.increment("t1")
        counts = c.get_counts()
        counts["t1"] = 999
        assert c.get_count("t1") == 1

    def test_reset(self):
        c = QueryCounter()
        c.increment("t1")
        c.increment("t1")
        c.reset("t1")
        assert c.get_count("t1") == 0

    def test_threshold_detection(self):
        c = QueryCounter()
        for _ in range(100):
            c.increment("hot_table")
        for _ in range(50):
            c.increment("cold_table")
        counts = c.get_counts()
        hot = {t for t, n in counts.items() if n >= 100}
        assert hot == {"hot_table"}


# --- Fake engine terminal ---


class _FakeEngine:
    """Records SQL passed to execute_engine; COUNT(*) returns the configured row count."""

    def __init__(self, count_result=1000):
        self._count = count_result
        self.sqls: list[str] = []

    def engine_physical(self, pg_sql):
        return pg_sql  # an engine that addresses catalog.schema.table as written

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        from provisa.executor.result import QueryResult

        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self._count,)], column_names=["c"])
        return QueryResult(rows=[], column_names=[])


# --- WarmTableManager promotion ---


class TestWarmPromotion:
    async def test_promotes_table_above_threshold(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("my_schema.orders")

        engine = _FakeEngine(5000)

        mgr = WarmTableManager(iceberg_catalog="iceberg", iceberg_schema="warm")
        promoted = await mgr.check_promotions(counter, engine, threshold=100, max_rows=10_000_000)

        assert promoted == ["my_schema.orders"]
        assert "my_schema.orders" in mgr.get_warm_tables()

        # Verify CTAS was issued
        calls = [c for c in engine.sqls]
        assert any("CREATE TABLE" in str(c) for c in calls)
        assert any("SELECT * FROM my_schema.orders" in str(c) for c in calls)

    async def test_skips_below_threshold(self):
        counter = QueryCounter()
        for _ in range(50):
            counter.increment("orders")

        engine = _FakeEngine()

        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(counter, engine, threshold=100)

        assert promoted == []
        assert mgr.get_warm_tables() == set()
        assert engine.sqls == []

    async def test_skips_already_warm(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("orders")

        engine = _FakeEngine(100)

        mgr = WarmTableManager()
        await mgr.check_promotions(counter, engine, threshold=100)
        # Second call — should not re-promote
        engine.sqls.clear()
        promoted = await mgr.check_promotions(counter, engine, threshold=100)
        assert promoted == []

    async def test_size_guard_skips_large_table(self):
        counter = QueryCounter()
        for _ in range(200):
            counter.increment("big_table")

        engine = _FakeEngine(20_000_000)

        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(counter, engine, threshold=100, max_rows=10_000_000)

        assert promoted == []
        assert mgr.get_warm_tables() == set()
        # Only COUNT(*) query should have been issued, not CTAS
        execute_calls = engine.sqls
        assert len(execute_calls) == 1
        assert "COUNT(*)" in str(execute_calls[0])


# --- WarmTableManager demotion ---


class TestWarmDemotion:
    async def test_demotes_table_below_threshold(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("orders")

        engine = _FakeEngine(500)

        mgr = WarmTableManager(iceberg_catalog="iceberg", iceberg_schema="warm")
        await mgr.check_promotions(counter, engine, threshold=100)
        assert "orders" in mgr.get_warm_tables()

        # Reset counter to simulate low usage
        counter.reset("orders")
        engine.sqls.clear()

        demoted = await mgr.check_demotions(counter, engine, threshold=100)
        assert demoted == ["orders"]
        assert mgr.get_warm_tables() == set()

        # Verify DROP TABLE was issued
        calls = [c for c in engine.sqls]
        assert any("DROP TABLE IF EXISTS" in str(c) for c in calls)

    async def test_keeps_warm_table_above_threshold(self):
        counter = QueryCounter()
        for _ in range(200):
            counter.increment("orders")

        engine = _FakeEngine(500)

        mgr = WarmTableManager()
        await mgr.check_promotions(counter, engine, threshold=100)
        engine.sqls.clear()

        demoted = await mgr.check_demotions(counter, engine, threshold=100)
        assert demoted == []
        assert "orders" in mgr.get_warm_tables()

    async def test_get_warm_tables_returns_copy(self):
        mgr = WarmTableManager()
        tables = mgr.get_warm_tables()
        tables.add("injected")
        assert mgr.get_warm_tables() == set()


# --- REQ-240/241: hot precedence, opt-out, force ---


class TestWarmTierMembership:
    async def test_hot_table_not_promoted_to_warm(self):
        # REQ-241: a table the hot tier manages is never also promoted to warm.
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("countries")
        engine = _FakeEngine(50)
        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(
            counter, engine, threshold=100, hot_tables={"countries"}
        )
        assert promoted == []
        assert engine.sqls == []

    async def test_excluded_table_not_promoted(self):
        # REQ-240: warm: false opts a table out of warming.
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("orders")
        engine = _FakeEngine(50)
        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(counter, engine, threshold=100, excluded={"orders"})
        assert promoted == []

    async def test_forced_table_promoted_below_threshold(self):
        # REQ-240: warm: true forces promotion even with no query traffic.
        counter = QueryCounter()  # zero queries
        engine = _FakeEngine(50)
        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(
            counter, engine, threshold=100, forced={"reference_data"}
        )
        assert promoted == ["reference_data"]

    async def test_forced_still_respects_hot_precedence(self):
        counter = QueryCounter()
        engine = _FakeEngine(50)
        mgr = WarmTableManager()
        promoted = await mgr.check_promotions(
            counter, engine, threshold=100, forced={"t"}, hot_tables={"t"}
        )
        assert promoted == []


# --- The engine's own table addressing (per engine) ---

_FQN = '"bench_postgresql"."public"."orders"'


def _runtime(engine_key: str, *, rows: int = 50, fail: dict[str, Exception] | None = None):
    """The real engine runtime for ``engine_key`` with its terminal replaced by a recorder.
    ``fail`` maps a statement keyword to the error the engine answers it with."""
    from types import SimpleNamespace

    from provisa.executor.result import QueryResult
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    runtime = EngineRuntime(build_engine(engine_key), SimpleNamespace())
    sent: list[str] = []
    failing = fail if fail is not None else {}

    async def _execute_engine(sql, *_a, **_k):
        sent.append(sql)
        for keyword, error in failing.items():
            if keyword in sql:
                raise error
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(rows,)], column_names=["c"])
        return QueryResult(rows=[], column_names=[])

    runtime.execute_engine = _execute_engine  # type: ignore[method-assign]
    return runtime, sent, failing


def _busy_counter(table: str = _FQN, queries: int = 100) -> QueryCounter:
    counter = QueryCounter()
    for _ in range(queries):
        counter.increment(table)
    return counter


_WARM_NAME = '"""bench_postgresql"".""public"".""orders"""'
_ADDRESSING = {
    # An engine with a catalog level addresses the source table as registered.
    "trino": ('"bench_postgresql"."public"."orders"', '"iceberg"."warm_cache".' + _WARM_NAME),
    "duckdb": ('"bench_postgresql"."public"."orders"', '"iceberg"."warm_cache".' + _WARM_NAME),
    # Postgres has no catalog level (no cross-database references): catalog folds into schema.
    "pg": ('"bench_postgresql_public"."orders"', '"iceberg_warm_cache".' + _WARM_NAME),
}


class TestEngineAddressing:
    """Every statement the warm tier sends names tables the way the bound engine addresses them."""

    @pytest.mark.parametrize("engine_key", sorted(_ADDRESSING))
    async def test_promotion_addresses_tables_in_the_engines_own_naming(self, engine_key):
        source, warm = _ADDRESSING[engine_key]
        runtime, sent, _ = _runtime(engine_key)
        promoted = await WarmTableManager().check_promotions(
            _busy_counter(), runtime, threshold=100
        )
        assert promoted == [_FQN]
        assert sent == [
            f"SELECT COUNT(*) FROM {source}",
            f"CREATE TABLE {warm} AS SELECT * FROM {source}",
        ]

    @pytest.mark.parametrize("engine_key", sorted(_ADDRESSING))
    async def test_demotion_addresses_the_warm_copy_in_the_engines_own_naming(self, engine_key):
        _, warm = _ADDRESSING[engine_key]
        runtime, sent, _ = _runtime(engine_key)
        mgr = WarmTableManager()
        await mgr.check_promotions(_busy_counter(), runtime, threshold=100)
        sent.clear()
        assert await mgr.check_demotions(QueryCounter(), runtime, threshold=100) == [_FQN]
        assert sent == [f"DROP TABLE IF EXISTS {warm}"]


# --- A failing check is reported once per table per state ---


def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]


class TestFailingCheckIsVisibleOnce:
    async def test_a_failing_size_check_is_logged_once_not_once_per_tick(self, caplog):
        boom = RuntimeError("cross-database references are not implemented")
        runtime, sent, _ = _runtime("pg", fail={"COUNT(*)": boom})
        mgr, counter = WarmTableManager(), _busy_counter()
        with caplog.at_level("INFO", logger="provisa.cache.warm_tables"):
            for _ in range(5):
                assert await mgr.check_promotions(counter, runtime, threshold=100) == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert _FQN in errors[0] and "cross-database references" in errors[0]
        assert 'SELECT COUNT(*) FROM "bench_postgresql_public"."orders"' in errors[0]
        assert len(sent) == 5  # still checked every tick: the failure is not a permanent verdict
        assert mgr.failures() == {_FQN: "size check: RuntimeError: " + str(boom)}

    async def test_a_changed_failure_is_a_new_state_and_recovery_clears_it(self, caplog):
        runtime, _, failing = _runtime("pg", fail={"COUNT(*)": RuntimeError("first")})
        mgr, counter = WarmTableManager(), _busy_counter()
        with caplog.at_level("INFO", logger="provisa.cache.warm_tables"):
            await mgr.check_promotions(counter, runtime, threshold=100)
            failing.clear()
            failing["CREATE TABLE"] = RuntimeError("second")
            await mgr.check_promotions(counter, runtime, threshold=100)
            await mgr.check_promotions(counter, runtime, threshold=100)
            failing.clear()
            assert await mgr.check_promotions(counter, runtime, threshold=100) == [_FQN]
        errors = _errors(caplog)
        assert len(errors) == 2 and "first" in errors[0] and "second" in errors[1]
        assert mgr.failures() == {}
        assert any("recovered" in r.getMessage() for r in caplog.records)

    async def test_one_tables_failure_does_not_stop_the_others(self, caplog):
        other = '"bench_postgresql"."public"."customers"'
        runtime, _, _ = _runtime("pg", fail={'"orders"': RuntimeError("no such table")})
        counter = _busy_counter()
        for _ in range(100):
            counter.increment(other)
        mgr = WarmTableManager()
        with caplog.at_level("INFO", logger="provisa.cache.warm_tables"):
            assert await mgr.check_promotions(counter, runtime, threshold=100) == [other]
        assert list(mgr.failures()) == [_FQN]
        assert len(_errors(caplog)) == 1

    async def test_a_failing_demotion_is_logged_once_and_the_table_stays_warm(self, caplog):
        runtime, _, failing = _runtime("pg")
        mgr = WarmTableManager()
        await mgr.check_promotions(_busy_counter(), runtime, threshold=100)
        failing["DROP TABLE"] = RuntimeError("permission denied")
        with caplog.at_level("INFO", logger="provisa.cache.warm_tables"):
            for _ in range(3):
                assert await mgr.check_demotions(QueryCounter(), runtime, threshold=100) == []
        assert len(_errors(caplog)) == 1
        assert mgr.get_warm_tables() == {_FQN}
        assert mgr.failures() == {_FQN: "demotion: RuntimeError: permission denied"}


# --- The sweep runs in one worker: the scheduler holder ---


class TestSweepRunsUnderTheSchedulerHolder:
    """The sweep sizes, copies and drops tables in the engine's shared store, so only the worker
    holding the deployment's scheduler lock runs it (REQ-1900)."""

    @staticmethod
    async def _sweep_for(seconds: float, *, should_run, runtime, counter, manager=None):
        import asyncio

        from provisa.cache.warm_tables import sweep_loop

        task = asyncio.ensure_future(
            sweep_loop(
                manager if manager is not None else WarmTableManager(),
                counter,
                engine=lambda: runtime,
                hot_tables=set,
                should_run=should_run,
                interval=0.01,
                threshold=100,
                max_rows=10,  # the table is larger: sized every sweep, never copied
            )
        )
        await asyncio.sleep(seconds)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_a_worker_that_does_not_hold_the_lock_sends_nothing(self):
        runtime, sent, _ = _runtime("pg")
        await self._sweep_for(
            0.1, should_run=lambda: False, runtime=runtime, counter=_busy_counter()
        )
        assert sent == []

    async def test_the_holder_sweeps_every_interval(self):
        runtime, sent, _ = _runtime("pg")
        await self._sweep_for(
            0.1, should_run=lambda: True, runtime=runtime, counter=_busy_counter()
        )
        assert len(sent) >= 2
        assert set(sent) == {'SELECT COUNT(*) FROM "bench_postgresql_public"."orders"'}

    async def test_holding_is_asked_again_on_every_sweep(self):
        """The lock can move between workers; a worker that gains it starts sweeping."""
        runtime, sent, _ = _runtime("pg")
        answers = iter([False, False, True])

        def _holds() -> bool:
            return next(answers, True)

        await self._sweep_for(0.15, should_run=_holds, runtime=runtime, counter=_busy_counter())
        assert sent, "the worker never swept after gaining the lock"

    async def test_an_unexpected_sweep_error_is_logged_and_the_loop_goes_on(self, caplog):
        runtime, sent, _ = _runtime("pg")
        calls = {"n": 0}

        def _hot_tables() -> set[str]:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("hot tier unavailable")
            return set()

        import asyncio

        from provisa.cache.warm_tables import sweep_loop

        with caplog.at_level("INFO", logger="provisa.cache.warm_tables"):
            task = asyncio.ensure_future(
                sweep_loop(
                    WarmTableManager(),
                    _busy_counter(),
                    engine=lambda: runtime,
                    hot_tables=_hot_tables,
                    should_run=lambda: True,
                    interval=0.01,
                    threshold=100,
                    max_rows=10,
                )
            )
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert any("Error in warm-table sweep" in m for m in _errors(caplog))
        assert sent, "the loop stopped after one failed sweep"
