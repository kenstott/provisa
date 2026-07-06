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

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        from provisa.executor.trino import QueryResult

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
