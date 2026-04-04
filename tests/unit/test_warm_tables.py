# Copyright (c) 2025 Kenneth Stott
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

from unittest.mock import MagicMock, call

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


# --- Mock Trino helpers ---


def _mock_cursor(count_result=1000, fetchall_result=None):
    cursor = MagicMock()
    cursor.fetchone.return_value = (count_result,)
    cursor.fetchall.return_value = fetchall_result or []
    return cursor


def _mock_trino(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# --- WarmTableManager promotion ---


class TestWarmPromotion:
    def test_promotes_table_above_threshold(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("my_schema.orders")

        cursor = _mock_cursor(count_result=5000)
        conn = _mock_trino(cursor)

        mgr = WarmTableManager(iceberg_catalog="iceberg", iceberg_schema="warm")
        promoted = mgr.check_promotions(counter, conn, threshold=100, max_rows=10_000_000)

        assert promoted == ["my_schema.orders"]
        assert "my_schema.orders" in mgr.get_warm_tables()

        # Verify CTAS was issued
        calls = [c for c in cursor.execute.call_args_list]
        assert any("CREATE TABLE" in str(c) for c in calls)
        assert any("SELECT * FROM my_schema.orders" in str(c) for c in calls)

    def test_skips_below_threshold(self):
        counter = QueryCounter()
        for _ in range(50):
            counter.increment("orders")

        cursor = _mock_cursor()
        conn = _mock_trino(cursor)

        mgr = WarmTableManager()
        promoted = mgr.check_promotions(counter, conn, threshold=100)

        assert promoted == []
        assert mgr.get_warm_tables() == set()
        cursor.execute.assert_not_called()

    def test_skips_already_warm(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("orders")

        cursor = _mock_cursor(count_result=100)
        conn = _mock_trino(cursor)

        mgr = WarmTableManager()
        mgr.check_promotions(counter, conn, threshold=100)
        # Second call — should not re-promote
        cursor.reset_mock()
        promoted = mgr.check_promotions(counter, conn, threshold=100)
        assert promoted == []

    def test_size_guard_skips_large_table(self):
        counter = QueryCounter()
        for _ in range(200):
            counter.increment("big_table")

        cursor = _mock_cursor(count_result=20_000_000)
        conn = _mock_trino(cursor)

        mgr = WarmTableManager()
        promoted = mgr.check_promotions(counter, conn, threshold=100, max_rows=10_000_000)

        assert promoted == []
        assert mgr.get_warm_tables() == set()
        # Only COUNT(*) query should have been issued, not CTAS
        execute_calls = cursor.execute.call_args_list
        assert len(execute_calls) == 1
        assert "COUNT(*)" in str(execute_calls[0])


# --- WarmTableManager demotion ---


class TestWarmDemotion:
    def test_demotes_table_below_threshold(self):
        counter = QueryCounter()
        for _ in range(100):
            counter.increment("orders")

        cursor = _mock_cursor(count_result=500)
        conn = _mock_trino(cursor)

        mgr = WarmTableManager(iceberg_catalog="iceberg", iceberg_schema="warm")
        mgr.check_promotions(counter, conn, threshold=100)
        assert "orders" in mgr.get_warm_tables()

        # Reset counter to simulate low usage
        counter.reset("orders")
        cursor.reset_mock()

        demoted = mgr.check_demotions(counter, conn, threshold=100)
        assert demoted == ["orders"]
        assert mgr.get_warm_tables() == set()

        # Verify DROP TABLE was issued
        calls = [c for c in cursor.execute.call_args_list]
        assert any("DROP TABLE IF EXISTS" in str(c) for c in calls)

    def test_keeps_warm_table_above_threshold(self):
        counter = QueryCounter()
        for _ in range(200):
            counter.increment("orders")

        cursor = _mock_cursor(count_result=500)
        conn = _mock_trino(cursor)

        mgr = WarmTableManager()
        mgr.check_promotions(counter, conn, threshold=100)
        cursor.reset_mock()

        demoted = mgr.check_demotions(counter, conn, threshold=100)
        assert demoted == []
        assert "orders" in mgr.get_warm_tables()

    def test_get_warm_tables_returns_copy(self):
        mgr = WarmTableManager()
        tables = mgr.get_warm_tables()
        tables.add("injected")
        assert mgr.get_warm_tables() == set()
