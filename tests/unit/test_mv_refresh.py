# Copyright (c) 2026 Kenneth Stott
# Canary: 0d9da60a-a199-4890-a8cc-1cb2f7295a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for materialized view refresh engine.

Uses a mock Trino connection to test CTAS, atomic refresh, row count
tracking, and error handling without requiring a live Trino instance.
"""

from __future__ import annotations

import pytest

from provisa.executor.result import QueryResult
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql, _target_ref, refresh_mv
from provisa.mv.registry import MVRegistry


class _FakeEngine:
    """Records SQL; SHOW COLUMNS/COUNT(*) return configured rows. ``side_effect`` and
    ``raise_all`` reproduce the old cursor.execute side effects (e.g. table-not-found)."""

    def __init__(self, count=0, show_columns=None, side_effect=None, raise_all=None):
        self.count = count
        self.show_columns = show_columns or []
        self.sqls: list[str] = []
        self._side_effect = side_effect
        self._raise_all = raise_all

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if self._side_effect is not None:
            self._side_effect(sql)
        if self._raise_all is not None:
            raise self._raise_all
        if "SHOW COLUMNS" in sql:
            return QueryResult(rows=self.show_columns, column_names=[])
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


def _jp_mv(mv_id="mv-orders-customers"):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
    )


def _sql_mv(mv_id="mv-customer-stats"):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        sql=(
            "SELECT c.id AS customer_id, c.name, "
            "COUNT(o.id) AS order_count "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "GROUP BY c.id, c.name"
        ),
        refresh_interval=600,
    )


@pytest.mark.asyncio(loop_scope="session")
class TestBuildRefreshSQL:
    async def test_custom_sql_used_directly(self):
        mv = _sql_mv()
        result = await _build_refresh_sql(mv)
        assert result == mv.sql

    async def test_join_pattern_without_introspection(self):
        mv = _jp_mv()
        result = await _build_refresh_sql(mv)
        assert '"orders".*' in result
        assert 'LEFT JOIN "customers"' in result
        assert 'ON "orders"."customer_id" = "customers"."id"' in result

    async def test_join_pattern_with_column_introspection(self):
        mv = _jp_mv()
        engine = _FakeEngine(show_columns=[("id",), ("name",), ("email",)])
        result = await _build_refresh_sql(mv, engine)
        assert '"customers"."id" AS "customers__id"' in result
        assert '"customers"."name" AS "customers__name"' in result
        assert '"customers"."email" AS "customers__email"' in result
        assert '"orders".*' in result

    async def test_join_pattern_introspection_fallback(self):
        mv = _jp_mv()
        engine = _FakeEngine(raise_all=Exception("introspection failed"))
        result = await _build_refresh_sql(mv, engine)
        # Falls back to left.* only
        assert '"orders".*' in result

    async def test_no_sql_no_join_raises(self):
        mv = MVDefinition(
            id="bad-mv",
            source_tables=[],
            target_catalog="pg",
            target_schema="mv",
        )
        with pytest.raises(ValueError, match="neither sql nor join_pattern"):
            await _build_refresh_sql(mv)


class TestTargetRef:
    def test_fully_qualified(self):
        mv = _jp_mv()
        ref = _target_ref(mv)
        assert ref == '"postgresql"."mv_cache"."mv_mv_orders_customers"'


@pytest.mark.asyncio(loop_scope="session")
class TestRefreshMV:
    async def test_first_refresh_creates_table(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        # Table does not exist — raise on the "SELECT 1 FROM" existence probe.
        def side_effect(sql):
            if "SELECT 1 FROM" in sql:
                raise Exception("TABLE_NOT_FOUND")

        engine = _FakeEngine(count=42, side_effect=side_effect)
        await refresh_mv(engine, mv, registry)

        assert any("CREATE TABLE" in c for c in engine.sqls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 42

    async def test_subsequent_refresh_deletes_and_inserts(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        engine = _FakeEngine(count=100)  # SELECT 1 succeeds -> table exists
        await refresh_mv(engine, mv, registry)

        assert any("DELETE FROM" in c for c in engine.sqls)
        assert any("INSERT INTO" in c for c in engine.sqls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 100

    async def test_refresh_failure_marks_stale(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        engine = _FakeEngine(raise_all=Exception("connection lost"))
        await refresh_mv(engine, mv, registry)

        assert mv.status == MVStatus.STALE
        assert mv.last_error == "connection lost"

    async def test_refresh_marks_refreshing_during_execution(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        statuses_seen = []

        def capture_status(sql):
            statuses_seen.append(mv.status)

        engine = _FakeEngine(count=10, side_effect=capture_status)
        await refresh_mv(engine, mv, registry)

        assert MVStatus.REFRESHING in statuses_seen
