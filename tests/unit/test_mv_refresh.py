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

from unittest.mock import MagicMock, call

import pytest

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql, _target_ref, refresh_mv
from provisa.mv.registry import MVRegistry

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


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


class TestBuildRefreshSQL:
    def test_custom_sql_used_directly(self):
        mv = _sql_mv()
        result = _build_refresh_sql(mv)
        assert result == mv.sql

    def test_join_pattern_without_introspection(self):
        mv = _jp_mv()
        result = _build_refresh_sql(mv)
        assert '"orders".*' in result
        assert 'LEFT JOIN "customers"' in result
        assert 'ON "orders"."customer_id" = "customers"."id"' in result

    def test_join_pattern_with_column_introspection(self):
        mv = _jp_mv()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("id",), ("name",), ("email",)]

        result = _build_refresh_sql(mv, mock_conn)
        assert '"customers"."id" AS "customers__id"' in result
        assert '"customers"."name" AS "customers__name"' in result
        assert '"customers"."email" AS "customers__email"' in result
        assert '"orders".*' in result

    def test_join_pattern_introspection_fallback(self):
        mv = _jp_mv()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("introspection failed")

        result = _build_refresh_sql(mv, mock_conn)
        # Falls back to left.* only
        assert '"orders".*' in result

    def test_no_sql_no_join_raises(self):
        mv = MVDefinition(
            id="bad-mv",
            source_tables=[],
            target_catalog="pg",
            target_schema="mv",
        )
        with pytest.raises(ValueError, match="neither sql nor join_pattern"):
            _build_refresh_sql(mv)


class TestTargetRef:
    def test_fully_qualified(self):
        mv = _jp_mv()
        ref = _target_ref(mv)
        assert ref == '"postgresql"."mv_cache"."mv_mv_orders_customers"'


class TestRefreshMV:
    async def test_first_refresh_creates_table(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Table does not exist — raise on "SELECT 1 FROM" check
        from trino.exceptions import TrinoUserError
        def execute_side_effect(sql):
            if "SELECT 1 FROM" in sql:
                raise TrinoUserError({"errorName": "TABLE_NOT_FOUND", "message": "not found"})
        mock_cursor.execute.side_effect = execute_side_effect
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (42,)

        await refresh_mv(mock_conn, mv, registry)

        # Should have done CREATE TABLE AS
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("CREATE TABLE" in c for c in calls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 42

    async def test_subsequent_refresh_deletes_and_inserts(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (100,)

        await refresh_mv(mock_conn, mv, registry)

        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("DELETE FROM" in c for c in calls)
        assert any("INSERT INTO" in c for c in calls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 100

    async def test_refresh_failure_marks_stale(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("connection lost")

        await refresh_mv(mock_conn, mv, registry)

        assert mv.status == MVStatus.STALE
        assert mv.last_error == "connection lost"

    async def test_refresh_marks_refreshing_during_execution(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        statuses_seen = []

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        original_execute = mock_cursor.execute
        def capture_status(sql):
            statuses_seen.append(mv.status)
        mock_cursor.execute.side_effect = capture_status
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (10,)

        await refresh_mv(mock_conn, mv, registry)

        assert MVStatus.REFRESHING in statuses_seen