# Copyright (c) 2026 Kenneth Stott
# Canary: e3a7f2b1-5c9d-4e8a-b6f0-1d2c3a4b5e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for federation hint extraction (Phase AL)."""

from __future__ import annotations

import pytest

from provisa.compiler.hints import extract_hints


class TestExtractHints:
    def test_no_hints(self):
        sql = "SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert cleaned == sql
        assert props == {}

    def test_broadcast_hint(self):
        sql = "/*+ BROADCAST(orders) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "BROADCAST" not in cleaned
        assert props["join_distribution_type"] == "BROADCAST"

    def test_no_reorder_hint(self):
        sql = "/*+ NO_REORDER */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "NO_REORDER" not in cleaned
        assert props["join_reordering_strategy"] == "NONE"

    def test_broadcast_size_hint(self):
        sql = "/*+ BROADCAST_SIZE(orders, 512MB) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert props["join_max_broadcast_table_size"] == "512MB"

    def test_multiple_hints(self):
        sql = "/*+ BROADCAST(t1) NO_REORDER */ SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        cleaned, props = extract_hints(sql)
        assert props["join_distribution_type"] == "BROADCAST"
        assert props["join_reordering_strategy"] == "NONE"
        assert "/*+" not in cleaned

    def test_hint_removed_from_sql(self):
        sql = "/*+ BROADCAST(orders) */ SELECT id, amount FROM orders WHERE amount > 100"
        cleaned, _ = extract_hints(sql)
        assert cleaned == "SELECT id, amount FROM orders WHERE amount > 100"

    def test_broadcast_size_without_table_arg_ignored(self):
        """BROADCAST_SIZE requires at least 2 args; with 1 arg it should not set a prop."""
        sql = "/*+ BROADCAST_SIZE(orders) */ SELECT id FROM orders"
        _, props = extract_hints(sql)
        assert "join_max_broadcast_table_size" not in props

    def test_unknown_hint_ignored(self):
        sql = "/*+ UNKNOWN_HINT(foo) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert props == {}
        assert "UNKNOWN_HINT" not in cleaned

    def test_hint_case_insensitive(self):
        sql = "/*+ broadcast(orders) no_reorder */ SELECT id FROM orders"
        _, props = extract_hints(sql)
        assert props["join_distribution_type"] == "BROADCAST"
        assert props["join_reordering_strategy"] == "NONE"

    def test_sql_without_hint_unchanged_content(self):
        sql = "SELECT /* regular comment */ id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "regular comment" in cleaned
        assert props == {}


class TestExecuteTrinoSessionHints:
    """Verify that session_hints are injected as SET SESSION before the main query."""

    def test_session_hints_trigger_set_session(self):
        from unittest.mock import MagicMock
        from provisa.executor.trino import execute_trino

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        execute_trino(
            mock_conn,
            "SELECT id FROM orders",
            session_hints={"join_distribution_type": "BROADCAST"},
        )

        # cursor().execute should have been called for SET SESSION + main query
        calls = [c.args[0] for c in mock_cursor.execute.call_args_list]
        set_calls = [c for c in calls if c.startswith("SET SESSION")]
        assert len(set_calls) == 1
        assert "join_distribution_type" in set_calls[0]
        assert "BROADCAST" in set_calls[0]

    def test_no_session_hints_no_set_session(self):
        from unittest.mock import MagicMock
        from provisa.executor.trino import execute_trino

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        execute_trino(mock_conn, "SELECT id FROM orders")

        calls = [c.args[0] for c in mock_cursor.execute.call_args_list]
        set_calls = [c for c in calls if c.startswith("SET SESSION")]
        assert set_calls == []
