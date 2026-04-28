# Copyright (c) 2026 Kenneth Stott
# Canary: faf48f4c-8848-436e-96d3-1bbb45c9cbe1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for MV lifecycle: size guards, reclamation, orphan detection."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.refresh import (
    _probe_source_count,
    detect_orphans,
    drop_expired_orphans,
    reclaim_removed_mvs,
    refresh_mv,
)


def _mv(
    mv_id="mv-test",
    left="orders",
    right="customers",
    max_rows=1_000_000,
    orphan_grace_period=86400,
    enabled=True,
    target_table=None,
):
    return MVDefinition(
        id=mv_id,
        source_tables=[left, right],
        target_catalog="postgresql",
        target_schema="mv_cache",
        target_table=target_table,
        join_pattern=JoinPattern(
            left_table=left,
            left_column="customer_id",
            right_table=right,
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
        enabled=enabled,
        max_rows=max_rows,
        orphan_grace_period=orphan_grace_period,
    )


def _mock_cursor(fetchone_val=None, fetchall_val=None):
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_val
    cursor.fetchall.return_value = fetchall_val or []
    return cursor


def _mock_trino_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestSizeGuard:
    @pytest.mark.asyncio
    async def test_skips_when_count_exceeds_max_rows(self):
        """MV with source count > max_rows gets status skipped_size."""
        mv = _mv(max_rows=500)
        registry = MVRegistry()
        registry.register(mv)

        cursor = _mock_cursor(fetchone_val=(1000,))
        conn = _mock_trino_conn(cursor)

        await refresh_mv(conn, mv, registry)

        assert mv.status == MVStatus.SKIPPED_SIZE
        assert "exceeds max_rows" in mv.last_error

    @pytest.mark.asyncio
    async def test_proceeds_when_count_within_max_rows(self):
        """MV with source count <= max_rows proceeds with refresh."""
        mv = _mv(max_rows=2000)
        registry = MVRegistry()
        registry.register(mv)

        # First call: probe count. Second+: table existence check, etc.
        call_count = 0
        results = []

        def _execute_side_effect(sql):
            nonlocal call_count
            call_count += 1
            # Store for fetchone/fetchall routing
            results.append(sql)

        cursor = MagicMock()
        cursor.execute.side_effect = _execute_side_effect

        fetchone_calls = 0

        def _fetchone():
            nonlocal fetchone_calls
            fetchone_calls += 1
            if fetchone_calls == 1:
                return (100,)  # probe count
            return (100,)  # final row count

        cursor.fetchone.side_effect = _fetchone
        cursor.fetchall.return_value = []

        conn = _mock_trino_conn(cursor)

        # Simulate table not existing (TrinoUserError on existence check)
        import trino.exceptions
        original_execute = cursor.execute.side_effect
        exec_count = [0]

        def _execute_with_error(sql):
            exec_count[0] += 1
            if exec_count[0] == 3:  # The "SELECT 1 FROM target LIMIT 0" check
                raise trino.exceptions.TrinoUserError(
                    {"errorName": "TABLE_NOT_FOUND", "message": "not found"},
                    "not found",
                )

        cursor.execute.side_effect = _execute_with_error

        await refresh_mv(conn, mv, registry)

        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 100

    @pytest.mark.asyncio
    async def test_size_guard_exact_boundary(self):
        """MV with source count == max_rows proceeds (not exceeded)."""
        mv = _mv(max_rows=1000)
        registry = MVRegistry()
        registry.register(mv)

        exec_count = [0]
        fetchone_count = [0]

        def _execute(sql):
            exec_count[0] += 1
            if exec_count[0] == 3:
                import trino.exceptions
                raise trino.exceptions.TrinoUserError(
                    {"errorName": "TABLE_NOT_FOUND", "message": "not found"},
                    "not found",
                )

        def _fetchone():
            fetchone_count[0] += 1
            return (1000,)  # probe=1000 (equal, not exceeded), row_count=1000

        cursor = MagicMock()
        cursor.execute.side_effect = _execute
        cursor.fetchone.side_effect = _fetchone
        cursor.fetchall.return_value = []
        conn = _mock_trino_conn(cursor)

        await refresh_mv(conn, mv, registry)

        assert mv.status == MVStatus.FRESH


class TestReclamation:
    def test_identifies_removed_mvs(self):
        """MVs in registry but not in config get reclaimed."""
        registry = MVRegistry()
        mv1 = _mv(mv_id="keep-me", target_table="mv_keep_me")
        mv2 = _mv(mv_id="remove-me", target_table="mv_remove_me")
        registry.register(mv1)
        registry.register(mv2)

        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        config_ids = {"keep-me"}
        reclaimed = reclaim_removed_mvs(conn, registry, config_ids)

        assert reclaimed == ["remove-me"]
        assert registry.get("remove-me") is None
        assert registry.get("keep-me") is not None

    def test_no_reclamation_when_all_present(self):
        """No reclamation when all registry MVs are in config."""
        registry = MVRegistry()
        mv1 = _mv(mv_id="mv1")
        registry.register(mv1)

        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        reclaimed = reclaim_removed_mvs(conn, registry, {"mv1"})
        assert reclaimed == []

    def test_reclamation_drops_table(self):
        """Reclamation executes DROP TABLE IF EXISTS."""
        registry = MVRegistry()
        mv = _mv(mv_id="gone", target_table="mv_gone")
        registry.register(mv)

        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        reclaim_removed_mvs(conn, registry, set())

        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "DROP TABLE IF EXISTS" in sql
        assert "mv_gone" in sql


class TestOrphanDetection:
    def test_flags_unknown_tables(self):
        """Tables in schema but not in registry are flagged as orphans."""
        registry = MVRegistry()
        mv = _mv(mv_id="known", target_table="mv_known")
        registry.register(mv)

        cursor = MagicMock()
        cursor.fetchall.return_value = [("mv_known",), ("mv_unknown",), ("mv_stale",)]
        conn = _mock_trino_conn(cursor)

        orphans = detect_orphans(conn, registry, "mv_cache")

        assert orphans == ["mv_stale", "mv_unknown"]

    def test_no_orphans_when_all_known(self):
        """No orphans when all schema tables are in registry."""
        registry = MVRegistry()
        mv = _mv(mv_id="known", target_table="mv_known")
        registry.register(mv)

        cursor = MagicMock()
        cursor.fetchall.return_value = [("mv_known",)]
        conn = _mock_trino_conn(cursor)

        orphans = detect_orphans(conn, registry, "mv_cache")
        assert orphans == []


class TestOrphanGracePeriod:
    def test_grace_period_prevents_premature_deletion(self):
        """Orphans within grace period are not dropped."""
        tracker: dict[str, float] = {}
        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        dropped = drop_expired_orphans(
            conn, tracker, ["orphan_table"], grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == []
        assert "orphan_table" in tracker
        # Cursor should NOT have been called for DROP
        cursor.execute.assert_not_called()

    def test_drops_after_grace_period(self):
        """Orphans past grace period are dropped."""
        tracker: dict[str, float] = {
            "old_orphan": time.time() - 90000,  # >24h ago
        }
        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        dropped = drop_expired_orphans(
            conn, tracker, ["old_orphan"], grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == ["old_orphan"]
        assert "old_orphan" not in tracker
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "DROP TABLE IF EXISTS" in sql

    def test_removes_resolved_orphans_from_tracker(self):
        """Orphans that disappear from the list are removed from tracker."""
        tracker: dict[str, float] = {
            "was_orphan": time.time() - 100,
        }
        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        # Empty orphan list means "was_orphan" is no longer orphaned
        dropped = drop_expired_orphans(
            conn, tracker, [], grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == []
        assert "was_orphan" not in tracker

    def test_mixed_orphans_only_expired_dropped(self):
        """Only orphans past grace period are dropped; fresh ones are kept."""
        now = time.time()
        tracker: dict[str, float] = {
            "old": now - 90000,
            "new": now - 100,
        }
        cursor = _mock_cursor()
        conn = _mock_trino_conn(cursor)

        dropped = drop_expired_orphans(
            conn, tracker, ["old", "new"], grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == ["old"]
        assert "old" not in tracker
        assert "new" in tracker
