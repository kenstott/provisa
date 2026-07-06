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

import pytest

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.executor.result import QueryResult
from provisa.mv.refresh import (
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


class _FakeEngine:
    """Records SQL through the engine terminal; COUNT(*)/SHOW TABLES/SHOW COLUMNS return
    configured rows. ``fail_exists`` raises on the SELECT-1 existence probe (table absent)."""

    def __init__(self, count=0, show_tables=None, show_columns=None, fail_exists=False):
        self.count = count
        self.show_tables = show_tables or []
        self.show_columns = show_columns or []
        self.fail_exists = fail_exists
        self.sqls: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if self.fail_exists and "SELECT 1 FROM" in sql:
            raise Exception("TABLE_NOT_FOUND")
        if "SHOW TABLES" in sql:
            return QueryResult(rows=self.show_tables, column_names=[])
        if "SHOW COLUMNS" in sql:
            return QueryResult(rows=self.show_columns, column_names=[])
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


class TestSizeGuard:
    @pytest.mark.asyncio
    async def test_skips_when_count_exceeds_max_rows(self):
        """MV with source count > max_rows gets status skipped_size."""
        mv = _mv(max_rows=500)
        registry = MVRegistry()
        registry.register(mv)

        await refresh_mv(_FakeEngine(count=1000), mv, registry)

        assert mv.status == MVStatus.SKIPPED_SIZE
        assert "exceeds max_rows" in mv.last_error

    @pytest.mark.asyncio
    async def test_proceeds_when_count_within_max_rows(self):
        """MV with source count <= max_rows proceeds with refresh."""
        mv = _mv(max_rows=2000)
        registry = MVRegistry()
        registry.register(mv)

        await refresh_mv(_FakeEngine(count=100, fail_exists=True), mv, registry)

        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 100

    @pytest.mark.asyncio
    async def test_size_guard_exact_boundary(self):
        """MV with source count == max_rows proceeds (not exceeded)."""
        mv = _mv(max_rows=1000)
        registry = MVRegistry()
        registry.register(mv)

        await refresh_mv(_FakeEngine(count=1000, fail_exists=True), mv, registry)

        assert mv.status == MVStatus.FRESH


class TestReclamation:
    async def test_identifies_removed_mvs(self):
        """MVs in registry but not in config get reclaimed."""
        registry = MVRegistry()
        mv1 = _mv(mv_id="keep-me", target_table="mv_keep_me")
        mv2 = _mv(mv_id="remove-me", target_table="mv_remove_me")
        registry.register(mv1)
        registry.register(mv2)

        engine = _FakeEngine()

        config_ids = {"keep-me"}
        reclaimed = await reclaim_removed_mvs(engine, registry, config_ids)

        assert reclaimed == ["remove-me"]
        assert registry.get("remove-me") is None
        assert registry.get("keep-me") is not None

    async def test_no_reclamation_when_all_present(self):
        """No reclamation when all registry MVs are in config."""
        registry = MVRegistry()
        mv1 = _mv(mv_id="mv1")
        registry.register(mv1)

        engine = _FakeEngine()

        reclaimed = await reclaim_removed_mvs(engine, registry, {"mv1"})
        assert reclaimed == []

    async def test_reclamation_drops_table(self):
        """Reclamation executes DROP TABLE IF EXISTS."""
        registry = MVRegistry()
        mv = _mv(mv_id="gone", target_table="mv_gone")
        registry.register(mv)

        engine = _FakeEngine()

        await reclaim_removed_mvs(engine, registry, set())

        assert len(engine.sqls) == 1
        sql = engine.sqls[0]
        assert "DROP TABLE IF EXISTS" in sql
        assert "mv_gone" in sql


class TestOrphanDetection:
    async def test_flags_unknown_tables(self):
        """Tables in schema but not in registry are flagged as orphans."""
        registry = MVRegistry()
        mv = _mv(mv_id="known", target_table="mv_known")
        registry.register(mv)

        engine = _FakeEngine(show_tables=[("mv_known",), ("mv_unknown",), ("mv_stale",)])

        orphans = await detect_orphans(engine, registry, "mv_cache")

        assert orphans == ["mv_stale", "mv_unknown"]

    async def test_no_orphans_when_all_known(self):
        """No orphans when all schema tables are in registry."""
        registry = MVRegistry()
        mv = _mv(mv_id="known", target_table="mv_known")
        registry.register(mv)

        engine = _FakeEngine(show_tables=[("mv_known",)])

        orphans = await detect_orphans(engine, registry, "mv_cache")
        assert orphans == []


class TestOrphanGracePeriod:
    async def test_grace_period_prevents_premature_deletion(self):
        """Orphans within grace period are not dropped."""
        tracker: dict[str, float] = {}
        engine = _FakeEngine()

        dropped = await drop_expired_orphans(
            engine,
            tracker,
            ["orphan_table"],
            grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == []
        assert "orphan_table" in tracker
        # Cursor should NOT have been called for DROP
        assert engine.sqls == []

    async def test_drops_after_grace_period(self):
        """Orphans past grace period are dropped."""
        tracker: dict[str, float] = {
            "old_orphan": time.time() - 90000,  # >24h ago
        }
        engine = _FakeEngine()

        dropped = await drop_expired_orphans(
            engine,
            tracker,
            ["old_orphan"],
            grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == ["old_orphan"]
        assert "old_orphan" not in tracker
        assert len(engine.sqls) == 1
        sql = engine.sqls[0]
        assert "DROP TABLE IF EXISTS" in sql

    async def test_removes_resolved_orphans_from_tracker(self):
        """Orphans that disappear from the list are removed from tracker."""
        tracker: dict[str, float] = {
            "was_orphan": time.time() - 100,
        }
        engine = _FakeEngine()

        # Empty orphan list means "was_orphan" is no longer orphaned
        dropped = await drop_expired_orphans(
            engine,
            tracker,
            [],
            grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == []
        assert "was_orphan" not in tracker

    async def test_mixed_orphans_only_expired_dropped(self):
        """Only orphans past grace period are dropped; fresh ones are kept."""
        now = time.time()
        tracker: dict[str, float] = {
            "old": now - 90000,
            "new": now - 100,
        }
        engine = _FakeEngine()

        dropped = await drop_expired_orphans(
            engine,
            tracker,
            ["old", "new"],
            grace_period=86400,
            schema_name="mv_cache",
        )

        assert dropped == ["old"]
        assert "old" not in tracker
        assert "new" in tracker
