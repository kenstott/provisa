# Copyright (c) 2026 Kenneth Stott
# Canary: f54a4e24-a850-48a1-aaba-83f21c5d6c6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for materialized view registry."""

import time

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry


def _mv(
    mv_id="mv-orders-customers",
    left="orders", left_col="customer_id",
    right="customers", right_col="id",
    status=MVStatus.FRESH,
    enabled=True,
    refresh_interval=300,
):
    mv = MVDefinition(
        id=mv_id,
        source_tables=[left, right],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table=left,
            left_column=left_col,
            right_table=right,
            right_column=right_col,
            join_type="left",
        ),
        refresh_interval=refresh_interval,
        enabled=enabled,
    )
    mv.status = status
    return mv


class TestMVRegistryBasic:
    def test_register_and_get(self):
        reg = MVRegistry()
        mv = _mv()
        reg.register(mv)
        assert reg.get("mv-orders-customers") is mv
        assert reg.get("nonexistent") is None

    def test_all(self):
        reg = MVRegistry()
        mv1 = _mv(mv_id="mv1")
        mv2 = _mv(mv_id="mv2")
        reg.register(mv1)
        reg.register(mv2)
        assert len(reg.all()) == 2

    def test_register_replaces_existing(self):
        reg = MVRegistry()
        mv1 = _mv(mv_id="mv1", status=MVStatus.FRESH)
        mv2 = _mv(mv_id="mv1", status=MVStatus.STALE)
        reg.register(mv1)
        reg.register(mv2)
        assert reg.get("mv1").status == MVStatus.STALE
        assert len(reg.all()) == 1


class TestMVRegistryFresh:
    def test_get_fresh_returns_only_fresh_enabled(self):
        reg = MVRegistry()
        mv1 = _mv(mv_id="mv1", status=MVStatus.FRESH)
        mv2 = _mv(mv_id="mv2", status=MVStatus.STALE)
        mv3 = _mv(mv_id="mv3", status=MVStatus.FRESH, enabled=False)
        reg.register(mv1)
        reg.register(mv2)
        reg.register(mv3)
        fresh = reg.get_fresh()
        assert len(fresh) == 1
        assert fresh[0].id == "mv1"

    def test_get_fresh_empty_registry(self):
        reg = MVRegistry()
        assert reg.get_fresh() == []


class TestMVRegistryEnabled:
    def test_get_enabled(self):
        reg = MVRegistry()
        mv1 = _mv(mv_id="mv1", enabled=True)
        mv2 = _mv(mv_id="mv2", enabled=False)
        reg.register(mv1)
        reg.register(mv2)
        enabled = reg.get_enabled()
        assert len(enabled) == 1
        assert enabled[0].id == "mv1"


class TestMVRegistryDueForRefresh:
    def test_never_refreshed_is_due(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.STALE)
        reg.register(mv)
        assert len(reg.get_due_for_refresh()) == 1

    def test_recently_refreshed_not_due(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH, refresh_interval=300)
        mv.last_refresh_at = time.time()
        reg.register(mv)
        assert len(reg.get_due_for_refresh()) == 0

    def test_expired_refresh_is_due(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH, refresh_interval=300)
        mv.last_refresh_at = time.time() - 400
        reg.register(mv)
        assert len(reg.get_due_for_refresh()) == 1

    def test_refreshing_not_due(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.REFRESHING)
        reg.register(mv)
        assert len(reg.get_due_for_refresh()) == 0

    def test_disabled_not_due(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.STALE, enabled=False)
        reg.register(mv)
        assert len(reg.get_due_for_refresh()) == 0


class TestMVRegistryStateTransitions:
    def test_mark_stale_by_table(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH)
        reg.register(mv)
        affected = reg.mark_stale("orders")
        assert affected == ["mv-orders-customers"]
        assert mv.status == MVStatus.STALE

    def test_mark_stale_unrelated_table(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH)
        reg.register(mv)
        affected = reg.mark_stale("products")
        assert affected == []
        assert mv.status == MVStatus.FRESH

    def test_mark_stale_skips_disabled(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.DISABLED)
        reg.register(mv)
        affected = reg.mark_stale("orders")
        assert affected == []
        assert mv.status == MVStatus.DISABLED

    def test_mark_refreshing(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.STALE)
        reg.register(mv)
        reg.mark_refreshing("mv-orders-customers")
        assert mv.status == MVStatus.REFRESHING

    def test_mark_refreshed(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.REFRESHING)
        reg.register(mv)
        reg.mark_refreshed("mv-orders-customers", row_count=1000)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 1000
        assert mv.last_refresh_at is not None
        assert mv.last_error is None

    def test_mark_refresh_failed(self):
        reg = MVRegistry()
        mv = _mv(status=MVStatus.REFRESHING)
        reg.register(mv)
        reg.mark_refresh_failed("mv-orders-customers", "connection timeout")
        assert mv.status == MVStatus.STALE
        assert mv.last_error == "connection timeout"

    def test_mark_refreshing_nonexistent_is_noop(self):
        reg = MVRegistry()
        reg.mark_refreshing("nonexistent")  # should not raise

    def test_mark_refreshed_nonexistent_is_noop(self):
        reg = MVRegistry()
        reg.mark_refreshed("nonexistent", 0)  # should not raise

    def test_mark_refresh_failed_nonexistent_is_noop(self):
        reg = MVRegistry()
        reg.mark_refresh_failed("nonexistent", "err")  # should not raise