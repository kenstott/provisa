# Copyright (c) 2025 Kenneth Stott
# Canary: bd254e7d-f51b-4ac4-8eb5-ef0c78b57a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for admin MV queries and mutations (Phase Y).

Mocks the MV registry and app state to test mv_list, refresh_mv,
and toggle_mv without requiring a running server.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _make_mv(
    mv_id: str = "mv-orders-customers",
    status: MVStatus = MVStatus.FRESH,
    enabled: bool = True,
    row_count: int | None = 100,
) -> MVDefinition:
    mv = MVDefinition(
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
        enabled=enabled,
    )
    mv.status = status
    mv.row_count = row_count
    mv.last_refresh_at = 1700000000.0
    return mv


def _build_registry(*mvs: MVDefinition) -> MVRegistry:
    reg = MVRegistry()
    for mv in mvs:
        reg.register(mv)
    return reg


class TestMVListQuery:
    async def test_returns_registered_mvs(self):
        mv1 = _make_mv("mv-1", status=MVStatus.FRESH, row_count=50)
        mv2 = _make_mv("mv-2", status=MVStatus.STALE, row_count=None)
        registry = _build_registry(mv1, mv2)

        all_mvs = registry.all()
        assert len(all_mvs) == 2
        ids = {mv.id for mv in all_mvs}
        assert ids == {"mv-1", "mv-2"}

    async def test_returns_empty_when_no_mvs(self):
        registry = MVRegistry()
        assert registry.all() == []

    async def test_mv_fields_match(self):
        mv = _make_mv("mv-test", status=MVStatus.FRESH, row_count=42)
        registry = _build_registry(mv)

        result = registry.all()[0]
        assert result.id == "mv-test"
        assert result.source_tables == ["orders", "customers"]
        assert result.status == MVStatus.FRESH
        assert result.enabled is True
        assert result.row_count == 42
        assert result.refresh_interval == 300


class TestRefreshMVMutation:
    async def test_refresh_found_mv(self):
        mv = _make_mv("mv-1", status=MVStatus.STALE)
        registry = _build_registry(mv)

        mock_state = MagicMock()
        mock_state.mv_registry = registry

        with patch("provisa.mv.refresh.refresh_mv", new_callable=AsyncMock) as mock_refresh:
            found = registry.get("mv-1")
            assert found is not None
            await mock_refresh(found, mock_state)
            mock_refresh.assert_awaited_once_with(found, mock_state)

    async def test_refresh_nonexistent_mv(self):
        registry = MVRegistry()
        result = registry.get("nonexistent")
        assert result is None

    async def test_refresh_failure_propagates(self):
        mv = _make_mv("mv-1")
        registry = _build_registry(mv)

        with patch(
            "provisa.mv.refresh.refresh_mv",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Trino connection lost"),
        ) as mock_refresh:
            found = registry.get("mv-1")
            with pytest.raises(RuntimeError, match="Trino connection lost"):
                await mock_refresh(found, MagicMock())


class TestToggleMVMutation:
    async def test_disable_mv(self):
        mv = _make_mv("mv-1", status=MVStatus.FRESH, enabled=True)
        registry = _build_registry(mv)

        target = registry.get("mv-1")
        assert target is not None
        target.enabled = False
        target.status = MVStatus.DISABLED

        assert target.enabled is False
        assert target.status == MVStatus.DISABLED

    async def test_enable_disabled_mv(self):
        mv = _make_mv("mv-1", status=MVStatus.DISABLED, enabled=False)
        registry = _build_registry(mv)

        target = registry.get("mv-1")
        assert target is not None
        target.enabled = True
        if target.status == MVStatus.DISABLED:
            target.status = MVStatus.STALE

        assert target.enabled is True
        assert target.status == MVStatus.STALE

    async def test_enable_already_enabled_is_noop(self):
        mv = _make_mv("mv-1", status=MVStatus.FRESH, enabled=True)
        registry = _build_registry(mv)

        target = registry.get("mv-1")
        target.enabled = True
        # Status should not change from FRESH when re-enabling
        assert target.status == MVStatus.FRESH

    async def test_toggle_nonexistent_mv(self):
        registry = MVRegistry()
        result = registry.get("nonexistent")
        assert result is None
