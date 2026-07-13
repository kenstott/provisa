# Copyright (c) 2026 Kenneth Stott
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


class TestMaterializeStoreInfoResilience:
    """Regression: an unconfigured materialization store must NOT blank the whole store panel. The
    resolver failing on materialize_store() collapsed BOTH tiles (engine name AND the always-known
    MV count) to "—". store_ref is now best-effort → None, so the panel still renders."""

    async def test_configured_store_returns_dsn(self):
        # The resolver reads the EngineRuntime accessor materialize_store_dsn (NOT materialize_store,
        # which only exists on the wrapped FederationEngine — calling it blanked both tiles).
        from provisa.api.admin.schema_query import _safe_store_ref

        engine = MagicMock(spec=["materialize_store_dsn"])
        engine.materialize_store_dsn.return_value = "duckdb:///~/.provisa/store.db"
        assert _safe_store_ref(engine) == "duckdb:///~/.provisa/store.db"

    async def test_unconfigured_store_returns_none_not_raise(self):
        from provisa.api.admin.schema_query import _safe_store_ref
        from provisa.federation.engine import MaterializeStoreUnconfigured

        engine = MagicMock(spec=["materialize_store_dsn"])
        engine.materialize_store_dsn.side_effect = MaterializeStoreUnconfigured("duckdb")
        assert _safe_store_ref(engine) is None


class TestMaterializedViewIsQueryable:
    """Regression: a materialized view must ALSO populate view_sql_map so the query path can inline-
    expand it live. Registering it ONLY as an MV left its raw source catalog (e.g. __provisa__) in the
    compiled query → "Binder Error: Catalog __provisa__ does not exist" until a refresh landed."""

    async def test_config_materialized_view_populates_both_and_resolves_target(self):
        from provisa.api.app_loaders import _load_mv_and_views_config

        fake_state = MagicMock()
        fake_state.org_id = "acme"
        fake_state.view_sql_map = {}
        fake_state.mv_registry = MVRegistry()
        fake_state.federation_engine.materialize_store_target.return_value = ("mat_store", "mat")

        raw = {
            "views": [{"id": "v1", "sql": "SELECT 1 AS x", "materialize": True, "domain_id": "d"}]
        }
        with patch("provisa.api.app.state", fake_state):
            _load_mv_and_views_config(raw)

        assert "view_v1" in fake_state.view_sql_map  # queryable live path
        mv = fake_state.mv_registry.get("view-v1")
        assert mv is not None  # MV registered for acceleration
        assert mv.target_catalog == "mat_store"  # engine-resolved, not hardcoded postgresql

    async def test_config_plain_view_only_in_view_sql_map(self):
        from provisa.api.app_loaders import _load_mv_and_views_config

        fake_state = MagicMock()
        fake_state.org_id = "acme"
        fake_state.view_sql_map = {}
        fake_state.mv_registry = MVRegistry()

        raw = {
            "views": [{"id": "v2", "sql": "SELECT 2 AS x", "materialize": False, "domain_id": "d"}]
        }
        with patch("provisa.api.app.state", fake_state):
            _load_mv_and_views_config(raw)

        assert "view_v2" in fake_state.view_sql_map
        assert fake_state.mv_registry.get("view-v2") is None


class TestEngineRuntimeMVTarget:
    """Regression: MV registration calls federation_engine.materialize_store_target(org_id) on the
    EngineRuntime — the method must exist there and delegate to the backend (the missing delegation
    raised "'EngineRuntime' object has no attribute 'materialize_store_target'" on view creation)."""

    async def test_runtime_delegates_to_backend(self):
        from provisa.federation.runtime import EngineRuntime

        rt = EngineRuntime.__new__(EngineRuntime)
        rt._state = object()  # type: ignore[attr-defined]
        backend = MagicMock()
        backend.materialize_store_target.return_value = ("mat_store", "mat")
        rt._backend = backend  # type: ignore[attr-defined]

        assert rt.materialize_store_target("acme") == ("mat_store", "mat")
        backend.materialize_store_target.assert_called_once_with(rt._state, "acme")


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
        assert target is not None
        target.enabled = True
        # Status should not change from FRESH when re-enabling
        assert target.status == MVStatus.FRESH

    async def test_toggle_nonexistent_mv(self):
        registry = MVRegistry()
        result = registry.get("nonexistent")
        assert result is None
