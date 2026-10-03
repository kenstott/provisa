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
    expand it live. Registering it ONLY as an MV left its raw source catalog (e.g. __derived__) in the
    compiled query → "Binder Error: Catalog __derived__ does not exist" until a refresh landed."""

    async def test_a_views_entry_becomes_a_table_of_the_derived_source(self):
        """A ``views:`` entry is stored as the table it declares (core/config_loader.py
        views_as_tables), so the schema build registers it for inline expansion AND, when
        materialized, as an MV — the same path as a ``tables:`` entry with ``view_sql``. Before,
        it was registered for refresh only and never stored, so nothing could read it."""
        from provisa.core.config_loader import views_as_tables
        from provisa.core.models import DERIVED_SOURCE_ID, ProvisaConfig

        raw = {
            "views": [
                {
                    "id": "v-1",
                    "sql": "SELECT 1 AS x",
                    "materialize": True,
                    "refresh_interval": 60,
                    "domain_id": "d",
                    "columns": [{"name": "x", "visible_to": ["analyst"]}],
                }
            ]
        }
        views_as_tables(raw)
        assert "views" not in raw
        (entry,) = raw["tables"]
        assert entry == {
            "source_id": DERIVED_SOURCE_ID,
            "schema": "views",
            "table": "view_v_1",
            "view_sql": "SELECT 1 AS x",
            "materialize": True,
            "mv_refresh_interval": 60,
            "domain_id": "d",
            "columns": [{"name": "x", "visible_to": ["analyst"]}],
        }
        (table,) = ProvisaConfig.model_validate(
            {**raw, "sources": [], "domains": [], "roles": []}
        ).tables
        assert (table.view_sql, table.materialize, table.mv_refresh_interval) == (
            "SELECT 1 AS x",
            True,
            60,
        )

    async def test_a_views_entry_with_a_key_it_does_not_take_is_refused(self):
        from provisa.core.config_loader import views_as_tables

        with pytest.raises(ValueError, match=r"unknown keys \['source_id'\]"):
            views_as_tables(
                {"views": [{"id": "v2", "sql": "SELECT 2", "domain_id": "d", "source_id": "pg"}]}
            )


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


class TestAWriteMarksTheViewsThatReadItStale:
    """A SQL-defined view's MV names no ``source_tables`` (the event graph's inputs); the tables
    its SQL reads are recorded on it from the semantic SQL, and a write to one marks it stale."""

    def test_a_view_over_the_written_table_goes_stale_and_others_do_not(self):
        from provisa.mv.models import MVDefinition, MVStatus
        from provisa.mv.readable_inputs import read_table_names

        registry = MVRegistry()
        sql = "SELECT region, COUNT(*) AS n FROM sales.orders o JOIN sales.customers c ON 1=1 GROUP BY 1"
        for mv_id, view_sql in (("view-a", sql), ("view-b", "SELECT 1 AS x FROM hr.people")):
            registry.register(
                MVDefinition(
                    id=mv_id,
                    source_tables=[],
                    target_catalog="mat_store",
                    target_schema="mat",
                    target_table=f"mv_{mv_id}",
                    refresh_interval=300,
                    enabled=True,
                    sql=view_sql,
                    read_tables=read_table_names(view_sql),
                    status=MVStatus.FRESH,
                )
            )
        assert read_table_names(sql) == frozenset({"orders", "customers"})
        assert registry.mark_stale("orders") == ["view-a"]
        assert registry.get("view-a").status == MVStatus.STALE
        assert registry.get("view-b").status == MVStatus.FRESH
