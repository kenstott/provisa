# Copyright (c) 2026 Kenneth Stott
# Canary: 39f9e002-7509-46c5-aa85-1932898d99f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for per-tenant MV isolation (REQ-081)."""

from __future__ import annotations

from provisa.mv.models import MVDefinition
from provisa.mv.registry import MVRegistry


def _mv(mv_id: str, schema: str = "mv") -> MVDefinition:
    return MVDefinition(
        id=mv_id,
        source_tables=["orders"],
        target_catalog="iceberg",
        target_schema=schema,
    )


class TestMVRegistryTenantScoping:
    def test_tenant_registry_prefixes_ids(self):
        reg = MVRegistry(tenant_id="acme")
        mv = _mv("daily_sales")
        reg.register(mv)
        assert reg.get("daily_sales") is mv
        assert reg.get("acme:daily_sales") is None

    def test_tenant_registry_unregister(self):
        reg = MVRegistry(tenant_id="acme")
        mv = _mv("daily_sales")
        reg.register(mv)
        reg.unregister("daily_sales")
        assert reg.get("daily_sales") is None

    def test_tenant_registries_isolated(self):
        reg_a = MVRegistry(tenant_id="acme")
        reg_b = MVRegistry(tenant_id="globex")
        mv = _mv("report")
        reg_a.register(mv)
        assert reg_a.get("report") is mv
        assert reg_b.get("report") is None

    def test_single_tenant_registry_unchanged(self):
        reg = MVRegistry()
        mv = _mv("report")
        reg.register(mv)
        assert reg.get("report") is mv

    def test_mark_refreshing_uses_key(self):
        reg = MVRegistry(tenant_id="acme")
        mv = _mv("report")
        reg.register(mv)
        reg.mark_refreshing("report")
        from provisa.mv.models import MVStatus

        assert reg.get("report").status == MVStatus.REFRESHING

    def test_mark_refreshed_uses_key(self):
        reg = MVRegistry(tenant_id="acme")
        mv = _mv("report")
        reg.register(mv)
        reg.mark_refreshed("report", row_count=42)
        from provisa.mv.models import MVStatus

        result = reg.get("report")
        assert result.status == MVStatus.FRESH
        assert result.row_count == 42

    def test_mark_refresh_failed_uses_key(self):
        reg = MVRegistry(tenant_id="acme")
        mv = _mv("report")
        reg.register(mv)
        reg.mark_refresh_failed("report", error="timeout")
        from provisa.mv.models import MVStatus

        result = reg.get("report")
        assert result.status == MVStatus.STALE
        assert result.last_error == "timeout"


class TestMVDefinitionTenantSchema:
    def test_tenant_id_overrides_schema(self):
        mv = MVDefinition(
            id="report",
            source_tables=["orders"],
            target_catalog="iceberg",
            target_schema="mv",
            tenant_id="acme",
        )
        assert mv.target_schema == "acme_mv"

    def test_no_tenant_id_keeps_schema(self):
        mv = MVDefinition(
            id="report",
            source_tables=["orders"],
            target_catalog="iceberg",
            target_schema="mv",
        )
        assert mv.target_schema == "mv"
