# Copyright (c) 2026 Kenneth Stott
# Canary: 9b41e7c3-5d28-4f60-a712-3ce8d05b96f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1046/1047/1048/1049: the platform-storage allowance and the store an org's bytes land in.

Three things are asserted here: bytes are attributed to exactly one org, an org past its ceiling is
REJECTED rather than truncated, and an org that brought its own store is neither capped nor
measured — its materializations go to its disk, not the operator's.
"""

# Requirements: REQ-1046, REQ-1047, REQ-1048, REQ-1049

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError
from provisa.api.org_runtime import OrgRegistry, OrgRuntime, current_org
from provisa.federation.engine import FederationEngine, MaterializeStoreUnconfigured
from provisa.storage.byo import org_has_byo_store, org_store_dsn
from provisa.storage.quota import (
    org_store_schemas,
    require_storage_headroom,
    storage_ceiling,
    storage_report,
)

_GB = 1024**3
_PLATFORM_STORE = "postgresql://platform/store"
_ORG_STORE = "postgresql://acme-own-host/store"


def _registry(**storage_urls: str | None) -> OrgRegistry:
    registry = OrgRegistry()
    for org_id, url in storage_urls.items():
        runtime = OrgRuntime(org_id)
        runtime.storage_url = url
        registry.set(org_id, runtime)
    return registry


@pytest.fixture
def app_state(monkeypatch):
    """A stand-in for the module-level app state the storage seam reads through."""
    state = SimpleNamespace(
        admin_db=object(),
        org_registry=_registry(acme=None),
        federation_engine=SimpleNamespace(materialize_store_dsn=lambda: _PLATFORM_STORE),
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    return state


@pytest.fixture
def measured(monkeypatch):
    """Footprint the probe reports, and every level the meter was handed."""
    probe = SimpleNamespace(bytes=0, calls=[], metered=[])

    async def _bytes(store_dsn, org_id):
        probe.calls.append((store_dsn, org_id))
        return probe.bytes

    async def _meter(pool, org_id, n_bytes):
        probe.metered.append((org_id, n_bytes))

    monkeypatch.setattr("provisa.storage.quota.org_storage_bytes", _bytes)
    monkeypatch.setattr("provisa.core.commerce.meter_storage", _meter)
    return probe


def _cap(monkeypatch, resolved):
    async def _storage_cap_for_org(state, org_id):
        return resolved

    monkeypatch.setattr("provisa.core.commerce.storage_cap_for_org", _storage_cap_for_org)


class TestAttribution:
    def test_every_per_org_store_schema_is_counted(self):
        assert org_store_schemas("acme") == [
            "org_acme",
            "org_acme_mv_cache",
            "org_acme_api_cache",
            "org_acme_gql_cache",
        ]

    def test_one_org_is_never_billed_for_a_similarly_named_org(self):
        # The schemas are enumerated rather than prefix-matched precisely so that "acme" does not
        # absorb "acme_eu"'s bytes — org ids are user-chosen text.
        assert set(org_store_schemas("acme")).isdisjoint(org_store_schemas("acme_eu"))


class TestByo:
    def test_an_org_with_a_registered_store_reports_it(self, monkeypatch):
        state = SimpleNamespace(org_registry=_registry(acme=_ORG_STORE))
        monkeypatch.setattr("provisa.api.app.state", state, raising=False)
        assert org_store_dsn("acme") == _ORG_STORE
        assert org_has_byo_store("acme")

    def test_an_org_on_the_platform_store_and_an_unbuilt_org_have_none(self, app_state):
        assert org_store_dsn("acme") is None
        assert not org_has_byo_store("acme")
        assert not org_has_byo_store("never-built")

    async def test_a_byo_org_has_no_ceiling_even_on_a_capped_plan(self, app_state, monkeypatch):
        _cap(monkeypatch, (_GB, "trial"))
        app_state.org_registry = _registry(acme=_ORG_STORE)
        assert await storage_ceiling("acme") is None

    async def test_a_byo_org_is_reported_as_unmeasured_rather_than_as_zero(
        self, app_state, measured
    ):
        app_state.org_registry = _registry(acme=_ORG_STORE)
        assert await storage_report("acme") == {
            "org_id": "acme",
            "byo": True,
            "used_bytes": None,
            "ceiling_bytes": None,
        }
        # Nothing was probed: the bytes are in a bucket the platform does not enumerate.
        assert measured.calls == []


class TestRequireStorageHeadroom:
    async def test_a_deployment_with_no_ceiling_neither_probes_nor_meters(
        self, app_state, measured, monkeypatch
    ):
        # Self-hosted resolves no ceiling; there is no disk to police and nobody to bill.
        _cap(monkeypatch, None)
        await require_storage_headroom("acme", operation="MV mv1 refresh")
        assert measured.calls == []
        assert measured.metered == []

    async def test_an_org_under_its_allowance_passes_and_its_level_is_recorded(
        self, app_state, measured, monkeypatch
    ):
        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = 4 * _GB
        await require_storage_headroom("acme", operation="landing s.t")
        assert measured.calls == [(_PLATFORM_STORE, "acme")]
        # REQ-1049: metering happens on the measurement the check already took.
        assert measured.metered == [("acme", 4 * _GB)]

    @pytest.mark.parametrize("used", [10 * _GB, 11 * _GB])
    async def test_at_or_past_the_allowance_the_operation_is_refused(
        self, app_state, measured, monkeypatch, used
    ):
        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = used
        with pytest.raises(ApiError) as exc:
            await require_storage_headroom("acme", operation="MV mv1 refresh")
        assert exc.value.status_code == 507
        assert exc.value.code == "storage.quota_exceeded"
        assert exc.value.params["used_bytes"] == used
        assert exc.value.params["ceiling_bytes"] == 10 * _GB
        assert exc.value.params["plan"] == "starter"
        assert exc.value.params["operation"] == "MV mv1 refresh"
        # REQ-1047: the rejection names both exits, or the customer is stuck at the ceiling.
        assert "plan" in str(exc.value.detail)
        assert "bring-your-own storage" in str(exc.value.detail)
        # The level is recorded even on the rejected call — that is the hour the org was full.
        assert measured.metered == [("acme", used)]

    async def test_the_report_carries_the_footprint_against_the_ceiling(
        self, app_state, measured, monkeypatch
    ):
        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = 4 * _GB
        assert await storage_report("acme") == {
            "org_id": "acme",
            "byo": False,
            "used_bytes": 4 * _GB,
            "ceiling_bytes": 10 * _GB,
            "plan": "starter",
        }


class _FakeEngine:
    """Answers the probes ``refresh_mv`` runs, recording every statement it was asked to execute."""

    def __init__(self, count: int = 10) -> None:
        self.count = count
        self.sqls: list[str] = []

    async def execute_engine(self, sql, *args, **kwargs):
        from provisa.executor.result import QueryResult

        self.sqls.append(sql)
        if "SHOW COLUMNS" in sql:
            return QueryResult(rows=[], column_names=[])
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


class TestWriteSeams:
    """REQ-1047: both places bytes accumulate refuse when the org is at its ceiling."""

    @pytest.fixture(autouse=True)
    def _bind_org(self):
        token = current_org.set("acme")
        yield
        current_org.reset(token)

    @staticmethod
    def _mv():
        from provisa.mv.models import MVDefinition

        return MVDefinition(
            id="mv-orders",
            source_tables=["orders"],
            target_catalog="postgresql",
            target_schema="mv_cache",
            sql="SELECT id FROM orders",
            refresh_interval=300,
        )

    async def test_a_refresh_over_the_ceiling_is_held_as_mv_state_not_raised(
        self, app_state, measured, monkeypatch
    ):
        # A scheduled refresh has no request to answer with a 507, so the rejection is recorded on
        # the MV. Nothing is materialized: the pre-guard freshness probes read, nothing writes.
        from provisa.mv.models import MVStatus
        from provisa.mv.refresh import refresh_mv
        from provisa.mv.registry import MVRegistry

        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = 12 * _GB
        mv, registry, engine = self._mv(), MVRegistry(), _FakeEngine()
        registry.register(mv)

        await refresh_mv(engine, mv, registry)

        assert mv.status == MVStatus.SKIPPED_QUOTA
        assert "storage included with the starter plan" in str(mv.last_error)
        assert not [
            s for s in engine.sqls if s.lstrip().upper().startswith(("CREATE", "INSERT", "DELETE"))
        ]

    async def test_a_refresh_with_headroom_still_materializes(
        self, app_state, measured, monkeypatch
    ):
        from provisa.mv.models import MVStatus
        from provisa.mv.refresh import refresh_mv
        from provisa.mv.registry import MVRegistry

        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = _GB
        mv, registry, engine = self._mv(), MVRegistry(), _FakeEngine()
        registry.register(mv)

        await refresh_mv(engine, mv, registry)

        assert mv.status == MVStatus.FRESH

    async def test_landing_over_the_ceiling_is_refused_before_the_store_is_touched(
        self, app_state, measured, monkeypatch
    ):
        # Landing accumulates exactly as an MV does; an org that filled its allowance by landing
        # sources gets the same rejection, raised before anything is written.
        from provisa.federation import store_writer

        _cap(monkeypatch, (10 * _GB, "starter"))
        measured.bytes = 12 * _GB

        def _no_connection(*args, **kwargs):
            raise AssertionError("the store must not be opened once the quota is exceeded")

        monkeypatch.setattr(store_writer, "store_connection", _no_connection)

        with pytest.raises(ApiError) as exc:
            await store_writer.land(
                _PLATFORM_STORE,
                schema="org_acme",
                table="orders",
                columns=[("id", "INTEGER")],
                rows=[{"id": 1}],
            )
        assert exc.value.status_code == 507
        assert exc.value.params["operation"] == "landing org_acme.orders"


class TestMaterializeStorePrecedence:
    """REQ-1048: whose disk the bytes land on, resolved off the bound org."""

    @staticmethod
    def _engine(default_store: str | None) -> FederationEngine:
        return FederationEngine("test-engine", [], default_materialize_store=lambda: default_store)

    @pytest.fixture(autouse=True)
    def _unbind(self):
        token = current_org.set(None)
        yield
        current_org.reset(token)

    @pytest.fixture(autouse=True)
    def _no_configured_url(self, monkeypatch):
        monkeypatch.setattr("provisa.federation.engine.configured_materialize_url", lambda: None)

    def test_the_orgs_own_store_outranks_the_deployments_configuration(
        self, app_state, monkeypatch
    ):
        app_state.org_registry = _registry(acme=_ORG_STORE)
        monkeypatch.setattr(
            "provisa.federation.engine.configured_materialize_url", lambda: _PLATFORM_STORE
        )
        current_org.set("acme")
        assert self._engine(None).materialize_store() == _ORG_STORE

    def test_an_org_without_its_own_store_lands_on_the_platform_store(self, app_state, monkeypatch):
        monkeypatch.setattr(
            "provisa.federation.engine.configured_materialize_url", lambda: _PLATFORM_STORE
        )
        current_org.set("acme")
        assert self._engine("engine-default").materialize_store() == _PLATFORM_STORE

    def test_with_nothing_configured_the_engine_default_stands(self, app_state):
        current_org.set("acme")
        assert self._engine("engine-default").materialize_store() == "engine-default"

    def test_no_store_anywhere_is_a_hard_error_not_a_fallback(self, app_state):
        current_org.set("acme")
        with pytest.raises(MaterializeStoreUnconfigured):
            self._engine(None).materialize_store()
