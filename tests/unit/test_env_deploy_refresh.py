# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1d9a70-3e62-49bb-9a41-1b7fa2c6d8e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How much of an environment's runtime a deploy has to throw away (REQ-1544).

The delta decides, and nothing else does: an undo, a merge and a pipeline deploy that change the
same paths refresh identically. What is checked here is the decision itself -- which paths count as
connectivity, that the stored report an approval carries answers the same question as the live one,
and that the router turns each answer into the right act against the registry.
"""

# Requirements: REQ-1544, REQ-1543, REQ-1496, REQ-1488

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.core.env_copy import CopyReport, TableDelta
from provisa.core.env_deploy import DeployDelta, report_touches_connectivity

ORG = "acme"


class TestWhichChangesReachAConnection:
    def test_a_table_edit_does_not(self):
        delta = DeployDelta(changed=["sales/orders.table.yaml"])
        assert delta.touches_connectivity is False

    def test_a_source_does(self):
        assert DeployDelta(changed=["sources/warehouse.yaml"]).touches_connectivity is True

    @pytest.mark.parametrize(
        "path",
        ["api-sources/stripe.yaml", "kafka-sources/events.yaml", "kafka-sinks/audit.yaml"],
    )
    def test_every_connection_registry_counts_not_only_sources(self, path):
        """A stream and an HTTP source are reached over a connection exactly as a database is."""
        assert DeployDelta(added=[path]).touches_connectivity is True

    def test_a_removed_source_counts_as_much_as_an_added_one(self):
        """Its pool is open and points at a source the model no longer has."""
        assert DeployDelta(removed=["sources/warehouse.yaml"]).touches_connectivity is True

    def test_a_deploy_that_changed_nothing_reaches_no_connection(self):
        assert DeployDelta(unchanged=12).touches_connectivity is False

    def test_metrics_roles_and_views_are_derived_and_do_not(self):
        delta = DeployDelta(
            added=["metrics/revenue.yaml", "roles/analyst.yaml", "views/daily.yaml"],
            changed=["naming"],
        )
        assert delta.touches_connectivity is False


class TestTheSameQuestionAskedOfACopy:
    def _report(self, *tables: TableDelta) -> CopyReport:
        return CopyReport("dev", "prod", "merge", False, list(tables))

    def test_a_copy_that_wrote_a_source_reaches_a_connection(self):
        assert self._report(TableDelta("sources", added=["warehouse"])).touches_connectivity

    def test_a_copy_that_wrote_only_tables_does_not(self):
        assert not self._report(
            TableDelta("registered_tables", changed=["orders"])
        ).touches_connectivity

    def test_a_sources_delta_that_touched_nothing_does_not(self):
        """``unchanged`` alone is not a write: the row is the same row the pool was opened for."""
        assert not self._report(TableDelta("sources", unchanged=3)).touches_connectivity


class TestTheStoredReportAnApprovalCarries:
    """An approved request is applied by ``env_approvals``, which hands back the row's report."""

    def test_a_copys_report_is_read_by_table(self):
        report = CopyReport("dev", "prod", "merge", False, [TableDelta("sources", added=["w"])])
        assert report_touches_connectivity(report.as_dict()) is True

    def test_a_deploys_report_is_read_by_path(self):
        from provisa.core.env_deploy import DeployReport

        report = DeployReport("prod", "abc123", False, DeployDelta(changed=["sources/w.yaml"]))
        assert report_touches_connectivity(report.as_dict()) is True

    def test_the_two_shapes_agree_when_nothing_connective_changed(self):
        copy = CopyReport("dev", "prod", "merge", False, [TableDelta("metrics", added=["rev"])])
        from provisa.core.env_deploy import DeployReport

        deploy = DeployReport("prod", "abc123", False, DeployDelta(added=["metrics/rev.yaml"]))
        assert report_touches_connectivity(copy.as_dict()) is False
        assert report_touches_connectivity(deploy.as_dict()) is False


class _Registry:
    def __init__(self, cached: bool):
        self.cached = cached
        self.invalidated: list[str] = []

    def get(self, key):
        return object() if self.cached else None

    def invalidate(self, key):
        self.invalidated.append(key)


@pytest.fixture
def registry(monkeypatch):
    """A registry holding this environment's runtime, and a recompile that records rather than runs."""
    reg = _Registry(cached=True)
    recompiled: list[tuple[str | None, str | None]] = []

    class _State:
        org_registry = reg

    async def _rebuild_schemas(raw_config=None):
        from provisa.api.org_runtime import current_env, current_org

        recompiled.append((current_org.get(), current_env.get()))

    import provisa.api.app as app_module

    monkeypatch.setattr(er, "_state", _State)
    monkeypatch.setattr(app_module, "_rebuild_schemas", _rebuild_schemas)
    return reg, recompiled


class TestWhatTheRouterDoesWithTheAnswer:
    """The sync tests above settle the DECISION; these settle the act, so only they are async."""

    pytestmark = pytest.mark.asyncio

    async def test_a_connection_change_drops_the_environments_runtime(self, registry):
        reg, recompiled = registry
        assert await er._refresh(ORG, "dev", connectivity=True) == "rebuilt"
        assert reg.invalidated == [f"{ORG}_env_dev"] and recompiled == []

    async def test_a_model_change_recompiles_in_place_and_keeps_the_pools(self, registry):
        reg, recompiled = registry
        assert await er._refresh(ORG, "dev", connectivity=False) == "recompiled"
        assert reg.invalidated == []
        assert recompiled == [(ORG, "dev")]

    async def test_the_recompile_is_bound_to_the_environment_that_was_deployed(self, registry):
        """Not to prod and not to whatever the caller's request was bound to."""
        _reg, recompiled = registry
        await er._refresh(ORG, "feature-x", connectivity=False)
        assert recompiled == [(ORG, "feature-x")]

    async def test_the_binding_is_released_afterwards(self, registry):
        from provisa.api.org_runtime import current_env, current_org

        await er._refresh(ORG, "dev", connectivity=False)
        assert current_org.get() is None and current_env.get() is None

    async def test_prod_is_the_bare_org_key(self, registry):
        reg, _recompiled = registry
        await er._refresh(ORG, "prod", connectivity=True)
        assert reg.invalidated == [ORG]

    async def test_an_environment_nobody_has_built_is_left_alone(self, monkeypatch):
        reg = _Registry(cached=False)

        class _State:
            org_registry = reg

        monkeypatch.setattr(er, "_state", _State)
        assert await er._refresh(ORG, "dev", connectivity=True) == "uncached"
        assert reg.invalidated == []
