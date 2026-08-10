# Copyright (c) 2026 Kenneth Stott
# Canary: 3c5f8ab1-72d6-4e09-b4a3-9f61d27e05cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1412: the org's federation-engine lane — shared, isolated, or external.

An org administrator moves the org between three lanes. The mode is DERIVED from the ``orgs``
row (``external_engine_host`` set = external; else ``isolated_engine`` decides isolated vs
shared), and an external lane's coordinator outranks both the deployment's shared endpoint and
the SaaS isolated-host template — nothing else resolves a cluster the org itself operates.
"""

from __future__ import annotations

import pytest

from provisa.api.admin.org_engine_router import EXTERNAL, ISOLATED, MODES, SHARED, _mode_of
from provisa.api.app import state
from provisa.api.org_runtime import OrgRuntime, reset_current_org, set_current_org
from provisa.federation.trino_lifecycle import terminal_conn_kwargs


@pytest.fixture()
def external_org():
    """An org whose runtime points at a coordinator the ORG operates."""
    org_id = "extorg"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    rt.engine_endpoint = ("trino.acme.example.com", 8443)
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


@pytest.fixture()
def saas_isolated_org():
    org_id = "isolane"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


# ---- mode derivation ---------------------------------------------------------


def test_modes_are_the_three_lanes():
    assert MODES == (SHARED, ISOLATED, EXTERNAL)


def test_mode_is_derived_never_stored_twice():
    assert _mode_of(None, False) == SHARED
    assert _mode_of(None, True) == ISOLATED
    assert _mode_of("trino.acme.example.com", True) == EXTERNAL
    # An external host wins over the isolated flag: the org runs the cluster either way, and the
    # host is what says whose it is.
    assert _mode_of("trino.acme.example.com", False) == EXTERNAL


def test_orgs_table_carries_the_external_endpoint():
    from provisa.core.schema_admin import orgs

    assert orgs.c.external_engine_host.nullable is True
    assert orgs.c.external_engine_port.nullable is True


# ---- endpoint resolution (the lane that actually answers queries) ------------


def test_external_org_terminal_targets_its_own_coordinator(external_org, monkeypatch):
    # The SaaS template is configured and the org is flagged isolated, and still neither resolves
    # the endpoint — the org's own coordinator outranks both.
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    org_id, _rt = external_org
    token = set_current_org(org_id)
    try:
        kwargs = terminal_conn_kwargs(state)
    finally:
        reset_current_org(token)
    assert kwargs["host"] == "trino.acme.example.com"
    assert kwargs["port"] == 8443
    assert kwargs["schema"] == f"org_{org_id}"


def test_saas_isolated_org_still_uses_the_host_template(saas_isolated_org, monkeypatch):
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_PORT", "8080")
    org_id, rt = saas_isolated_org
    assert rt.engine_endpoint is None
    token = set_current_org(org_id)
    try:
        kwargs = terminal_conn_kwargs(state)
    finally:
        reset_current_org(token)
    assert kwargs["host"] == f"trino-{org_id}.internal"


def test_active_engine_endpoint_is_none_off_the_external_lane(saas_isolated_org):
    assert state.active_engine_endpoint is None
    token = set_current_org(saas_isolated_org[0])
    try:
        assert state.active_engine_endpoint is None
    finally:
        reset_current_org(token)


def test_active_engine_endpoint_exposes_the_external_coordinator(external_org):
    org_id, _rt = external_org
    token = set_current_org(org_id)
    try:
        assert state.active_engine_endpoint == ("trino.acme.example.com", 8443)
    finally:
        reset_current_org(token)


# ---- availability gate -------------------------------------------------------


def test_isolated_lane_is_unavailable_without_a_host_template(monkeypatch):
    """Offering isolation a deployment cannot resolve would sell a guarantee it cannot keep —
    isolated_engine_endpoint raises rather than falling back to the shared coordinator."""
    from provisa.api.admin.org_engine_router import _isolated_available

    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", raising=False)
    assert _isolated_available() is False
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    assert _isolated_available() is True


# ---- the surface is org-scoped, not deployment-wide --------------------------


def test_endpoints_are_gated_on_org_settings():
    """The engine KIND is platform_settings; which lane an org runs on is the org's own setting,
    so an org_admin holds it in either tenancy mode (REQ-1349)."""
    import inspect

    from provisa.api.admin import org_engine_router as mod

    for fn in (mod.get_org_engine, mod.set_org_engine):
        src = inspect.getsource(fn)
        assert "require_org_settings(request)" in src
        assert "require_platform_settings" not in src
