# Copyright (c) 2026 Kenneth Stott
# Canary: df6db77a-f3cc-4e83-befd-ac2b8cc184b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A wire sign-in under multitenancy reads the platform plane with the deployment's org bound
(REQ-1266, REQ-1337). pgwire, Flight SQL, gRPC, Bolt and MCP judge cross_org before the session's
org is known, from what the principal holds on the platform plane -- the deployment org's
assignments, read as the HTTP middleware reads them (set_current_org(default_org_id)). Before, the
read ran with no org bound, and the tenant reader (ActiveOrgPool -> state.model_db) refused every
pgwire basic sign-in with "No active org bound" before ReadyForQuery."""

# Requirements: REQ-1266, REQ-1337

from __future__ import annotations

import asyncio
import types

import pytest

from provisa.auth import held_assignments
from provisa.auth.models import AuthIdentity, RoleAssignment
from provisa.core.request_context import current_org, require_current_org

_USER = AuthIdentity(
    user_id="admin-of-acme", email=None, display_name=None, roles=[], active_org_id=None
)


def _state():
    return types.SimpleNamespace(
        auth_config={
            "provider": "basic",
            "default_role": "analyst",
            "assignments_source": "provisa",
            "default_assignments": [],
        },
        multitenancy=True,
        org_id="root",
        config=None,
        platform_roles={},
        user_attributes={},
        settings_overrides={"derived_roles": []},
    )


@pytest.fixture
def platform_reads(monkeypatch):
    """The tenant reader, as the real one behaves: it resolves the bound org's model store and
    refuses when none is bound. Records the org each read ran in."""
    read_in: list[str] = []

    async def _read(db_pool, identity, default_assignments):  # noqa: ARG001  # the reader's
        read_in.append(require_current_org())
        return [RoleAssignment("org_admin", "*")]

    async def _not_claimant(state, user_id):  # noqa: ARG001  # the reader's
        return False

    monkeypatch.setattr(held_assignments, "read_assignments", _read)
    monkeypatch.setattr(held_assignments, "is_bootstrap_claimant", _not_claimant)
    # A wire sign-in arrives with no org bound (the suite's session binds one).
    unbound = current_org.set(None)
    yield read_in
    current_org.reset(unbound)


def test_cross_org_is_judged_with_the_deployment_org_bound(platform_reads):
    asyncio.run(held_assignments.may_act_cross_org(_state(), _USER))
    assert platform_reads == ["root"]
    assert current_org.get() is None  # the binding does not outlive the read


def test_cross_org_reads_the_platform_plane_even_inside_a_tenant_binding(platform_reads):
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("acme")
    try:
        asyncio.run(held_assignments.may_act_cross_org(_state(), _USER))
        assert current_org.get() == "acme"
    finally:
        reset_current_org(token)
    assert platform_reads == ["root"]


def test_a_pgwire_sign_in_resolves_its_org_before_any_tenant_read(platform_reads, monkeypatch):
    """The pgwire path that refused admin-of-<org> at startup (tests/integration/test_leak_loop)."""
    from provisa.api import app as app_module
    from provisa.api import org_resolve
    from provisa.pgwire.server import _resolve_and_build_org

    resolved: list[dict] = []

    async def _resolve(state, **kwargs):  # noqa: ARG001  # resolve_session_org's
        resolved.append(kwargs)
        return kwargs["requested_org"]

    built: list[str] = []

    async def _build(org_id):
        built.append(org_id)

    monkeypatch.setattr(org_resolve, "resolve_session_org", _resolve)
    monkeypatch.setattr(app_module, "ensure_serving_runtime", _build)
    org = asyncio.run(_resolve_and_build_org(_state(), _USER, None, "acme"))
    assert org == "acme"
    assert built == ["acme"]
    assert platform_reads == ["root"]
    assert resolved[0]["can_act_any_org"] is False
