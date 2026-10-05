# Copyright (c) 2026 Kenneth Stott
# Canary: 53a06b5c-e290-4f24-a3e6-44df23508aaa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's role ids are judged by that org's own definitions (REQ-1266, REQ-1573).

The same role id can carry different rights in different orgs. The environment gate reads the
capabilities of the org the request names, from that org's PROD definitions: not the deployment
org's, and not an environment's own copy, which must not be able to grant the right to select it.
"""

# Requirements: REQ-1266, REQ-1573

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.org_runtime import OrgRuntime, runtime_key

pytestmark = pytest.mark.unbound

_ENV_RIGHT = "environment_management"


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "root"
    root = state.org_registry.get("root")
    assert root is not None
    root.roles = {"developer": {"id": "developer", "capabilities": [_ENV_RIGHT]}}
    acme = OrgRuntime(org_id="acme")
    acme.roles = {"developer": {"id": "developer", "capabilities": ["usage"]}}
    state.org_registry.set("acme", acme)
    feature = OrgRuntime(org_id="acme", env="feature")
    feature.roles = {"developer": {"id": "developer", "capabilities": [_ENV_RIGHT]}}
    state.org_registry.set(runtime_key("acme", "feature"), feature)
    monkeypatch.setattr("provisa.api.app.state", state)

    async def _built(org_id: str, env: str | None = None):
        """Every runtime here is built already; a real build needs the admin plane."""
        rt = state.org_registry.get(runtime_key(org_id, env))
        assert rt is not None, org_id
        return rt

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _built)
    return state


def _developer():
    return SimpleNamespace(user_id="u1", roles=["developer"])


async def test_the_env_gate_reads_the_named_orgs_own_prod_definitions(state):
    from provisa.auth.middleware import org_env_capabilities

    caps = await org_env_capabilities(_developer(), "acme")
    assert caps is not None and _ENV_RIGHT not in caps  # not root's grant, not the branch's copy


async def test_the_deployment_org_is_judged_by_its_own_definitions(state):
    from provisa.auth.middleware import org_env_capabilities

    caps = await org_env_capabilities(_developer(), "root")
    assert caps is not None and _ENV_RIGHT in caps


async def test_the_env_gate_leaves_nothing_bound(state):
    from provisa.auth.middleware import org_env_capabilities
    from provisa.core.request_context import current_env, current_org

    await org_env_capabilities(_developer(), "acme")
    assert current_org.get() is None and current_env.get() is None
