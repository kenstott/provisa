# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8a17-9d4b-4f63-a1e0-7b3f6d9c2e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Work is served by the runtime of the org it is bound to, or refused by name (REQ-1266).

The state routes every per-org read (roles, compiled contexts, the model store) through the
runtime of the org the work is bound to. Two holes answered from the DEFAULT org's runtime instead:
work bound to an org whose runtime was not built in this process (never yet, or dropped by an
engine wake or an org deletion while a scheduled job for it kept firing), and work bound to no org
at all. Either way one org's roles, model and data were served to work that was not its own."""

# Requirements: REQ-1266

from __future__ import annotations

import pytest

from provisa.api.app import AppState
from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import reset_current_org, set_current_org

# The refusals are of work bound to no org, so nothing is bound here unless a test binds it.
pytestmark = pytest.mark.unbound


def _state() -> AppState:
    state = AppState()
    state.org_id = "acme"
    runtime = OrgRuntime(org_id="acme")
    runtime.roles = {"acme_analyst": {"id": "acme_analyst"}}
    runtime.engine_url = "trino://acme-engine:8080"
    state.org_registry.set("acme", runtime)
    return state


def test_work_bound_to_an_org_with_no_runtime_is_refused_by_name():
    state = _state()
    token = set_current_org("globex")
    try:
        with pytest.raises(RuntimeError, match="'globex'"):
            _ = state.roles  # never acme's roles
    finally:
        reset_current_org(token)


def test_work_bound_to_no_org_is_refused():
    state = _state()
    with pytest.raises(RuntimeError, match="No active org bound"):
        _ = state.roles  # never the deployment org's roles


def test_an_org_with_no_runtime_is_not_handed_another_orgs_engine():
    state = _state()
    token = set_current_org("globex")
    try:
        assert state.active_engine_url is None  # never acme's engine
    finally:
        reset_current_org(token)


def test_the_deployment_org_is_served_by_its_own_runtime_when_bound():
    state = _state()
    token = set_current_org("acme")
    try:
        assert "acme_analyst" in state.roles
        assert state.active_engine_url == "trino://acme-engine:8080"
    finally:
        reset_current_org(token)


async def test_a_single_org_session_binds_the_deployment_org():
    from provisa.api.org_resolve import resolve_session_org

    state = _state()
    state.multitenancy = False
    assert await resolve_session_org(state, user_id="u1") == "acme"
