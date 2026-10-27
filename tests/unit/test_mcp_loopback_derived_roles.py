# Copyright (c) 2026 Kenneth Stott
# Canary: e546618b-67d6-4dee-b789-acdd4ac283b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1952 on loopback MCP: a tool call naming a declared derived role, beside a data role,
acts as their meta-role with the derived role as its qualifier and binds the role's attribute,
as the unsecured HTTP, Flight and gRPC routes do -- never as the data role alone."""

from __future__ import annotations

import contextvars
from types import SimpleNamespace

import pytest

from provisa.api.mcp import server as mcp_server
from provisa.core.models import DerivedRole
from provisa.core.request_context import current_session_vars
from provisa.security import meta_role


@pytest.fixture
def loopback(monkeypatch):
    built: list = []
    monkeypatch.setattr(
        "provisa.core.settings_registry.value",
        lambda key: "analyst,region__eu" if key == "mcp.role" else None,
    )

    def _ensure(state, members, qualifiers=()):
        built.append((list(members), tuple(qualifiers)))
        return "meta:analyst+region+region__eu"

    monkeypatch.setattr(meta_role, "ensure_meta_role", _ensure)
    state = SimpleNamespace(
        settings_overrides={},
        config=SimpleNamespace(derived_roles=[DerivedRole(attribute="region", description="d")]),
        roles={"analyst": {}},
    )
    return state, built


def test_a_named_derived_role_is_a_qualifier_and_binds_its_attribute(loopback):
    state, built = loopback

    def _call():
        role = mcp_server.named_role("analyst,region__eu", state)
        return role, dict(current_session_vars.get())

    role, bound = contextvars.copy_context().run(_call)
    assert role == "meta:analyst+region+region__eu"
    ((members, qualifiers),) = built
    assert (members, set(qualifiers)) == (["analyst"], {"region", "region__eu"})
    assert bound.get("region") == "eu"


def test_one_named_data_role_is_that_role(loopback):
    state, built = loopback
    assert contextvars.copy_context().run(lambda: mcp_server.named_role("analyst", state)) == (
        "analyst"
    )
    assert built == []


def test_a_role_beyond_the_pin_is_still_refused_by_name(loopback):
    state, _ = loopback
    with pytest.raises(PermissionError, match="region__us"):
        mcp_server.named_role("analyst,region__us", state)
