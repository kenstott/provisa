# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MCP describe_role (REQ-2082): an agent learns the role it acts as -- a meta-role with its
members -- and the roles it may switch to, each with its description; never a row rule, mask,
grant or condition, nor why a result was narrowed."""

# Requirements: REQ-2082

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.api.mcp import tools

pytestmark = pytest.mark.unit

_SECRETS = ("region = 'eu'", "SECRET_MASK", "salary", "cost_center")


def _state():
    meta = "meta:analyst+region+region__eu"
    return SimpleNamespace(
        roles={
            "analyst": {
                "id": "analyst",
                "description": "Queries the sales domain.",
                "capabilities": ["query_development"],
                "domain_access": ["sales"],
            },
            "steward": {
                "id": "steward",
                "description": "Governs the sales domain.",
                "capabilities": ["masking_config"],
                "domain_access": ["sales"],
            },
            meta: {"id": meta, "description": "Acts as analyst (Queries the sales domain.)"},
        },
        meta_roles={meta: ("analyst",)},
        meta_qualifiers={meta: ("region", "region__eu")},
        settings_overrides={
            "derived_roles": [{"attribute": "region", "description": "The member's region."}]
        },
        config=SimpleNamespace(derived_roles=[]),
        # What a role is granted or denied, which the answer must never carry.
        masking_rules={("orders", "analyst"): {"salary": "SECRET_MASK"}},
        rls_contexts={"analyst": SimpleNamespace(rules={1: "region = 'eu'"})},
        column_grants={"cost_center": ["steward"]},
    )


def test_the_answer_is_the_role_and_what_it_is_for():
    out = tools.describe_role(_state(), "analyst", {"analyst"})
    assert out == {
        "role": {"id": "analyst", "description": "Queries the sales domain."},
        "can_switch_to": [{"id": "analyst", "description": "Queries the sales domain."}],
    }


def test_a_meta_role_names_its_members_and_its_derived_roles():
    meta = "meta:analyst+region+region__eu"
    out = tools.describe_role(_state(), meta, {"analyst", "region", "region__eu"})
    assert out["members"] == [
        {"id": "analyst", "description": "Queries the sales domain."},
        {"id": "region", "description": "The member's region."},
        {"id": "region__eu", "description": "Held when the user's region is eu"},
    ]


def test_no_rule_mask_grant_or_condition_is_in_the_answer():
    meta = "meta:analyst+region+region__eu"
    text = json.dumps(tools.describe_role(_state(), meta, {"analyst", "region", "region__eu"}))
    assert not [s for s in _SECRETS if s in text]
    assert "capabilities" not in text and "domain_access" not in text


def test_a_role_not_held_is_not_offered():
    out = tools.describe_role(_state(), "analyst", {"analyst"})
    assert "steward" not in json.dumps(out)


def test_a_remote_caller_may_switch_to_what_its_token_holds_and_its_derived_roles(monkeypatch):
    from provisa.api.mcp import server
    from provisa.auth import derived_roles

    state = _state()
    identity = SimpleNamespace(user_id="u1", roles=["analyst"])
    monkeypatch.setattr(
        derived_roles, "principal_of", lambda _s, _i: ({}, {"region__eu": "region", "region": ""})
    )
    i_token = server._request_identity.set(identity)
    r_token = server._request_role.set("analyst")
    try:
        held = server.switchable_roles(state)
    finally:
        server._request_identity.reset(i_token)
        server._request_role.reset(r_token)
    assert held == {"analyst", "region", "region__eu"}
    assert "steward" not in held


def test_a_stdio_caller_may_switch_to_the_pinned_roles(monkeypatch):
    from provisa.api.mcp import server

    monkeypatch.setattr(server, "_pinned_stdio_role", lambda: "analyst, steward")
    assert server.switchable_roles(_state()) == {"analyst", "steward"}


async def test_the_server_offers_the_tool():
    from provisa.api.mcp.server import build_mcp_server
    from tests.unit.test_mcp_server import _make_state

    names = {t.name for t in await build_mcp_server(_make_state()).list_tools()}
    assert "describe_role" in names
