# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6b3f92-5e17-4c48-a2e9-8b1c4d7f3a50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A set of held roles acts as one ephemeral meta-role, a child of every member (REQ-1620)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.rls import RLSContext
from provisa.security import meta_role
from provisa.security.meta_role import (
    MetaRoleNamed,
    ensure_meta_role,
    meta_role_id,
    resolve_requested_role,
)


def _tm(table_id: int) -> SimpleNamespace:
    return SimpleNamespace(table_id=table_id)


def _state(*, rls: dict[str, RLSContext], reads: dict[str, list[int]], roles: dict[str, dict]):
    columns = [
        {"column_name": "region", "visible_to": ["east", "west"], "unmasked_to": ["boss"]},
        {"column_name": "email", "visible_to": ["east", "west", "boss"], "unmasked_to": ["boss"]},
    ]
    tables = [{"id": 1, "domain_id": "sales", "columns": columns}]
    built: dict = {}
    state = SimpleNamespace(
        roles=dict(roles),
        rls_contexts=dict(rls),
        contexts={
            r: SimpleNamespace(tables={str(t): _tm(t) for t in ids}) for r, ids in reads.items()
        },
        masking_rules={(1, r): {"email": ("mask", "varchar")} for r in roles if r != "boss"},
        tracked_functions={},
        tracked_webhooks={},
        meta_roles={},
        role_build_inputs={
            "tables": tables,
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
    )
    return state, built


@pytest.fixture(autouse=True)
def _no_surface_build(monkeypatch):
    """The schema/context build is the stored roles' own (app_loaders.register_role_surface); here
    only what the meta-role is given is under test."""
    calls: list = []

    def _register(state, role, rls):
        state.rls_contexts[role["id"]] = rls
        calls.append(role["id"])

    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", _register)
    return calls


def _role(rid: str, *, caps=(), domains=("sales",), max_rows=None, session_vars=None) -> dict:
    return {
        "id": rid,
        "capabilities": list(caps),
        "domain_access": list(domains),
        "max_rows": max_rows,
        "rate_limit": None,
        "session_vars": session_vars or {},
    }


def test_two_filtered_roles_see_the_or_of_their_filters():
    state, _ = _state(
        rls={
            "east": RLSContext(rules={1: "region = 'east'"}),
            "west": RLSContext(rules={1: "region = 'west'"}),
        },
        reads={"east": [1], "west": [1]},
        roles={"east": _role("east"), "west": _role("west")},
    )
    meta = ensure_meta_role(state, ["west", "east"])
    assert meta == "meta:east+west"
    assert state.rls_contexts[meta].rules == {1: "(region = 'east') OR (region = 'west')"}


def test_a_member_reading_the_table_unfiltered_lifts_the_filter():
    state, _ = _state(
        rls={"east": RLSContext(rules={1: "region = 'east'"}), "boss": RLSContext.empty()},
        reads={"east": [1], "boss": [1]},
        roles={"east": _role("east"), "boss": _role("boss", caps=["write"], domains=["*"])},
    )
    meta = ensure_meta_role(state, ["east", "boss"])
    assert state.rls_contexts[meta].rules == {}
    # Unmasked where any member is unmasked; the union of rights and reach.
    assert (1, meta) not in state.masking_rules
    role = state.roles[meta]
    assert role["capabilities"] == ["write"] and role["domain_access"] == ["*"]


def test_a_member_that_cannot_read_the_table_does_not_lift_its_filter():
    state, _ = _state(
        rls={"east": RLSContext(rules={1: "region = 'east'"}), "hr": RLSContext.empty()},
        reads={"east": [1], "hr": []},
        roles={"east": _role("east"), "hr": _role("hr", domains=["hr"])},
    )
    meta = ensure_meta_role(state, ["east", "hr"])
    assert state.rls_contexts[meta].rules == {1: "(region = 'east')"}


def test_each_members_role_constants_are_written_into_its_own_term():
    expr = "region = current_setting('provisa.region')"
    state, _ = _state(
        rls={"east": RLSContext(rules={1: expr}), "west": RLSContext(rules={1: expr})},
        reads={"east": [1], "west": [1]},
        roles={
            "east": _role("east", session_vars={"region": "east"}),
            "west": _role("west"),
        },
    )
    meta = ensure_meta_role(state, ["east", "west"])
    assert state.rls_contexts[meta].rules[1] == (
        "(region = 'east') OR (region = current_setting('provisa.region'))"
    )


def test_masked_for_every_member_stays_masked_and_grants_reach_the_child():
    state, _ = _state(
        rls={"east": RLSContext.empty(), "west": RLSContext.empty()},
        reads={"east": [1], "west": [1]},
        roles={"east": _role("east", max_rows=10), "west": _role("west", max_rows=50)},
    )
    meta = ensure_meta_role(state, ["east", "west"])
    assert state.masking_rules[(1, meta)] == {"email": ("mask", "varchar")}
    columns = state.role_build_inputs["tables"][0]["columns"]
    assert meta in columns[0]["visible_to"] and meta in columns[1]["visible_to"]
    assert state.roles[meta]["max_rows"] == 50


def test_built_once_per_generation(_no_surface_build):
    state, _ = _state(
        rls={"east": RLSContext.empty(), "west": RLSContext.empty()},
        reads={"east": [1], "west": [1]},
        roles={"east": _role("east"), "west": _role("west")},
    )
    ensure_meta_role(state, ["east", "west"])
    ensure_meta_role(state, ["west", "east"])
    assert _no_surface_build == ["meta:east+west"]


def test_a_named_meta_role_is_refused_and_every_named_role_must_be_held():
    state, _ = _state(rls={}, reads={}, roles={"east": _role("east"), "west": _role("west")})
    with pytest.raises(MetaRoleNamed):
        resolve_requested_role(state, {"east", "west"}, "meta:east+west")
    with pytest.raises(PermissionError, match="'hr' is not assigned"):
        resolve_requested_role(state, {"east", "west"}, "east,hr")
    assert resolve_requested_role(state, {"east"}, "east") == "east"


def test_the_id_is_the_sorted_set():
    assert meta_role_id(["west", "east", "west"]) == "meta:east+west"
    assert meta_role.is_meta_role_id("meta:a+b") and not meta_role.is_meta_role_id("east")
