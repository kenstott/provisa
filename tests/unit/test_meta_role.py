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
from tests.role_state import RoleState


def _tm(table_id: int) -> SimpleNamespace:
    return SimpleNamespace(table_id=table_id)


_EMAIL_MASKS = [
    {"to": ["boss"], "mask": None},
    {"to": ["*"], "mask": {"type": "constant", "value": None}},
]


def _state(*, rls: dict[str, RLSContext], reads: dict[str, list[int]], roles: dict[str, dict]):
    columns = [
        {"column_name": "region", "visible_to": ["east", "west"]},
        {"column_name": "email", "visible_to": ["east", "west", "boss"]},
    ]
    tables = [{"id": 1, "domain_id": "sales", "columns": columns}]
    built: dict = {}
    state = RoleState(
        roles=dict(roles),
        rls_contexts=dict(rls),
        contexts={
            r: SimpleNamespace(tables={str(t): _tm(t) for t in ids}) for r, ids in reads.items()
        },
        masking_rules={(1, r): {"email": ("mask", "varchar")} for r in roles if r != "boss"},
        # REQ-2059: email is unmasked to boss, and masked to NULL for everyone else.
        masked_columns=[(1, "email", _EMAIL_MASKS, "varchar")],
        tracked_functions={},
        tracked_webhooks={},
        meta_roles={},
        meta_resolved_at={},
        role_build_inputs={
            # Each stored role's chain, as the build keeps it: these roles have no parent.
            "role_chains": {r: [r] for r in roles},
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


def test_a_member_with_no_data_surface_adds_nothing(_no_surface_build):
    """A control-plane member (REQ-1327: platform_admin, the cross_org right) has no data surface
    at all; building the set of held roles indexed it and the model rebuild failed with
    KeyError 'platform_admin' (core e2e lane, 87d09e9c5: setTableDraft refused)."""
    state, _ = _state(
        rls={"east": RLSContext(rules={1: "region = 'east'"})},
        reads={"east": [1]},
        roles={
            "east": _role("east"),
            "platform_admin": _role("platform_admin", caps=["cross_org"], domains=["*"]),
        },
    )
    assert "platform_admin" not in state.surfaces
    # It is no member: the set acts as the one role that reaches data.
    assert ensure_meta_role(state, ["east", "platform_admin"]) == "east"
    assert state.rls_contexts["east"].rules == {1: "region = 'east'"}


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
    [(rule, data_type)] = state.masking_rules[(1, meta)].values()
    assert (rule.mask_type.value, rule.value, data_type) == ("constant", None, "varchar")
    assert list(state.masking_rules[(1, meta)]) == ["email"]
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


def test_a_held_role_with_no_data_surface_is_no_member():
    """REQ-1327: platform_admin has no data surface. "Role: All" acting as it and the org's roles
    built a meta-role that failed on its surface (KeyError 'platform_admin') and took its
    control-plane rights; the set acts as the roles that reach data."""
    state, _ = _state(
        rls={"east": RLSContext.empty(), "west": RLSContext.empty()},
        reads={"east": [1], "west": [1]},
        roles={
            "east": _role("east"),
            "west": _role("west"),
            "platform_admin": _role("platform_admin", caps=("cross_org",), domains=()),
        },
    )
    meta = ensure_meta_role(state, ["east", "platform_admin", "west"])
    assert meta == "meta:east+west"
    assert "cross_org" not in state.roles[meta]["capabilities"]
    assert ensure_meta_role(state, ["east", "platform_admin"]) == "east"
    with pytest.raises(PermissionError, match="No schema available"):
        ensure_meta_role(state, ["platform_admin", "nobody"])


# --- write grants (REQ-2074): folded as reads are --------------------------------------------


def _grant(role, operation, columns=(), rows=None, check=None, presets=()):
    return {
        "role": role,
        "operation": operation,
        "columns": list(columns),
        "rows": rows,
        "check": check,
        "presets": list(presets),
    }


def _meta_grants(grants: list[dict], members: list[str]) -> dict[str, dict]:
    state, _ = _state(
        rls={
            "east": RLSContext(rules={1: "region = 'east'"}),
            "west": RLSContext(rules={1: "region = 'west'"}),
        },
        reads={"east": [1], "west": [1]},
        roles={"east": _role("east", caps=["write"]), "west": _role("west", caps=["write"])},
    )
    state.role_build_inputs["tables"][0]["write_grants"] = grants
    meta = ensure_meta_role(state, members)
    return {
        g["operation"]: g
        for g in state.role_build_inputs["tables"][0]["write_grants"]
        if g["role"] == meta
    }


_PLACED = {"column": "status", "source": "literal", "value": "placed"}
_NOW = {"column": "status", "source": "now"}


def test_a_meta_roles_write_grant_is_the_fold_of_its_members():
    grants = [
        _grant("east", "update", ["region"]),  # rows and check: east's read filter
        _grant("west", "update", ["email"], rows="email IS NOT NULL", check="TRUE"),
        _grant("west", "delete", rows="region = 'west'"),
    ]
    folded = _meta_grants(grants, ["west", "east"])
    assert set(folded) == {"update", "delete"}  # the ops any member holds; no insert
    update = folded["update"]
    assert update["columns"] == ["region", "email"]
    assert update["rows"] == "(region = 'east') OR (email IS NOT NULL)"
    assert update["check"] == "TRUE"  # a holder admits every row
    assert folded["delete"]["rows"] == "(region = 'west')"


def test_the_fold_does_not_depend_on_the_order_of_the_members():
    grants = [
        _grant("east", "insert", ["region"], check="region = 'east'"),
        _grant("west", "insert", ["email"], check="region = 'west'"),
    ]
    one = _meta_grants(grants, ["east", "west"])["insert"]
    other = _meta_grants([dict(g) for g in grants], ["west", "east"])["insert"]
    assert set(one["columns"]) == set(other["columns"]) == {"region", "email"}
    assert one["check"] == other["check"]  # members are taken in the meta-role's own order


def test_presets_every_holder_sets_alike_are_kept_and_others_dropped():
    agree = _meta_grants(
        [
            _grant("east", "insert", ["region"], presets=[_PLACED]),
            _grant("west", "insert", ["email"], presets=[_PLACED]),
        ],
        ["east", "west"],
    )["insert"]
    assert agree["presets"] == [_PLACED]
    differ = _meta_grants(
        [
            _grant("east", "insert", ["region"], presets=[_PLACED]),
            _grant("west", "insert", ["email", "status"], presets=[]),
        ],
        ["east", "west"],
    )["insert"]
    assert differ["presets"] == []
    assert "status" in differ["columns"]  # a holder lets the statement supply it
