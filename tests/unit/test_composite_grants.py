# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Composite grants: an entry ``a+b`` in a grant list is held by a reader holding ALL of a and b,
each by inheritance; across a list's entries the reader needs any one (REQ-2058)."""

# Requirements: REQ-2058

from __future__ import annotations

from types import SimpleNamespace

from provisa.security import meta_role
from provisa.security.inheritance import expand_grants, grant_parts, holds_grant

CHAINS = {
    "developer": ["developer"],
    "analyst": ["analyst", "developer"],
    "marketing": ["marketing"],
}


def test_an_entry_names_every_role_it_asks_for():
    assert grant_parts("analyst+region__eu") == {"analyst", "region__eu"}
    assert grant_parts("analyst") == {"analyst"}


def test_a_composite_is_held_only_by_a_reader_holding_all_of_it():
    chains = {**CHAINS, "meta:m": ["meta:m", "analyst", "developer", "marketing"]}
    granted = ["developer+marketing"]
    assert holds_grant("meta:m", granted, chains)  # developer by inheritance, marketing named
    assert not holds_grant("analyst", granted, chains)
    assert not holds_grant("marketing", granted, chains)


def test_entries_are_any_of_and_a_plain_entry_keeps_its_meaning():
    assert holds_grant("analyst", ["marketing+analyst", "developer"], CHAINS)


def test_expansion_reaches_a_role_whose_chain_holds_a_composite():
    col = {"column_name": "email", "visible_to": ["developer+marketing"], "unmasked_to": []}
    chains = {**CHAINS, "meta:m": ["meta:m", "analyst", "developer", "marketing"]}
    expand_grants([col], chains)
    assert col["visible_to"] == ["developer+marketing", "meta:m"]


def test_metric_and_command_grant_lists_take_composites_too():
    metric = {"name": "revenue", "visible_to": ["analyst+marketing"]}
    command = {"name": "tidy", "visible_to": ["analyst+marketing"]}
    chains = {**CHAINS, "meta:m": ["meta:m", "analyst", "developer", "marketing"]}
    expand_grants([metric, command], chains)
    assert "meta:m" in metric["visible_to"] and "meta:m" in command["visible_to"]


def test_a_meta_role_holds_its_members_ancestors_and_its_pseudo_roles(monkeypatch):
    # analyst (child of developer) named with region__eu reaches "developer+region__eu".
    col = {
        "column_name": "sender",
        "visible_to": ["developer+region__eu"],
        "writable_by": [],
        "unmasked_to": ["developer+region__eu"],
    }
    state = SimpleNamespace(
        roles={"analyst": {"id": "analyst", "capabilities": [], "domain_access": ["*"]}},
        role_build_inputs={
            "role_chains": {"analyst": ["analyst", "developer"]},
            "tables": [{"id": 1, "columns": [col]}],
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
        masking_rules={(1, "analyst"): {"sender": SimpleNamespace(mask_type="constant")}},
        meta_roles={},
        meta_qualifiers={},
    )
    monkeypatch.setattr(meta_role, "_rls", lambda *_a: None)
    monkeypatch.setattr(meta_role, "_role", lambda members, mid: {"id": mid})
    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", lambda *_a: None)
    meta_id = "meta:analyst+region+region__eu"
    meta_role._build(state, meta_id, ["analyst"], ("region", "region__eu"))
    assert meta_id in col["visible_to"]
    # Its mask is lifted: the composite in unmasked_to names everything it holds.
    assert (1, meta_id) not in state.masking_rules


def test_the_member_alone_does_not_reach_it():
    assert not holds_grant(
        "analyst", ["developer+region__eu"], {"analyst": ["analyst", "developer"]}
    )
