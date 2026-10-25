# Copyright (c) 2026 Kenneth Stott
# Canary: 168ad904-03dd-45d1-9acb-e840f596afe0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's ordered mask list: a reader reads the column through the first entry it holds --
by inheritance, composites included, ``*`` for everyone -- and unmasked when it holds none
(REQ-2059). A meta-role is judged by its own set, like any role."""

# Requirements: REQ-2059

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import Column
from provisa.security import meta_role
from provisa.security.masking import (
    MaskType,
    entry_for,
    masks_report,
    reader_masks,
    resolve,
)

CONSTANT = {"type": "constant", "value": "withheld"}
DOMAIN_ONLY = {"type": "regex", "pattern": "^[^@]+", "replace": "***"}
MASKS = [
    {"to": ["contractor"], "mask": CONSTANT},
    {"to": ["analyst+region__eu"], "mask": None},
    {"to": ["analyst"], "mask": DOMAIN_ONLY},
    {"to": ["*"], "mask": CONSTANT},
]
CHAINS = {
    "developer": ["developer"],
    "analyst": ["analyst", "developer"],
    "contractor": ["contractor"],
    "region": ["region"],
    "region__eu": ["region__eu", "region"],
}


def chain_of(role: str) -> list[str]:
    return CHAINS.get(role, [role])


def test_the_first_entry_a_reader_holds_decides():
    assert entry_for(MASKS, ["analyst", "developer"]) == 2
    rule = resolve(MASKS, ["analyst", "developer"])
    assert rule is not None and rule.mask_type == MaskType.regex


def test_a_composite_entry_needs_every_role_of_it():
    assert resolve(MASKS, ["analyst", "developer", "region__eu", "region"]) is None


def test_an_entry_before_it_applies_first_deny_by_order():
    rule = resolve(MASKS, ["contractor", "analyst", "developer", "region__eu", "region"])
    assert rule is not None and rule.value == "withheld"


def test_everyone_is_the_reader_holding_nothing_else():
    rule = resolve(MASKS, ["marketing"])
    assert rule is not None and rule.value == "withheld"


def test_a_reader_no_entry_names_reads_the_column_unmasked():
    assert resolve([{"to": ["contractor"], "mask": CONSTANT}], ["analyst"]) is None


def test_an_entry_reaches_the_children_of_the_role_it_names():
    masks = [{"to": ["developer"], "mask": None}, {"to": ["*"], "mask": CONSTANT}]
    assert resolve(masks, ["analyst", "developer"]) is None


def test_each_readers_masks_are_keyed_as_the_masking_rules_are():
    masked = [(7, "email", MASKS, "varchar")]
    assert reader_masks(masked, "analyst+region__eu", ["region__eu", "analyst"]) == {}
    out = reader_masks(masked, "marketing", ["marketing"])
    assert list(out) == [(7, "marketing")] and out[(7, "marketing")]["email"][1] == "varchar"


def test_a_meta_role_is_judged_by_its_own_set(monkeypatch):
    state = SimpleNamespace(
        roles={
            r: {"id": r, "description": "Test role.", "capabilities": [], "domain_access": ["*"]}
            for r in ("analyst", "contractor")
        },
        role_build_inputs={
            "role_chains": CHAINS,
            "tables": [],
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
        masked_columns=[(1, "email", MASKS, "varchar")],
        masking_rules={},
        meta_roles={},
        meta_qualifiers={},
    )
    from provisa.compiler.rls import RLSContext

    monkeypatch.setattr(meta_role, "_rls", lambda *_a: RLSContext.empty())
    monkeypatch.setattr(meta_role, "_role", lambda members, mid: {"id": mid})
    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", lambda *_a: None)
    # analyst named with region__eu: the composite entry before analyst's own.
    meta_role._build(state, "meta:a", ["analyst"], ("region", "region__eu"))
    assert (1, "meta:a") not in state.masking_rules
    # analyst and contractor: contractor's entry comes first.
    meta_role._build(state, "meta:b", ["analyst", "contractor"])
    assert state.masking_rules[(1, "meta:b")]["email"][0].value == "withheld"


def test_a_rebuild_publishes_a_live_meta_roles_masks_from_its_chain():
    state = SimpleNamespace(
        meta_roles={"meta:a": ("analyst",)}, meta_qualifiers={"meta:a": ("region__eu",)}
    )
    masked = [(1, "email", MASKS, "varchar")]
    rules = meta_role.with_meta_masks(state, {}, masked, CHAINS)
    assert (1, "meta:a") not in rules
    gone = SimpleNamespace(meta_roles={"meta:x": ("left",)}, meta_qualifiers={})
    assert meta_role.with_meta_masks(gone, {}, masked, CHAINS) == {}


def test_the_report_flags_an_entry_an_earlier_one_covers():
    masks = [
        {"to": ["developer"], "mask": None},
        {"to": ["analyst"], "mask": CONSTANT},  # analyst holds developer: entry 0 first
        {"to": ["*"], "mask": CONSTANT},
    ]
    report = masks_report(masks, chain_of)
    assert report["unreachable"] == [{"entry": 1, "covered_by": 0}]
    assert report["unlisted_unmasked"] is False


def test_the_report_flags_entries_after_everyone_and_an_entry_for_no_one():
    masks = [{"to": [], "mask": CONSTANT}, {"to": ["*"], "mask": CONSTANT}, {"to": ["analyst"]}]
    masks[2]["mask"] = None
    report = masks_report(masks, chain_of)
    assert report["unreachable"] == [
        {"entry": 0, "covered_by": None},
        {"entry": 2, "covered_by": 1},
    ]


def test_a_composite_after_its_parts_own_entry_is_unreachable_but_before_it_is_not():
    assert masks_report(MASKS, chain_of)["unreachable"] == []
    swapped = [MASKS[0], MASKS[2], MASKS[1], MASKS[3]]
    assert masks_report(swapped, chain_of)["unreachable"] == [{"entry": 2, "covered_by": 1}]


def test_the_report_warns_when_no_entry_is_for_everyone():
    assert masks_report([{"to": ["contractor"], "mask": CONSTANT}], chain_of)["unlisted_unmasked"]
    assert masks_report([], chain_of)["unlisted_unmasked"] is False


def test_the_report_names_the_entry_chosen_roles_read_through():
    assert masks_report(MASKS, chain_of, ["analyst", "region__eu"])["applies"] == 1
    assert masks_report(MASKS, chain_of, ["analyst"])["applies"] == 2
    assert (
        masks_report([{"to": ["contractor"], "mask": CONSTANT}], chain_of, ["x"])["applies"] is None
    )


def test_a_mask_entry_is_one_of_the_three_masks_and_never_a_fake():
    Column(name="email", visible_to=[], masks=[{"to": ["*"], "mask": CONSTANT}])
    with pytest.raises(ValueError, match="a fake is not a role mask"):
        Column(name="email", visible_to=[], masks=[{"to": ["*"], "mask": {"type": "fake"}}])
    with pytest.raises(ValueError, match="not one of regex, constant, truncate"):
        Column(name="email", visible_to=[], masks=[{"to": ["*"], "mask": {"type": "hash"}}])


def test_an_embedding_column_takes_no_mask_but_may_list_unmasked_entries():
    with pytest.raises(ValueError, match="an embedding column cannot be masked"):
        Column(name="v", visible_to=[], embedding=True, masks=[{"to": ["*"], "mask": CONSTANT}])
    Column(name="v", visible_to=[], embedding=True, masks=[{"to": ["*"], "mask": None}])


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        role_chains={"analyst": ["analyst", "developer"], "developer": ["developer"]},
        settings_overrides={
            "derived_roles": [{"attribute": "region", "description": "The member's region."}]
        },
        config=SimpleNamespace(derived_roles=[]),
    )


def test_a_derived_roles_chain_is_it_and_its_claim_type():
    from provisa.api.admin.masks_router import chain_of as route_chain

    state = _state()
    assert route_chain(state, "analyst") == ["analyst", "developer"]
    assert route_chain(state, "region__eu") == ["region__eu", "region"]
    assert route_chain(state, "region") == ["region"]
    assert route_chain(state, "dept__ops") == ["dept__ops"]  # dept is not a declared claim type


def test_the_check_route_answers_the_report_for_the_list_as_edited(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import provisa.api.admin.masks_router as masks_router
    import provisa.api.app as app_mod

    asked: list[str] = []
    monkeypatch.setattr(
        masks_router, "require_capability_request", lambda _req, cap: asked.append(cap)
    )
    monkeypatch.setattr(app_mod, "state", _state())
    app = FastAPI()
    app.include_router(masks_router.router)
    body = {
        "masks": [
            {"to": ["developer"], "mask": None},
            {"to": ["analyst"], "mask": CONSTANT},
            {"to": ["region__eu"], "mask": DOMAIN_ONLY},
        ],
        "roles": ["marketing", "region__eu"],
    }
    resp = TestClient(app).post("/admin/masks/check", json=body)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "unreachable": [{"entry": 1, "covered_by": 0}],
        "unlisted_unmasked": True,
        "applies": 2,
        "patterns": [],
    }
    assert asked == ["masking_config"]
