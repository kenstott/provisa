# Copyright (c) 2026 Kenneth Stott
# Canary: f8ca099f-73d0-433f-89b2-49d587b38582
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every role and every row rule says what it is for (REQ-2080, REQ-2081), and derived roles are
declared one at a time, each with its description, read with the values seen and what names them
(REQ-2078, REQ-2079)."""

# Requirements: REQ-2078, REQ-2079, REQ-2080, REQ-2081

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from provisa.api.errors import ApiError
from provisa.core.models import DerivedRole, RLSRule, Role

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_role_with_no_description_is_refused_by_name(blank):
    with pytest.raises(ValidationError, match="Role 'intern' needs a description"):
        Role(id="intern", description=blank, capabilities=[], domain_access=["*"])


def test_a_row_rule_with_no_description_is_refused():
    with pytest.raises(ValidationError, match="row rule for 'analyst' on 'orders' needs"):
        RLSRule(table_id="orders", role_id="analyst", filter="1 = 1", description="")


def test_a_derived_role_needs_a_description_and_a_valid_attribute():
    assert DerivedRole(attribute="Region", description=" x ").model_dump() == {
        "attribute": "region",
        "description": "x",
    }
    with pytest.raises(ValidationError):
        DerivedRole(attribute="region", description="")
    with pytest.raises(ValidationError):
        DerivedRole(attribute="9region", description="x")


def test_every_seeded_role_has_a_description():
    from provisa.core.db import SEEDED_ROLE_DESCRIPTIONS, SEEDED_ROLE_IDS

    assert set(SEEDED_ROLE_DESCRIPTIONS) == SEEDED_ROLE_IDS
    assert all(d.strip() for d in SEEDED_ROLE_DESCRIPTIONS.values())


def test_the_schema_seed_writes_the_same_descriptions():
    import re

    from provisa.core.db import SEEDED_ROLE_DESCRIPTIONS

    sql = (_REPO / "provisa" / "core" / "schema.sql").read_text()
    written = {
        rid: text.replace("''", "'")
        for text, rid in re.findall(
            r"UPDATE roles SET description = '((?:[^']|'')*)' WHERE id = '(\w+)' AND", sql
        )
    }
    assert written == SEEDED_ROLE_DESCRIPTIONS


def _configs() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "*.yaml", "*.yml"], cwd=_REPO, capture_output=True, text=True
    ).stdout.split()
    return [_REPO / f for f in listed if not f.startswith(("docs/", ".github/"))]


def test_every_role_and_row_rule_a_config_ships_has_a_description():
    loaded = 0
    for path in _configs():
        try:
            body = yaml.safe_load(path.read_text())
        except yaml.YAMLError:
            continue
        if not isinstance(body, dict):
            continue
        roles = [r for r in body.get("roles") or [] if isinstance(r, dict)]
        rules = [r for r in body.get("rls_rules") or [] if isinstance(r, dict)]
        for role in roles:
            Role(**role)
        for rule in rules:
            RLSRule(**rule)
        for entry in body.get("derived_roles") or []:
            DerivedRole(**entry)
        loaded += len(roles) + len(rules)
    assert loaded > 0


def test_a_meta_role_is_described_by_its_members():
    from provisa.security.meta_role import meta_description

    members = [
        {"id": "steward", "description": "Governs sales."},
        {"id": "analyst", "description": "Queries sales."},
    ]
    assert meta_description(members) == (
        "Acts as analyst (Queries sales.); steward (Governs sales.)"
    )


@pytest.mark.parametrize(
    ("entry", "named"),
    [
        ("region", True),
        ("region__eu", True),
        ("analyst+region__eu", True),
        ("regional", False),
        ("analyst", False),
    ],
)
def test_a_grant_entry_names_a_derived_role_or_one_of_its_values(entry, named):
    from provisa.core.repositories.derived_role import names_it

    assert names_it(entry, "region") is named


def test_the_roles_endpoint_refuses_an_empty_description():
    from provisa.api.admin.roles_router import _description

    with pytest.raises(ApiError) as refused:
        _description("intern", "  ")
    assert refused.value.status_code == 422
    assert refused.value.code == "roles.description_missing"
    assert _description("intern", " Reads sales. ") == "Reads sales."


class _Store:
    """The org's settings rows, as write_org_overrides and read_org_overrides keep them."""

    def __init__(self, rows: dict):
        self.rows = dict(rows)

    async def write(self, _db, updates, *, updated_by):
        self.rows.update(updates)
        return list(updates)

    async def read(self, _db):
        return dict(self.rows)


@pytest.fixture
def org(monkeypatch):
    from provisa.api import app as app_mod
    from provisa.api.admin import derived_roles_router as router
    from provisa.core import org_settings

    deployment = [DerivedRole(attribute="region", description="The member's region.")]
    store = _Store({})
    state = SimpleNamespace(
        settings_overrides={},
        config=SimpleNamespace(derived_roles=deployment),
        model_db=object(),
    )
    monkeypatch.setattr(app_mod, "state", state)
    monkeypatch.setattr(router, "require_capability_request", lambda *_a: None)
    monkeypatch.setattr(org_settings, "write_org_overrides", store.write)
    monkeypatch.setattr(org_settings, "read_org_overrides", store.read)
    return state, store


def _request():
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="u1")))


def test_declaring_a_derived_role_copies_the_deployments_list_and_adds_to_it(org):
    from provisa.api.admin.derived_roles_router import DescriptionBody, put_derived_role
    from provisa.auth.derived_roles import declared_derived_role_entries

    state, store = org
    asyncio.run(put_derived_role("Department", DescriptionBody(description="Team."), _request()))
    assert store.rows["derived_roles"] == [
        {"attribute": "region", "description": "The member's region."},
        {"attribute": "department", "description": "Team."},
    ]
    assert [e.attribute for e in declared_derived_role_entries(state)] == ["region", "department"]


def test_a_derived_role_with_no_description_is_refused_and_nothing_is_stored(org):
    from provisa.api.admin.derived_roles_router import DescriptionBody, put_derived_role

    _state, store = org
    with pytest.raises(ApiError) as refused:
        asyncio.run(put_derived_role("department", DescriptionBody(description=" "), _request()))
    assert refused.value.code == "derived_roles.description_missing"
    assert store.rows == {}


def test_stopping_a_derived_role_keeps_the_others(org):
    from provisa.api.admin.derived_roles_router import delete_derived_role

    _state, store = org
    asyncio.run(delete_derived_role("region", _request()))
    assert store.rows["derived_roles"] == []
    with pytest.raises(ApiError) as missing:
        asyncio.run(delete_derived_role("region", _request()))
    assert missing.value.status_code == 404
