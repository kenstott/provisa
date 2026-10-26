# Copyright (c) 2026 Kenneth Stott
# Canary: 33771ffa-c886-43b5-91b2-046cde66dd02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: a sandbox visitor holds the sandbox role (and the demo's own roles its tour runs as),
never org_admin. It reads a copied demo's columns because their grants name standard roles the
sandbox inherits -- viewer, data_steward -- or a demo role it holds; no grant is rewritten for it."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.core.db import standard_role_dicts
from provisa.security.inheritance import holds_grant, role_chains

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"
_PUBLISHED = sorted(
    d.name
    for d in _NAMED.iterdir()
    if (d / "demo.yaml").is_file()
    and (yaml.safe_load((d / "demo.yaml").read_text()) or {}).get("published")
)


def _config(demo: str) -> dict:
    return yaml.safe_load((_NAMED / demo / "config.yaml").read_text())


@pytest.mark.parametrize("demo", _PUBLISHED)
def test_the_visitor_reads_every_column_of_the_demo(demo):
    config = _config(demo)
    own = [
        {
            "id": r["id"],
            "capabilities": r.get("capabilities") or [],
            "parents": r.get("parents") or [],
        }
        for r in config.get("roles") or []
    ]
    chains = role_chains([*standard_role_dicts(), *own])
    held = ["sandbox", *(r["id"] for r in own)]
    unread = [
        f"{table.get('id') or table.get('table')}.{column['name']}"
        for table in config.get("tables") or []
        for column in table.get("columns") or []
        if not any(holds_grant(role, column.get("visible_to") or [], chains) for role in held)
    ]
    assert unread == []


@pytest.mark.parametrize("demo", _PUBLISHED)
def test_no_grant_names_org_admin_for_a_reader(demo):
    """Read is granted to data_steward (org_admin and the sandbox inherit it), never to org_admin by
    name: a grant naming org_admin would be one the sandbox visitor cannot hold."""
    named = [
        f"{table.get('id') or table.get('table')}.{column['name']}"
        for table in _config(demo).get("tables") or []
        for column in table.get("columns") or []
        if "org_admin" in (column.get("visible_to") or [])
    ]
    assert named == []


def test_the_visitor_holds_the_standard_roles_its_tour_acts_as():
    """REQ-2155: a tour step may act as a standard role (hasuramigration's analyst); the visitor is
    granted it with the demo's own roles, since a role is acted as only when it is held."""
    from provisa.api.demo_templates import tour_standard_roles

    assert tour_standard_roles(_NAMED / "hasuramigration") == ["analyst"]
    assert tour_standard_roles(_NAMED / "storefront") == []  # its tour acts as its own roles


def test_a_tour_acting_as_a_withheld_standard_role_refuses_the_demo(tmp_path):
    from provisa.api.demo_templates import tour_standard_roles

    (tmp_path / "tour.yaml").write_text(
        yaml.safe_dump({"steps": [{"key": "a", "route": "/sql", "role": "org_admin"}]})
    )
    with pytest.raises(ValueError, match="org_admin"):
        tour_standard_roles(tmp_path)
