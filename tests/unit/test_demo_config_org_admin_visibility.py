# Copyright (c) 2026 Kenneth Stott
# Canary: 3ad9f0c7-51be-4e62-8a7c-6f0d2b19e4a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1323: every shipped config must be visible to ``org_admin``.

These are the configs a self-created demo org loads (``include_demo``), and that org's creator
holds exactly one role in them: ``org_admin``. Column visibility is strict membership —
``schema_gen._build_visible_tables`` keeps a column only when its ``visible_to`` is empty or names
the role, and drops the whole table when nothing survives. So a demo column listing
``[platform_admin, analyst]`` is invisible to the person who asked for the demo, which is the
reported "parts of the demo data, but not all".

All three are checked, not just the repo default: the deployed image runs
``PROVISA_CONFIG=/app/config/provisa-install.yaml`` (``packaging/linux/first-launch.sh``), and the
setup wizard starts from ``provisa-install-base.yaml`` — a fix to ``provisa.yaml`` alone would
leave production exactly as broken.

Empty ``visible_to`` is unrestricted (native filter params) and is deliberately left alone.
"""

from __future__ import annotations

import os

import pytest
import yaml

_CONFIG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "config"
)
_CONFIGS = ("provisa.yaml", "provisa-install.yaml", "provisa-install-base.yaml")


@pytest.fixture(scope="module", params=_CONFIGS)
def demo_config(request) -> dict:
    with open(os.path.join(_CONFIG_DIR, request.param)) as fh:
        return yaml.safe_load(fh)


def _chains(demo_config) -> dict[str, list[str]]:
    """Each role's ancestors (REQ-2155): the standard roles and the config's own."""
    from provisa.core.db import standard_role_dicts
    from provisa.security.inheritance import role_chains

    own = [
        {
            "id": r["id"],
            "capabilities": r.get("capabilities") or [],
            "parents": r.get("parents") or [],
        }
        for r in demo_config.get("roles") or []
    ]
    return role_chains([*standard_role_dicts(), *own])


def test_org_admin_has_full_domain_access_from_the_seed(demo_config):
    # REQ-1349, REQ-2155: org_admin is a standard role, Provisa's alone -- no config may carry an
    # org_admin role block -- and its definition reaches every domain with the org-scoped rights.
    from provisa.core.db import STANDARD_ROLE_RIGHTS, STANDARD_ROLES

    assert "org_admin" not in {r["id"] for r in demo_config.get("roles") or []}
    for right in ("org_settings", "observability"):
        assert right in STANDARD_ROLE_RIGHTS["org_admin"], right
    assert STANDARD_ROLES["org_admin"].domain_access == ("*",)


def test_every_restricted_demo_column_is_visible_to_org_admin(demo_config):
    """A grant reaches org_admin when it names org_admin or a role it inherits (REQ-2155)."""
    from provisa.security.inheritance import holds_grant

    chains = _chains(demo_config)
    hidden = [
        f"{t['table']}.{c['name']}"
        for t in demo_config["tables"]
        for c in t["columns"]
        if c.get("visible_to") and not holds_grant("org_admin", c["visible_to"], chains)
    ]
    assert not hidden, hidden


def test_every_restricted_demo_function_and_webhook_is_visible_to_org_admin(demo_config):
    from provisa.security.inheritance import holds_grant

    chains = _chains(demo_config)
    callables = (demo_config.get("functions") or []) + (demo_config.get("webhooks") or [])
    hidden = [
        c["name"]
        for c in callables
        if c.get("visible_to") and not holds_grant("org_admin", c["visible_to"], chains)
    ]
    assert not hidden, hidden
