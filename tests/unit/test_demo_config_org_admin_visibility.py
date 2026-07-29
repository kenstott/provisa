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


def test_org_admin_is_a_configured_role_with_full_domain_access(demo_config):
    role = next(r for r in demo_config["roles"] if r["id"] == "org_admin")
    assert role["domain_access"] == ["*"], role


def test_every_restricted_demo_column_is_visible_to_org_admin(demo_config):
    hidden = [
        f"{t['table']}.{c['name']}"
        for t in demo_config["tables"]
        for c in t["columns"]
        if c.get("visible_to") and "org_admin" not in c["visible_to"]
    ]
    assert not hidden, hidden


def test_every_restricted_demo_function_and_webhook_is_visible_to_org_admin(demo_config):
    callables = (demo_config.get("functions") or []) + (demo_config.get("webhooks") or [])
    hidden = [
        c["name"] for c in callables if c.get("visible_to") and "org_admin" not in c["visible_to"]
    ]
    assert not hidden, hidden
