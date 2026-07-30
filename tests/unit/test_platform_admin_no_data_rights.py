# Copyright (c) 2026 Kenneth Stott
# Canary: 7446e6eb-1796-47d2-8b1d-a7058c552e04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1297: platform_admin holds no data-plane rights anywhere.

The write-path choke points that make that hold regardless of what a config file or an admin surface
asks for: column grants never name it, and its role definition cannot be redefined away from the
control-plane-only capability set schema.sql seeds.
"""

from __future__ import annotations

import pytest
import yaml

from provisa.core.models import Role
from provisa.core.repositories import role as role_repo
from provisa.core.repositories.table import _control_plane_role_ids, _ungrant_control_plane

CONFIGS = [
    "config/provisa.yaml",
    "config/provisa-install.yaml",
    "config/provisa-install-base.yaml",
]


def test_column_grants_drop_platform_admin():
    # REQ-1337: the strip is keyed on the ``cross_org`` RIGHT, never a role name. The caller resolves
    # the control-plane set from the org's own roles table; here it stands in for the schema.sql seed.
    control_plane = {"platform_admin"}
    assert _ungrant_control_plane(["platform_admin", "analyst", "org_admin"], control_plane) == [
        "analyst",
        "org_admin",
    ]
    assert _ungrant_control_plane(["platform_admin"], control_plane) == []
    assert _ungrant_control_plane(None, control_plane) == []


@pytest.mark.asyncio
async def test_control_plane_roles_resolve_from_the_orgs_own_roles_table():
    # The in-memory state.roles map is populated by build_org_runtime AFTER the config load has
    # registered every table, so resolving the strip from it during a load answered "no control-plane
    # roles" and left platform_admin on every column grant of a freshly provisioned org.
    class _RolesConn:
        async def execute_core(self, _stmt):
            class _R:
                @staticmethod
                def fetchall():
                    return [
                        ("platform_admin", ["cross_org", "org_management"]),
                        ("org_admin", ["query_development", "write"]),
                        ("analyst", None),
                    ]

            return _R()

    assert await _control_plane_role_ids(_RolesConn()) == {"platform_admin"}  # type: ignore[arg-type]


class _RecordingConn:
    def __init__(self) -> None:
        self.upserts: list[dict] = []

    async def upsert(self, _table, values, **_kw):
        self.upserts.append(values)


@pytest.mark.asyncio
async def test_role_upsert_refuses_to_redefine_platform_admin():
    conn = _RecordingConn()
    await role_repo.upsert(
        conn,  # type: ignore[arg-type]  # fake connection records the write instead of issuing it
        Role(
            id="platform_admin", capabilities=["source_registration", "admin"], domain_access=["*"]
        ),
    )
    assert conn.upserts == [], "platform_admin's definition belongs to schema.sql alone"


@pytest.mark.asyncio
async def test_role_upsert_still_writes_other_roles():
    conn = _RecordingConn()
    await role_repo.upsert(
        conn,  # type: ignore[arg-type]
        Role(id="analyst", capabilities=["query_development"], domain_access=["sales"]),
    )
    assert [u["id"] for u in conn.upserts] == ["analyst"]


@pytest.mark.parametrize("path", CONFIGS)
def test_shipped_config_never_names_platform_admin(path):
    # The chips on a tenant org's columns came from the shipped config, not from code: it listed
    # platform_admin in every visible_to and redefined the seeded role with data capabilities.
    cfg = yaml.safe_load(open(path))

    def walk(node, where):
        if isinstance(node, dict):
            for k, v in node.items():
                if k in ("visible_to", "writable_by", "unmasked_to") and v:
                    assert "platform_admin" not in v, f"{path}:{where}/{k}"
                walk(v, f"{where}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{where}[{i}]")

    walk(cfg, "")
    assert "platform_admin" not in {r["id"] for r in cfg.get("roles") or []}
    assert "platform_admin" not in {
        a["role_id"] for a in (cfg.get("auth") or {}).get("default_assignments") or []
    }
