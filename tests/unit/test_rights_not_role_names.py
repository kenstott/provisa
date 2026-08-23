# Copyright (c) 2026 Kenneth Stott
# Canary: 6f0a2b91-4c7d-4c1a-9f2e-0b5d8a3c71ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1337: authorization is decided by RIGHTS, never by role names.

Two things are asserted here. First, the conversion primitives in provisa.security.rights behave as
the whole codebase now assumes: claims resolve to the union of their roles' capabilities, a role is
control-plane exactly when it carries cross_org, and platform bypass reads capabilities only.
Second — the part that actually protects the rule — no gate in the tree tests a system role id.
A deployment that mints a second control-plane role must get identical treatment without any code
naming it, and that only holds if nothing compares against the name.
"""

from __future__ import annotations

import ast
import pathlib

import pytest
from fastapi import HTTPException

from provisa.security.rights import (
    Capability,
    can_act_cross_org,
    capabilities_for_claims,
    has_platform_bypass,
    is_control_plane_role,
    role_ids_from_claims,
)

# Mirrors the schema.sql seed. platform_admin's list is exactly the control-plane rights; org_admin
# owns the data plane and holds neither platform_settings (multitenant) nor cross_org (ever).
SEEDED_ROLES: dict[str, dict] = {
    "platform_admin": {
        "id": "platform_admin",
        "capabilities": ["admin", "superadmin", "platform_settings", "cross_org"],
    },
    "org_admin": {
        "id": "org_admin",
        "capabilities": ["user_management", "table_registration", "query_development"],
    },
    "analyst": {"id": "analyst", "capabilities": ["usage"]},
}


def test_role_ids_from_claims_strips_the_domain_suffix():
    assert role_ids_from_claims(["analyst:sales", "org_admin", " developer:hr "]) == {
        "analyst",
        "org_admin",
        "developer",
    }


def test_capabilities_for_claims_unions_the_roles_rights():
    assert capabilities_for_claims(["analyst:sales", "org_admin"], SEEDED_ROLES) == {
        "usage",
        "user_management",
        "table_registration",
        "query_development",
    }


def test_an_unknown_role_grants_nothing():
    # The claim names a role the registry does not define. Granting anything here would let a forged
    # or stale claim manufacture rights out of a name.
    assert capabilities_for_claims(["platform_admin_v2"], SEEDED_ROLES) == set()
    assert capabilities_for_claims(["platform_admin"], None) == set()


def test_platform_bypass_reads_capabilities_not_the_role_id():
    assert has_platform_bypass({"admin"})
    assert has_platform_bypass({"superadmin"})
    assert not has_platform_bypass({"platform_admin"}), (
        "the role id must never be accepted as a right"
    )
    assert not has_platform_bypass({"user_management", "table_registration"})


def test_control_plane_is_decided_by_cross_org():
    assert is_control_plane_role("platform_admin", SEEDED_ROLES)
    assert not is_control_plane_role("org_admin", SEEDED_ROLES)
    assert not is_control_plane_role("analyst", SEEDED_ROLES)
    # A second control-plane role gets the same treatment with no code naming it.
    minted = {**SEEDED_ROLES, "fleet_operator": {"capabilities": ["cross_org"]}}
    assert is_control_plane_role("fleet_operator", minted)
    assert not is_control_plane_role("unknown", minted)


def test_can_act_cross_org():
    assert can_act_cross_org({"cross_org"})
    assert can_act_cross_org({"admin"})  # platform bypass subsumes it
    assert not can_act_cross_org({"user_management"})
    assert not can_act_cross_org(set())


def test_org_admin_never_holds_cross_org_in_the_seed():
    caps = capabilities_for_claims(["org_admin"], SEEDED_ROLES)
    assert Capability.CROSS_ORG.value not in caps
    assert not can_act_cross_org(caps), "org authority is confined to the org being acted in"


# --- the rule itself: no gate compares against a system role id ---------------------------------

# The only two FUNCTIONS in which a role id may be compared, both deciding something about a grant
# rather than about what a caller may do. Exempting the function and not the module matters: the
# inviter's own authorization gate lives in invites_router alongside the exempt one, and it stays
# scanned. An entry here is a hole in the rule, so a function earns one only by carrying a
# comparison that cannot be re-expressed as a right. (Constructing an assignment is not a
# comparison, so the seed and grant sites need no exemption at all.)
_GRANT_FUNCTIONS = {
    # Guards platform_admin's seeded row DEFINITION against redefinition by a config file or an admin
    # surface. Reading the row's own rights to decide whether to let those rights be rewritten is
    # circular, so the identity of the protected row is the only available test.
    ("provisa/core/repositories/role.py", "upsert"),
    # Validates which role an invitation may CONFER and into which org — a statement about the grant
    # being minted, not about the inviter's authority.
    ("provisa/api/admin/invites_router.py", "resolve_invite_role"),
    # Portable (SQLite/non-PG) mirror of apply_tenancy_role_grants's own grant-plane UPDATEs — the
    # PG path expresses the identical role-id-keyed asserts as raw SQL text (not a Python Compare
    # node), so only this Core-based mirror needs the exemption; same REQ-1337 grant semantics.
    ("provisa/core/db.py", "_apply_tenancy_role_grants_portable"),
}

_ROLE_ID_LITERALS = {"platform_admin", "org_admin", "developer", "analyst"}
# The same ids reached through the named constants — a gate written as `role.id == PLATFORM_ADMIN_ROLE`
# is exactly the violation, spelled indirectly.
_ROLE_ID_CONSTANTS = {
    "PLATFORM_ADMIN_ROLE",
    "ORG_ADMIN_ROLE",
    "DEVELOPER_ROLE",
    "ANALYST_ROLE",
    "SYSTEM_ROLE_IDS",
}


def _python_sources() -> list[pathlib.Path]:
    root = pathlib.Path(__file__).resolve().parents[2] / "provisa"
    return sorted(root.rglob("*.py"))


def _exempt_line_ranges(tree: ast.AST, rel: str) -> list[tuple[int, int]]:
    """The line spans of the exempt functions in this module."""
    spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if (rel, node.name) in _GRANT_FUNCTIONS:
                spans.append((node.lineno, node.end_lineno or node.lineno))
    return spans


def _comparisons_against_role_ids(tree: ast.AST) -> list[tuple[int, str]]:
    """Every ``x == "<role id>"`` / ``x in {"<role id>", ...}`` comparison in a module.

    A comparison is the shape a GATE takes; a bare literal (a dict key, a seed value, a log message)
    is not, so only Compare nodes are reported.

    A comparison against a SQLAlchemy column (``table.c.role_id == ORG_ADMIN_ROLE``) is excluded: it
    SELECTS a grant row to read or revoke, which is the grant plane, not a decision about what the
    caller may do. The caller's own authorization on those same handlers is a separate check, and
    that one is still scanned.
    """
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        if any(
            isinstance(o, ast.Attribute)
            and isinstance(o.value, ast.Attribute)
            and o.value.attr == "c"
            for o in [node.left, *node.comparators]
        ):
            continue  # SQLAlchemy column predicate — row selection
        for operand in [node.left, *node.comparators]:
            elts = (
                operand.elts if isinstance(operand, (ast.Set, ast.List, ast.Tuple)) else [operand]
            )
            for e in elts:
                if isinstance(e, ast.Constant) and e.value in _ROLE_ID_LITERALS:
                    hits.append((node.lineno, str(e.value)))
                elif isinstance(e, ast.Name) and e.id in _ROLE_ID_CONSTANTS:
                    hits.append((node.lineno, e.id))
                elif isinstance(e, ast.Attribute) and e.attr in _ROLE_ID_CONSTANTS:
                    hits.append((node.lineno, e.attr))
    return hits


@pytest.mark.parametrize("path", _python_sources(), ids=lambda p: p.name)
def test_no_module_gates_on_a_role_id(path: pathlib.Path):
    root = pathlib.Path(__file__).resolve().parents[2]
    rel = path.relative_to(root).as_posix()
    tree = ast.parse(path.read_text())
    exempt = _exempt_line_ranges(tree, rel)
    hits = [
        h
        for h in _comparisons_against_role_ids(tree)
        if not any(lo <= h[0] <= hi for lo, hi in exempt)
    ]
    assert not hits, (
        f"{rel} compares against a role id at lines {sorted({h[0] for h in hits})} — REQ-1337 "
        "requires the gate to name a RIGHT and let the seed decide which role carries it"
    )


# --- the platform-settings gate ------------------------------------------------------------------


class _Identity:
    def __init__(self, user_id: str, roles: list[str]) -> None:
        self.user_id = user_id
        self.roles = roles


class _Request:
    def __init__(self, identity) -> None:
        self.state = type("S", (), {"identity": identity, "active_org_id": "acme"})()


def _guard(identity, roles=SEEDED_ROLES, *, monkeypatch=None):
    """Run the platform-settings gate against a roles registry.

    The gate resolves the caller's claims through the live app state, so the registry is swapped
    wholesale — a unit test never builds app state, and an unpopulated registry would silently mean
    "no rights", which is not what any of these cases is asserting.
    """
    import provisa.api.app as app_mod
    from provisa.api.admin._platform_guard import require_platform_settings

    monkeypatch.setattr(app_mod, "state", type("S", (), {"roles": roles})())
    return require_platform_settings(_Request(identity))  # pyright: ignore[reportArgumentType]


def test_platform_settings_allows_a_holder_of_the_right(monkeypatch):
    # platform_admin holds platform_settings in BOTH tenancy modes.
    _guard(_Identity("root", ["platform_admin"]), monkeypatch=monkeypatch)


def test_platform_settings_denies_a_multitenant_org_admin(monkeypatch):
    # org_admin's seeded list carries no platform_settings: this is the multitenant grant, and the
    # deployment-wide settings surface is closed to it.
    with pytest.raises(HTTPException) as ei:
        _guard(_Identity("alice", ["org_admin"]), monkeypatch=monkeypatch)
    assert ei.value.status_code == 403


def test_platform_settings_allows_a_single_tenant_org_admin(monkeypatch):
    # apply_tenancy_role_grants adds platform_settings to org_admin in a single-tenant deployment;
    # the SAME gate then admits it. Tenancy decides the grant, never the gate.
    granted = {
        **SEEDED_ROLES,
        "org_admin": {
            "id": "org_admin",
            "capabilities": [*SEEDED_ROLES["org_admin"]["capabilities"], "platform_settings"],
        },
    }
    _guard(_Identity("alice", ["org_admin"]), granted, monkeypatch=monkeypatch)


def test_platform_settings_allows_dev_anonymous(monkeypatch):
    _guard(None, monkeypatch=monkeypatch)
    _guard(_Identity("anonymous", []), monkeypatch=monkeypatch)
