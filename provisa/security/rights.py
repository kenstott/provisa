# Copyright (c) 2026 Kenneth Stott
# Canary: dfc5dbe7-500f-4850-980a-123862c96eaf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Distinct rights model — independently configured per role (REQ-042).

Each capability gates a specific operation. Missing capability → rejection.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import Enum

# Requirements: REQ-001, REQ-002, REQ-003, REQ-038, REQ-042, REQ-125, REQ-263

# The catalog (meta) domain id and its GOVERNANCE column set (REQ-1132/REQ-1134). Defined here in the
# low-level rights module so every query surface (schema build, cypher, SQL validation) shares ONE
# source of truth — the meta visibility rules must be identical across all languages. CORE meta columns
# are structural (names/types/keys) and drive discovery; GOVERNANCE columns expose the security posture
# (visible_to, masking secrets, view SQL) and require the view_governance capability (or admin).
META_DOMAIN_ID = "meta"
GOVERNANCE_META_COLUMNS: frozenset[str] = frozenset(
    {
        "visible_to",
        "unmasked_to",
        "writable_by",
        "mask_type",
        "mask_pattern",
        "mask_replace",
        "mask_value",
        "mask_precision",
        "view_sql",
        "native_filter_type",
        "scope",
    }
)


class Capability(str, Enum):  # REQ-042, REQ-060
    SOURCE_REGISTRATION = "source_registration"
    TABLE_REGISTRATION = "table_registration"
    CREATE_RELATIONSHIP = "create_relationship"
    ACCESS_CONFIG = "access_config"
    QUERY_DEVELOPMENT = "query_development"
    APPROVE_VIEW = "approve_view"
    FULL_RESULTS = "full_results"  # bypass sampling mode
    ADMIN = "admin"
    USAGE = "usage"
    AD_HOC_QUERY = "ad_hoc_query"
    READ_RESTRICTED = "read_restricted"
    APPROVE_RELATIONSHIP = "approve_relationship"
    CREATE_VIEW = "create_view"
    COLUMN_GRANT = "column_grant"
    USER_MANAGEMENT = "user_management"
    MASKING_CONFIG = "masking_config"
    VIEW_GOVERNANCE = "view_governance"  # REQ-1134: see meta GOVERNANCE columns (visible_to, masks, grants)
    SUPERADMIN = "superadmin"
    # REQ-1337: read/modify DEPLOYMENT-WIDE settings — federation engine, cache storage, encryption
    # provider, auth provider, the config file itself, query-engine lifecycle. Distinct from ADMIN so
    # the surface is gated by a RIGHT rather than by a role name: platform_admin always holds it,
    # org_admin holds it only in a single-tenant deployment (see apply_tenancy_role_grants).
    PLATFORM_SETTINGS = "platform_settings"
    # REQ-1337: act in an org the principal is not a member of — bind any org on a protocol session,
    # administer any org's invites and lifecycle. Held by platform_admin only; org_admin never holds
    # it in either tenancy mode, because org authority is confined to the org being acted in.
    CROSS_ORG = "cross_org"
    IGNORE_RELATIONSHIPS = "ignore_relationships"
    WRITE = "write"  # REQ-868: global mutation-execute capability (alias EXECUTE_MUTATION)


# REQ-1297: the four system role ids are the whole role vocabulary — every org schema seeds
# exactly these, all with org_id NULL and identical capabilities everywhere. The former role ids
# 'admin' and 'superadmin' are retired: they survive only as CAPABILITY strings (Capability.ADMIN /
# Capability.SUPERADMIN above, which check_capability treats as the wildcard), and platform_admin is
# the only role carrying them. Nothing resolves the retired ROLE ids — seed time rewrites existing
# assignments naming them to platform_admin.
PLATFORM_ADMIN_ROLE = "platform_admin"
ORG_ADMIN_ROLE = "org_admin"
DEVELOPER_ROLE = "developer"
ANALYST_ROLE = "analyst"
SYSTEM_ROLE_IDS: frozenset[str] = frozenset(
    {PLATFORM_ADMIN_ROLE, ORG_ADMIN_ROLE, DEVELOPER_ROLE, ANALYST_ROLE}
)


def is_tenant_org(org_id: str | None, root_org_id: str) -> bool:  # REQ-1297
    """True when ``org_id`` names a TENANT org — any bound org other than the deployment's root.

    platform_admin is ACTIVELY IGNORED in a tenant org: not merely un-granted, but stripped from the
    resolved assignment set so nothing downstream — capability resolution, /auth/me, the acting role,
    the UI's role picker — can see it there. The platform operator administers org lifecycle and
    infrastructure; a tenant org's data is governed only by roles that org itself granted.
    """
    return org_id is not None and org_id != root_org_id


def role_ids_from_claims(claims: Iterable[str]) -> set[str]:  # REQ-1297
    """The bare role ids in a claim set, dropping the optional ``:domain`` suffix.

    Identity only — NEVER an authorization answer. Every gate must resolve these ids to capabilities
    (``capabilities_for_claims``) and test a RIGHT (REQ-1337).
    """
    return {c.split(":")[0].strip() for c in claims}


def capabilities_for_claims(  # REQ-1337
    claims: Iterable[str], roles: dict[str, dict] | None
) -> set[str]:
    """Resolve role CLAIMS to the union of the capabilities those roles carry.

    The one conversion from role identity to rights, shared by every surface that receives raw
    claims (pgwire, bolt, MCP, the HTTP middleware). ``roles`` is the loaded roles registry; a claim
    naming a role absent from it contributes nothing — an unknown role grants no rights.
    """
    caps: set[str] = set()
    for role_id in role_ids_from_claims(claims):
        role = (roles or {}).get(role_id) or {}
        for c in role.get("capabilities") or []:
            caps.add(c)
    return caps


# REQ-1297: the two capability strings that mean "unrestricted". Only platform_admin carries them
# (see the schema.sql seed), but they are CAPABILITIES, not role ids — which is the whole point:
# a gate names the right, and the seed decides which role holds it. check_capability/has_capability
# above treat ADMIN the same way.
PLATFORM_BYPASS_CAPABILITIES: frozenset[str] = frozenset(
    {Capability.ADMIN.value, Capability.SUPERADMIN.value}
)


def has_platform_bypass(capabilities: Iterable[str]) -> bool:  # REQ-1297, REQ-1337
    """True when a RESOLVED CAPABILITY set carries platform authority.

    Reads RIGHTS ONLY — the platform_admin role id is deliberately not consulted here, and no gate
    anywhere may substitute it (REQ-1337). A surface holding raw role CLAIMS resolves them through
    ``capabilities_for_claims`` first.
    """
    return bool(set(capabilities) & PLATFORM_BYPASS_CAPABILITIES)


def is_control_plane_role(role_id: str, roles: dict[str, dict] | None) -> bool:  # REQ-1337
    """True when a role is a CONTROL-PLANE role — decided by the rights it carries, not its name.

    A control-plane role holds ``cross_org``: authority over org lifecycle, invites and
    infrastructure across orgs. REQ-1327 keeps such a role off the data plane entirely (no schema is
    generated for it, and it is never the acting data role), and REQ-1297 makes it unresolvable
    inside a tenant org. Both rules read this right, so a deployment that mints another
    control-plane role gets the same treatment without any code naming it.
    """
    role = (roles or {}).get(role_id) or {}
    return Capability.CROSS_ORG.value in (role.get("capabilities") or [])


def can_act_cross_org(capabilities: Iterable[str]) -> bool:  # REQ-1337
    """True when a resolved capability set may act in an org the principal is not a member of."""
    caps = set(capabilities)
    return Capability.CROSS_ORG.value in caps or has_platform_bypass(caps)


class InsufficientRightsError(Exception):
    """Raised when a role lacks the required capability."""

    def __init__(self, role_id: str, required: Capability):
        self.role_id = role_id
        self.required = required
        super().__init__(f"Role {role_id!r} lacks required capability: {required.value}")


def check_capability(  # REQ-002, REQ-003, REQ-042
    role: dict[str, object],
    required: Capability,
) -> None:
    """Check that a role has the required capability.

    Raises InsufficientRightsError if not.
    """
    capabilities = role.get("capabilities", [])
    if not isinstance(capabilities, (list, tuple, set, frozenset)):
        capabilities = []
    if required.value not in capabilities and Capability.ADMIN.value not in capabilities:
        role_id = role["id"]
        raise InsufficientRightsError(str(role_id), required)


def has_capability(
    role: dict[str, object], capability: Capability
) -> bool:  # REQ-001, REQ-002, REQ-042
    """Check without raising."""
    capabilities = role.get("capabilities", [])
    if not isinstance(capabilities, (list, tuple, set, frozenset)):
        capabilities = []
    return capability.value in capabilities or Capability.ADMIN.value in capabilities


# The two meta views whose ROWS describe registered tables/columns and are therefore subject to
# the REQ-1132 row-level neighbourhood scoping. Each maps to the meta-view column that carries the
# DESCRIBED table's id, so the row filter is "<column> IN (<reachable table ids>)".
META_ROW_SCOPED_VIEWS: dict[str, str] = {
    "registered_tables_meta": "id",
    "table_columns_meta": "table_id",
}


def compute_meta_row_scope(
    role: dict[str, object] | None,
    tables: list[dict],
    relationships: list[dict] | None,
) -> set[int] | None:
    """REQ-1132: the set of DESCRIBED table ids whose meta rows a role may see, or ``None`` when
    NO row filter applies (all rows visible).

    ``None`` (unfiltered) is returned for the two tiers that see the whole catalog: an ADMIN role,
    and a role holding the meta DOMAIN GRANT (or global ``*``/empty domain access). Every other
    (DEFAULT-tier) role is confined to its directly-accessible tables — those in a domain the role
    can access — PLUS 1-hop neighbours over user-defined/semantic relationships (the ``relationships``
    registry holds only user relationships; auto-derived FK/catalog edges are never stored there, so
    they are excluded by construction). Discovery is bidirectional, EXCEPT a relationship flagged
    ``hide_target_meta`` suppresses the TARGET from discovery via that edge (the source stays
    discoverable from the target side). Computed (function-target) relationships have no concrete
    target table and contribute no neighbour.
    """
    if role is None:
        return None
    if has_capability(role, Capability.ADMIN):
        return None
    accessible = role.get("domain_access") or []
    if not isinstance(accessible, (list, tuple, set, frozenset)):
        accessible = []
    if not accessible or "*" in accessible or META_DOMAIN_ID in accessible:
        return None  # meta domain grant / global access → the whole catalog

    directly = {t["id"] for t in tables if t.get("domain_id") in accessible}
    visible = set(directly)
    for rel in relationships or []:
        sid = rel.get("source_table_id")
        tid = rel.get("target_table_id")
        if sid is None or tid is None or tid == "":
            continue  # computed/function relationship: no concrete target table
        if sid in directly and not rel.get("hide_target_meta"):
            visible.add(tid)
        if tid in directly:
            visible.add(sid)
    return visible
