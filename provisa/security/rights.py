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
# (visible_to, masking secrets, view SQL) and require the view_governance capability.
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
    USAGE = "usage"
    READ_RESTRICTED = "read_restricted"
    APPROVE_RELATIONSHIP = "approve_relationship"
    CREATE_VIEW = "create_view"
    COLUMN_GRANT = "column_grant"
    USER_MANAGEMENT = "user_management"
    MASKING_CONFIG = "masking_config"
    VIEW_GOVERNANCE = (
        "view_governance"  # REQ-1134: see meta GOVERNANCE columns (visible_to, masks, grants)
    )
    # REQ-1337: read/modify DEPLOYMENT-WIDE settings — federation engine, cache storage, encryption
    # provider, auth provider, the config file itself, query-engine lifecycle. Distinct from ADMIN so
    # the surface is gated by a RIGHT rather than by a role name: platform_admin always holds it,
    # org_admin holds it only in a single-tenant deployment (see apply_tenancy_role_grants).
    PLATFORM_SETTINGS = "platform_settings"
    # REQ-1337: act in an org the principal is not a member of — bind any org on a protocol session,
    # administer any org's invites and lifecycle. Held by platform_admin only; org_admin never holds
    # it in either tenancy mode, because org authority is confined to the org being acted in.
    CROSS_ORG = "cross_org"
    # REQ-1349: read/modify settings scoped to the ORG being acted in — the org's AI model / NL
    # provider overrides, its domains, its scheduled tasks, its pending creation requests. Never
    # deployment-wide (that is PLATFORM_SETTINGS) and never another org (that is CROSS_ORG).
    # org_admin holds it in both tenancy modes; it is the right that makes the Admin tab useful to
    # an org administrator without handing them the platform.
    ORG_SETTINGS = "org_settings"
    # REQ-1349: READ-ONLY performance and health surfaces — overview stats, system health, traces.
    # Held by org_admin so a tenant operator can see how their org is performing; the data behind
    # these surfaces is scoped to the acting org unless the principal also holds CROSS_ORG.
    OBSERVABILITY = "observability"
    # REQ-1573: create or delete an environment and reach the environments admin surface. Creating
    # one spends the org's plan ceiling and deleting one drops a schema, so it is an authoring right
    # rather than a settings one — a developer holds it while holding no ORG_SETTINGS at all.
    ENVIRONMENT_MANAGEMENT = "environment_management"
    # REQ-1573: be served by an environment other than prod — the right the org-routing middleware
    # checks when a request names one. Held by org_admin and developer; an analyst works in prod.
    ENVIRONMENT_SWITCH = "environment_switch"
    # REQ-1590: the business glossary's two rights. Reading the glossary is not administering the
    # org — an analyst looks a term up to understand a column — so it is its own right rather than
    # ORG_SETTINGS, which gated the whole surface and shut every non-admin out of it. GLOSSARY_RW is
    # curation on top of that visibility: rename, definitions, ref moves, edges, experts, and the
    # AI generation endpoints that persist. A curator is granted both; granting RW alone leaves the
    # surface unreachable, because the page itself is gated on READ.
    GLOSSARY_READ = "glossary_read"
    GLOSSARY_RW = "glossary_rw"
    # REQ-1592: curate ANY term in the org, whatever its domains and whoever authored it. The
    # override the two narrower rules need to stay safe: an enterprise-wide term (domains ``["*"]``)
    # belongs to no domain in particular, and a term with authors is theirs — both would otherwise be
    # unmaintainable the moment their people leave. Seeded to org_admin only; GLOSSARY_RW remains the
    # ordinary curator's right, bounded by the domains its ROLE carries.
    ORG_GLOSSARY_RW = "org_glossary_rw"
    # REQ-1634: data products' two rights, on the same seam as the glossary pair above. Reading
    # which products exist is how an analyst/developer/modeler finds what a domain already
    # publishes, so it is a read right rather than ORG_SETTINGS. Creating and deleting a product
    # is catalog curation, on the same footing as TABLE_REGISTRATION — org_admin only.
    DATA_PRODUCT_READ = "data_product_read"
    DATA_PRODUCT_RW = "data_product_rw"
    IGNORE_RELATIONSHIPS = "ignore_relationships"
    WRITE = "write"  # REQ-868: global mutation-execute capability (alias EXECUTE_MUTATION)
    # The two rights the wire surfaces read off a role's list by name. Members here so that the
    # vocabulary a role may be given (unknown_capabilities) is the vocabulary the code consults.
    # Held by roles, checked by nothing a client can reach: it gated CREATE TABLE AS and DDL over
    # pgwire, and nothing is defined through a query protocol any more
    # (provisa/compiler/definitions.py). Its only remaining check is in executor/ctas.run_ctas,
    # which no client entry calls.
    DDL = "ddl"
    NO_AGGREGATIONS = "no_aggregations"  # REQ-197: withholds the _aggregate root fields


# REQ-1297: the four system role ids are the whole role vocabulary — every org schema seeds
# exactly these, all with org_id NULL and identical capabilities everywhere. The former role ids
# 'admin' and 'superadmin' are retired, and neither word is a capability: no string stands in for
# another right (REQ-1327), so check_capability reads the right it is asked about and nothing else.
# Nothing resolves the retired ROLE ids — seed time rewrites existing assignments naming them to
# platform_admin.
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


def domain_access_for_claims(  # REQ-1530
    claims: Iterable[str], roles: dict[str, dict] | None
) -> set[str]:
    """The union of the DOMAIN SCOPES those roles carry.

    The companion of ``capabilities_for_claims``: that one answers what kind of act a member may
    perform, this one answers which objects the act may touch. Read off ``roles.domain_access``,
    because the scope belongs to the ROLE and not to the individual grant — the ``:domain`` suffix a
    claim may carry records which grant was made and is deliberately not consulted here, so that an
    authorization question has one answer rather than two that can disagree (REQ-1530).

    A claim naming a role absent from the registry contributes nothing, matching
    ``capabilities_for_claims``: an unknown role grants no rights and therefore reaches no domains.
    Interpret the result with ``provisa.core.env_authority.domains_within``, which is the one place
    that decides what ``*`` means.
    """
    out: set[str] = set()
    for role_id in role_ids_from_claims(claims):
        role = (roles or {}).get(role_id)
        if role is None:
            continue
        access = role.get("domain_access")
        if access is None:
            raise ValueError(
                f"role {role_id!r} was loaded without domain_access: the column is NOT NULL on the "
                "roles table, so this is a registry built by a loader that dropped the field"
            )
        out.update(access)
    return out


def effective_domain_access_role(role_id: str, roles: dict[str, dict] | None) -> dict:  # REQ-1620
    """``roles[role_id]``, with domain_access widened to the union of every role the caller is
    currently acting as (``request_context.current_role_claims`` — the UI's "Role: All").

    THE one place ``role_id``'s single-role dict is turned into the read-scope answer a V001
    check (or a Cypher label_map's cross-domain visibility) tests against, so every surface that
    reaches the governed pipeline (Cypher, GraphQL, REST, bolt, gRPC, Flight) resolves "All"
    identically instead of each re-deriving it. ``role_id`` itself, and every OTHER field on the
    role (capabilities, RLS, masking, session_vars), stays single-role — only domain_access
    follows the full claim set, matching ``domain_access_for_claims`` (REQ-1530): a real RBAC
    union, grant if ANY assigned role allows it, never an intersection.
    """
    roles = roles or {}
    role = roles.get(role_id) or {}
    from provisa.core.request_context import current_role_claims

    claims = current_role_claims.get()
    if not claims or len(claims) <= 1:
        return role
    union = domain_access_for_claims(claims, roles)
    return {**role, "domain_access": sorted(union)}


def domain_access_for_capability(  # REQ-1592
    claims: "Iterable[str]", roles: dict[str, dict] | None, capability: str
) -> set[str]:
    """The domain scopes carried only by those roles that grant ``capability``.

    ``capabilities_for_claims`` and ``domain_access_for_claims`` union INDEPENDENTLY across every
    role a member holds, which lets two harmless grants compose into one that was never made: a
    role carrying ``glossary_rw`` in sales plus a read-only role scoped to finance resolves to
    ``glossary_rw`` AND finance, and the member curates a domain no role gave them curation in.
    Pairing the right with its own scope closes that: the answer here is the union of
    ``domain_access`` over the roles that actually carry the named right, and nothing else.

    Interpret the result with ``provisa.core.env_authority.domains_within``, the one place that
    decides what ``*`` means on a role's scope.
    """
    out: set[str] = set()
    for role_id in role_ids_from_claims(claims):
        role = (roles or {}).get(role_id)
        if role is None:
            continue
        if capability not in (role.get("capabilities") or []):
            continue
        access = role.get("domain_access")
        if access is None:
            raise ValueError(
                f"role {role_id!r} was loaded without domain_access: the column is NOT NULL on the "
                "roles table, so this is a registry built by a loader that dropped the field"
            )
        out.update(access)
    return out


# REQ-1337: the two PLATFORM rights. Holding either is authority over the deployment rather than over
# an org's data: ``platform_settings`` reaches the deployment-wide settings, ``cross_org`` reaches
# orgs the principal is not a member of. Nothing stands in for them — there is no capability that
# means "every right", so a gate is passed by the right it names and by nothing else.
PLATFORM_RIGHTS: frozenset[str] = frozenset(
    {Capability.PLATFORM_SETTINGS.value, Capability.CROSS_ORG.value}
)

#: The deployment acting on its own behalf: the bootstrap claim, seating an org's creator, and
#: redeeming an invitation whose issue was already checked against its inviter. It holds both
#: platform rights by definition — it is the thing they are rights over.
DEPLOYMENT_GRANTER: frozenset[str] = PLATFORM_RIGHTS


def platform_rights_in(capabilities: Iterable[str] | None) -> set[str]:  # REQ-1337
    """The platform rights present in a capability list."""
    return set(capabilities or ()) & PLATFORM_RIGHTS


def unknown_capabilities(capabilities: Iterable[str] | None) -> list[str]:  # REQ-042
    """The strings in a capability list that name no right at all.

    A role's capabilities are a closed vocabulary — :class:`Capability`. A string outside it is
    read by no gate, so storing one records a grant that means nothing today and whatever a later
    release happens to make of the word.
    """
    known = {c.value for c in Capability}
    return sorted({c for c in (capabilities or ()) if c not in known})


class PlatformRoleGrantError(Exception):
    """A role carrying platform rights was granted by someone who does not hold them."""

    def __init__(self, role_id: str, missing: Iterable[str]):
        self.role_id = role_id
        self.missing = sorted(missing)
        super().__init__(
            f"role {role_id!r} carries platform rights the granter does not hold: "
            + ", ".join(self.missing)
        )


def check_role_grant(  # REQ-1337
    role_id: str,
    role_capabilities: Iterable[str] | None,
    granter_capabilities: Iterable[str],
) -> None:
    """The ONE rule for conferring a role: a platform right is granted only by its holder.

    Every path that writes a role assignment asks this — the users surface, an invitation, an
    auto-join, an environment redemption, the bootstrap claim. ``user_management`` lets an org
    administrator manage their org's people; it must not let them hand out authority over the
    deployment, which they do not hold. A role carrying no platform right passes for any granter:
    whether the granter may manage users at all is the calling surface's gate, not this one.

    Raises :class:`PlatformRoleGrantError` naming the rights the granter lacks.
    """
    missing = platform_rights_in(role_capabilities) - set(granter_capabilities)
    if missing:
        raise PlatformRoleGrantError(role_id, missing)


def carries_platform_right(role_id: str, roles: dict[str, dict] | None) -> bool:  # REQ-1337
    """True when a role carries ANY platform right — ``cross_org`` or ``platform_settings``.

    Wider than :func:`is_control_plane_role`, and used for one thing: deciding which assignments a
    TENANT org ignores. Platform authority is conferred in the root org only, so a tenant org's
    schema naming a role that carries it resolves to nothing there. It is not the test for "has no
    data plane": a single-tenant deployment grants ``platform_settings`` to org_admin, which is the
    data-plane administrator and must keep its schema and its acting role.
    """
    role = (roles or {}).get(role_id) or {}
    return bool(platform_rights_in(role.get("capabilities")))


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
    return Capability.CROSS_ORG.value in set(capabilities)


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
    if required.value not in capabilities:
        role_id = role["id"]
        raise InsufficientRightsError(str(role_id), required)


def has_capability(
    role: dict[str, object], capability: Capability
) -> bool:  # REQ-001, REQ-002, REQ-042
    """Check without raising."""
    capabilities = role.get("capabilities", [])
    if not isinstance(capabilities, (list, tuple, set, frozenset)):
        capabilities = []
    return capability.value in capabilities


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

    ``None`` (unfiltered) is returned for the tier that sees the whole catalog: a role holding
    the meta DOMAIN GRANT (or global ``*``/empty domain access). Every other
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
