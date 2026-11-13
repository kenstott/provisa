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
from typing import Any, cast

from provisa.transpiler.metadata_views import (
    CATALOG_ROW_SCOPES,
    CATALOG_VIEW_NAMES,
    NEUTRAL_VIEW_NAMES,
)

# Requirements: REQ-001, REQ-002, REQ-003, REQ-038, REQ-042, REQ-125, REQ-263

# The catalog (meta) domain id and its GOVERNANCE column set (REQ-1132/REQ-1134). Defined here in the
# low-level rights module so every query surface (schema build, cypher, SQL validation) shares ONE
# source of truth — the meta visibility rules must be identical across all languages. CORE meta columns
# are structural (names/types/keys) and drive discovery; GOVERNANCE columns expose the security posture
# (visible_to, masking secrets, view SQL) and require the view_governance capability.
META_DOMAIN_ID = "meta"
# REQ-2163: the ontology system schema -- the glossary's definitions (and the Class tables
# generated from them), as meta holds the model's structure.
ONTOLOGY_DOMAIN_ID = "ontology"
OPS_DOMAIN_ID = "ops"
GOVERNANCE_META_COLUMNS: frozenset[str] = frozenset(
    {
        "visible_to",
        "masks",
        "view_sql",
        "native_filter_type",
        "scope",
        # REQ-2091: how a sensitive column is hidden, as its masks are.
        "fake",
        "synthetic_rule",
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
    APPROVE_RELATIONSHIP = "approve_relationship"
    CREATE_VIEW = "create_view"
    COLUMN_GRANT = "column_grant"
    USER_MANAGEMENT = "user_management"
    # REQ-2219: set a principal's entitlement lists, per role grant and kind. Scoped by the roles
    # kind: roles [dev] reaches dev and the roles with dev as an ancestor.
    ASSIGN_ENTITLEMENTS = "assign_entitlements"
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
    # REQ-2155: manage the environments the caller owns (created_by) -- create one, becoming its
    # owner, and delete one it owns; ENVIRONMENT_MANAGEMENT manages any.
    ENVIRONMENT_OWNED_MANAGEMENT = "environment_owned_management"
    # REQ-1942: change an environment's data choices -- its data mode, its sources' bindings,
    # read-only or read-write, its synthetic settings -- including creating it reading real data.
    # Held by org_admin alone: a developer switching an environment to Test (fake) with no fakes
    # declared would expose real data.
    ENVIRONMENT_DATA = "environment_data"
    # REQ-1943: reveal or hide a sensitive column -- add or remove a sensitive tag on a column,
    # set or clear a tag's Sensitive data option, change a sensitive column's role masks, fake,
    # synthetic rule or column grants -- in every environment, prod included.
    SENSITIVE_DATA = "sensitive_data"
    # REQ-1590: the business glossary's two rights. Reading the glossary is not administering the
    # org — an analyst looks a term up to understand a column — so it is its own right rather than
    # ORG_SETTINGS, which gated the whole surface and shut every non-admin out of it. GLOSSARY_RW is
    # curation on top of that visibility: rename, definitions, ref moves, edges, experts, and the
    # AI generation endpoints that persist. A curator is granted both; granting RW alone leaves the
    # surface unreachable, because the page itself is gated on READ.
    GLOSSARY_READ = "glossary_read"
    # REQ-1132: read the meta domain, the model's structure. Viewer does not hold it: a role holds
    # it by naming it, under a parent holding it (REQ-2155). A column granted to viewer in meta is
    # the holder's (inheritance.expand_meta_grants); with no grant a role has the default tier.
    CATALOG_READ = "catalog_read"
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
    # is catalog curation, on the same footing as TABLE_REGISTRATION: org_admin's. REQ-2188: and the
    # rw of a role named in a product's Owner field, held to that product by a generated row rule.
    DATA_PRODUCT_READ = "data_product_read"
    DATA_PRODUCT_RW = "data_product_rw"
    # REQ-2155: data quality's own rights, so managing checks no longer needs TABLE_REGISTRATION.
    # DQ_READ reads check definitions and results; DQ_RW manages checks, expectations and
    # promises (REQ-2157).
    DQ_READ = "dq_read"
    DQ_RW = "dq_rw"
    # REQ-2229: send OpenLineage run events to the receiver, for a tool's service account.
    LINEAGE_REPORT = "lineage_report"
    IGNORE_RELATIONSHIPS = "ignore_relationships"
    WRITE = "write"  # REQ-868: global mutation-execute capability (alias EXECUTE_MUTATION)
    # The right the wire surfaces read off a role's list by name. A member here so that the
    # vocabulary a role may be given (unknown_capabilities) is the vocabulary the code consults.
    NO_AGGREGATIONS = "no_aggregations"  # REQ-197: withholds the _aggregate root fields
    # REQ-1921: set, change or remove where the data of a table, a source or a materialized view
    # lives (its region), for the region values the role's grant lists (``residency_values``),
    # beside the domain ownership the edit needs anyway. Exists only when the platform declares
    # regions; region is never an access dimension.
    DATA_RESIDENCY = "data_residency"
    # REQ-870: run the commands the role is assigned, in the domains it lists. Alone it reads no
    # data (reads_data). The read-only catalog commands of meta stay under CATALOG_READ.
    COMMAND_EXECUTION = "command_execution"


# REQ-1132: the system domains and the capability that holds each, THE list every other reads.
# Access to one is a capability, inherited as any capability is; inheritance.flatten_role_dicts adds
# the domains a role's capabilities open to its domain_access. A role's stored domain_access names
# data domains only, and "*" means every data domain.
SYSTEM_DOMAIN_CAPABILITIES: dict[str, str] = {
    META_DOMAIN_ID: Capability.CATALOG_READ.value,
    ONTOLOGY_DOMAIN_ID: Capability.GLOSSARY_READ.value,
    OPS_DOMAIN_ID: Capability.OBSERVABILITY.value,
}
SYSTEM_DOMAINS: frozenset[str] = frozenset(SYSTEM_DOMAIN_CAPABILITIES)
# The system domains a statement may join to with no registered relationship (REQ-1132, REQ-2163).
# Ops rows are data about the org's activity: a join to them is a relationship like any other.
TRAVERSABLE_DOMAINS: frozenset[str] = SYSTEM_DOMAINS - {OPS_DOMAIN_ID}


class MissingDomainCapability(PermissionError):
    """A read of a system domain by a role without its capability, refused naming the capability on
    every surface: to that role the table is in no schema, which would otherwise read as unknown."""

    def __init__(self, role_id: str, domain_id: str) -> None:
        self.role_id = role_id
        self.domain_id = domain_id
        self.capability = SYSTEM_DOMAIN_CAPABILITIES[domain_id]
        super().__init__(missing_capability_message(role_id, domain_id))


class ListedOnly(PermissionError):
    """A read of a table a role can only list: its metadata is the role's, its data never is
    (REQ-1132, REQ-2153)."""

    def __init__(self, role_id: str, table: str) -> None:
        self.role_id = role_id
        self.table = table
        super().__init__(listed_only_message(role_id, table))


def listed_only_message(role_id: str, table: str) -> str:
    return f"Role {role_id!r} can only list {table!r}: its metadata can be read, its data cannot"


def missing_capability_message(role_id: str, domain_id: str) -> str:
    capability = SYSTEM_DOMAIN_CAPABILITIES[domain_id]
    return (
        f"Role {role_id!r} lacks the {capability!r} capability, which reads the {domain_id} domain"
    )


def normalized_reach(domains: Iterable[str]) -> list[str]:
    """A flattened ``domain_access``: ``"*"`` stands for every data domain, so it stands alone
    among them, and the system domains a role holds stay beside it."""
    listed = set(domains)
    return sorted({ALL_DOMAINS} | (listed & SYSTEM_DOMAINS) if ALL_DOMAINS in listed else listed)


# REQ-1297: the four system role ids are the whole role vocabulary — every org schema seeds
# exactly these, all with org_id NULL and identical capabilities everywhere. The former role ids
# 'admin' and 'superadmin' are retired, and neither word is a capability: no string stands in for
# another right (REQ-1327), so check_capability reads the right it is asked about and nothing else.
# Nothing resolves the retired ROLE ids — seed time rewrites existing assignments naming them to
# platform_admin.
PLATFORM_ADMIN_ROLE = "platform_admin"
ORG_ADMIN_ROLE = "org_admin"
#: A meta-role's id: this prefix, then its members joined by ``+`` (security/meta_role.py).
META_PREFIX = "meta:"


def is_meta_role_id(role_id: str | None) -> bool:
    """Whether ``role_id`` names a meta-role (REQ-2188)."""
    return bool(role_id) and str(role_id).startswith(META_PREFIX)


def members_of(role_id: str) -> tuple[str, ...]:  # REQ-2188
    """The roles ``role_id`` names: a meta-role's, from its id (meta_role.meta_role_id), else
    itself. For a caller that holds no runtime: a session variable bound at a transport's edge."""
    if not role_id.startswith(META_PREFIX):
        return (role_id,)
    return tuple(role_id[len(META_PREFIX) :].split("+"))


def _draft_right_reach(role: dict, roles: dict[str, dict]) -> set[str]:  # REQ-1921
    """The domains ``role`` holds the right a draft is read by: table registration, the right a
    draft's preview needs (A DRAFT IS SERVED TO ORG_ADMIN, decided 2026-11-12). Held as every
    right is: by the role, a role it inherits from, or a member of a meta role (``acting_chain``,
    REQ-2188), each paired with the domains of the role carrying it (REQ-1944,
    :func:`domain_access_for_capability`). The one place the right is named."""
    return domain_access_for_capability(
        role["acting_chain"], roles, Capability.TABLE_REGISTRATION.value
    )


def registers_tables_in(role: dict, roles: dict[str, dict], domain_id: str) -> bool:  # REQ-1921
    """Whether ``role`` (a built role, state.roles) holds table registration reaching
    ``domain_id``: a draft of that domain is served to it."""
    reach = _draft_right_reach(role, roles)
    return bool(reach) and reaches_domain(sorted(reach), domain_id)


def draft_reach(role: dict, roles: dict[str, dict]) -> frozenset[str] | None:  # REQ-1921
    """The domains whose drafts are served to ``role``: those it holds table registration in
    (:func:`registers_tables_in`); None for every data domain, empty for none."""
    reach = _draft_right_reach(role, roles)
    if not reach:
        return frozenset()
    if reaches_all_domains(sorted(reach)):
        return None
    return frozenset(reach)


DEVELOPER_ROLE = "developer"
ANALYST_ROLE = "analyst"
#: The standard role every role builds on (provisa/core/standard_roles.yaml).
VIEWER_ROLE = "viewer"
SYSTEM_ROLE_IDS: frozenset[str] = frozenset(
    {PLATFORM_ADMIN_ROLE, ORG_ADMIN_ROLE, DEVELOPER_ROLE, ANALYST_ROLE}
)


def domains_added(
    before: Iterable[str] | None, after: Iterable[str], *, empty_is_all: bool = False
) -> set[str]:  # REQ-1531
    """The domains a change ADDS to a list of domains: those in ``after`` and not in ``before``.

    ``before`` is None for an object being created, which reached nothing. ``ALL_DOMAINS`` in a
    list is every domain: a list that gains it adds ``ALL_DOMAINS`` and nothing narrower, and a
    list that already had it gains nothing whatever else it now names. ``empty_is_all`` is for a
    list whose EMPTY state means every domain (a source's ``allowed_domains``), so that emptying
    it is seen as the widening it is.
    """

    def _reach(domains: Iterable[str]) -> set[str]:
        reach = {d for d in domains if d}
        return {ALL_DOMAINS} if empty_is_all and not reach else reach

    was = set() if before is None else _reach(before)
    now = _reach(after)
    if ALL_DOMAINS in was:
        return set()
    if ALL_DOMAINS in now:
        return {ALL_DOMAINS}
    return now - was


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


def acting_claims(identity: Any) -> list[str]:
    """The role claims ``identity`` acts as in this request: what rights are read from. Raises
    when nothing bound them -- an identity the auth middleware has not resolved has no acting
    roles, and reading every role it holds instead would hand it rights it did not ask to use."""
    acting = getattr(identity, "acting_roles", None)
    if acting is None:
        raise RuntimeError(
            f"identity {getattr(identity, 'user_id', '?')!r} has no acting roles: rights are read "
            "from the roles a request acts as, which the auth middleware binds"
        )
    return list(acting)


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
        if role is None or command_only(
            role.get("capabilities")
        ):  # REQ-870: its reach is commands'
            continue
        access = role.get("domain_access")
        if access is None:
            raise ValueError(
                f"role {role_id!r} was loaded without domain_access: the column is NOT NULL on the "
                "roles table, so this is a registry built by a loader that dropped the field"
            )
        out.update(access)
    return out


def residency_values_for_claims(  # REQ-1921
    claims: Iterable[str], roles: dict[str, dict] | None
) -> set[str] | None:
    """The region values the data_residency grants of those roles cover (the org's region ids
    and ``residency.NO_REGION``), or None when none of them holds data_residency."""
    covered: set[str] | None = None
    for role_id in role_ids_from_claims(claims):
        role = (roles or {}).get(role_id)
        if role is None or Capability.DATA_RESIDENCY.value not in (role.get("capabilities") or []):
            continue
        covered = (covered or set()) | set(role["residency_values"])
    return covered


class UnknownRoleError(LookupError):
    """A role id that names no loaded role."""

    def __init__(self, role_id: str):
        self.role_id = role_id
        super().__init__(f"No role {role_id!r} is loaded")


def require_role(roles: dict[str, dict] | None, role_id: str) -> dict:  # REQ-042
    """``roles[role_id]`` — or :class:`UnknownRoleError`. Never an empty stand-in.

    Governance reads everything it decides off the role: its capabilities, its domain scope, its
    row cap. A missing role read as ``{}`` is a role with no restrictions recorded, which every
    reader would have to remember to treat as "nothing" rather than "anything". So there is no
    such value: the role is there, or the request fails.
    """
    role = (roles or {}).get(role_id)
    if role is None:
        raise UnknownRoleError(role_id)
    return role


#: The one entry in a role's ``domain_access`` that means every domain.
ALL_DOMAINS = "*"


def reaches_all_domains(domain_access: Iterable[str] | None) -> bool:  # REQ-039, REQ-471, REQ-1530
    """Whether a ROLE's ``domain_access`` reaches every DATA domain — THE one place that is decided.

    A role reaches the domains it lists and no others. ``"*"`` is the only way to say all: an
    EMPTY list is no domains, never "unrestricted". (A column's or action's ``visible_to=[]`` is a
    different list with the opposite reading — visible to every role — and is not decided here.)

    The single exemption is single-domain mode (``naming.use_domains: false``): the deployment has
    one domain, domains are not a gate, and every role reaches it whatever its list says. It is
    asked here so that no reader re-derives it.

    ``None`` is not a scope. It is a role that was never loaded, and reading it as either answer
    would be a guess, so it raises.
    """
    if domain_access is None:
        raise ValueError(
            "a role's domain_access is missing: the column is NOT NULL on the roles table, so "
            "this is a role that was not loaded, and a missing role reaches nothing by default"
        )
    from provisa.core import domain_policy

    return domain_policy.single_domain() or ALL_DOMAINS in domain_access


def reaches_domain(domain_access: Iterable[str] | None, domain_id: str) -> bool:
    """Whether a role's ``domain_access`` reaches ``domain_id`` (see :func:`reaches_all_domains`).

    ``"*"`` is every DATA domain: a system domain is reached only when it is listed, which
    ``inheritance.flatten_role_dicts`` does for the role holding its capability (REQ-1132)."""
    if domain_id in SYSTEM_DOMAIN_CAPABILITIES:
        return domain_id in (domain_access or ())
    return reaches_all_domains(domain_access) or domain_id in (domain_access or ())


def reads_data(capabilities: Iterable[str] | None) -> bool:
    """Whether a role holding ``capabilities`` reads data: a right other than running commands.
    A role with no capabilities reads nothing, and nor does one holding only command_execution,
    whose domains are where its commands run (REQ-870)."""
    return bool(set(capabilities or ()) - {Capability.COMMAND_EXECUTION.value})


def command_only(capabilities: Iterable[str] | None) -> bool:
    """Whether ``capabilities`` are command_execution and nothing else (REQ-870)."""
    return set(capabilities or ()) == {Capability.COMMAND_EXECUTION.value}


#: On a meta-role only: the domains its commands run in, the reach of the members holding
#: command_execution (security/meta_role.py). Each member's right stays paired with its own reach.
COMMAND_DOMAIN_ACCESS = "command_domain_access"


def lists_data_domain(domain_access: Iterable[str] | None) -> bool:
    """Whether ``domain_access`` names a data domain or ``"*"``: what a system domain added for its
    capability does not make a role that lists no domain a role with a data surface."""
    return bool(set(domain_access or ()) - SYSTEM_DOMAINS)


def acts_in_domain(domain_access: Iterable[str] | None, domain_id: str) -> bool:
    """Whether a role may WRITE or run a command in ``domain_id``: it reaches it, and a system
    domain only as a role reaching every domain. A capability opens a system domain to read; it
    brings no write and no command (REQ-1132)."""
    if not reaches_domain(domain_access, domain_id):
        return False
    return domain_id not in SYSTEM_DOMAINS or reaches_all_domains(domain_access)


def reaches_command_domain(domain_access: Iterable[str] | None, domain_id: str) -> bool:
    """Command admission's reach: :func:`acts_in_domain`, or meta for a role holding catalog_read,
    whose commands (the APOC set, REQ-2168) read the catalog and are admitted by their grants."""
    if domain_id == META_DOMAIN_ID:
        return reaches_domain(domain_access, domain_id)
    return acts_in_domain(domain_access, domain_id)


# --- which columns a role is served (REQ-039, REQ-1133, REQ-1959) --------------------------------

from provisa.core.column_scope import SCOPE_LISTED, SCOPE_PUBLIC  # noqa: E402

# A column's ``scope`` (core/column_scope.py): ``domain`` and ``listed`` are served within the
# domains a role reaches, ``public`` also outside them. ``listed`` adds only names in the catalog.


def column_served(role: dict, domain_id: str | None, column: dict, *, reaches: bool) -> bool:
    """Whether ``role`` is served ``column`` of a table in ``domain_id`` — THE rule, read by the
    schema build, SQL governance, the direct-read (V001) check and the catalog's row scope.

    ``reaches``: whether the role reaches the table's domain, as the caller decides it (a domain
    it lists, ``*``, or a system domain whose capability it holds).

    The column's own grant comes first, and it is the only thing that says who: ``visible_to``
    names the role (a grant to an ancestor is folded down to its descendants when roles load,
    security.inheritance.expand_grants), and an empty list is no role (REQ-2154). Within reach
    that is all. Outside reach a column is served only when it is ``public``: publishing removes
    the domain condition for that column and nothing else -- its grant, masks, row rules and
    limits apply as they do within the domain. A ``listed`` column is served as a ``domain`` one:
    its names in the catalog are not data. An ``own-domain`` column is served as a ``domain`` one
    to the roles its grant names once the build has narrowed that grant to the roles whose own
    domain access names the table's domain (``inheritance.narrow_own_domain_grants``, REQ-2153). A parameter column of an API table
    (``native_filter_type``) is an argument, not data, and publishes nothing.
    """
    if role["id"] not in (column.get("visible_to") or ()):
        return False
    if reaches:
        return True
    return column.get("scope") == SCOPE_PUBLIC and not column.get("native_filter_type")


def served_roles(  # REQ-2152
    roles: dict[str, dict], chains: dict[str, list[str]], domain_id: str, column: dict
) -> frozenset[str]:
    """The roles of ``roles`` (id → definition) a column of a table in ``domain_id`` is served to,
    by :func:`column_served`: its stored grant expanded down ``chains`` as the schema build
    expands it (``inheritance.expand_grants``) and narrows an own-domain one
    (``inheritance.narrow_own_domain_grants``, each definition carrying its own domain access),
    each role's reach its own ``domain_access``."""
    from provisa.security.inheritance import expand_grants, narrow_own_domain_grants

    granted = {**column, "visible_to": list(column.get("visible_to") or [])}
    expand_grants([granted], chains)
    narrow_own_domain_grants([{"domain_id": domain_id, "columns": [granted]}], roles.values())
    return frozenset(
        role_id
        for role_id, definition in roles.items()
        if reads_data(definition["capabilities"])  # REQ-2155, REQ-870
        and column_served(
            {"id": role_id},
            domain_id,
            granted,
            reaches=reaches_domain(definition["domain_access"], domain_id),
        )
    )


def served_across_domains(role: dict, table: dict) -> bool:
    """Whether ``role``, which does not reach ``table``'s domain, is served the table anyway: at
    least one of its data columns is public and granted to it (REQ-1959)."""
    domain_id = table.get("domain_id")
    return any(
        column_served(role, domain_id, column, reaches=False)
        for column in table.get("columns") or []
    )


def serves_table(role: dict, table: dict) -> bool:
    """Whether ``role`` is served ``table``: at least one of its data columns, by the one column
    rule (:func:`column_served`) — within a domain the role reaches, or published across domains
    (REQ-1959). A parameter column is an argument, not data."""
    domain_id = cast("str", table.get("domain_id"))
    reaches = reaches_domain(role["domain_access"], domain_id)
    return any(
        not column.get("native_filter_type")
        and column_served(role, domain_id, column, reaches=reaches)
        for column in table.get("columns") or []
    )


def is_control_plane_definition(capabilities: Iterable[str] | None) -> bool:  # REQ-1337
    """True when a role's capabilities are platform rights and nothing else.

    Such a role is over the deployment and holds no data right for a domain scope to apply to.
    Decided by the RIGHTS it carries, never by its id.
    """
    caps = set(capabilities or ())
    return bool(caps & PLATFORM_RIGHTS) and not (caps - PLATFORM_RIGHTS)


def role_domain_problem(  # REQ-039, REQ-1530
    role_id: str, capabilities: Iterable[str] | None, domain_access: Iterable[str] | None
) -> str | None:
    """Why a role may not be DEFINED with this ``domain_access``, or None when it may.

    A role is always one or more domains, or all: its list names at least one domain, or ``"*"``.
    An empty list is never what an administrator means to save — it reads no data — so it is
    refused where the role is defined rather than discovered when the role is used. Both
    arguments are the role's EFFECTIVE values (its own plus what its parent chain hands down).

    The exceptions hold no data right, so they list no domain: a role whose capabilities are
    platform rights and nothing else (:func:`is_control_plane_definition`), and a role with no
    capabilities (REQ-2155): a name, session values, a command's assignee or a building block.
    """
    listed = set(domain_access or ())
    if listed & SYSTEM_DOMAINS:
        return (
            f"Role {role_id!r} lists {sorted(listed & SYSTEM_DOMAINS)}: a system domain is held as "
            "a capability, not listed"
        )
    if listed:
        return None
    if is_control_plane_definition(capabilities) or not set(capabilities or ()):
        return None
    return f'Role {role_id!r} must list at least one domain, or "*" for all domains'


def effective_domain_access_role(role_id: str, roles: dict[str, dict] | None) -> dict:  # REQ-1620
    """``roles[role_id]``: the read scope a V001 check (or a Cypher label map's cross-domain
    visibility) tests against. A request acting as several held roles acts as their meta-role
    (security/meta_role.py), whose own domain_access is the union of theirs — so the union
    needs no special case here. A missing acting role is an error, never an empty one."""
    return require_role(roles or {}, role_id)


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


# The meta views whose ROWS describe registered tables/columns and are therefore subject to the
# REQ-1132 row-level neighbourhood scoping. Each maps to the meta-view columns that carry the
# DESCRIBED tables' ids, so the row filter is "<column> IN (<reachable table ids>)" for each.
#: REQ-1921: the meta view holding a draft's row (``draft`` true), which a role holding table
#: registration in the draft's domain reads and no other role does (compiler/stage2;
#: :func:`draft_reach`).
DRAFT_ROWS_VIEW = "registered_tables_meta"

META_ROW_SCOPED_VIEWS: dict[str, tuple[str, ...]] = {
    "registered_tables_meta": ("id",),
    "table_columns_meta": ("table_id",),
    # REQ-2145: a column's tags are seen where the column is.
    "effective_column_tags": ("table_id",),
    # REQ-2190: a table's catalog entry, its columns' and its keys'; a foreign key is read where
    # the table it references is too.
    "table_details": ("id",),
    "column_details": ("table_id",),
    "table_keys": ("table_id", "referenced_table_id"),
    # REQ-194: a name is read where the table it names (or the table holding it) is.
    "model_names_meta": ("table_id",),
}
#: REQ-2208 (THE TAGS REPORT): the relations of tags on objects, each (domain, relation). A row is
#: read exactly when the tag admin reads that assignment (api.admin.schema_query.
#: _readable_assignments): by the domains of its object and the right to read a row rule, a role's
#: definition or a metric (compiler.stage2.tag_row_predicate), never by the table scope above.
TAG_ROW_VIEWS: frozenset[tuple[str, str]] = frozenset(
    {(META_DOMAIN_ID, "tag_instances"), (OPS_DOMAIN_ID, "tags_report")}
)
#: REQ-2208 (decision 2026-11-13): the relations connecting tags, library documents and sections,
#: and objects, each (domain, relation) to whether its rows may name an object. A row is read where
#: its document is, as a library document is (its ``read_document_domains``); a row naming an object
#: (its ``object_key`` set) is read only where the tag on that object is too, by the tags report's
#: rule (compiler.stage2.library_row_predicate).
LIBRARY_ROW_VIEWS: dict[tuple[str, str], bool] = {
    (META_DOMAIN_ID, "library_documents"): False,
    (META_DOMAIN_ID, "tag_citations"): True,
    (META_DOMAIN_ID, "section_tags"): False,
    (META_DOMAIN_ID, "policy_coverage"): True,
}
#: REQ-2190: the pg_catalog and information_schema views, each a row's expressions naming the
#: tables it is about; a row about none is every role's (provisa.transpiler.metadata_views).
META_CATALOG_ROW_SCOPES = CATALOG_ROW_SCOPES


#: REQ-2153: the meta view of listed names; a reader sees those of domains it does not reach.
LISTED_NAMES_VIEW = "listed_names"
#: REQ-1132: the meta tables a role reads with no meta grant, their rows scoped to the tables it can
#: read, write or list (compute_meta_row_scope) and their core columns served by
#: inheritance.expand_meta_grants: the tables naming no more than that, and what a SQL client's
#: catalog is made of. What it reads of meta beyond them is catalog_read's.
DEFAULT_TIER_META_TABLES: frozenset[str] = frozenset(
    {*META_ROW_SCOPED_VIEWS, *CATALOG_VIEW_NAMES, *NEUTRAL_VIEW_NAMES, LISTED_NAMES_VIEW}
)

# REQ-1593, REQ-2163: the ontology schema's definition views, each with the columns holding the
# domains (as |a|b|) of the terms its row is about. A row is read only when each of those terms is
# readable to the caller by the glossary's own rule (provisa.core.glossary.readable_term): a term
# with no domains, an enterprise ("*") term, or one sharing a domain with the caller.
GLOSSARY_SCOPED_VIEWS: dict[str, tuple[str, ...]] = {
    "_terms_ontology": ("term_domains",),
    "_term_refs_ontology": ("term_domains",),
    "_term_edges_ontology": ("from_term_domains", "to_term_domains"),
    "_domains_ontology": ("term_domains",),
    # A triple is read where both its terms are (an IRI or literal object names no term).
    "_axioms_ontology": ("subject_domains", "object_domains"),
    "_closure_ontology": ("subject_domains", "object_domains"),
}

# REQ-2163: registered tables SQL alone serves: no GraphQL field and no Cypher label (other
# languages reach them only where a language means to, as SPARQL lowers its RDFS/OWL/SKOS
# patterns to ontology._axioms). Each is (domain id, table name).
SQL_ONLY_TABLES: frozenset[tuple[str, str]] = frozenset(
    {(ONTOLOGY_DOMAIN_ID, "_axioms"), (ONTOLOGY_DOMAIN_ID, "_closure")}
)
# REQ-2190: what a SQL catalog reads of the model, which is no table of it to a person: SQL alone
# serves them, and neither the admin catalog nor anything built on it (the tables page, the ERD)
# lists them.
SQL_CATALOG_TABLES: frozenset[tuple[str, str]] = frozenset(
    (META_DOMAIN_ID, name) for name in (*CATALOG_VIEW_NAMES, *NEUTRAL_VIEW_NAMES)
)
SQL_ONLY_TABLES |= SQL_CATALOG_TABLES


def lists_table(table: dict) -> bool:
    """Whether every role of the org lists ``table``: it holds a listed column (REQ-2153), in a
    data domain."""
    if table.get("domain_id") in SYSTEM_DOMAINS or table.get("draft"):
        return False
    return any(c.get("scope") == SCOPE_LISTED for c in table.get("columns") or [])


def writes_table(role: dict, table: dict) -> bool:
    """Whether ``role`` holds a write grant on ``table``, in a domain it reaches."""
    if not reaches_domain(role["domain_access"], cast("str", table.get("domain_id"))):
        return False
    return any(g["role"] == role["id"] for g in table.get("write_grants") or [])


def compute_meta_row_scope(
    role: dict[str, object],
    tables: list[dict],
    relationships: list[dict] | None,
) -> set[int] | None:
    """REQ-1132: the set of DESCRIBED table ids whose meta rows a role may see, or ``None`` when
    NO row filter applies (all rows visible). The pgwire catalog and every catalog function read
    meta, so this is their scope too.

    ``None`` (unfiltered) is returned for the tier that sees the whole catalog: a role reaching
    every data domain and every system domain the model has a table of. A table of a system domain
    is described only to a role holding that domain, or served one of its columns.
    Holding catalog_read opens the domain and not its rows. An empty ``domain_access`` is no
    domains, so such a role sees no meta rows but the listed; the role itself is required -- a
    missing role is not a tier. Every other (DEFAULT-tier) role is confined to the tables it can
    read (:func:`serves_table`), write (:func:`writes_table`) or list (:func:`lists_table`), PLUS
    1-hop neighbours of the first two over user-defined/semantic relationships (the
    ``relationships`` registry holds only user relationships; auto-derived FK/catalog edges are
    never stored there, so they are excluded by construction). Discovery is bidirectional, EXCEPT a
    relationship flagged ``hide_target_meta`` suppresses the TARGET from discovery via that edge
    (the source stays discoverable from the target side). Computed (function-target) relationships
    have no concrete target table and contribute no neighbour.
    """
    accessible = cast("list[str]", role["domain_access"])

    def described(table: dict) -> bool:
        domain_id = table.get("domain_id")
        if domain_id not in SYSTEM_DOMAINS or domain_id in accessible:
            return True
        # Meta's default tier is read, and is none of the tables the role holds a grant to.
        return domain_id != META_DOMAIN_ID and serves_table(role, table)

    if reaches_all_domains(accessible):
        if all(described(t) for t in tables):
            return None  # the whole catalog
        return {t["id"] for t in tables if described(t)}
    # An EMPTY list falls through: no domain is directly reachable, so no meta row is either.

    # Its own tables: exactly those it is served a column of, by the one column rule -- in a domain
    # it reaches or published to it across domains (REQ-1959) -- or holds a write grant on. Reaching
    # a table's domain is not enough: a table every column of which is granted to other roles is not
    # described to this one.
    directly = {
        t["id"] for t in tables if described(t) and (serves_table(role, t) or writes_table(role, t))
    }
    visible = set(directly)
    system = {t["id"] for t in tables if not described(t)}
    for rel in relationships or []:
        sid = rel.get("source_table_id")
        tid = rel.get("target_table_id")
        if sid is None or tid is None or tid == "":
            continue  # computed/function relationship: no concrete target table
        if sid in directly and not rel.get("hide_target_meta"):
            visible.add(tid)
        if tid in directly:
            visible.add(sid)
    return (visible - system) | {t["id"] for t in tables if lists_table(t)}


def unlisted_columns(role: dict, tables: list[dict]) -> set[tuple[int, str]]:
    """The (table id, column) of each table ``role`` only lists whose column is not listed: the
    catalog names a listed table's listed columns to it and no others (REQ-2153)."""
    return {
        (t["id"], c["column_name"])
        for t in tables
        if lists_table(t) and not serves_table(role, t) and not writes_table(role, t)
        for c in t.get("columns") or []
        if c.get("scope") != SCOPE_LISTED
    }
