# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9f1a-2c4d-5e6f-7a8b-9c0d1e2f3a4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Server-side capability enforcement for admin GraphQL mutations."""

# Requirements: REQ-042, REQ-060, REQ-434, REQ-1530, REQ-1531, REQ-1591

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.security.rights import (
    capabilities_for_claims,
    domain_access_for_capability,
    domain_access_for_claims,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    import strawberry
    import strawberry.types

_ANONYMOUS = "anonymous"


def _identity_from_info(info: "strawberry.types.Info") -> object | None:
    request = (
        info.context.get("request")
        if isinstance(info.context, dict)
        else getattr(info.context, "request", None)
    )
    if request is None:
        return None
    return getattr(request.state, "identity", None)


def _resolved_capabilities(identity, state) -> set[str]:
    """Return the union of capabilities across all of the identity's role assignments."""
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return set()
    # REQ-1337: RIGHTS ONLY. The role id is never folded in as a pseudo-capability — a gate reads
    # the rights a role carries, and the seed (schema.sql + apply_tenancy_role_grants) is the single
    # place that decides which role carries which right.
    return capabilities_for_claims(getattr(identity, "roles", []), getattr(state, "roles", {}))


def env_gate_capabilities(identity, state) -> set[str] | None:
    """The capability set an ENVIRONMENT gate should read, or ``None`` for no gate (REQ-1573).

    ``None`` and ``set()`` are different answers. An unsecured deployment resolves the anonymous dev
    principal for every request — the documented enforcement skip every other capability gate makes
    (``require_capability``) — and returning an empty set there would refuse a branch to the only
    principal a demo install has. A real user with no rights returns an empty set and is refused.
    """
    return env_gate_capabilities_for(identity, getattr(state, "roles", {}))


def env_gate_capabilities_for(identity, roles: dict) -> set[str] | None:  # REQ-1573
    """``env_gate_capabilities`` judged by the role definitions ``roles`` -- those of the org whose
    environment is being selected (REQ-1266)."""
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    return capabilities_for_claims(getattr(identity, "roles", []), roles)


def _domain_access(identity, state) -> set[str]:
    """The domain IDs this identity may act in (REQ-1530).

    Read off the ROLES the identity holds — ``roles.domain_access`` — and not off the ``:domain``
    suffix a claim may carry. The suffix records which grant was made; the scope is the role's, so
    that an org_admin narrows a developer by giving them a role whose domain_access names their
    domains rather than by granting a role on a domain.
    """
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return set()
    return domain_access_for_claims(getattr(identity, "roles", []), getattr(state, "roles", {}))


def require_capability(  # REQ-042, REQ-060
    info: "strawberry.types.Info", capability: str, domain_id: str | None = None
) -> None:
    """Raise PermissionError if the caller lacks the required capability.

    In dev mode (identity is None or anonymous) enforcement is skipped so
    the admin UI works without auth configured.

    Args:
        info: Strawberry resolver info carrying the request context.
        capability: capability string, e.g. 'table_registration'.
        domain_id: if provided, also verify the caller has access to this domain.
    """
    from provisa.api.app import state

    identity = _identity_from_info(info)

    # Dev / no-auth mode — skip enforcement
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return

    caps = _resolved_capabilities(identity, state)
    if capability not in caps:
        raise PermissionError(f"Missing capability: {capability!r}")

    if domain_id is not None:
        require_domain(info, domain_id)


def require_inspectable_role(info: "strawberry.types.Info", role_id: str) -> None:  # REQ-273
    """May this caller ask what a query compiles to AS ``role_id``? See
    :func:`require_inspectable_role_request`."""
    identity = _identity_from_info(info)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    # An identity was read off the request, so the request is there.
    request = info.context["request"] if isinstance(info.context, dict) else info.context.request
    require_inspectable_role_request(request, role_id)


def require_inspectable_role_request(request, role_id: str) -> None:  # REQ-273, REQ-004
    """May this caller inspect, or test-run, what ``role_id`` is served?

    A role named in an admin request follows the rule for a role named anywhere else: it is one
    the caller holds, or the request is refused (:func:`provisa.api.acting_role.held_role`). The
    one exception is the holder of ``access_config`` — the right that administers row filters and
    visibility — who inspects what any role is served as part of that work.
    """
    from provisa.api.acting_role import held_role
    from provisa.api.app import state
    from provisa.security.rights import Capability

    identity = getattr(request.state, "identity", None)
    if identity is not None and Capability.ACCESS_CONFIG.value in _resolved_capabilities(
        identity, state
    ):
        return
    held_role(request, role_id)


def require_trigger_role(request, role_id: str) -> None:  # REQ-1003
    """May this caller save a scheduled SQL trigger that runs AS ``role_id``?

    A trigger's statement runs as its role on every firing, so saving one is acting as that role:
    the role is one the caller holds (:func:`provisa.api.acting_role.held_role`, whose refusal
    names the role), or the caller holds ``cross_org``. Without this, the right that schedules a
    trigger would be a way to run statements as any role of the org, ``org_admin`` included.
    """
    from provisa.api.acting_role import held_role
    from provisa.api.app import state
    from provisa.security.rights import can_act_cross_org

    identity = getattr(request.state, "identity", None)
    if identity is not None and can_act_cross_org(_resolved_capabilities(identity, state)):
        return
    held_role(request, role_id)


def require_domain(info: "strawberry.types.Info", domain_id: str) -> None:  # REQ-1530, REQ-1531
    """The domain half of the gate ALONE: may this caller act on objects in ``domain_id``?

    Separate from :func:`require_capability` because some acts are permitted by more than one
    right — registering a view is allowed to a holder of ``create_view`` OR ``query_development`` —
    and the question "which domains may you touch" has one answer regardless of which of those
    rights carried the caller in. Both functions honour the same two exemptions: dev/no-auth, and
    single-domain mode where a domain gates nothing.
    """
    from provisa.api.app import state
    from provisa.core import domain_policy

    identity = _identity_from_info(info)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    if domain_policy.single_domain():
        return  # single-domain mode: domain is not a gate
    from provisa.core.env_authority import may_change_domain

    if not may_change_domain(sorted(_domain_access(identity, state)), domain_id):
        raise PermissionError(f"No access to domain {domain_id!r}")


def require_reach_of_added_domains(  # REQ-1531
    info: "strawberry.types.Info", before, after, *, empty_is_all: bool = False
) -> None:
    """Gate a change that ADDS domains to a role's ``domain_access`` or a source's
    ``allowed_domains``: the caller must reach each domain the change adds (all of them, when it
    adds ``*``). Adding a domain to a role is how reach is handed out and adding one to a source
    is how a source is opened to a domain, so neither may hand out more than the caller holds.
    Closing a source to a domain needs no reach. A role is different (REQ-1531, amended
    2026-10-07): redefining one, removing a domain from it or deleting it is an act in every
    domain it reaches, which its update and delete paths check before this one. ``before`` is
    None for an object being created."""
    from provisa.security.rights import domains_added

    for domain_id in sorted(domains_added(before, after, empty_is_all=empty_is_all)):
        require_domain(info, domain_id)


def require_reach_of_added_domains_request(  # REQ-1531
    request, before, after, *, empty_is_all: bool = False
) -> None:
    """The Request twin of :func:`require_reach_of_added_domains`, for a REST router."""
    from provisa.security.rights import domains_added

    for domain_id in sorted(domains_added(before, after, empty_is_all=empty_is_all)):
        require_domain_request(request, domain_id)


def require_capability_request(request, capability: str) -> None:  # REQ-1531
    """The same gate for a REST admin router, which has a Request rather than a resolver Info.

    REQ-1531: the capability check grew up in the GraphQL resolver layer, so an admin REST router
    reaching the same table enforced nothing. A role carries both capabilities and domain_access
    (REQ-1530), which makes minting one the way a member would widen their own scope — so the REST
    path must ask the same question the mutation asks. Raises ``ApiError(403)`` because that is what
    a router's caller can render; the dev/no-auth exemption is unchanged.
    """
    from provisa.api.app import state
    from provisa.api.errors import ApiError

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    caps = _resolved_capabilities(identity, state)
    if capability not in caps:
        raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")


def allowed_domains_request(request) -> frozenset[str] | None:  # REQ-1591
    """The domains a REST caller may act in, or ``None`` when domains gate nothing for it.

    ``None`` is an answer, not a missing value: it is returned for the same two exemptions the
    GraphQL gate honours — dev/no-auth and single-domain mode — and for a
    role whose ``domain_access`` is ``["*"]``. Callers narrow a query with the frozenset and skip
    narrowing entirely on ``None``, which keeps "unlimited" distinct from "limited to nothing".
    """
    from provisa.api.app import state
    from provisa.core import domain_policy
    from provisa.core.env_authority import domains_within

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    if domain_policy.single_domain():
        return None
    allowed = domains_within(sorted(_domain_access(identity, state)))
    return None if allowed is None else frozenset(allowed)


def role_definitions_visible(request, role_id_claims) -> "Callable[[str], bool]":
    """Which role definitions the caller may read in full: every one for a ``user_management``
    holder, otherwise only the roles the caller itself holds (the client builds its own rights from
    them). Any other role is visible by id alone."""
    from provisa.security.rights import role_ids_from_claims

    if has_capability_request(request, "user_management"):
        return lambda _role_id: True
    own = role_ids_from_claims(role_id_claims)
    return lambda role_id: role_id in own


def has_capability_request(request, capability: str) -> bool:  # REQ-1592
    """Non-raising capability check for a REST admin router — the Request twin of
    :func:`has_capability`.

    For a right that WIDENS what a caller may do rather than admitting them to a surface:
    ``org_glossary_rw`` overrides the glossary's domain and stewardship rules, so the router asks
    whether the caller holds it and takes a different path, instead of refusing. Honours the same
    dev/no-auth exemption as :func:`require_capability_request`.
    """
    from provisa.api.errors import ApiError

    try:
        require_capability_request(request, capability)
        return True
    except ApiError:
        return False


def allowed_domains_for_capability_request(  # REQ-1592
    request, capability: str
) -> frozenset[str] | None:
    """The domains a REST caller may exercise ``capability`` in — :func:`allowed_domains_request`
    narrowed to the roles that actually carry the right.

    Same two exemptions and the same ``None``-means-unlimited contract; the difference is which
    roles contribute scope. Use this wherever the act being authorized is the named right itself,
    so that holding a right in one domain and a different right in another cannot compose into the
    first right in the second domain.
    """
    from provisa.api.app import state
    from provisa.core import domain_policy
    from provisa.core.env_authority import domains_within

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    if domain_policy.single_domain():
        return None
    scoped = domain_access_for_capability(
        getattr(identity, "roles", []), getattr(state, "roles", {}), capability
    )
    allowed = domains_within(sorted(scoped))
    return None if allowed is None else frozenset(allowed)


def require_domain_request(request, domain_id: str) -> None:  # REQ-1591
    """The domain half of the gate for a REST router — the Request twin of :func:`require_domain`."""
    from provisa.api.errors import ApiError

    allowed = allowed_domains_request(request)
    if allowed is not None and domain_id not in allowed:
        raise ApiError(403, "auth.domain_denied", f"No access to domain {domain_id!r}")


def has_capability(info: "strawberry.types.Info", capability: str) -> bool:  # REQ-434
    """Non-raising capability check (REQ-434 gating).

    Returns True when the caller holds the capability — including dev/no-auth mode. Used to decide whether a governed create
    proceeds or is queued as a creation request.
    """
    try:
        require_capability(info, capability)
        return True
    except PermissionError:
        return False


# --- REQ-1944: a governance right is exercised only in the domains of the roles that carry it ---


def _rights_scope(identity, state, rights: "tuple[str, ...]") -> frozenset[str] | None:
    """The domains any of ``rights`` reaches for ``identity``, ``None`` for every domain.

    Paired per role (``domain_access_for_capability``), never the union over every role held:
    a role carrying ``masking_config`` in sales plus a read-only role in finance must not resolve
    to masking in finance (REQ-1592's composition gap, REQ-1944).
    """
    from provisa.core.env_authority import domains_within

    claims = getattr(identity, "roles", [])
    roles = getattr(state, "roles", {})
    scoped: set[str] = set()
    for right in rights:
        scoped |= domain_access_for_capability(claims, roles, right)
    allowed = domains_within(sorted(scoped))
    return None if allowed is None else frozenset(allowed)


def right_reach(identity, state, right: str) -> frozenset[str] | None:  # REQ-1944, REQ-1948
    """The domains ``identity`` may exercise ``right`` in: ``None`` for every domain, the empty
    set when no role it holds carries the right.

    The same reading :func:`right_domain_refusal` makes, handed back as a scope for a caller that
    asks whether the right reaches ANY of several domains rather than all of them. The dev/no-auth
    principal is exempt, as at every capability gate; single-domain mode keeps the right check
    and drops the domain check.
    """
    from provisa.core import domain_policy

    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    if right not in _resolved_capabilities(identity, state):
        return frozenset()
    if domain_policy.single_domain():
        return None
    return _rights_scope(identity, state, (right,))


def right_domain_refusal(  # REQ-1944
    identity, state, rights: "str | tuple[str, ...]", domains: "Iterable[str]"
) -> str | None:
    """Why ``identity`` may not exercise ``rights`` (any one of them) on objects in ``domains``,
    or None when it may.

    ``domains`` are the domains of what the edit CHANGES. ``"*"`` among them names an object of
    the whole org (a tag definition): only a right reaching every domain may change it. The
    refusal names the domain. The dev/no-auth principal is exempt, as at every capability gate;
    single-domain mode keeps the right check and drops the domain check.
    """
    from provisa.core import domain_policy
    from provisa.security.rights import ALL_DOMAINS

    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    wanted = (rights,) if isinstance(rights, str) else tuple(rights)
    held = _resolved_capabilities(identity, state)
    if not set(wanted) & held:
        return "Missing capability: " + " or ".join(repr(r) for r in wanted)
    if domain_policy.single_domain():
        return None
    allowed = _rights_scope(identity, state, wanted)
    if allowed is None:
        return None
    label = " or ".join(repr(r) for r in wanted)
    for domain_id in sorted(set(domains)):
        if domain_id == ALL_DOMAINS:
            return f"{label} must reach every domain to change an object of the whole org"
        if domain_id not in allowed:
            return f"No access to domain {domain_id!r} for {label}"
    return None


def require_right_in_domains(  # REQ-1944
    info: "strawberry.types.Info", rights: "str | tuple[str, ...]", domains: "Iterable[str]"
) -> None:
    """GraphQL gate: :func:`right_domain_refusal` raised as ``PermissionError``."""
    from provisa.api.app import state

    refusal = right_domain_refusal(_identity_from_info(info), state, rights, domains)
    if refusal is not None:
        raise PermissionError(refusal)


def holds_right_in_domains(  # REQ-1944
    info: "strawberry.types.Info", rights: "str | tuple[str, ...]", domains: "Iterable[str]"
) -> bool:
    """Non-raising :func:`require_right_in_domains`."""
    from provisa.api.app import state

    return right_domain_refusal(_identity_from_info(info), state, rights, domains) is None


def require_right_in_domains_request(  # REQ-1944
    request, rights: "str | tuple[str, ...]", domains: "Iterable[str]"
) -> None:
    """REST twin of :func:`require_right_in_domains`, raising ``ApiError(403)``."""
    from provisa.api.app import state
    from provisa.api.errors import ApiError

    refusal = right_domain_refusal(getattr(request.state, "identity", None), state, rights, domains)
    if refusal is None:
        return
    code = "auth.missing_capability" if refusal.startswith("Missing") else "auth.domain_denied"
    raise ApiError(403, code, refusal)
