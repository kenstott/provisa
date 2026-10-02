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
    from collections.abc import Callable

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
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return None
    return _resolved_capabilities(identity, state)


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
    """May this caller ask what a query compiles to AS ``role_id``?

    A role named in an admin request follows the rule for a role named anywhere else: it is one
    the caller holds, or the request is refused (:func:`provisa.api.acting_role.held_role`). The
    one exception is the holder of ``access_config`` — the right that administers row filters and
    visibility — who inspects what any role's query compiles to as part of that work.
    """
    from provisa.api.acting_role import held_role
    from provisa.api.app import state
    from provisa.security.rights import Capability

    identity = _identity_from_info(info)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    if Capability.ACCESS_CONFIG.value in _resolved_capabilities(identity, state):
        return
    # An identity was read off the request, so the request is there.
    request = info.context["request"] if isinstance(info.context, dict) else info.context.request
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
