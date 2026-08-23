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

# Requirements: REQ-042, REQ-060, REQ-434, REQ-1530, REQ-1531

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.security.rights import (
    capabilities_for_claims,
    domain_access_for_claims,
    has_platform_bypass,
)

if TYPE_CHECKING:
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
    if has_platform_bypass(caps):
        return  # the platform administrator bypasses all capability checks (REQ-1297)

    if capability not in caps:
        raise PermissionError(f"Missing capability: {capability!r}")

    if domain_id is not None:
        require_domain(info, domain_id)


def require_domain(info: "strawberry.types.Info", domain_id: str) -> None:  # REQ-1530, REQ-1531
    """The domain half of the gate ALONE: may this caller act on objects in ``domain_id``?

    Separate from :func:`require_capability` because some acts are permitted by more than one
    right — registering a view is allowed to a holder of ``create_view`` OR ``query_development`` —
    and the question "which domains may you touch" has one answer regardless of which of those
    rights carried the caller in. Both functions honour the same three exemptions: dev/no-auth,
    the platform administrator's bypass, and single-domain mode where a domain gates nothing.
    """
    from provisa.api.app import state
    from provisa.core import domain_policy

    identity = _identity_from_info(info)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    if has_platform_bypass(_resolved_capabilities(identity, state)):
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
    a router's caller can render; the dev/no-auth and platform-bypass exemptions are unchanged.
    """
    from provisa.api.app import state
    from provisa.api.errors import ApiError

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return
    caps = _resolved_capabilities(identity, state)
    if has_platform_bypass(caps):
        return
    if capability not in caps:
        raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")


def has_capability(info: "strawberry.types.Info", capability: str) -> bool:  # REQ-434
    """Non-raising capability check (REQ-434 gating).

    Returns True when the caller holds the capability — including dev/no-auth mode
    and admins (who bypass all checks). Used to decide whether a governed create
    proceeds or is queued as a creation request.
    """
    try:
        require_capability(info, capability)
        return True
    except PermissionError:
        return False
