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

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import strawberry
    import strawberry.types

_ANONYMOUS = "anonymous"


def _identity_from_info(info: "strawberry.types.Info") -> object | None:
    request = info.context.get("request") if isinstance(info.context, dict) else getattr(info.context, "request", None)
    if request is None:
        return None
    return getattr(request.state, "identity", None)


def _resolved_capabilities(identity, state) -> set[str]:
    """Return the union of capabilities across all of the identity's role assignments."""
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return set()
    roles: dict[str, dict] = getattr(state, "roles", {})
    caps: set[str] = set()
    for assignment_claim in getattr(identity, "roles", []):
        claim = assignment_claim.strip()
        role_id = claim.split(":")[0] if ":" in claim else claim
        role = roles.get(role_id) or {}
        for c in (role.get("capabilities") or []):
            caps.add(c)
    return caps


def _domain_access(identity, state) -> set[str]:
    """Return the set of domain IDs accessible to this identity (empty = none)."""
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return set()
    domains: set[str] = set()
    for claim in getattr(identity, "roles", []):
        claim = claim.strip()
        domain = claim.split(":", 1)[1] if ":" in claim else "*"
        domains.add(domain)
    return domains


def require_capability(info: "strawberry.types.Info", capability: str, domain_id: str | None = None) -> None:
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
    if "superadmin" in caps or "admin" in caps:
        return  # admins bypass all capability checks

    if capability not in caps:
        raise PermissionError(f"Missing capability: {capability!r}")

    if domain_id is not None:
        from provisa.core import domain_policy

        if domain_policy.single_domain():
            return  # single-domain mode: domain is not a gate
        domains = _domain_access(identity, state)
        if "*" not in domains and domain_id not in domains:
            raise PermissionError(f"No access to domain {domain_id!r}")
