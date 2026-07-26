# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f0c72-9d84-4e13-8b26-71c4a90ef5d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Org resolution for the non-HTTP protocol entrypoints (REQ-1266).

The HTTP path resolves ``active_org_id`` in ``AuthMiddleware`` and binds it in the
``_OrgRoutingMiddleware``. The wire protocols (pgwire/bolt/flight/gRPC) authenticate a
principal but have no middleware chain, so they resolve the org here — once, at session
establishment — using the SAME membership rule as the HTTP middleware:

  - single-org (multitenancy off): every session binds the default org (``state.org_id``);
    the ContextVar is left unset and the AppState shims resolve the default runtime.
  - multitenant: the authenticated ``user_id`` is looked up in ``user_org_memberships``.
    A platform admin (``admin``/``superadmin``) or a client-supplied org they belong to is
    honored; a lone membership auto-selects; ambiguity or a non-member request RAISES — no
    silent default (a wrong default here is a cross-tenant data escape).

The resolved org id is stored on the protocol's session object and bound around each query
via ``current_org`` (``set_current_org``/``reset_current_org``).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from provisa.core.schema_admin import user_org_memberships


class OrgResolutionError(Exception):
    """Org could not be resolved for an authenticated principal — must fail the session."""


async def resolve_session_org(
    state: Any,
    *,
    user_id: str | None,
    is_platform_admin: bool = False,
    requested_org: str | None = None,
) -> str | None:
    """Resolve the org a protocol session should bind, or None to use the default runtime.

    Returns None for single-org deployments (caller leaves ``current_org`` unset → default
    runtime). Under multitenancy, returns the org id to bind; raises :class:`OrgResolutionError`
    when the principal is unresolvable to exactly one permitted org.
    """
    if not getattr(state, "multitenancy", False):
        return None

    member_org_ids: list[str] = []
    if user_id is not None and state.admin_db is not None:
        async with state.admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(user_org_memberships.c.org_id).where(
                    user_org_memberships.c.user_id == user_id
                )
            )
            member_org_ids = [dict(r._mapping)["org_id"] for r in result.fetchall()]

    if requested_org is not None:
        if is_platform_admin or requested_org in member_org_ids:
            return requested_org
        raise OrgResolutionError(f"principal not a member of org {requested_org!r}")
    if len(member_org_ids) == 1:
        return member_org_ids[0]
    if is_platform_admin:
        # A platform admin with no single membership and no explicit request acts on the
        # default org's data plane (org CRUD is a separate platform-plane concern).
        return None
    raise OrgResolutionError(
        "org selection required: authenticated principal belongs to "
        f"{len(member_org_ids)} orgs and none was requested"
    )
