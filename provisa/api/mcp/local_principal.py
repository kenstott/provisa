# Copyright (c) 2026 Kenneth Stott
# Canary: c931233f-91c0-4484-b1ec-b02bcdd30a39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The principal a local MCP server acts for (REQ-1105, A LOCAL MCP SERVER ACTS FOR A PRINCIPAL
ITS OPERATOR NAMES).

Over stdio (and a loopback HTTP call that carries no bearer) no token arrives, so the operator's
local MCP configuration names the principal, ``mcp.principal`` (``PROVISA_MCP_PRINCIPAL``),
beside the role it pins (``mcp.role``). The principal is the user id a sign-in token carries.
Every call is refused while the setting names no one, and while it names someone who is not in
the org the call is served in -- the membership rule the wire protocols bind an org by.

The identity built here is bound by the one binding every transport uses
(``auth.derived_roles.principal_scope``): its attributes are what the identity provider supplied
at the principal's last sign-in, under the org's own record, and its entitlement lists (REQ-2219)
are the identity provider's as the store mirrors them, with what Provisa holds for it -- what the
same principal's queries are narrowed by on every other client.
"""

# Requirements: REQ-1105, REQ-2219

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import select

from provisa.auth.models import AuthIdentity
from provisa.core.org_membership import bindable_memberships

SETTING = "mcp.principal"
ENV = "PROVISA_MCP_PRINCIPAL"

#: The claim the mirrored entitlement lists ride in on the identity built here.
_LISTS_CLAIM = "provisa_mcp_entitlements"


class LocalPrincipalRefused(PermissionError):
    """A local MCP call with no principal it may act for: named in the message."""


def configured_principal() -> str:
    """The principal the operator named, or :class:`LocalPrincipalRefused` naming the setting."""
    from provisa.core import settings_registry

    value = settings_registry.value(SETTING)
    if value is None or not str(value).strip():
        raise LocalPrincipalRefused(
            f"{SETTING} ({ENV}) is not set: a local MCP server acts for the principal its "
            "operator names, beside the role it pins in mcp.role (PROVISA_MCP_ROLE)"
        )
    return str(value).strip()


async def _in_org(state: Any, principal: str, org_id: str) -> bool:
    """Whether ``principal`` is in ``org_id``. Under multitenancy: a member it can work in, as
    every session-binding path reads membership. One org: an active local account, a member, or
    someone who has signed in to it."""
    from provisa.core.schema_admin import local_users
    from provisa.core.schema_org import user_directory

    if state.admin_db is None:
        raise RuntimeError("no control plane to find the local MCP principal in")
    async with state.admin_db.acquire() as conn:
        rows = await conn.execute_core(bindable_memberships(principal))
        if any(r._mapping["org_id"] == org_id for r in rows.fetchall()):
            return True
        if getattr(state, "multitenancy", False):
            return False
        local = await conn.execute_core(
            select(local_users.c.id).where(
                local_users.c.id == principal, local_users.c.is_active.is_(True)
            )
        )
        if local.first() is not None:
            return True
    async with state.model_db.acquire() as conn:
        signed_in = await conn.execute_core(
            select(user_directory.c.user_id).where(user_directory.c.user_id == principal)
        )
        return signed_in.first() is not None


async def _provider_attributes(state: Any, principal: str) -> dict[str, str]:
    """What ``principal``'s identity provider supplied at its last sign-in (REQ-1383)."""
    from provisa.core.schema_org import user_idp_attributes

    async with state.model_db.acquire() as conn:
        rows = await conn.execute_core(
            select(user_idp_attributes.c.name, user_idp_attributes.c.value).where(
                user_idp_attributes.c.user_id == principal
            )
        )
        return {r.name: r.value for r in rows.fetchall()}


async def resolve(state: Any, org_id: str, pinned_roles: Iterable[str]) -> AuthIdentity:
    """The identity a local call acts for in ``org_id``, holding the roles the operator pinned.
    Refused (:class:`LocalPrincipalRefused`) when none is named or the one named is not in the
    org."""
    principal = configured_principal()
    if not await _in_org(state, principal, org_id):
        raise LocalPrincipalRefused(
            f"{SETTING} ({ENV}) names {principal!r}, who is not in org {org_id!r}"
        )
    lists = state.idp_entitlements.get(principal, {})
    claims: dict[str, Any] = dict(await _provider_attributes(state, principal))
    claims[_LISTS_CLAIM] = {
        role: {kind: list(ids) for kind, ids in kinds.items()} for role, kinds in lists.items()
    }
    return AuthIdentity(
        user_id=principal,
        email=None,
        display_name=None,
        roles=list(pinned_roles),
        raw_claims=claims,
        entitlements_claim=_LISTS_CLAIM,
    )
