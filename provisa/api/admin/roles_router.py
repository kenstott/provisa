# Copyright (c) 2026 Kenneth Stott
# Canary: 3609341a-3f5c-4918-8172-e920f27bdeb7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Org-scoped role CRUD endpoints."""

# Requirements: REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531, REQ-1539

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, insert, or_, select, update

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.core.schema_org import roles

if TYPE_CHECKING:
    from provisa.core.database import Database

router = APIRouter(prefix="/admin/roles", tags=["admin"])

#: The two roles no environment may redefine (REQ-1539).
#:
#: Every other seeded role -- ``analyst``, ``developer``, ``modeler`` -- IS editable, because the
#: ``roles`` table lives in the environment's own schema: each environment holds its own copy of
#: every row, seeded once at creation and never written by a merge, a load or a checkout. Editing
#: ``developer`` in dev therefore changes dev and nothing else, which is the whole mechanism by
#: which a lower lane holds different rights from prod.
#:
#: ``org_admin`` and ``platform_admin`` are held out because they are the roles that carry
#: ``user_management`` itself. An org_admin who narrowed their own role would lock the environment
#: out of being administered at all, with no second administrator to undo it -- so the lock-out is
#: prevented by construction rather than by a check that counts who is left.
_UNEDITABLE: frozenset[str] = frozenset({"org_admin", "platform_admin"})


def _active_org(request: Request) -> str:
    """REQ-1276/REQ-1317: the org is whatever ``AuthMiddleware`` resolved for this request — Host
    subdomain, or the ``x-org-provisa`` header on the control-plane host. Never a client-supplied
    ``x-org-id``, and never a default: the middleware sets ``active_org_id`` on every request that
    reaches a router, so a missing value is a wiring bug, not a case to paper over.
    """
    org_id = getattr(request.state, "active_org_id", None)
    if org_id is None:
        raise ApiError(401, "roles.org_selection_required", "Org selection required")
    return org_id


def _pool(_request: Request) -> "Database":  # pyright: ignore[reportUnusedParameter]
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


class CreateRoleBody(BaseModel):
    id: str
    capabilities: list[str]
    domain_access: list[str]


class UpdateRoleBody(BaseModel):
    capabilities: list[str] | None = None
    domain_access: list[str] | None = None


@router.get("/")
async def list_roles(request: Request):  # REQ-042, REQ-059, REQ-060
    org_id = _active_org(request)
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(roles.c.id, roles.c.capabilities, roles.c.domain_access, roles.c.org_id)
            .where(or_(roles.c.org_id.is_(None), roles.c.org_id == org_id))
            .order_by(roles.c.id)
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/")
async def create_role(body: CreateRoleBody, request: Request):  # REQ-042, REQ-059, REQ-060, REQ-215
    # REQ-1531: a role carries capabilities AND domain_access, so minting one widens scope.
    require_capability_request(request, "user_management")
    org_id = _active_org(request)
    pool = _pool(request)
    async with pool.acquire() as conn:
        await conn.execute_core(
            insert(roles).values(
                id=body.id,
                capabilities=body.capabilities,
                domain_access=body.domain_access,
                org_id=org_id,
            )
        )
    return {
        "id": body.id,
        "capabilities": body.capabilities,
        "domain_access": body.domain_access,
        "org_id": org_id,
    }


@router.put("/{role_id}")
async def update_role(
    role_id: str, body: UpdateRoleBody, request: Request
):  # REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531, REQ-1539
    require_capability_request(request, "user_management")  # REQ-1531: see create_role
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(roles.c.id, roles.c.capabilities, roles.c.domain_access, roles.c.org_id).where(
                roles.c.id == role_id
            )
        )
        existing = result.fetchone()
        if existing is None:
            raise ApiError(404, "roles.not_found", "Role not found")
        existing = dict(existing._mapping)
        if role_id in _UNEDITABLE:
            raise ApiError(
                400, "roles.cannot_modify_administrative", "Cannot modify administrative roles"
            )

        new_caps = body.capabilities if body.capabilities is not None else existing["capabilities"]
        new_domains = (
            body.domain_access if body.domain_access is not None else existing["domain_access"]
        )

        await conn.execute_core(
            update(roles)
            .where(roles.c.id == role_id)
            .values(capabilities=new_caps, domain_access=new_domains)
        )
    return {
        "id": role_id,
        "capabilities": new_caps,
        "domain_access": new_domains,
        "org_id": existing["org_id"],
    }


@router.delete("/{role_id}")
async def delete_role(role_id: str, request: Request):  # REQ-042, REQ-059, REQ-060, REQ-1531
    require_capability_request(request, "user_management")  # REQ-1531: see create_role
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(roles.c.org_id).where(roles.c.id == role_id))
        existing = result.fetchone()
        if existing is None:
            raise ApiError(404, "roles.not_found", "Role not found")
        if existing._mapping["org_id"] is None:
            raise ApiError(400, "roles.cannot_delete_system", "Cannot delete system roles")

        await conn.execute_core(_delete(roles).where(roles.c.id == role_id))
    return {"deleted": role_id}
