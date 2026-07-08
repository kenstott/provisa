# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Org-scoped role CRUD endpoints."""

# Requirements: REQ-042, REQ-059, REQ-060, REQ-215

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, insert, or_, select, update

from provisa.core.schema_org import roles

if TYPE_CHECKING:
    from provisa.core.database import Database

router = APIRouter(prefix="/admin/roles", tags=["admin"])


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
    org_id = request.headers.get("x-org-id", "root")
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
    org_id = request.headers.get("x-org-id", "root")
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
):  # REQ-042, REQ-059, REQ-060, REQ-215
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(roles.c.id, roles.c.capabilities, roles.c.domain_access, roles.c.org_id).where(
                roles.c.id == role_id
            )
        )
        existing = result.fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Role not found")
        existing = dict(existing._mapping)
        if existing["org_id"] is None:
            raise HTTPException(status_code=400, detail="Cannot modify system roles")

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
async def delete_role(role_id: str, request: Request):  # REQ-042, REQ-059, REQ-060
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(roles.c.org_id).where(roles.c.id == role_id))
        existing = result.fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Role not found")
        if existing._mapping["org_id"] is None:
            raise HTTPException(status_code=400, detail="Cannot delete system roles")

        await conn.execute_core(_delete(roles).where(roles.c.id == role_id))
    return {"deleted": role_id}
