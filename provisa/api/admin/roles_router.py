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

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/admin/roles", tags=["admin"])


def _pool(_request: Request) -> asyncpg.Pool:  # pyright: ignore[reportUnusedParameter]
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
        rows = await conn.fetch(
            "SELECT id, capabilities, domain_access, org_id FROM roles "
            "WHERE org_id IS NULL OR org_id = $1 ORDER BY id",
            org_id,
        )
    return [dict(r) for r in rows]


@router.post("/")
async def create_role(body: CreateRoleBody, request: Request):  # REQ-042, REQ-059, REQ-060, REQ-215
    org_id = request.headers.get("x-org-id", "root")
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO roles (id, capabilities, domain_access, org_id) "
            "VALUES ($1, $2, $3, $4) RETURNING id, capabilities, domain_access, org_id",
            body.id,
            body.capabilities,
            body.domain_access,
            org_id,
        )
    return dict(row)


@router.put("/{role_id}")
async def update_role(
    role_id: str, body: UpdateRoleBody, request: Request
):  # REQ-042, REQ-059, REQ-060, REQ-215
    pool = _pool(request)
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id, capabilities, domain_access, org_id FROM roles WHERE id = $1",
            role_id,
        )
        if existing is None:
            raise HTTPException(status_code=404, detail="Role not found")
        if existing["org_id"] is None:
            raise HTTPException(status_code=400, detail="Cannot modify system roles")

        new_caps = body.capabilities if body.capabilities is not None else existing["capabilities"]
        new_domains = (
            body.domain_access if body.domain_access is not None else existing["domain_access"]
        )

        row = await conn.fetchrow(
            "UPDATE roles SET capabilities = $1, domain_access = $2 WHERE id = $3 "
            "RETURNING id, capabilities, domain_access, org_id",
            new_caps,
            new_domains,
            role_id,
        )
    return dict(row)


@router.delete("/{role_id}")
async def delete_role(role_id: str, request: Request):  # REQ-042, REQ-059, REQ-060
    pool = _pool(request)
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT org_id FROM roles WHERE id = $1",
            role_id,
        )
        if existing is None:
            raise HTTPException(status_code=404, detail="Role not found")
        if existing["org_id"] is None:
            raise HTTPException(status_code=400, detail="Cannot delete system roles")

        await conn.execute("DELETE FROM roles WHERE id = $1", role_id)
    return {"deleted": role_id}
