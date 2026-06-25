# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Superadmin CRUD endpoints for orgs and org membership."""

# Requirements: REQ-042, REQ-059, REQ-060, REQ-125

from __future__ import annotations

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/admin/orgs", tags=["admin"])


def _require_superadmin(request: Request) -> None:  # REQ-042, REQ-125
    """Raise 403 if the caller is not an admin/superadmin. Dev mode (anonymous) is allowed."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, _app_state)
    if "superadmin" not in caps and "admin" not in caps:
        raise HTTPException(status_code=403, detail="Superadmin required")


def _pool() -> asyncpg.Pool:
    from provisa.api.app import state

    assert state.pg_pool is not None
    return state.pg_pool


class CreateOrgBody(BaseModel):
    id: str
    name: str


class RenameOrgBody(BaseModel):
    name: str


class AddMemberBody(BaseModel):
    user_id: str


@router.get("/")
async def list_orgs(request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, created_by, created_at FROM orgs ORDER BY id")
    return [dict(r) for r in rows]


@router.post("/")
async def create_org(body: CreateOrgBody, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO orgs (id, name) VALUES ($1, $2) RETURNING id, name, created_by, created_at",
            body.id,
            body.name,
        )
    return dict(row)


@router.put("/{org_id}")
async def rename_org(org_id: str, body: RenameOrgBody, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orgs SET name = $1 WHERE id = $2 RETURNING id, name, created_by, created_at",
            body.name,
            org_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Org not found")
    return dict(row)


@router.delete("/{org_id}")
async def delete_org(org_id: str, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    if org_id == "root":
        raise HTTPException(status_code=400, detail="Cannot delete the root org")
    pool = _pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM orgs WHERE id = $1", org_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Org not found")
    return {"deleted": org_id}


@router.get("/{org_id}/members")
async def list_members(org_id: str, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT m.user_id, p.email, p.display_name, p.provider, m.created_at "
            "FROM user_org_memberships m "
            "LEFT JOIN user_profiles p ON p.user_id = m.user_id "
            "WHERE m.org_id = $1 ORDER BY m.user_id",
            org_id,
        )
    return [dict(r) for r in rows]


@router.post("/{org_id}/members")
async def add_member(org_id: str, body: AddMemberBody, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        org_exists = await conn.fetchval("SELECT 1 FROM orgs WHERE id = $1", org_id)
        if not org_exists:
            raise HTTPException(status_code=404, detail="Org not found")
        await conn.execute(
            "INSERT INTO user_org_memberships (user_id, org_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            body.user_id,
            org_id,
        )
    return {"user_id": body.user_id, "org_id": org_id}


@router.delete("/{org_id}/members/{user_id}")
async def remove_member(org_id: str, user_id: str, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM user_org_memberships WHERE user_id = $1 AND org_id = $2",
            user_id,
            org_id,
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Membership not found")
    return {"deleted": {"user_id": user_id, "org_id": org_id}}
