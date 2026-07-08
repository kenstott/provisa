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

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, select, update

from provisa.core.database import Database
from provisa.core.schema_admin import orgs, user_org_memberships, user_profiles

if TYPE_CHECKING:
    pass

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


def _pool() -> Database:
    # Tenant control plane — used for org schema (de)provisioning.
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


def _admin_pool() -> Database:
    # Platform control plane — orgs registry and org membership.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


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
    pool = _admin_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.name, orgs.c.created_by, orgs.c.created_at).order_by(orgs.c.id)
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/")
async def create_org(body: CreateOrgBody, request: Request):  # REQ-042, REQ-059, REQ-701
    _require_superadmin(request)
    import os
    from pathlib import Path

    from provisa.core.org_provisioning import provision_org

    # orgs registry -> platform control plane; org schema -> tenant control plane.
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            orgs.insert()
            .values(id=body.id, name=body.name)
            .returning(orgs.c.id, orgs.c.name, orgs.c.created_by, orgs.c.created_at)
        )
        row = result.fetchone()

    schema_sql_path = Path(__file__).parent.parent.parent / "core" / "schema.sql"
    schema_sql = schema_sql_path.read_text() if schema_sql_path.exists() else ""
    redis_url = os.environ.get("REDIS_URL")
    redis_password = os.environ.get("PROVISA_REDIS_ORG_PASSWORD")
    await provision_org(
        _pool(),
        schema_sql,
        org_id=body.id,
        redis_url=redis_url,
        redis_password=redis_password,
    )
    return dict(row._mapping)


@router.put("/{org_id}")
async def rename_org(org_id: str, body: RenameOrgBody, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(name=body.name)
            .returning(orgs.c.id, orgs.c.name, orgs.c.created_by, orgs.c.created_at)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Org not found")
    return dict(row._mapping)


@router.delete("/{org_id}")
async def delete_org(org_id: str, request: Request):  # REQ-042, REQ-059, REQ-701
    _require_superadmin(request)
    import os

    from provisa.core.org_provisioning import deprovision_org

    if org_id == "root":
        raise HTTPException(status_code=400, detail="Cannot delete the root org")
    # orgs registry -> platform; org schema teardown -> tenant.
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(_delete(orgs).where(orgs.c.id == org_id))
    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail="Org not found")
    redis_url = os.environ.get("REDIS_URL")
    await deprovision_org(_pool(), org_id, redis_url=redis_url)
    return {"deleted": org_id}


@router.get("/{org_id}/members")
async def list_members(org_id: str, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                user_org_memberships.c.user_id,
                user_profiles.c.email,
                user_profiles.c.display_name,
                user_profiles.c.provider,
                user_org_memberships.c.created_at,
            )
            .select_from(
                user_org_memberships.outerjoin(
                    user_profiles,
                    user_profiles.c.user_id == user_org_memberships.c.user_id,
                )
            )
            .where(user_org_memberships.c.org_id == org_id)
            .order_by(user_org_memberships.c.user_id)
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/{org_id}/members")
async def add_member(org_id: str, body: AddMemberBody, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        exists_result = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
        if exists_result.fetchone() is None:
            raise HTTPException(status_code=404, detail="Org not found")
        await conn.upsert(
            user_org_memberships,
            {"user_id": body.user_id, "org_id": org_id},
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )
    return {"user_id": body.user_id, "org_id": org_id}


@router.delete("/{org_id}/members/{user_id}")
async def remove_member(org_id: str, user_id: str, request: Request):  # REQ-042, REQ-059
    _require_superadmin(request)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            _delete(user_org_memberships).where(
                user_org_memberships.c.user_id == user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail="Membership not found")
    return {"deleted": {"user_id": user_id, "org_id": org_id}}
