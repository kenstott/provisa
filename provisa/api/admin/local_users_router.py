# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CRUD endpoints for local_users (basic auth user management)."""

# Requirements: REQ-124, REQ-125, REQ-042

from __future__ import annotations

from typing import Any

import bcrypt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, func, insert, select, update

from provisa.core.database import Database
from provisa.core.schema_admin import local_users
from provisa.core.schema_org import user_role_assignments

router = APIRouter(prefix="/admin/users", tags=["admin"])


class CreateUserBody(BaseModel):
    username: str
    password: str
    email: str | None = None
    display_name: str | None = None
    roles: list[str] = []
    attributes: dict[str, Any] = {}


class AssignmentBody(BaseModel):
    role_id: str
    domain_id: str


class UpdateUserBody(BaseModel):
    email: str | None = None
    display_name: str | None = None
    roles: list[str] | None = None
    attributes: dict[str, Any] | None = None
    is_active: bool | None = None


class ChangePasswordBody(BaseModel):
    password: str


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _strip_hash(row) -> dict:
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    d.pop("password_hash", None)
    return d


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # user_role_assignments lives in the tenant control plane.
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


def _admin_pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # local_users lives in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


@router.post("/")
async def create_user(body: CreateUserBody, request: Request):  # REQ-124, REQ-125
    import uuid

    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        # id is generated app-side (portable) rather than via a PG-specific
        # server-side UUID default — the platform control plane may be
        # any SQLAlchemy backend.
        result = await conn.execute_core(
            insert(local_users)
            .values(
                id=str(uuid.uuid4()),
                username=body.username,
                password_hash=_hash(body.password),
                email=body.email,
                display_name=body.display_name,
                # JSON columns take Python objects directly.
                roles=body.roles,
                attributes=body.attributes,
            )
            .returning(local_users)
        )
        row = result.fetchone()
    return _strip_hash(row)


@router.get("/")
async def list_users(request: Request):
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(local_users).order_by(local_users.c.created_at))
        rows = result.fetchall()
    return [_strip_hash(r) for r in rows]


@router.get("/{user_id}")
async def get_user(user_id: str, request: Request):
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(local_users).where(local_users.c.id == user_id))
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return _strip_hash(row)


@router.put("/{user_id}")
async def update_user(user_id: str, body: UpdateUserBody, request: Request):
    pool = _admin_pool(request)
    values: dict[str, Any] = {}
    if body.email is not None:
        values["email"] = body.email
    if body.display_name is not None:
        values["display_name"] = body.display_name
    if body.roles is not None:
        # JSON columns take Python objects directly.
        values["roles"] = body.roles
    if body.attributes is not None:
        values["attributes"] = body.attributes
    if body.is_active is not None:
        values["is_active"] = body.is_active
    if not values:
        raise HTTPException(status_code=400, detail="No fields to update")
    values["updated_at"] = func.now()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(local_users)
            .where(local_users.c.id == user_id)
            .values(**values)
            .returning(local_users)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return _strip_hash(row)


@router.patch("/{user_id}/password")
async def change_password(user_id: str, body: ChangePasswordBody, request: Request):  # REQ-124
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(local_users)
            .where(local_users.c.id == user_id)
            .values(password_hash=_hash(body.password), updated_at=func.now())
            .returning(local_users.c.id)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": row[0]}


@router.delete("/{user_id}")
async def delete_user(user_id: str, request: Request):
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            _delete(local_users).where(local_users.c.id == user_id).returning(local_users.c.id)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"deleted": row[0]}


@router.get("/{user_id}/assignments")
async def list_assignments(user_id: str, request: Request):
    pool = _pool(request)
    t = user_role_assignments
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.id, t.c.role_id, t.c.domain_id, t.c.created_at)
            .where(t.c.user_id == user_id)
            .order_by(t.c.role_id, t.c.domain_id)
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/{user_id}/assignments")
async def add_assignment(user_id: str, body: AssignmentBody, request: Request):  # REQ-042
    pool = _pool(request)
    t = user_role_assignments
    async with pool.acquire() as conn:
        # Insert-if-absent (idempotent assignment), then read the row back.
        await conn.upsert(
            t,
            {"user_id": user_id, "role_id": body.role_id, "domain_id": body.domain_id},
            index_elements=["user_id", "role_id", "domain_id"],
            update_columns=[],
        )
        result = await conn.execute_core(
            select(t.c.id, t.c.role_id, t.c.domain_id).where(
                t.c.user_id == user_id,
                t.c.role_id == body.role_id,
                t.c.domain_id == body.domain_id,
            )
        )
        row = result.fetchone()
    return (
        dict(row._mapping)
        if row
        else {"user_id": user_id, "role_id": body.role_id, "domain_id": body.domain_id}
    )


@router.delete("/{user_id}/assignments/{assignment_id}")
async def remove_assignment(user_id: str, assignment_id: int, request: Request):
    pool = _pool(request)
    t = user_role_assignments
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            _delete(t).where(t.c.id == assignment_id, t.c.user_id == user_id).returning(t.c.id)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return {"deleted": row[0]}
