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

from __future__ import annotations

from typing import Any

import bcrypt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

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
    d = dict(row)
    d.pop("password_hash", None)
    return d


def _pool(request: Request):
    from provisa.api.app import state
    return state.pg_pool


@router.post("/")
async def create_user(body: CreateUserBody, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO local_users (username, password_hash, email, display_name, roles, attributes)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            body.username,
            _hash(body.password),
            body.email,
            body.display_name,
            body.roles,
            body.attributes,
        )
    return _strip_hash(row)


@router.get("/")
async def list_users(request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM local_users ORDER BY created_at")
    return [_strip_hash(r) for r in rows]


@router.get("/{user_id}")
async def get_user(user_id: str, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM local_users WHERE id = $1", user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return _strip_hash(row)


@router.put("/{user_id}")
async def update_user(user_id: str, body: UpdateUserBody, request: Request):
    pool = _pool(request)
    sets = []
    params: list[Any] = []
    idx = 1
    if body.email is not None:
        sets.append(f"email = ${idx}")
        params.append(body.email)
        idx += 1
    if body.display_name is not None:
        sets.append(f"display_name = ${idx}")
        params.append(body.display_name)
        idx += 1
    if body.roles is not None:
        sets.append(f"roles = ${idx}")
        params.append(body.roles)
        idx += 1
    if body.attributes is not None:
        sets.append(f"attributes = ${idx}")
        params.append(body.attributes)
        idx += 1
    if body.is_active is not None:
        sets.append(f"is_active = ${idx}")
        params.append(body.is_active)
        idx += 1
    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")
    sets.append("updated_at = NOW()")
    params.append(user_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE local_users SET {', '.join(sets)} WHERE id = ${idx} RETURNING *",
            *params,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return _strip_hash(row)


@router.patch("/{user_id}/password")
async def change_password(user_id: str, body: ChangePasswordBody, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE local_users SET password_hash = $1, updated_at = NOW() WHERE id = $2 RETURNING id",
            _hash(body.password),
            user_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": row["id"]}


@router.delete("/{user_id}")
async def delete_user(user_id: str, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM local_users WHERE id = $1 RETURNING id", user_id
        )
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"deleted": row["id"]}


@router.get("/{user_id}/assignments")
async def list_assignments(user_id: str, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, role_id, domain_id, created_at FROM user_role_assignments "
            "WHERE user_id = $1 ORDER BY role_id, domain_id",
            user_id,
        )
    return [dict(r) for r in rows]


@router.post("/{user_id}/assignments")
async def add_assignment(user_id: str, body: AssignmentBody, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO user_role_assignments (user_id, role_id, domain_id) "
            "VALUES ($1, $2, $3) ON CONFLICT (user_id, role_id, domain_id) DO NOTHING RETURNING id, role_id, domain_id",
            user_id, body.role_id, body.domain_id,
        )
    return dict(row) if row else {"user_id": user_id, "role_id": body.role_id, "domain_id": body.domain_id}


@router.delete("/{user_id}/assignments/{assignment_id}")
async def remove_assignment(user_id: str, assignment_id: int, request: Request):
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM user_role_assignments WHERE id = $1 AND user_id = $2 RETURNING id",
            assignment_id, user_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return {"deleted": row["id"]}
