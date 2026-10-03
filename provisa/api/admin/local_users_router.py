# Copyright (c) 2026 Kenneth Stott
# Canary: 64bedf57-2b37-4ead-b2fe-34e8f8e267a8
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
from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, func, insert, select, update

from provisa.api.admin._platform_guard import require_role_grantable
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.auth.scram_store import write_verifier
from provisa.core.database import Database
from provisa.core.schema_admin import local_users
from provisa.core.org_membership import SELF_ROLE_CHANGE_MESSAGE, is_self_role_change
from provisa.core.schema_org import user_role_assignments
from provisa.security.rights import Capability

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
    # user_role_assignments is the org's model (REQ-1919): its model store.
    from provisa.api.app import state

    assert state.model_db is not None
    return state.model_db


def _admin_pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # local_users lives in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


def _require_user_management(request: Request) -> None:
    require_capability_request(request, Capability.USER_MANAGEMENT.value)


def _refuse_platform_role(request: Request, role_ids: list[str]) -> None:
    """Granting or removing a role that carries platform rights is its holder's act.

    user_management lets an org administrator manage their org's people; it must not let them mint
    a platform administrator. The rule is ``rights.check_role_grant`` — the one every grant path
    asks — read against the role's resolved capabilities, so no role name is tested.
    """
    from provisa.api.app import state

    roles = getattr(state, "roles", {})
    for role_id in role_ids:
        require_role_grantable(request, role_id, (roles.get(role_id) or {}).get("capabilities"))


@router.post("/")
async def create_user(body: CreateUserBody, request: Request):  # REQ-124, REQ-125
    import uuid

    _require_user_management(request)
    _refuse_platform_role(request, body.roles)

    pool = _admin_pool(request)
    user_id = str(uuid.uuid4())
    async with pool.acquire() as conn:
        # id is generated app-side (portable) rather than via a PG-specific
        # server-side UUID default — the platform control plane may be
        # any SQLAlchemy backend.
        result = await conn.execute_core(
            insert(local_users)
            .values(
                id=user_id,
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
    # REQ-1394: the SCRAM verifier is derived here because this is one of only two moments the
    # plaintext password exists. A user created before SCRAM was configured has none until they
    # change their password, which is why pgwire must tolerate its absence.
    await write_verifier(pool, user_id, body.username, body.password)
    return _strip_hash(row)


@router.get("/")
async def list_users(request: Request):
    _require_user_management(request)
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(local_users).order_by(local_users.c.created_at))
        rows = result.fetchall()
    return [_strip_hash(r) for r in rows]


@router.get("/{user_id}")
async def get_user(user_id: str, request: Request):
    _require_user_management(request)
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(local_users).where(local_users.c.id == user_id))
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "users.user_not_found", "User not found")
    return _strip_hash(row)


def _reject_self_role_change(request: Request, user_id: str) -> None:  # REQ-1308
    """403 when the caller is targeting their own role assignment."""
    identity = getattr(request.state, "identity", None)
    actor = getattr(identity, "user_id", None) if identity is not None else None
    if actor == "anonymous":
        actor = None
    if is_self_role_change(actor, user_id):
        raise ApiError(403, "users.self_role_change", SELF_ROLE_CHANGE_MESSAGE)


@router.put("/{user_id}")
async def update_user(user_id: str, body: UpdateUserBody, request: Request):
    _require_user_management(request)
    values: dict[str, Any] = {}
    if body.email is not None:
        values["email"] = body.email
    if body.display_name is not None:
        values["display_name"] = body.display_name
    if body.roles is not None:
        # REQ-1308: the local_users.roles list is a claims source, so editing your own is a self
        # role change no less than editing your own assignment row.
        _reject_self_role_change(request, user_id)
        _refuse_platform_role(request, body.roles)
        # JSON columns take Python objects directly.
        values["roles"] = body.roles
    if body.attributes is not None:
        values["attributes"] = body.attributes
    if body.is_active is not None:
        values["is_active"] = body.is_active
    if not values:
        raise ApiError(400, "users.no_fields_to_update", "No fields to update")
    values["updated_at"] = func.now()
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(local_users)
            .where(local_users.c.id == user_id)
            .values(**values)
            .returning(local_users)
        )
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "users.user_not_found", "User not found")
    return _strip_hash(row)


@router.patch("/{user_id}/password")
async def change_password(user_id: str, body: ChangePasswordBody, request: Request):  # REQ-124
    identity = getattr(request.state, "identity", None)
    # A user may set their own password; setting anyone else's is user management.
    if getattr(identity, "user_id", None) != user_id or identity is None:
        _require_user_management(request)
    pool = _admin_pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(local_users)
            .where(local_users.c.id == user_id)
            .values(password_hash=_hash(body.password), updated_at=func.now())
            .returning(local_users.c.id, local_users.c.username)
        )
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "users.user_not_found", "User not found")
    # REQ-1394: the other moment a plaintext password exists. A user who sets a password after
    # SCRAM is turned on gains a verifier here; that is the whole migration path, since bcrypt
    # hashes cannot be converted into one.
    await write_verifier(pool, row[0], row[1], body.password)
    return {"id": row[0]}


def _acts_across_orgs(request: Request) -> bool:
    """Whether the caller holds the cross-org right (or is the unauthenticated dev identity,
    which holds every right)."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state
    from provisa.security.rights import can_act_cross_org

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return True
    return can_act_cross_org(_resolved_capabilities(identity, state))


@router.delete("/{user_id}")
async def delete_user(user_id: str, request: Request):  # REQ-1302, REQ-1305, REQ-1307, REQ-1918
    """Remove a user. What that means depends on who asks, and the answer's ``scope`` says which.

    The holder of the cross-org right deletes the ACCOUNT, everywhere (``scope: "account"``):
    the same removal the person could ask for themselves, ``org_membership.remove_account``.
    An org administrator removes the person FROM THEIR ORG (``scope: "org"``): the account, and
    the person's place in any other org, are not theirs to end. Either way the last org_admin
    of an org is refused, and so is the deployment's last platform_admin.
    """
    from provisa.api.admin.orgs_router import _caller_user_id, _org_model_db, _org_record_db
    from provisa.core.org_membership import (
        AccountRemovalRefused,
        LastOrgAdminError,
        assert_not_last_org_admin,
        record_admin_action,
        remove_account,
        remove_from_org,
    )

    across_orgs = _acts_across_orgs(request)
    if not across_orgs:
        _require_user_management(request)
    admin_db = _admin_pool(request)
    if across_orgs:
        if not await _account_exists(admin_db, user_id):
            raise ApiError(404, "users.user_not_found", "User not found")
        try:
            removed = await remove_account(
                admin_db,
                _pool(request),
                user_id,
                model_db_of=_org_model_db,
                record_db_of=_org_record_db,
            )
        except AccountRemovalRefused as refused:
            if refused.reason == "last_org_admin":
                raise ApiError(
                    409,
                    "users.last_org_admin",
                    f"{refused}. Promote another org_admin in each, or delete the "
                    "organization, first.",
                    user=user_id,
                    orgs=", ".join(refused.orgs),
                ) from refused
            raise ApiError(
                409,
                "users.last_platform_admin",
                f"{refused}. Grant platform_admin to another user first.",
                user=user_id,
            ) from refused
        return {"scope": "account", **removed}

    org_id = getattr(request.state, "active_org_id", None)
    if org_id is None:
        raise ApiError(
            409, "users.no_active_org", "no org is bound to this request; sign in to an org first"
        )
    model_db = await _org_model_db(org_id)
    try:
        await assert_not_last_org_admin(model_db, user_id, org_id)
    except LastOrgAdminError as exc:
        raise ApiError(
            409,
            "users.last_org_admin",
            f"{user_id} is the last org_admin of: {org_id}. Promote another org_admin in each, "
            "or delete the organization, first.",
            user=user_id,
            orgs=org_id,
        ) from exc
    if not await remove_from_org(admin_db, model_db, user_id, org_id):
        raise ApiError(404, "users.user_not_found", "User not found")
    await record_admin_action(
        model_db,
        action="remove_member",
        actor_id=_caller_user_id(request) or "anonymous",
        subject_id=user_id,
        detail={"org_id": org_id},
    )
    return {"scope": "org", "removed": {"user_id": user_id, "org_id": org_id}}


async def _account_exists(admin_db: Database, user_id: str) -> bool:
    """Whether anything of the person's is recorded: a credential row, a profile or a membership."""
    from provisa.core.schema_admin import user_org_memberships, user_profiles

    async with admin_db.acquire() as conn:
        for table, column in (
            (local_users, local_users.c.id),
            (user_profiles, user_profiles.c.user_id),
            (user_org_memberships, user_org_memberships.c.user_id),
        ):
            found = await conn.execute_core(
                select(column).select_from(table).where(column == user_id)
            )
            if found.fetchone() is not None:
                return True
    return False


@router.get("/{user_id}/assignments")
async def list_assignments(user_id: str, request: Request):
    _require_user_management(request)
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
    _require_user_management(request)
    _reject_self_role_change(request, user_id)  # REQ-1308
    _refuse_platform_role(request, [body.role_id])
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
    _require_user_management(request)
    _reject_self_role_change(request, user_id)  # REQ-1308
    pool = _pool(request)
    t = user_role_assignments
    async with pool.acquire() as conn:
        held = await conn.execute_core(
            select(t.c.role_id).where(t.c.id == assignment_id, t.c.user_id == user_id)
        )
        held_row = held.fetchone()
        if held_row is not None:
            _refuse_platform_role(request, [held_row[0]])
        result = await conn.execute_core(
            _delete(t).where(t.c.id == assignment_id, t.c.user_id == user_id).returning(t.c.id)
        )
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "users.assignment_not_found", "Assignment not found")
    return {"deleted": row[0]}
