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

import asyncio
import logging
import re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, select, update

from provisa.core.database import Database
from provisa.core.schema_admin import orgs, user_org_memberships, user_profiles

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/orgs", tags=["admin"])

# Strong refs to in-flight provisioning tasks so the event loop does not GC them mid-run.
_provisioning_tasks: set[asyncio.Task] = set()


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
    include_demo: bool = False
    # REQ-1268: optional regex an invitee's email must match to join (e.g. "@acme\\.com$").
    email_rule: str | None = None
    # REQ-1269: when true, any user whose email matches email_rule auto-joins with auto_join_role.
    auto_join: bool = False
    auto_join_role: str | None = None


class RenameOrgBody(BaseModel):
    name: str


class OrgPolicyBody(BaseModel):
    # REQ-1268/REQ-1269: join policy an org admin may edit after creation.
    email_rule: str | None = None
    auto_join: bool = False
    auto_join_role: str | None = None


def _validate_org_policy(email_rule: str | None, auto_join: bool, auto_join_role: str | None) -> None:
    """Reject an uncompilable email rule (REQ-1268) or auto_join without a role (REQ-1269)."""
    if email_rule is not None:
        try:
            re.compile(email_rule)
        except re.error as exc:
            raise HTTPException(status_code=400, detail=f"Invalid email rule: {exc}") from exc
    if auto_join and not auto_join_role:
        raise HTTPException(status_code=400, detail="auto_join requires auto_join_role")


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


async def _provision_org_task(org_id: str, include_demo: bool, created_by: str | None) -> None:
    # REQ-1266: async provisioning. Runs the full Part-1 per-org build (schema + PG role + Redis ACL
    # + the queryable data-plane runtime), then grants the creator org_admin inside the new org's
    # schema, then flips provisioning_state. The one allowed catch: a failure is PERSISTED to
    # provisioning_error, never swallowed — the poll endpoint surfaces it.
    import os
    from pathlib import Path

    from provisa.api.app import build_org_runtime
    from provisa.core.org_membership import grant_org_role, notify_org_ready
    from provisa.core.org_provisioning import provision_org

    try:
        schema_sql_path = Path(__file__).parent.parent.parent / "core" / "schema.sql"
        schema_sql = schema_sql_path.read_text() if schema_sql_path.exists() else ""
        await provision_org(
            _pool(),
            schema_sql,
            org_id=org_id,
            redis_url=os.environ.get("REDIS_URL"),
            redis_password=os.environ.get("PROVISA_REDIS_ORG_PASSWORD"),
        )
        rt = await build_org_runtime(org_id, include_demo=include_demo)
        # The creator's org_admin role assignment lands in the org's own schema — possible only now
        # the schema + seeded org_admin row exist. Membership (admin plane) was granted synchronously.
        if created_by is not None:
            assert rt.tenant_db is not None
            await grant_org_role(rt.tenant_db, created_by, "org_admin")
        async with _admin_pool().acquire() as conn:
            await conn.execute_core(
                update(orgs)
                .where(orgs.c.id == org_id)
                .values(provisioning_state="ready", provisioning_error=None)
            )
        if created_by is not None:
            notify_org_ready(org_id, created_by)
    except Exception as exc:  # persist-not-swallow (REQ-1266)
        log.exception("org provisioning failed for %s", org_id)
        async with _admin_pool().acquire() as conn:
            await conn.execute_core(
                update(orgs)
                .where(orgs.c.id == org_id)
                .values(provisioning_state="failed", provisioning_error=str(exc))
            )


@router.post("/")
async def create_org(body: CreateOrgBody, request: Request):  # REQ-042, REQ-059, REQ-701, REQ-1266
    # Self-service: any authenticated (non-anonymous) user may create an org and becomes its
    # org_admin. Dev/no-auth (anonymous) is allowed, matching _require_superadmin's dev bypass — but
    # then there is no real identity to own the org (created_by stays None).
    identity = getattr(request.state, "identity", None)
    created_by = getattr(identity, "user_id", None) if identity is not None else None
    if created_by == "anonymous":
        created_by = None
    if identity is not None and created_by is None:
        raise HTTPException(status_code=401, detail="Authentication required to create an org")

    _validate_org_policy(body.email_rule, body.auto_join, body.auto_join_role)

    # Register the org immediately as "provisioning" and grant the creator membership synchronously
    # (admin plane) so they own it at once; the schema + data plane build in the background task.
    async with _admin_pool().acquire() as conn:
        exists = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == body.id))
        if exists.fetchone() is not None:
            raise HTTPException(status_code=409, detail="Org already exists")
        result = await conn.execute_core(
            orgs.insert()
            .values(
                id=body.id,
                name=body.name,
                created_by=created_by,
                provisioning_state="provisioning",
                seeded_demo=body.include_demo,
                email_rule=body.email_rule,
                auto_join=body.auto_join,
                auto_join_role=body.auto_join_role,
            )
            .returning(orgs.c.id, orgs.c.name, orgs.c.created_by, orgs.c.provisioning_state)
        )
        row = result.fetchone()
        if created_by is not None:
            await conn.upsert(
                user_org_memberships,
                {"user_id": created_by, "org_id": body.id},
                index_elements=["user_id", "org_id"],
                update_columns=[],
            )

    task = asyncio.create_task(_provision_org_task(body.id, body.include_demo, created_by))
    _provisioning_tasks.add(task)
    task.add_done_callback(_provisioning_tasks.discard)
    return dict(row._mapping)


@router.get("/{org_id}/status")
async def org_status(org_id: str, request: Request):  # REQ-1266
    # Poll target for the onboarding UI. Visible to the org creator or a platform admin.
    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            select(
                orgs.c.id,
                orgs.c.name,
                orgs.c.created_by,
                orgs.c.provisioning_state,
                orgs.c.provisioning_error,
            ).where(orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Org not found")
    record = dict(row._mapping)
    if user_id not in (None, "anonymous") and record["created_by"] not in (None, user_id):
        from provisa.api.app import state as _app_state
        from provisa.api.admin.capabilities import _resolved_capabilities

        caps = _resolved_capabilities(identity, _app_state)
        if "superadmin" not in caps and "admin" not in caps:
            raise HTTPException(status_code=403, detail="Not permitted to view this org")
    return record


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


@router.patch("/{org_id}/settings")
async def update_org_settings(org_id: str, body: OrgPolicyBody, request: Request):  # REQ-1268/1269
    """Edit an org's join policy (email rule + auto-join). Org admin of this org, or platform admin."""
    from provisa.api.admin.invites_router import _require_org_admin

    await _require_org_admin(request, org_id)
    _validate_org_policy(body.email_rule, body.auto_join, body.auto_join_role)
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(
                email_rule=body.email_rule,
                auto_join=body.auto_join,
                auto_join_role=body.auto_join_role,
            )
            .returning(orgs.c.id, orgs.c.email_rule, orgs.c.auto_join, orgs.c.auto_join_role)
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
