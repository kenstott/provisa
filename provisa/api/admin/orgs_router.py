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

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import delete as _delete, func, select, update

from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.schema_admin import orgs, user_org_memberships, user_profiles
from provisa.security.rights import has_platform_bypass

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/orgs", tags=["admin"])

# Strong refs to in-flight provisioning tasks so the event loop does not GC them mid-run.
_provisioning_tasks: set[asyncio.Task] = set()


def _require_platform_admin(request: Request) -> None:  # REQ-042, REQ-125, REQ-1297
    """Raise 403 if the caller is not a platform_admin. Dev mode (anonymous) is allowed."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, _app_state)
    if not has_platform_bypass(caps):
        raise ApiError(403, "orgs.platform_admin_required", "platform_admin required")


def _caller_user_id(request: Request) -> str | None:
    """The authenticated user id, or None in dev/no-auth mode (anonymous)."""
    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    return None if user_id in (None, "anonymous") else user_id


async def _org_tenant_db(org_id: str) -> Database:  # REQ-1305
    """The Database scoped to ``org_<org_id>``'s schema, building the org runtime if needed.

    Every two-plane operation writes tenant rows through this: user_role_assignments and the org's
    admin_audit_log live inside the org's own schema, so the process-global state.tenant_db (which
    points at whichever org the request resolved) is the wrong handle for acting on another org.
    """
    from provisa.api.app import ensure_org_runtime

    rt = await ensure_org_runtime(org_id)
    if rt.tenant_db is None:
        raise ApiError(
            409,
            "orgs.no_tenant_runtime",
            f"Org {org_id!r} has no tenant runtime — it may still be provisioning.",
            org=org_id,
        )
    return rt.tenant_db


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
            raise ApiError(
                400, "orgs.invalid_email_rule", f"Invalid email rule: {exc}", error=str(exc)
            ) from exc
    if auto_join and not auto_join_role:
        raise ApiError(
            400, "orgs.auto_join_requires_role", "auto_join requires auto_join_role"
        )


# REQ-1309: the org id becomes the PostgreSQL schema name ``org_<id>`` and the Host subdomain
# (REQ-1276), so it must be a legal *unquoted* SQL identifier and a legal DNS label at once. That
# intersection is a leading lowercase letter followed by lowercase letters and digits. The
# requirement names hyphens as well; they are excluded deliberately, because every org-schema DDL
# site interpolates the name unquoted (``SET search_path TO org_<id>``, ``CREATE ROLE role_<id>``,
# ``org_<id>_mv_cache``, the compiler's ``org_<id>__<catalog>`` catalog names) and a hyphen there is
# a syntax error surfacing during background provisioning — the exact failure mode REQ-1309 exists to
# prevent. 40 chars keeps ``org_<id>_mv_cache`` inside PostgreSQL's 63-byte identifier limit.
_ORG_ID_PATTERN = re.compile(r"[a-z][a-z0-9]{1,39}")

# REQ-1309: each collides with the deployment's own org or with a reserved subdomain. Schema
# names beginning "pg_" and "information_schema" need no entry — the id pattern forbids the
# underscore they all contain, so they can never be chosen in the first place.
_RESERVED_ORG_IDS = frozenset({"root", "default", "public", "admin", "api", "www"})

# REQ-1311: a backstop against runaway or automated creation, not a product tier.
_MAX_ORGS_PER_USER = 100


def _validate_new_org_id(org_id: str) -> None:  # REQ-1309
    """Reject an org id that is not a legal schema name and subdomain, or that is reserved.

    Creation-time only: the id is immutable afterward (rename_org changes the display name alone),
    so this is the single point at which the value is ever chosen.
    """
    if not _ORG_ID_PATTERN.fullmatch(org_id):
        raise ApiError(
            400,
            "orgs.invalid_org_id",
            (
                f"Invalid org id {org_id!r}: 2-40 characters, lowercase letters and digits only, "
                "starting with a letter. The id becomes the database schema name and the subdomain, "
                "and cannot be changed later."
            ),
            org=org_id,
        )
    if org_id in _RESERVED_ORG_IDS:
        raise ApiError(
            400,
            "orgs.org_id_reserved",
            (
                f"Org id {org_id!r} is reserved. Reserved ids: "
                f"{', '.join(sorted(_RESERVED_ORG_IDS))}."
            ),
            org=org_id,
            reserved=", ".join(sorted(_RESERVED_ORG_IDS)),
        )


class AddMemberBody(BaseModel):
    user_id: str


@router.get("/")
async def list_orgs(request: Request):  # REQ-042, REQ-059
    _require_platform_admin(request)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.name, orgs.c.created_by, orgs.c.created_at).order_by(orgs.c.id)
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


async def _refresh_root_registry_view() -> None:  # REQ-1301
    """Rebuild root's org-registry view after the set of orgs changes.

    The view names each org's schema explicitly (org_admin holders are tenant-plane state, one
    table per org), so a new or removed org changes the statement, not just the rows it returns.
    Rebuilding it here is what keeps "who runs org acme" answerable the moment acme exists.
    """
    from provisa.api.app import state
    from provisa.api.org_runtime import reset_current_org, set_current_org
    from provisa.api.startup_seed import seed_org_registry_view

    token = set_current_org(state.org_id)
    try:
        await seed_org_registry_view()
    finally:
        reset_current_org(token)


async def _provision_org_task(org_id: str, include_demo: bool, created_by: str | None) -> None:
    # REQ-1266: async provisioning. Runs the full Part-1 per-org build (schema + PG role + Redis ACL
    # + the queryable data-plane runtime), then grants the creator org_admin inside the new org's
    # schema, then flips provisioning_state. The one allowed catch: a failure is PERSISTED to
    # provisioning_error, never swallowed — the poll endpoint surfaces it.
    import os
    from pathlib import Path

    from provisa.api.app import build_org_runtime, state as _app_state
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
        # REQ-1322: build under the registry's per-org lock, never by calling build_org_runtime
        # directly. Membership is granted synchronously at create, so the creator's very first
        # follow-up request (the status poll) already routes to this org and lazily builds it
        # through ensure_org_runtime. Two concurrent builds both replay the seeding DDL, the
        # second CREATE VIEW collides on pg_type, and provisioning fails with the org half-seeded
        # — the "parts of the demo data, and no meta/ops domains" failure.
        rt = await _app_state.org_registry.rebuild(
            org_id, lambda oid: build_org_runtime(oid, include_demo=include_demo)
        )
        # The creator's org_admin role assignment lands in the org's own schema — possible only now
        # the schema + seeded org_admin row exist. Membership (admin plane) was granted synchronously.
        if created_by is not None:
            assert rt.tenant_db is not None
            await grant_org_role(rt.tenant_db, created_by, "org_admin")
        # The registry view is refreshed BEFORE the row flips to ready: an org the root registry
        # cannot see is not finished provisioning, and flipping first would let a poller read
        # "ready" and then have the except-branch rewrite the same row to "failed".
        await _refresh_root_registry_view()
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
    # org_admin. Dev/no-auth (anonymous) is allowed, matching _require_platform_admin's dev bypass — but
    # then there is no real identity to own the org (created_by stays None).
    identity = getattr(request.state, "identity", None)
    created_by = getattr(identity, "user_id", None) if identity is not None else None
    if created_by == "anonymous":
        created_by = None
    if identity is not None and created_by is None:
        raise ApiError(
            401, "orgs.auth_required_create", "Authentication required to create an org"
        )

    _validate_new_org_id(body.id)
    _validate_org_policy(body.email_rule, body.auto_join, body.auto_join_role)

    # Register the org immediately as "provisioning" and grant the creator membership synchronously
    # (admin plane) so they own it at once; the schema + data plane build in the background task.
    async with _admin_pool().acquire() as conn:
        if created_by is not None:
            # REQ-1311: cap creation per user. Deleted orgs are gone from the registry so they
            # cannot count; failed orgs are excluded explicitly (REQ-1315 — they hold no data and
            # the creator can retry or delete them).
            owned = await conn.execute_core(
                select(func.count())
                .select_from(orgs)
                .where(orgs.c.created_by == created_by, orgs.c.provisioning_state != "failed")
            )
            if (owned.scalar() or 0) >= _MAX_ORGS_PER_USER:
                raise ApiError(
                    409,
                    "orgs.limit_reached",
                    (
                        f"Organization limit reached: a single account may create at most "
                        f"{_MAX_ORGS_PER_USER} organizations. Delete one you no longer need to "
                        "create another."
                    ),
                    limit=_MAX_ORGS_PER_USER,
                )
        exists = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == body.id))
        if exists.fetchone() is not None:
            raise ApiError(409, "orgs.already_exists", "Org already exists")
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
        raise ApiError(404, "orgs.not_found", "Org not found")
    record = dict(row._mapping)
    if user_id not in (None, "anonymous") and record["created_by"] not in (None, user_id):
        from provisa.api.app import state as _app_state
        from provisa.api.admin.capabilities import _resolved_capabilities

        caps = _resolved_capabilities(identity, _app_state)
        if not has_platform_bypass(caps):
            raise ApiError(403, "orgs.view_not_permitted", "Not permitted to view this org")
    return record


@router.put("/{org_id}")
async def rename_org(org_id: str, body: RenameOrgBody, request: Request):  # REQ-042, REQ-059
    _require_platform_admin(request)
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
        raise ApiError(404, "orgs.not_found", "Org not found")
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
        raise ApiError(404, "orgs.not_found", "Org not found")
    return dict(row._mapping)


@router.get("/{org_id}/config-export")
async def export_org_config(org_id: str, request: Request):  # REQ-1304
    """The org's live config YAML — the snapshot the UI downloads before confirming a deletion.

    Not gated by the ``config_live_export`` opt-in: that flag exists because a normalized patch
    cannot stay faithful to a hand-authored config file, and this is a standalone snapshot rather
    than a patch. A build failure surfaces as 500 so the UI aborts the deletion instead of
    destroying an org whose shape was never captured.
    """
    from provisa.api.admin.config_export import build_live_config_yaml
    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.api.org_runtime import reset_current_org, set_current_org

    await _require_org_admin(request, org_id)
    async with _admin_pool().acquire() as conn:
        exists = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
        if exists.fetchone() is None:
            raise ApiError(404, "orgs.not_found", "Org not found")
    await _org_tenant_db(org_id)  # bind the org's runtime before the exporter reads state
    token = set_current_org(org_id)
    try:
        yaml_text = await build_live_config_yaml()
    finally:
        reset_current_org(token)
    return Response(
        content=yaml_text,
        media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{org_id}-config.yaml"'},
    )


@router.delete("/{org_id}")
async def delete_org(org_id: str, request: Request, confirm: str | None = None):  # REQ-1300/1312
    """Destroy an org: tenant schema, control-plane rows, registry entry and cached runtime.

    Available to a platform_admin (any org) or the org's own org_admin — deleting the org one
    administers is a legitimate act, and it is the documented way out for a last org_admin
    (REQ-1302). ``confirm`` must repeat the org id: the UI's typed confirmation is echoed to the
    server so a single unconfirmed request cannot destroy an organization whatever surface it
    arrives on. A failed org (REQ-1315) holds no data and is exempt from that ceremony.

    REQ-1312: billing/usage records and audit entries are not touched. Tenant rows die with the
    schema; membership, invite and auto-join opt-out rows cascade from the registry row.
    """
    import os

    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_provisioning import deprovision_org
    from provisa.core.schema_admin import org_auto_join_optouts, org_invites

    await _require_org_admin(request, org_id)
    if org_id == "root":
        # REQ-1300/REQ-1296: root carries the deployment's own admin surface and demo assets.
        raise ApiError(400, "orgs.cannot_delete_root", "Cannot delete the root org")
    async with _admin_pool().acquire() as conn:
        found = await conn.execute_core(
            select(orgs.c.id, orgs.c.provisioning_state).where(orgs.c.id == org_id)
        )
        row = found.fetchone()
        if row is None:
            raise ApiError(404, "orgs.not_found", "Org not found")
        if row[1] != "failed" and confirm != org_id:
            raise ApiError(
                400,
                "orgs.delete_confirm_required",
                (
                    f"Deleting {org_id!r} destroys all of its data permanently — no one, including "
                    f"a platform administrator, can recover it. Repeat the org id in the 'confirm' "
                    f"parameter to proceed."
                ),
                org=org_id,
            )
        # Explicit deletes rather than relying on FK cascade: the platform control plane may be
        # backed by an engine where the registry tables were created without them.
        await conn.execute_core(
            _delete(user_org_memberships).where(user_org_memberships.c.org_id == org_id)
        )
        await conn.execute_core(_delete(org_invites).where(org_invites.c.org_id == org_id))
        await conn.execute_core(
            _delete(org_auto_join_optouts).where(org_auto_join_optouts.c.org_id == org_id)
        )
        await conn.execute_core(_delete(orgs).where(orgs.c.id == org_id))
    redis_url = os.environ.get("REDIS_URL")
    await deprovision_org(_pool(), org_id, redis_url=redis_url)
    # Evict the cached runtime last: an in-flight request must not rebuild it from a registry row
    # that still exists.
    from provisa.api.app import state as _app_state

    _app_state.org_registry.invalidate(org_id)
    # REQ-1301: the registry view names the dropped schema, so it must stop naming it before anyone
    # queries it again.
    await _refresh_root_registry_view()
    return {"deleted": org_id}


@router.post("/{org_id}/retry")
async def retry_provisioning(org_id: str, request: Request):  # REQ-1315
    """Re-run provisioning for an org whose background build failed.

    Drops whatever partial schema the failure left behind and starts the build from the beginning.
    Available to the creator or a platform_admin. Only a ``failed`` org may be retried — retrying a
    ready one would drop a live schema, and retrying one still provisioning would race the task
    already running.
    """
    import os

    from provisa.core.org_provisioning import deprovision_org

    caller = _caller_user_id(request)
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.created_by, orgs.c.provisioning_state, orgs.c.seeded_demo).where(
                orgs.c.id == org_id
            )
        )
        row = result.fetchone()
        if row is None:
            raise ApiError(404, "orgs.not_found", "Org not found")
        created_by, prov_state, seeded_demo = row[0], row[1], row[2]
        if caller is not None and created_by != caller:
            _require_platform_admin(request)
        if prov_state != "failed":
            raise ApiError(
                409,
                "orgs.retry_not_failed",
                f"Org {org_id!r} is {prov_state!r}, not 'failed' — nothing to retry.",
                org=org_id,
                state=str(prov_state),
            )
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(provisioning_state="provisioning", provisioning_error=None)
        )
    from provisa.api.app import state as _app_state

    _app_state.org_registry.invalidate(org_id)
    await deprovision_org(_pool(), org_id, redis_url=os.environ.get("REDIS_URL"))
    task = asyncio.create_task(_provision_org_task(org_id, bool(seeded_demo), created_by))
    _provisioning_tasks.add(task)
    task.add_done_callback(_provisioning_tasks.discard)
    return {"id": org_id, "provisioning_state": "provisioning"}


@router.get("/{org_id}/members")
async def list_members(org_id: str, request: Request):  # REQ-042, REQ-059, REQ-1305
    """The org's roster, each row carrying whether that person holds org_admin.

    Membership is admin-plane and the org_admin holding is tenant-plane, so the two are read from
    different databases and joined here. The flag is what lets the team page show a promote or
    demote control per person, and what tells it which removals REQ-1302 will refuse — without it
    the page can only offer every action to everyone and let the server say no.
    """
    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_membership import org_admin_user_ids

    await _require_org_admin(request, org_id)
    admin_ids = await org_admin_user_ids(await _org_tenant_db(org_id))
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
    return [{**dict(r._mapping), "is_org_admin": r._mapping["user_id"] in admin_ids} for r in rows]


@router.post("/{org_id}/members")
async def add_member(org_id: str, body: AddMemberBody, request: Request):  # REQ-042, REQ-059
    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_membership import clear_auto_join_suppression

    # Onboarding a member is the same authority as offboarding one (remove_member): the org's own
    # org_admin, or a platform_admin for any org. Requiring platform_admin here would leave an
    # org_admin able to remove people from their org but not add them (REQ-1305).
    await _require_org_admin(request, org_id)
    pool = _admin_pool()
    async with pool.acquire() as conn:
        exists_result = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
        if exists_result.fetchone() is None:
            raise ApiError(404, "orgs.not_found", "Org not found")
        await conn.upsert(
            user_org_memberships,
            {"user_id": body.user_id, "org_id": org_id},
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )
    # REQ-1306: an explicit add is an affirmative act and outranks an earlier departure.
    await clear_auto_join_suppression(pool, body.user_id, org_id)
    return {"user_id": body.user_id, "org_id": org_id}


@router.delete("/{org_id}/members/{user_id}")
async def remove_member(org_id: str, user_id: str, request: Request):  # REQ-1302, REQ-1305
    """Offboard a person from an org across BOTH planes.

    Available to the org's own org_admin and to a platform_admin for any org. Deleting only the
    control-plane membership would leave the tenant-plane role assignments in ``org_<id>`` behind,
    so re-adding the person silently restores every privilege they held before.

    Rejected when it would remove the org's last org_admin (REQ-1302).
    """
    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_membership import (
        LastOrgAdminError,
        assert_not_last_org_admin,
        record_admin_action,
        remove_from_org,
    )

    await _require_org_admin(request, org_id)
    tenant_db = await _org_tenant_db(org_id)
    try:
        await assert_not_last_org_admin(tenant_db, user_id, org_id)
    except LastOrgAdminError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not await remove_from_org(_admin_pool(), tenant_db, user_id, org_id):
        raise ApiError(404, "orgs.membership_not_found", "Membership not found")
    await record_admin_action(
        tenant_db,
        action="remove_member",
        actor_id=_caller_user_id(request) or "anonymous",
        subject_id=user_id,
        detail={"org_id": org_id},
    )
    return {"deleted": {"user_id": user_id, "org_id": org_id}}


@router.post("/{org_id}/leave")
async def leave_org(org_id: str, request: Request):  # REQ-1306
    """Leave an org of one's own accord.

    The same two-plane removal an org_admin would perform (REQ-1305), plus an auto-join opt-out so
    the next sign-in matching the org's email rule does not silently undo the departure. The last
    org_admin cannot leave — promote another first, or delete the org (REQ-1300).
    """
    from provisa.core.org_membership import (
        LastOrgAdminError,
        assert_not_last_org_admin,
        record_admin_action,
        remove_from_org,
        suppress_auto_join,
    )

    user_id = _caller_user_id(request)
    if user_id is None:
        raise ApiError(401, "orgs.auth_required_leave", "Authentication required to leave an org")
    tenant_db = await _org_tenant_db(org_id)
    try:
        await assert_not_last_org_admin(tenant_db, user_id, org_id)
    except LastOrgAdminError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not await remove_from_org(_admin_pool(), tenant_db, user_id, org_id):
        raise ApiError(404, "orgs.membership_not_found", "Membership not found")
    await suppress_auto_join(_admin_pool(), user_id, org_id)
    await record_admin_action(
        tenant_db,
        action="leave_org",
        actor_id=user_id,
        subject_id=user_id,
        detail={"org_id": org_id},
    )
    return {"left": org_id, "user_id": user_id}


@router.post("/{org_id}/admins/{user_id}")
async def grant_org_admin_role(org_id: str, user_id: str, request: Request):  # REQ-1303, REQ-1308
    """Grant org_admin in ``org_id``. Org_admin of that org, or platform_admin for any org.

    REQ-1303: this is the recovery path when an org's administrators are unreachable — whatever
    verification satisfies the platform_admin that the request is legitimate is a business process
    outside the product. The act is written into THAT org's trail, so the intervention is visible to
    the org afterward.

    REQ-1308: a caller can never grant themselves a role they were not assigned.
    """
    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_membership import grant_org_admin, record_admin_action

    await _require_org_admin(request, org_id)
    actor = _caller_user_id(request)
    if actor is not None and actor == user_id:
        raise ApiError(
            403,
            "orgs.self_role_change",
            "A user cannot change their own role. Ask another administrator (REQ-1308).",
        )
    async with _admin_pool().acquire() as conn:
        exists = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
        if exists.fetchone() is None:
            raise ApiError(404, "orgs.not_found", "Org not found")
    tenant_db = await _org_tenant_db(org_id)
    await grant_org_admin(_admin_pool(), tenant_db, user_id, org_id)
    await record_admin_action(
        tenant_db,
        action="grant_org_admin",
        actor_id=actor or "anonymous",
        subject_id=user_id,
        detail={"org_id": org_id, "role_id": "org_admin"},
    )
    return {"org_id": org_id, "user_id": user_id, "role_id": "org_admin"}


@router.delete("/{org_id}/admins/{user_id}")
async def revoke_org_admin_role(org_id: str, user_id: str, request: Request):  # REQ-1302, REQ-1303
    """Revoke org_admin in ``org_id``, leaving the person a member. Audited into the org's trail.

    Rejected when they are the last org_admin (REQ-1302), and when the caller is the subject
    (REQ-1308).
    """
    from sqlalchemy import delete as _sa_delete

    from provisa.api.admin.invites_router import _require_org_admin
    from provisa.core.org_membership import (
        LastOrgAdminError,
        assert_not_last_org_admin,
        record_admin_action,
    )
    from provisa.core.schema_org import user_role_assignments
    from provisa.security.rights import ORG_ADMIN_ROLE

    await _require_org_admin(request, org_id)
    actor = _caller_user_id(request)
    if actor is not None and actor == user_id:
        raise ApiError(
            403,
            "orgs.self_role_change",
            "A user cannot change their own role. Ask another administrator (REQ-1308).",
        )
    tenant_db = await _org_tenant_db(org_id)
    try:
        await assert_not_last_org_admin(tenant_db, user_id, org_id)
    except LastOrgAdminError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            _sa_delete(user_role_assignments).where(
                user_role_assignments.c.user_id == user_id,
                user_role_assignments.c.role_id == ORG_ADMIN_ROLE,
            )
        )
    if (result.rowcount or 0) == 0:
        raise ApiError(
            404,
            "orgs.not_org_admin",
            f"{user_id} does not hold org_admin in {org_id}",
            user=user_id,
            org=org_id,
        )
    await record_admin_action(
        tenant_db,
        action="revoke_org_admin",
        actor_id=actor or "anonymous",
        subject_id=user_id,
        detail={"org_id": org_id, "role_id": "org_admin"},
    )
    return {"revoked": {"org_id": org_id, "user_id": user_id, "role_id": "org_admin"}}
