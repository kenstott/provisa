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
from sqlalchemy import insert, or_, select, update

from provisa.api.admin._platform_guard import role_definition_problem
from provisa.api.admin.capabilities import require_capability_request, role_definitions_visible
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

    assert state.model_db is not None
    return state.model_db


class CreateRoleBody(BaseModel):
    id: str
    capabilities: list[str]
    domain_access: list[str]
    parent_role_id: str | None = None  # REQ-1677


class UpdateRoleBody(BaseModel):
    capabilities: list[str] | None = None
    domain_access: list[str] | None = None
    parent_role_id: str | None = None  # REQ-1677: None leaves the parent unchanged


@router.get("/")
async def list_roles(request: Request):  # REQ-042, REQ-059, REQ-060
    org_id = _active_org(request)
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                roles.c.id,
                roles.c.capabilities,
                # REQ-1602: the rights the role is shown but does not hold -- the client dims and
                # badges those surfaces instead of dropping them.
                roles.c.demonstrated,
                roles.c.domain_access,
                roles.c.org_id,
                roles.c.parent_role_id,  # REQ-1677
            )
            .where(or_(roles.c.org_id.is_(None), roles.c.org_id == org_id))
            .order_by(roles.c.id)
        )
        rows = result.fetchall()
    identity = getattr(request.state, "identity", None)
    full = role_definitions_visible(request, getattr(identity, "roles", []))
    return [dict(r._mapping) if full(r.id) else {"id": r.id} for r in rows]


async def _require_reach_of_added(
    conn, request: Request, own_before, parent_before, own_after, parent_after
) -> None:  # REQ-1531
    """The caller reaches every domain this change adds to what the role reaches — the domains
    it lists and the ones it inherits (REQ-1677). ``own_before`` is None for a new role."""
    from provisa.api.admin.capabilities import require_reach_of_added_domains_request
    from provisa.core.repositories import role as role_repo
    from provisa.security.inheritance import effective_domain_access

    rows = await role_repo.list_all(conn)

    def _reach(own, parent_id):
        inherited = effective_domain_access(parent_id, rows) if parent_id else []
        return [*own, *inherited]

    before = None if own_before is None else _reach(own_before, parent_before)
    require_reach_of_added_domains_request(request, before, _reach(own_after, parent_after))


async def _require_reach_of_existing(conn, request: Request, role_id: str) -> None:  # REQ-1531
    """Changing or removing a role is an act in every domain it reaches now -- listed or
    inherited -- so the caller's user_management must reach each. A role that does not exist is
    the act's own not-found."""
    from provisa.api.admin.capabilities import require_right_in_domains_request
    from provisa.core.repositories import role as role_repo
    from provisa.security.inheritance import effective_domain_access

    rows = await role_repo.list_all(conn)
    if any(r["id"] == role_id for r in rows):
        require_right_in_domains_request(
            request, "user_management", effective_domain_access(role_id, rows)
        )


@router.post("/")
async def create_role(body: CreateRoleBody, request: Request):  # REQ-042, REQ-059, REQ-060, REQ-215
    # REQ-1531: a role carries capabilities AND domain_access, so minting one widens scope.
    require_capability_request(request, "user_management")
    org_id = _active_org(request)
    pool = _pool(request)
    async with pool.acquire() as conn:
        await _check_parent(conn, body.id, body.parent_role_id)  # REQ-1677
        await _check_definition(
            conn, request, body.id, body.capabilities, body.domain_access, body.parent_role_id
        )
        # REQ-1531: a role hands out reach; its creator must hold what it lists and inherits.
        await _require_reach_of_added(
            conn, request, None, None, body.domain_access, body.parent_role_id
        )
        await conn.execute_core(
            insert(roles).values(
                id=body.id,
                capabilities=body.capabilities,
                domain_access=body.domain_access,
                org_id=org_id,
                parent_role_id=body.parent_role_id,
            )
        )
    from provisa.api.app import _rebuild_schemas

    # The role's rights and schema are live once it is made, as on the GraphQL path and delete.
    await _rebuild_schemas()
    return {
        "id": body.id,
        "capabilities": body.capabilities,
        "domain_access": body.domain_access,
        "org_id": org_id,
        "parent_role_id": body.parent_role_id,
    }


async def _check_definition(  # REQ-042, REQ-1337, REQ-1530
    conn,
    request: Request,
    role_id: str,
    capabilities: list[str],
    domain_access: list[str],
    parent_id: str | None,
) -> None:
    """Refuse a definition that lists no domain, names an unknown capability, or carries a
    platform right the caller may not define — its own values or the ones its parent chain hands
    down (REQ-1677 folds a parent's rights and domains into the role). See
    ``role_definition_problem``."""
    inherited: list[str] = []
    inherited_domains: list[str] = []
    if parent_id is not None:
        from provisa.security.inheritance import effective_capabilities, effective_domain_access

        result = await conn.execute_core(
            select(roles.c.id, roles.c.capabilities, roles.c.domain_access, roles.c.parent_role_id)
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
        inherited = effective_capabilities(parent_id, rows)
        inherited_domains = effective_domain_access(parent_id, rows)
    problem = role_definition_problem(
        request,
        capabilities,
        inherited,
        role_id=role_id,
        domain_access=domain_access,
        inherited_domain_access=inherited_domains,
    )
    if problem is not None:
        raise problem


async def _check_parent(conn, role_id: str, parent_id: str | None) -> None:  # REQ-1677
    """Refuse a parent that is missing, the role itself, or would close a cycle."""
    from provisa.security.inheritance import parent_map, parent_problem

    if parent_id is None:
        return
    result = await conn.execute_core(select(roles.c.id, roles.c.parent_role_id))
    existing = [dict(r._mapping) for r in result.fetchall()]
    problem = parent_problem(role_id, parent_id, parent_map(existing))
    if problem is not None:
        raise ApiError(400, "roles.parent_invalid", problem)


@router.put("/{role_id}")
async def update_role(
    role_id: str, body: UpdateRoleBody, request: Request
):  # REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531, REQ-1539
    require_capability_request(request, "user_management")  # REQ-1531: see create_role
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                roles.c.id,
                roles.c.capabilities,
                roles.c.domain_access,
                roles.c.org_id,
                roles.c.parent_role_id,
            ).where(roles.c.id == role_id)
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

        new_parent = (
            body.parent_role_id if body.parent_role_id is not None else existing["parent_role_id"]
        )
        await _require_reach_of_existing(conn, request, role_id)  # REQ-1531
        if new_parent != existing["parent_role_id"]:
            await _check_parent(conn, role_id, new_parent)  # REQ-1677
        await _check_definition(conn, request, role_id, new_caps, new_domains, new_parent)
        await _require_reach_of_added(
            conn,
            request,
            existing["domain_access"],
            existing["parent_role_id"],
            new_domains,
            new_parent,
        )
        await conn.execute_core(
            update(roles)
            .where(roles.c.id == role_id)
            .values(capabilities=new_caps, domain_access=new_domains, parent_role_id=new_parent)
        )
    from provisa.api.app import _rebuild_schemas

    # The edit is live at once, as on the GraphQL path and delete.
    await _rebuild_schemas()
    return {
        "id": role_id,
        "capabilities": new_caps,
        "domain_access": new_domains,
        "org_id": existing["org_id"],
        "parent_role_id": new_parent,
    }


@router.delete("/{role_id}")
async def delete_role(role_id: str, request: Request):  # REQ-042, REQ-059, REQ-060, REQ-1531
    require_capability_request(request, "user_management")  # REQ-1531: see create_role
    from provisa.api.app import _rebuild_schemas
    from provisa.core.repositories import role as role_repo

    pool = _pool(request)
    async with pool.acquire() as conn:
        await _require_reach_of_existing(conn, request, role_id)  # REQ-1531
        try:
            deleted = await role_repo.delete(conn, role_id)
        except role_repo.RoleDeleteRefused as refused:
            if refused.reason == "dependents":
                # REQ-1918: nothing is removed; every dependent is named.
                raise ApiError(
                    409,
                    "roles.has_dependents",
                    str(refused),
                    role=role_id,
                    count=len(refused.dependents),
                    dependents=[d.as_dict() for d in refused.dependents],
                ) from refused
            raise ApiError(
                400, "roles.cannot_delete_system", "Cannot delete system roles"
            ) from refused
    if not deleted:
        raise ApiError(404, "roles.not_found", "Role not found")
    # The role's built schema and context must not outlive it; the GraphQL path rebuilds too.
    await _rebuild_schemas()
    return {"deleted": role_id}
