# Copyright (c) 2026 Kenneth Stott
# Canary: 6a0f2c85-47db-4e19-9b3a-2d61c40fe7b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The environments of one organization (REQ-1487..REQ-1491, REQ-1504, REQ-1523).

An environment is CREATED BY DEPLOYING a model into a name (REQ-1488): there is no create-then-
populate step here, and no endpoint that makes an environment holding nothing. POST reserves the
name against the org's plan ceiling, provisions the schema and its stores, and carries the model
in — one act, and if any part of it fails the ones that landed are undone, because an environment
that exists and holds half a model is worse than one that does not exist.

Every environment this creates is UNBOUND (REQ-1491), whatever it was created from. Binding is a
second, deliberate act against an environment that already exists, which is what makes it
impossible for one call to produce an environment pointed at production.
"""

# Requirements: REQ-1487, REQ-1488, REQ-1489, REQ-1490, REQ-1491, REQ-1504, REQ-1523, REQ-1524,
# REQ-1527, REQ-1528, REQ-1529

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, select

from provisa.api.env_routing import SWITCH_CAPABILITY
from provisa.api.errors import ApiError
from provisa.core import env_approvals, env_ci, env_remote
from provisa.core.database import Database
from provisa.core.env_authority import owns_environment
from provisa.core.env_copy import MERGE, copy_model, plan_copy
from provisa.core.env_deploy import (
    DeployError,
    deploy_tree,
    plan_deploy,
    report_touches_connectivity,
)
from provisa.core.env_retire import RetirementError, retire_environment
from provisa.core.env_store import (
    EnvironmentLimitError,
    get_env,
    list_envs,
    set_expiry,
    set_protected,
)
from provisa.core.environments import PROD, EnvironmentNameError
from provisa.core.schema_admin import user_org_memberships

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/orgs/{org_id}/environments", tags=["admin"])


def _state():
    """The app state the commerce seam reads a plan ceiling from (REQ-1513)."""
    from provisa.api.app import state

    return state


def _admin_pool() -> Database:
    """The platform control plane, which is where an environment's registry row lives — routing
    reads it before it has chosen a schema to read anything else from (REQ-1488)."""
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


def _pool() -> Database:
    """The tenant control plane, which holds every org schema and therefore every environment."""
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


def _caller_user_id(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    return None if user_id in (None, "anonymous") else user_id


async def _guard(request: Request, org_id: str) -> str | None:
    """An act on the ORGANIZATION's environments — protecting one, deleting one, deciding a merge
    into a protected one. These stay org_admin acts (REQ-1488, REQ-1504, REQ-1528). Returns the
    acting user for the audit entry."""
    from provisa.api.admin.invites_router import _require_org_admin

    await _require_org_admin(request, org_id)
    return _caller_user_id(request)


#: The right to create an environment and to reach the environments surface at all (REQ-1573).
MANAGE_CAPABILITY = "environment_management"


async def _member(request: Request, org_id: str, *rights: str) -> str | None:
    """A member of the org holding at least one of ``rights`` (REQ-1528, REQ-1573).

    CREATING an environment is REQ-1528's whole privilege-expansion path: a member holding no
    model-editing rights acquires them by creating an environment and owning it, and there is no
    other route. That is why the guard is not org_admin — an org_admin is a member too, so nothing
    they could do before is refused here.

    REQ-1573 narrows "any member" to "a member whose role carries the right". The argument for
    leaving it open was that the authority is useless without bindings, which holds against an
    attacker and not against an accident: an environment is a schema and a place in the org's plan
    ceiling, and an analyst has no reason to make one. org_admin and developer carry the right in
    the seed; analyst and modeler do not.
    """
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state as _app_state
    from provisa.security.rights import can_act_cross_org

    user_id = _caller_user_id(request)
    if user_id is None:
        return None  # dev mode — no auth configured, matching _require_org_admin
    identity = request.state.identity
    capabilities = _resolved_capabilities(identity, _app_state)
    if can_act_cross_org(capabilities):
        return user_id
    if not set(rights) & capabilities:
        raise ApiError(
            403,
            "environments.capability_required",
            "This requires one of the " + ", ".join(repr(r) for r in rights) + " capabilities.",
            org=org_id,
        )
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
        if result.fetchone() is None:
            raise ApiError(
                403,
                "environments.membership_required",
                f"Acting on the environments of {org_id!r} requires membership of it.",
                org=org_id,
            )
    return user_id


async def _guard_within(request: Request, org_id: str, name: str) -> str | None:
    """An act INSIDE one environment. Held either by an org_admin, or by whoever created the
    environment — who holds org_admin's model-editing rights within it and nowhere else (REQ-1528).

    The owner's authority is derived from ``environments.created_by`` at this moment rather than
    read from a grant table, so it cannot describe an environment that no longer exists.
    """
    user_id = _caller_user_id(request)
    if user_id is not None and await owns_environment(_admin_pool(), org_id, name, user_id):
        return user_id
    return await _guard(request, org_id)


async def _member_count(org_id: str) -> int:
    """How many members the org has — the condition REQ-1504 reads prod's protection from."""
    async with _admin_pool().acquire() as conn:
        result = await conn.execute_core(
            select(func.count())
            .select_from(user_org_memberships)
            .where(user_org_memberships.c.org_id == org_id)
        )
        row = result.fetchone()
        assert row is not None  # COUNT over an empty table is a row holding 0, never no row
        return int(row[0])


async def _known(org_id: str, name: str) -> dict:
    row = await get_env(_admin_pool(), org_id, name)
    if row is None:
        raise ApiError(
            404,
            "environments.unknown",
            f"Organization {org_id!r} has no environment {name!r}.",
            org=org_id,
            env=name,
        )
    return row


async def _audit(org_id: str, actor: str | None, action: str, name: str, detail: dict) -> None:
    """Record the act in the ORG's own trail (REQ-1488), where the org can see it."""
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.org_membership import record_admin_action

    await record_admin_action(
        await _org_tenant_db(org_id),
        action=action,
        actor_id=actor or "anonymous",
        subject_id=name,
        detail=detail,
    )


class CreateEnvBody(BaseModel):
    name: str
    # The environment whose model the new one starts from. prod by default: it is the environment
    # every org is guaranteed to have (REQ-1487).
    from_env: str = PROD
    expires_at: datetime | None = None
    # REQ-1538: whether the new environment INHERITS ``from_env``'s connections — the host, port,
    # database, username and the rest of the coordinates that say where a source actually points.
    # OFF BY DEFAULT, and deliberately so: the whole reason to make a dev environment from prod is
    # to get prod's model without pointing at prod's databases, so resolving production's
    # connections has to be asked for rather than arrived at by leaving a box alone.
    #
    # REQ-1529 calls the off case a BASE and the on case a BRANCH; that is the same distinction
    # named from the user's side. A base carries its own connections and is what others inherit
    # from; a branch resolves them from ``from_env`` by reference, which is why creating one asks
    # its creator for no credentials. Either way the model is copied whole and no credential is:
    # credentials live on binding columns, which REQ-1491 keeps out of every copy.
    inherit_connections: bool = False


class PatchEnvBody(BaseModel):
    expires_at: datetime | None = None
    clear_expiry: bool = False
    protected: bool | None = None


class MergeBody(BaseModel):
    from_env: str
    # REQ-1490: an object the source no longer has is removed only when the merge asks for it, and
    # asking is a separate confirmation, so a merge cannot silently empty an environment.
    removals: bool = False
    dry_run: bool = False
    # REQ-1542: end the source environment once its work has landed. Off by default -- a merge is
    # a copy, and a copy that also deleted an environment unless told otherwise would make the
    # ordinary "take my changes" call destructive. A feature branch that is done says so here.
    retire_source: bool = False
    # REQ-1549: and whether the retirement reaches the REMOTE branch as well. Separate from
    # ``retire_source`` because the remote is where the work survives a lost volume (REQ-1546):
    # ending an environment here never implies ending the copy that is somewhere else.
    retire_remote: bool = False
    # REQ-1550: REQUIRED on a merge, and not for bookkeeping. A merge lands as ONE squashed commit
    # (REQ-1545), so the range of work it stands for is unreadable from the history afterwards and
    # this sentence is the only account of what that range was. The generated provenance -- source
    # and sha -- cannot say it: it names where the work came from, not what the work does.
    message: str = ""


class DeployBody(BaseModel):
    """REQ-1496: the tree to make into this environment's model, named by ref."""

    # A branch name or a sha. A branch is resolved to the sha it points at NOW and that sha is what
    # is loaded and reported, so the answer never describes a different commit than the one applied.
    ref: str
    dry_run: bool = False
    # REQ-1539: whether the deploy applies the creation-only classes -- roles above all. OFF except
    # when the deploy is what CREATES the environment, because a tree carries the roles of whatever
    # control plane projected it, and a desktop's self-granted rights must not arrive with it.
    seed: bool = False
    message: str = ""


class RepoIntegrationBody(BaseModel):
    # REQ-1527: both optional, both nullable. The remote may carry a secret reference
    # ("https://${env:GIT_TOKEN}@github.com/acme/model.git") and is never resolved on this door.
    remote: str | None = None
    status_webhook: str | None = None


class RemoteProbeBody(BaseModel):
    # REQ-1537: the remote to probe. Absent means "the one already stored", which is how the panel
    # re-checks a mirror it did not just edit; present is the candidate an operator is typing, and
    # probing it BEFORE it is saved is the point — a typo is caught while the field is still open.
    remote: str | None = None


class RemoteCreateBody(BaseModel):
    # REQ-1537: the remote to create, named explicitly. There is no "create the stored one" form:
    # the operator answers a question about a specific address, and the address they answered about
    # is the one that must be created, even if another request changed the stored value meanwhile.
    remote: str
    # Private is the default because a model projection describes an org's data estate — table
    # names, domains, relationships — and a public mirror publishes it.
    private: bool = True


class DecideBody(BaseModel):
    approve: bool
    note: str | None = None


@router.get("")
async def list_environments(request: Request, org_id: str) -> dict:
    # A member sees the org's environments: they cannot decide which one to create theirs from, or
    # which to propose into, without seeing them (REQ-1528). REQ-1573: either environment right
    # answers — the switcher lists them to choose one, the admin surface to manage them.
    await _member(request, org_id, MANAGE_CAPABILITY, SWITCH_CAPABILITY)
    return {
        "environments": [_with_history(org_id, r) for r in await list_envs(_admin_pool(), org_id)]
    }


def _with_history(org_id: str, row: dict) -> dict:
    """Say which way REQ-1543's history is open, where the environment itself is read.

    REQ-1553: an undo offered whatever the cursor holds is a control that lies at both ends of the
    line -- at the first commit there is nothing behind, and at the top of a run nothing ahead --
    and the person finds out by pressing it and being refused. Both answers are already in the row
    or one parent lookup away, so they travel with the environment instead.
    """
    from provisa.core.env_repo import RepositoryError, parent_of

    here = row["deployed_sha"]
    # REQ-1553: and the beginning of the line is the ENVIRONMENT's beginning, not the repository's.
    # A branch is seeded at its source's tip, so a parent exists one step below the environment's
    # first own commit -- and that parent is the source's, which is why ``origin_sha`` stops the
    # walk there rather than the absence of a parent doing it.
    #
    # REQ-1524: the repository is a PROJECTION, never an authority, so this node's object store not
    # holding the commit the control plane names is a state the design ADMITS -- it is what DRIFTED
    # means. The environment still exists and still lists; what it has lost is its history, so the
    # two history answers are the ones that go false. This is not a fallback for a missing value:
    # ``deployed_sha`` is present and authoritative, and the question being answered here is
    # strictly "can this node walk back from it", which a projection without the object cannot.
    try:
        behind = parent_of(org_id, here) if here is not None else None
    except RepositoryError:
        behind = None
    row["can_undo"] = behind is not None and behind != row["origin_sha"]
    row["can_redo"] = row["redo_sha"] is not None
    return row


@router.post("")
async def create_environment(request: Request, org_id: str, body: CreateEnvBody) -> dict:
    """Create an environment by deploying another one's model into a name (REQ-1488).

    Order is the point: the name is validated and the plan ceiling checked BEFORE anything is
    provisioned, so a refusal leaves no schema behind, and the compensating rollback undoes the
    schema and the row if the model does not land.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_create import create_environment as _create_env

    # REQ-1529: binding a base is an org_admin's act, so creating one is too. A branch is open to
    # any member, because branching is REQ-1528's only path to model-editing rights.
    actor = await (
        _member(request, org_id, MANAGE_CAPABILITY)
        if body.inherit_connections
        else _guard(request, org_id)
    )
    await _known(org_id, body.from_env)  # the source has to exist before anything is reserved
    # REQ-1488: an environment is a schema, so a plane without schemas cannot hold one. Refused
    # here, before the name is reserved — provisioning would otherwise report success and write
    # the new environment's model into the org's only namespace.
    if _pool().dialect != "postgresql":
        raise ApiError(
            409,
            "environments.plane_unsupported",
            f"Environments need a PostgreSQL control plane; this one is "
            f"{_pool().dialect!r}. Only {PROD!r} exists here.",
            org=org_id,
            env=body.name,
        )

    try:
        report = await _create_env(
            _state(),
            _admin_pool(),
            _pool(),
            await _org_tenant_db(org_id),
            org_id,
            body.name,
            from_env=body.from_env,
            created_by=actor,
            expires_at=body.expires_at,
            branched_from=body.from_env if body.inherit_connections else None,
            note=f"created from {body.from_env}",
        )
    except EnvironmentNameError as exc:
        raise ApiError(
            400, "environments.invalid_name", str(exc), org=org_id, env=body.name
        ) from exc
    except EnvironmentLimitError as exc:
        raise ApiError(
            402,
            "environments.limit_reached",
            str(exc),
            org=org_id,
            held=exc.held,
            limit=exc.limit,
            plan=exc.plan,
        ) from exc

    await _audit(
        org_id,
        actor,
        "environment.create",
        body.name,
        {
            "from": body.from_env,
            "inherit_connections": body.inherit_connections,
            **report.as_dict(),
        },
    )
    return {
        "environment": await get_env(_admin_pool(), org_id, body.name),
        "copy": report.as_dict(),
    }


@router.delete("/{name}")
async def delete_environment(
    request: Request,
    org_id: str,
    name: str,
    delete_branch: bool = False,
    delete_remote_branch: bool = False,
) -> dict:
    """Drop an environment's schemas, its stores and its row. prod is refused (REQ-1487).

    The BRANCH is kept (REQ-1542). A person deleting an environment has asked to stop paying for
    its schemas, not to lose the record of what it held, and the ref is that record. A merge that
    retires its source is the case where the branch goes too, because there the work has landed
    somewhere else and the feature is over.

    THE REMOTE BRANCH IS A SEPARATE ASK (REQ-1546). Deleting it is what makes the deletion final --
    the remote is where the work survives a lost volume -- so it happens only when named, and only
    after the local retirement succeeded: an org left with the remote copy of an environment it
    could not retire still has its work.
    """
    actor = await _guard(request, org_id)
    await _known(org_id, name)
    try:
        outcome = await retire_environment(
            _pool(), _admin_pool(), org_id, name, drop_branch=delete_branch
        )
    except RetirementError as exc:
        raise ApiError(409, "environments.prod_immutable", str(exc), org=org_id, env=name) from exc
    remote_deleted = None
    if delete_remote_branch:
        remote = await _remote_of(org_id)
        try:
            await _remotely(
                org_id,
                env_ci.delete_remote_branch,
                org_id,
                name,
                remote,
                user_id=_caller_user_id(request),
            )
        except _remote_failures() as exc:
            raise ApiError(
                400, "environments.remote_unwritable", str(exc), org=org_id, env=name
            ) from exc
        _forget_pushed(org_id, name)
        remote_deleted = True
    await _audit(
        org_id,
        actor,
        "environment.delete",
        name,
        {**outcome, "remote_branch_deleted": remote_deleted},
    )
    return {
        "deleted": name,
        "branch_deleted": outcome["branch_deleted"],
        "remote_branch_deleted": remote_deleted,
    }


@router.patch("/{name}")
async def patch_environment(request: Request, org_id: str, name: str, body: PatchEnvBody) -> dict:
    """Set what the registry row holds and the schema cannot: expiry (REQ-1523) and whether a merge
    into this environment waits for someone else's approval (REQ-1504)."""
    actor = await _guard(request, org_id)
    await _known(org_id, name)
    detail: dict = {}
    try:
        if body.clear_expiry:
            await set_expiry(_admin_pool(), org_id, name, None)
            detail["expires_at"] = None
        elif body.expires_at is not None:
            await set_expiry(_admin_pool(), org_id, name, body.expires_at)
            detail["expires_at"] = body.expires_at.isoformat()
        if body.protected is not None:
            await set_protected(_admin_pool(), org_id, name, body.protected)
            detail["protected"] = body.protected
    except EnvironmentNameError as exc:
        raise ApiError(409, "environments.prod_immutable", str(exc), org=org_id, env=name) from exc
    if detail:
        await _audit(org_id, actor, "environment.update", name, detail)
    return {"environment": await get_env(_admin_pool(), org_id, name)}


@router.post("/{name}/merge")
async def merge_into_environment(request: Request, org_id: str, name: str, body: MergeBody) -> dict:
    """Merge another environment's model into this one by identity (REQ-1490).

    Every excluded class in the target is left exactly as it was: a dev environment does not lose
    its bindings, its grants or its secrets by taking a newer model from prod.

    A PROTECTED target is not refused — it is PROPOSED to (REQ-1504). The same call from the same
    person becomes a request somebody else reads, because the requester has no way to make it
    anything else and no reason to be told to go elsewhere. A dry run stays a dry run either way:
    it writes nothing, so there is nothing for an approver to hold.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db

    actor = await _guard_within(request, org_id, body.from_env)
    await _known(org_id, name)
    await _known(org_id, body.from_env)
    if body.from_env == name:
        raise ApiError(
            400,
            "environments.same_environment",
            "An environment cannot be merged into itself.",
            org=org_id,
            env=name,
        )
    # REQ-1550: the comment is required, and the nested option cannot be asked for on its own.
    # Deleting the remote branch is a modifier on retiring the source, not an independent act --
    # a merge that left the local environment standing and deleted its only off-volume copy would
    # be strictly destructive, and no interface offers it.
    if not body.message.strip():
        raise ApiError(
            400,
            "environments.message_required",
            "A merge needs a comment: it lands as one squashed commit, so this is the only "
            "account of the range of work it stands for.",
            org=org_id,
            env=name,
        )
    if body.retire_remote and not body.retire_source:
        raise ApiError(
            400,
            "environments.remote_without_local",
            "Deleting the remote branch is an option ON retiring the source, not instead of it.",
            org=org_id,
            env=body.from_env,
        )
    db = await _org_tenant_db(org_id)
    protected = await env_approvals.is_protected(
        _admin_pool(), org_id, name, await _member_count(org_id)
    )
    if protected and not body.dry_run:
        merge_request = await env_approvals.request_merge(
            _admin_pool(),
            db,
            org_id,
            source_env=body.from_env,
            target_env=name,
            requested_by=actor or "anonymous",
            message=body.message,
            removals=body.removals,
            retire_source=body.retire_source,
            retire_remote=body.retire_remote,
        )
        await _audit(
            org_id,
            actor,
            "environment.merge_requested",
            name,
            {"request_id": merge_request["id"], "from": body.from_env, **merge_request["report"]},
        )
        return {"request": _rendered(merge_request), "applied": False, "requires_approval": True}

    run = plan_copy if body.dry_run else copy_model
    report = await run(db, org_id, body.from_env, name, mode=MERGE, removals=body.removals)
    retired = None
    refreshed = None
    squashed = None
    if not body.dry_run:
        await _audit(org_id, actor, "environment.merge", name, report.as_dict())
        squashed = await _squash(org_id, body.from_env, name, actor, body.message)
        refreshed = await _refresh(org_id, name, connectivity=report.touches_connectivity)
        if body.retire_source:
            retired = await _retire(org_id, actor, body.from_env, remote=body.retire_remote)
    return {
        "report": report.as_dict(),
        "applied": not body.dry_run,
        "requires_approval": protected,
        "retired": retired,
        "refreshed": refreshed,
        "squashed": squashed,
    }


async def _squash(
    org_id: str, source: str, target: str, actor: str | None, message: str
) -> str | None:
    """Record the merge as ONE commit on the target's branch (REQ-1545).

    A merge does not replay the source's history into the target. It copies a model by identity, so
    what the target gains is a single new state, and the honest projection of that is a single
    commit: the source's line FROM ITS CURRENT SHA BACK to where the two branches parted, collapsed
    into one. The message names the source and the sha it was at, which is the whole provenance a
    squash can carry and the only thing a reader later needs to find the range it stood for.

    The source's own branch is untouched. Its commits stay where they are and stay deployable by
    sha, so the squash adds a state to the target rather than rewriting anything.

    THE OPERATOR'S COMMENT LEADS (REQ-1550) and the provenance follows it in parentheses. A reader
    scanning the target's log wants to know what the merge DID; the source and the sha it was at
    are how they find the range afterwards, and neither sentence can be written by the other.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_repo import write_through
    from provisa.core.environments import org_schema

    source_row = await _known(org_id, source)
    at = source_row["deployed_sha"]
    provenance = f"merge {source} into {target}" if at is None else f"merge {source}@{at[:12]}"
    subject = f"{message.strip()} ({provenance})"
    db = await _org_tenant_db(org_id)
    async with db.acquire() as conn:
        return await write_through(
            conn, _admin_pool(), org_id, target, org_schema(org_id, target), subject, actor
        )


async def _refresh(org_id: str, env: str, *, connectivity: bool) -> str:
    """Bring the environment's cached runtime back in step with the model just written (REQ-1544).

    A deploy changes rows; a runtime built from the old ones keeps serving them, so a refresh is
    not an optimisation here but the thing that makes the deploy visible. What varies is HOW MUCH
    of the runtime has to go, and the DELTA decides it rather than the kind of call — an undo, a
    merge and a pipeline deploy that change the same paths refresh identically.

    Nothing cached is nothing to refresh: the next request builds the runtime from the rows this
    deploy just wrote, which is already the newest state.
    """
    from provisa.api.org_runtime import (
        reset_current_env,
        reset_current_org,
        runtime_key,
        set_current_env,
        set_current_org,
    )

    registry = _state().org_registry
    key = runtime_key(org_id, env)
    if registry.get(key) is None:
        return "uncached"
    if connectivity:
        registry.invalidate(key)
        return "rebuilt"

    # The model changed and the connections did not, so the pools this runtime holds are still
    # pools to the right databases. Recompiling in place keeps them open, which is what makes
    # moving along a branch's history (REQ-1543) cost a compile rather than a reconnect.
    from provisa.api.app import _rebuild_schemas

    org_token = set_current_org(org_id)
    env_token = set_current_env(env)
    try:
        await _rebuild_schemas()
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)
    return "recompiled"


async def _retire(org_id: str, actor: str | None, name: str, *, remote: bool = False) -> dict:
    """End an environment because a merge said its work is done (REQ-1542).

    Runs AFTER the merge applied, never before: the target has the model, and only then is there
    nothing left in the source that is not somewhere else. The branch goes with it, because an
    environment and its branch are one thing — the commits stay in the object store and stay
    reachable by sha, so this removes a name and not a history.

    THE REMOTE BRANCH IS THE SEPARATE ASK (REQ-1549). Local retirement leaves the org's git host
    holding the branch, which is the copy that survives a lost volume; deleting it too is what a
    finished feature says explicitly, and it happens only after the local retirement succeeded.
    """
    try:
        outcome = await retire_environment(_pool(), _admin_pool(), org_id, name, drop_branch=True)
    except RetirementError as exc:
        raise ApiError(409, "environments.prod_immutable", str(exc), org=org_id, env=name) from exc
    remote_deleted = None
    if remote:
        url = await _remote_of(org_id)
        try:
            await _remotely(org_id, env_ci.delete_remote_branch, org_id, name, url, user_id=actor)
        except _remote_failures() as exc:
            raise ApiError(
                400, "environments.remote_unwritable", str(exc), org=org_id, env=name
            ) from exc
        _forget_pushed(org_id, name)
        remote_deleted = True
    outcome = {**outcome, "remote_branch_deleted": remote_deleted}
    await _audit(org_id, actor, "environment.retired", name, outcome)
    return outcome


@router.post("/{name}/deploy")
async def deploy_into_environment(
    request: Request, org_id: str, name: str, body: DeployBody
) -> dict:
    """Make the tree at ``ref`` this environment's model (REQ-1496).

    THIS IS WHERE A BUILD BECOMES A MODEL. Everything before it -- a commit, a merged branch, a
    green pipeline -- is a proposal; nothing in Provisa applies a tree to an environment except a
    call to this endpoint, carrying an identity, naming the environment that will hold the result.
    There is no watcher and no webhook that does it on noticing a commit, which is what keeps the
    repository a projection rather than an authority (REQ-1524) and keeps the reviewers of a pull
    request from having applied a model they only read as text.

    A pipeline reaches the same door through ``provisa env deploy``: a machine may deploy, because a
    deploy is an INVOCATION against a NAMED control plane and the credential it carries is the org
    delegating its own standing. What does not exist is the other thing -- something inside this
    deployment that turns somebody else's merge into a change here.

    The ref is resolved to a SHA before anything is planned, and the sha is what is loaded: a
    branch that moves between the plan and the apply would otherwise make the report describe a
    commit that was never applied.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_files import load as load_files
    from provisa.core.env_repo import files_at, resolve_sha
    from provisa.core.env_store import set_position

    actor = await _guard_within(request, org_id, name)
    await _known(org_id, name)
    sha = _readable(org_id, body.ref, lambda: resolve_sha(org_id, body.ref))
    tree = load_files(_readable(org_id, body.ref, lambda: files_at(org_id, sha)))
    db = await _org_tenant_db(org_id)
    protected = await env_approvals.is_protected(
        _admin_pool(), org_id, name, await _member_count(org_id)
    )
    try:
        if protected and not body.dry_run:
            deploy_request = await env_approvals.request_deploy(
                _admin_pool(),
                db,
                org_id,
                ref=body.ref,
                sha=sha,
                tree=tree,
                target_env=name,
                requested_by=actor or "anonymous",
                message=body.message,
                seed=body.seed,
            )
            await _audit(
                org_id,
                actor,
                "environment.deploy_requested",
                name,
                {"request_id": deploy_request["id"], **deploy_request["report"]},
            )
            return {
                "request": _rendered(deploy_request),
                "applied": False,
                "requires_approval": True,
            }

        run = plan_deploy if body.dry_run else deploy_tree
        report = await run(db, org_id, name, tree, ref=sha, seed=body.seed)
    except DeployError as exc:
        # REQ-1496: a tree that does not hold is refused WHOLE. Nothing partial has landed -- the
        # decomposition raises before a statement is issued -- so the environment is untouched.
        raise ApiError(
            422, "environments.tree_does_not_hold", str(exc), org=org_id, env=name, ref=body.ref
        ) from exc
    refreshed = None
    if not body.dry_run:
        await _audit(org_id, actor, "environment.deploy", name, report.as_dict())
        # REQ-1543: a deploy is where the environment now IS, and it ends any run of undos --
        # applying a chosen tree is the environment choosing a future, not resuming the old one.
        await set_position(_admin_pool(), org_id, name, deployed_sha=sha, redo_sha=None)
        refreshed = await _refresh(org_id, name, connectivity=report.delta.touches_connectivity)
    return {
        "report": report.as_dict(),
        "applied": not body.dry_run,
        "requires_approval": protected,
        "refreshed": refreshed,
    }


async def _move(request: Request, org_id: str, name: str, forward: bool) -> dict:
    """Move the environment one commit along its own history (REQ-1543).

    UNDO AND REDO ARE THE SAME ACT in opposite directions, so they are one function: both resolve a
    neighbouring commit, apply its tree through the ORDINARY deploy of REQ-1496 -- same transaction,
    same validation, same audit -- and then move the cursor. There is no second path that writes a
    model, which is what keeps an environment from ever holding a tree no commit describes.

    NOTHING IS LOST EITHER WAY. Moving back does not remove a commit; it adds a position. The tree
    an undo stepped away from stays in the object store and stays deployable by sha even after the
    cursor that named it is cleared.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_files import load as load_files
    from provisa.core.env_repo import files_at, parent_of, step_toward
    from provisa.core.env_store import set_position

    actor = await _guard_within(request, org_id, name)
    row = await _known(org_id, name)
    here = row["deployed_sha"]
    if here is None:
        raise ApiError(
            409,
            "environments.no_position",
            f"Environment {name!r} has no recorded position in its history yet.",
            org=org_id,
            env=name,
        )
    top = row["redo_sha"]
    if forward:
        if top is None:
            raise ApiError(
                409,
                "environments.nothing_to_redo",
                f"Environment {name!r} has not stepped back from anything.",
                org=org_id,
                env=name,
            )
        target = _readable(org_id, top, lambda: step_toward(org_id, here, top))
        if target is None:
            # The run of undos was abandoned by a later edit, so there is no line forward to walk.
            # The cursor is cleared rather than pointed at a commit off this environment's line.
            await set_position(_admin_pool(), org_id, name, deployed_sha=here, redo_sha=None)
            raise ApiError(
                409,
                "environments.redo_abandoned",
                f"Environment {name!r} was edited after stepping back, so there is nothing ahead.",
                org=org_id,
                env=name,
            )
        # Arriving back at the top ends the run: there is nothing further forward to remember.
        cursor = None if target == top else top
    else:
        target = _readable(org_id, here, lambda: parent_of(org_id, here))
        if target == row["origin_sha"]:
            # The floor: below it the commits belong to the environment this one was created from.
            target = None
        if target is None:
            raise ApiError(
                409,
                "environments.nothing_to_undo",
                f"Environment {name!r} is at the first commit of its history.",
                org=org_id,
                env=name,
            )
        # The top is remembered from the FIRST step back of a run, so a second undo does not
        # overwrite it with the position the first one left -- that is what makes redo multi-step.
        cursor = top or here

    tree = load_files(_readable(org_id, target, lambda: files_at(org_id, target)))
    db = await _org_tenant_db(org_id)
    try:
        report = await deploy_tree(db, org_id, name, tree, ref=target)
    except DeployError as exc:
        raise ApiError(
            422, "environments.tree_does_not_hold", str(exc), org=org_id, env=name, ref=target
        ) from exc
    await set_position(_admin_pool(), org_id, name, deployed_sha=target, redo_sha=cursor)
    action = "environment.redo" if forward else "environment.undo"
    await _audit(org_id, actor, action, name, {"from": here, "to": target, **report.as_dict()})
    refreshed = await _refresh(org_id, name, connectivity=report.delta.touches_connectivity)
    return {
        "report": report.as_dict(),
        "deployed_sha": target,
        "redo_sha": cursor,
        "refreshed": refreshed,
    }


@router.post("/{name}/undo")
async def undo_environment(request: Request, org_id: str, name: str) -> dict:
    """Put the environment back to the commit before the one it is at (REQ-1543)."""
    return await _move(request, org_id, name, forward=False)


@router.post("/{name}/redo")
async def redo_environment(request: Request, org_id: str, name: str) -> dict:
    """Step forward again toward the position the undo departed from (REQ-1543)."""
    return await _move(request, org_id, name, forward=True)


def _rendered(merge_request: dict, state: str | None = None) -> dict:
    """A request as an API reader sees it, with ``state`` overridden by the DERIVED one when the
    caller has computed it (REQ-1504). Staleness is never stored, so it can only arrive this way."""
    rendered = dict(merge_request)
    for column in ("requested_at", "decided_at", "applied_at"):
        value = rendered.get(column)
        if value is not None:
            rendered[column] = value.isoformat()
    if state is not None:
        rendered["state"] = state
    return rendered


@router.get("/-/merge-requests")
async def list_merge_requests(request: Request, org_id: str, open_only: bool = False) -> dict:
    """Every proposed merge, each carrying the state it actually has right now (REQ-1504).

    The path segment is ``-`` because an environment cannot be named that (REQ-1523's name rules),
    so this route can never be shadowed by a real environment.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db

    await _member(request, org_id, MANAGE_CAPABILITY, SWITCH_CAPABILITY)
    db = await _org_tenant_db(org_id)
    rows = await env_approvals.list_requests(_admin_pool(), org_id, open_only=open_only)
    return {
        "requests": [
            _rendered(row, await env_approvals.effective_state(db, org_id, row)) for row in rows
        ]
    }


@router.get("/-/merge-requests/{request_id}")
async def get_merge_request(request: Request, org_id: str, request_id: int) -> dict:
    """One request, with the report as it was produced — which is what the approver reviews."""
    from provisa.api.admin.orgs_router import _org_tenant_db

    await _member(request, org_id, MANAGE_CAPABILITY, SWITCH_CAPABILITY)
    row = await _request_or_404(org_id, request_id)
    state = await env_approvals.effective_state(await _org_tenant_db(org_id), org_id, row)
    return {"request": _rendered(row, state)}


@router.post("/-/merge-requests/{request_id}/decide")
async def decide_merge_request(
    request: Request, org_id: str, request_id: int, body: DecideBody
) -> dict:
    """Approve or reject, and on approval apply exactly the merge that was reviewed (REQ-1504).

    Deciding is an org_admin act and the requester is refused their own request — the two together
    are what makes the approval a second person's, which is the whole of what protection means.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db

    actor = await _guard(request, org_id)
    await _request_or_404(org_id, request_id)
    try:
        decided = await env_approvals.decide(
            _admin_pool(),
            await _org_tenant_db(org_id),
            org_id,
            request_id,
            approve=body.approve,
            decided_by=actor or "anonymous",
            note=body.note,
        )
    except env_approvals.MergeRequestError as exc:
        raise ApiError(
            409, "environments.merge_request_undecidable", str(exc), org=org_id, request=request_id
        ) from exc
    action = "environment.merge_approved" if body.approve else "environment.merge_rejected"
    await _audit(
        org_id,
        actor,
        action,
        decided["target_env"],
        {"request_id": request_id, "from": decided["source_env"], **decided["report"]},
    )
    # REQ-1542: the retirement was part of what was proposed and part of what was approved, so it
    # happens on the approval and not on the request — a request that ended its own environment
    # while waiting would destroy the thing the approver was still deciding about.
    refreshed = None
    retired = None
    squashed = None
    if decided["state"] == "applied":
        if decided["source_ref"] is None:
            # REQ-1545: an approved merge lands the same single commit a direct merge lands. What
            # was approved is the state, not a history, so the target's branch gains one squash.
            squashed = await _squash(
                org_id,
                decided["source_env"],
                decided["target_env"],
                actor,
                # REQ-1550: the REQUESTER's comment, carried on the request row since it was made.
                # An approver decides whether the work lands, not what it is called.
                decided["message"],
            )
        else:
            # REQ-1543: an approved deploy applied the tree at that sha, so that is where the
            # environment now is, and it ends any run of undos exactly as a direct deploy does.
            from provisa.core.env_store import set_position

            await set_position(
                _admin_pool(),
                org_id,
                decided["target_env"],
                deployed_sha=decided["source_sha"],
                redo_sha=None,
            )
        # REQ-1544: an approved request is applied by ``env_approvals``, so the target's cached
        # runtime is stale the moment the approval returns exactly as it is after a direct call.
        refreshed = await _refresh(
            org_id,
            decided["target_env"],
            connectivity=report_touches_connectivity(decided["report"]),
        )
        if decided["retire_source"]:
            retired = await _retire(
                org_id, actor, decided["source_env"], remote=decided["retire_remote"]
            )
    return {
        "request": _rendered(decided),
        "retired": retired,
        "refreshed": refreshed,
        "squashed": squashed,
    }


@router.get("/{name}/merge-preview")
async def preview_merge(
    request: Request, org_id: str, name: str, from_env: str, removals: bool = False
) -> dict:
    """What merging ``from_env`` into this environment WOULD do, applying none of it (REQ-1527).

    This is the gate a pipeline asserts against, and it is a GET for a reason: the same answer is
    reachable through ``POST /merge`` with ``dry_run``, but a CI job that got that flag wrong would
    APPLY the merge it meant to inspect. A method that cannot write is the one to hand a runner.

    It runs ``plan_copy`` -- the same code path the merge itself runs, with ``apply`` off -- so a
    check that passes describes the merge that follows rather than an approximation of it. The
    answer is advisory to Provisa: REQ-1504's approval is what actually holds a merge, and a
    deployment whose git host is unreachable still requests, approves and applies exactly as it did.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db

    await _guard_within(request, org_id, from_env)
    await _known(org_id, name)
    await _known(org_id, from_env)
    if from_env == name:
        raise ApiError(
            400,
            "environments.same_environment",
            "An environment cannot be merged into itself.",
            org=org_id,
            env=name,
        )
    db = await _org_tenant_db(org_id)
    report = await plan_copy(db, org_id, from_env, name, mode=MERGE, removals=removals)
    return {
        "report": report.as_dict(),
        "applied": False,
        "requires_approval": await env_approvals.is_protected(
            _admin_pool(), org_id, name, await _member_count(org_id)
        ),
    }


@router.get("/-/repo-integration")
async def get_repo_integration(request: Request, org_id: str) -> dict:
    """Where the org's projection is mirrored and where its status is reported (REQ-1527)."""
    await _guard(request, org_id)
    integration = await env_ci.read_integration(_admin_pool(), org_id)
    return {
        "remote": integration.remote,
        "status_webhook": integration.status_webhook,
        "configured": integration.configured,
    }


@router.put("/-/repo-integration")
async def set_repo_integration(request: Request, org_id: str, body: RepoIntegrationBody) -> dict:
    """Set both halves; ``null`` clears one (REQ-1527).

    Written whole rather than patched: the two are what the org's pipeline is wired to, and an
    org that means to stop mirroring says so by sending a remote of ``null``.

    The remote is stored VERBATIM, secret references included, so a token never lands in the
    control plane (REQ-125, REQ-1525). It is resolved at push time and nowhere else, which is also
    why the value read back is the reference rather than the URL a push actually used.
    """
    actor = await _guard(request, org_id)
    integration = await env_ci.write_integration(
        _admin_pool(), org_id, remote=body.remote, status_webhook=body.status_webhook
    )
    await _audit(
        org_id,
        actor,
        "environment.repo_integration",
        "-",
        {"remote": integration.remote, "status_webhook": integration.status_webhook},
    )
    return {
        "remote": integration.remote,
        "status_webhook": integration.status_webhook,
        "configured": integration.configured,
    }


@router.post("/-/repo-integration/probe")
async def probe_repo_remote(request: Request, org_id: str, body: RemoteProbeBody) -> dict:
    """Does the configured (or proposed) remote name a repository that exists (REQ-1537)?

    Read-only, and deliberately so: the answer decides what the operator is ASKED next, and a probe
    that created what it failed to find would make the question rhetorical.

    The remote is resolved through the secrets provider inside ``env_remote`` exactly as a push
    resolves it (REQ-125), and nothing derived from the resolved URL comes back through this door —
    the probe redacts userinfo before it builds the line a browser will show.
    """
    await _guard(request, org_id)
    remote = body.remote
    if remote is None:
        integration = await env_ci.read_integration(_admin_pool(), org_id)
        remote = integration.remote
    if not remote:
        raise ApiError(
            400,
            "environments.no_remote",
            "This organization has no repository remote configured.",
            org=org_id,
        )
    try:
        probe = await _remotely(org_id, env_remote.probe, remote, user_id=_caller_user_id(request))
    except (env_remote.RemoteError, KeyError, ValueError) as exc:
        # KeyError is an unset secret reference and ValueError an unknown provider — both are the
        # operator's own configuration answering, so both are 400s that say what is wrong.
        raise ApiError(400, "environments.remote_unreadable", str(exc), org=org_id) from exc
    return probe.as_dict()


@router.post("/-/repo-integration/create-remote")
async def create_repo_remote(request: Request, org_id: str, body: RemoteCreateBody) -> dict:
    """Create the repository the remote names, because an operator asked for it (REQ-1537).

    Provisa never reaches this on its own. A missing repository is as likely to be a typo as an
    omission, and creating one unasked would leave an org mirroring into an address nobody meant
    while the intended repository stayed empty — so the probe reports, a person answers, and this
    endpoint is what their answer calls.

    Audited like any other write to the org's integration: this creates a repository in somebody
    else's namespace, with a credential the org supplied, and the record of who asked belongs with
    the record of who wired the remote up.
    """
    actor = await _guard(request, org_id)
    try:
        probe = await _remotely(
            org_id,
            env_remote.create,
            body.remote,
            private=body.private,
            user_id=_caller_user_id(request),
        )
    except (env_remote.RemoteError, KeyError, ValueError) as exc:
        raise ApiError(400, "environments.remote_not_created", str(exc), org=org_id) from exc
    await _audit(
        org_id,
        actor,
        "environment.repo_remote_created",
        "-",
        {"kind": probe.kind, "target": probe.target, "private": body.private},
    )
    return probe.as_dict()


#: What a remote can answer with that is somebody's INPUT rather than a broken server (REQ-1546).
#: The remote refusing the credential, hanging up, not being a repository; and the operator's own
#: configuration answering -- ``KeyError`` is an unset secret reference, ``ValueError`` an unknown
#: provider. Every one of these is a 400 that says what is wrong.
def _remote_failures() -> tuple[type[BaseException], ...]:
    from dulwich.client import HTTPUnauthorized
    from dulwich.errors import GitProtocolError, HangupException, NotGitRepository

    return (
        GitProtocolError,
        HangupException,
        HTTPUnauthorized,
        NotGitRepository,
        KeyError,
        ValueError,
        OSError,
    )


async def _remotely(org_id: str, call, *args, user_id: str | None, **kwargs):
    """Run a git call that will resolve the remote's secret references, with the org bound.

    A remote is stored VERBATIM, references and all (REQ-1525), and resolved only inside the call
    that actually uses it. The org whose secrets those references name therefore has to be bound
    AROUND that call rather than at the point of substitution, where there is no org and no
    connection to read one with (REQ-1557).

    REQ-1560: the ACTING person is bound too, so a remote written as ``${user:GIT_TOKEN}`` pushes
    under the credential of whoever is pushing. That is the point of the personal vault -- a
    commit lands as the person who made it, and one member cannot push as another because there
    is no reference that names another person's token.
    """
    from provisa.core import secrets_store

    async with secrets_store.bound(_admin_pool(), org_id, user_id=user_id):
        return await asyncio.to_thread(call, *args, **kwargs)


async def _remote_of(org_id: str) -> str:
    """The org's configured remote, or a 400 saying there is none to act against."""
    integration = await env_ci.read_integration(_admin_pool(), org_id)
    if not integration.remote:
        raise ApiError(
            400,
            "environments.no_remote",
            "This organization has no repository remote configured.",
            org=org_id,
        )
    return integration.remote


def _track_pushed(org_id: str, env: str, sha: str) -> None:
    """Record that the remote now holds ``sha`` on ``env``, so the status stops saying unsynced.

    A push that returned is a push the remote accepted, so the tracking ref is written from what we
    sent rather than from a second round trip. This is the ONE place a tracking ref is written
    outside a fetch, and it still never touches ``refs/heads``: what the remote holds and what an
    environment holds stay separate namespaces (REQ-1541).
    """
    from dulwich.objects import ObjectID

    from provisa.core.env_repo import ensure_repo, remote_ref

    ensure_repo(org_id).refs[remote_ref(env)] = ObjectID(sha.encode())


def _forget_pushed(org_id: str, env: str) -> None:
    """Drop the tracking ref for a branch that is gone from the remote, so no status claims it."""
    from provisa.core.env_repo import ensure_repo, remote_ref

    refs = ensure_repo(org_id).refs
    ref = remote_ref(env)
    if ref in refs.as_dict():
        del refs[ref]


@router.get("/-/repo-integration/sync")
async def repo_sync_state(request: Request, org_id: str) -> dict:
    """WHICH BRANCHES HOLD WORK THE REMOTE DOES NOT (REQ-1546).

    Answered from refs alone, so rendering the badge never dials another organization's git host
    with that organization's credential. ``behind`` is therefore as of the last FETCH, which is the
    same honesty the remote-branch list has: this is what the control plane KNOWS, not what the
    host holds this second.
    """
    from provisa.core.env_repo import branches, remote_branches, sync_state

    # REQ-1552: read by MEMBERS, not just org_admins. A member owning an environment can push it,
    # and somebody who can push has to be able to see that a push is owed -- an admin-only badge
    # would leave the person who made the change as the one person who cannot see its state.
    await _member(request, org_id, MANAGE_CAPABILITY, SWITCH_CAPABILITY)
    names = sorted({*branches(org_id), *remote_branches(org_id)})
    integration = await env_ci.read_integration(_admin_pool(), org_id)
    return {
        # REQ-1552: WHETHER THERE IS A REMOTE AT ALL travels with the counts, because "in sync" and
        # "mirrored nowhere" look identical otherwise. The URL itself is not here -- it carries
        # secret references and stays behind the org_admin read (REQ-1525).
        # A status webhook is not a mirror: `configured` is true for either, and the question here
        # is only whether the model reaches a remote at all.
        "remote_configured": integration.remote is not None,
        "branches": {name: sync_state(org_id, name) for name in names},
    }


@router.post("/{name}/push")
async def push_environment(request: Request, org_id: str, name: str) -> dict:
    """Send this environment's branch to the remote, on request (REQ-1546).

    The mirror after every commit is BEST EFFORT and says so: REQ-1527 has an unreachable remote
    not fail the edit that reached it, which means a branch can hold work the remote does not. This
    is the repair for exactly that, and the reason the answer to "is my work safe" is a state
    somebody can act on rather than a line in a log nobody reads.
    """
    from provisa.core.env_repo import sync_state, tip

    actor = await _guard_within(request, org_id, name)
    await _known(org_id, name)
    remote = await _remote_of(org_id)
    try:
        await _remotely(org_id, env_ci.push, org_id, name, remote, user_id=_caller_user_id(request))
    except _remote_failures() as exc:
        raise ApiError(
            400, "environments.remote_unwritable", str(exc), org=org_id, env=name
        ) from exc
    pushed = tip(org_id, name)
    if pushed is not None:
        _track_pushed(org_id, name, pushed)
    await _audit(org_id, actor, "environment.repo_push", name, {"sha": pushed})
    return {"pushed": pushed, "sync": sync_state(org_id, name)}


class ReviewBody(BaseModel):
    """REQ-1551: ask the git host to review this environment's branch."""

    # The branch to merge INTO. Absent means the one this environment was branched from
    # (REQ-1549), which is where its work belongs and the only target Provisa can name on its own.
    into: str | None = None
    # REQ-1550: required, exactly as on a merge, and for the same reason -- it is the account of
    # the work that the squashed history will not carry. Here it is also what the reviewers read.
    message: str = ""


@router.post("/{name}/review")
async def request_review(request: Request, org_id: str, name: str, body: ReviewBody) -> dict:
    """Open a pull request on the org's git host for this environment's branch (REQ-1551).

    THIS IS THE MERGE, ASKED FOR WHERE THE RULE LIVES. When the target branch is governed by pull
    requests on the host, an approval row inside Provisa decides nothing: the host will refuse the
    push that a local merge produces, and the people who would approve it are reviewing on the
    host. So the request is opened there, and what comes back is the link.

    The branch is PUSHED FIRST, in this call, because a host cannot review refs it has never
    received and a review request that found nothing to review would be reported as a host error
    rather than as the missing push it is.
    """
    from provisa.core.env_remote import RemoteError, open_pull_request
    from provisa.core.env_repo import sync_state, tip

    actor = await _guard_within(request, org_id, name)
    row = await _known(org_id, name)
    if not body.message.strip():
        raise ApiError(
            400,
            "environments.message_required",
            "A review request needs a comment: it is what the reviewers read.",
            org=org_id,
            env=name,
        )
    # REQ-1549: the branch it came from is the default target, and an environment that was created
    # without one has no answer here. That is refused rather than guessed -- proposing a merge into
    # prod because nothing else was recorded is not a default, it is an accident.
    target = body.into or row["branched_from"]
    if target is None:
        raise ApiError(
            400,
            "environments.no_merge_target",
            f"{name!r} records no environment it was branched from, so the branch to review it "
            "into has to be named.",
            org=org_id,
            env=name,
        )
    await _known(org_id, target)
    if target == name:
        raise ApiError(
            400,
            "environments.same_environment",
            "An environment cannot be merged into itself.",
            org=org_id,
            env=name,
        )
    remote = await _remote_of(org_id)
    try:
        await _remotely(org_id, env_ci.push, org_id, name, remote, user_id=_caller_user_id(request))
    except _remote_failures() as exc:
        raise ApiError(
            400, "environments.remote_unwritable", str(exc), org=org_id, env=name
        ) from exc
    pushed = tip(org_id, name)
    if pushed is not None:
        _track_pushed(org_id, name, pushed)
    try:
        pull_request = await asyncio.to_thread(
            open_pull_request,
            remote,
            head=name,
            base=target,
            title=f"{name} -> {target}",
            body=body.message.strip(),
        )
    except RemoteError as exc:
        raise ApiError(
            400, "environments.review_unavailable", str(exc), org=org_id, env=name
        ) from exc
    await _audit(
        org_id,
        actor,
        "environment.review_requested",
        name,
        {"into": target, "sha": pushed, **pull_request},
    )
    return {"pull_request": pull_request, "pushed": pushed, "sync": sync_state(org_id, name)}


@router.post("/{name}/pull")
async def pull_environment(request: Request, org_id: str, name: str) -> dict:
    """Take what the remote holds for this environment and MAKE IT THE MODEL (REQ-1547).

    A fetch brings the remote's branches in as tracking refs and moves nothing, on purpose: a
    branch under ``refs/heads`` is written by the write-through and by nothing else, so a fetch that
    landed there would let another system reposition an environment behind the control plane's back
    (REQ-1541). Updating local is therefore an APPLY of the fetched sha -- the ordinary REQ-1496
    path, same transaction, same validation, same audit -- and the branch moves because the
    write-through commits the model that apply produced, which is the only way a ref ever moves.

    Refused when the two lines have DIVERGED. Both sides then hold commits the other does not, and
    choosing whose work survives is not a decision this endpoint gets to make quietly.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_files import load as load_files
    from provisa.core.env_repo import files_at, merge_base, sync_state
    from provisa.core.env_store import set_position

    actor = await _guard_within(request, org_id, name)
    await _known(org_id, name)
    remote = await _remote_of(org_id)
    try:
        await _remotely(org_id, env_ci.fetch, org_id, remote, user_id=_caller_user_id(request))
    except _remote_failures() as exc:
        raise ApiError(400, "environments.remote_unreadable", str(exc), org=org_id) from exc
    state = sync_state(org_id, name)
    if state["remote"] is None:
        raise ApiError(
            404,
            "environments.no_remote_branch",
            f"The remote has no branch named {name!r}.",
            org=org_id,
            env=name,
        )
    # REQ-1556: the commit the branch and the fetched sha last shared. Both doors below are asked
    # the same question of it -- a refusal that names the objects both sides touched, and an apply
    # that names the ones it carried away -- so it is resolved once, before either is taken.
    base_sha = (
        merge_base(org_id, state["local"], state["remote"]) if state["local"] is not None else None
    )
    if state["diverged"]:
        # Named, not resolved. Whoever now has to decide whose work survives is deciding about
        # particular objects, and "the two lines diverged" is not a statement about any of them.
        collided = await _collisions(org_id, name, base_sha, state["remote"])
        raise ApiError(
            409,
            "environments.diverged",
            f"{name!r} and its remote branch both hold commits the other does not.",
            org=org_id,
            env=name,
            base=base_sha,
            conflicts=[c.as_dict() for c in collided],
        )
    if state["behind"] == 0:
        return {"applied": False, "sync": state}
    sha = state["remote"]
    tree = load_files(_readable(org_id, sha, lambda: files_at(org_id, sha)))
    db = await _org_tenant_db(org_id)
    try:
        report = await deploy_tree(db, org_id, name, tree, ref=sha, base_sha=base_sha)
    except DeployError as exc:
        raise ApiError(
            422, "environments.tree_does_not_hold", str(exc), org=org_id, env=name, ref=sha
        ) from exc
    await set_position(_admin_pool(), org_id, name, deployed_sha=sha, redo_sha=None)
    await _audit(org_id, actor, "environment.repo_pull", name, {"sha": sha, **report.as_dict()})
    refreshed = await _refresh(org_id, name, connectivity=report.delta.touches_connectivity)
    return {
        "applied": True,
        "report": report.as_dict(),
        "sync": sync_state(org_id, name),
        "refreshed": refreshed,
    }


@router.post("/-/repo-integration/fetch")
async def fetch_repo_remote(request: Request, org_id: str) -> dict:
    """Bring the org's remote branches into this repository, on request (REQ-1541).

    THIS IS HOW A REVIEW THAT HAPPENED ELSEWHERE GETS HERE. The projection is pushed to the org's
    own git host, the pull request is opened, reviewed and merged THERE, and this is the step that
    brings the merged branch back so a deploy can name it: ``origin/main`` is a ref in this
    repository once a fetch has run, and a ref nobody fetched before that.

    Deliberately a POST that a person or a pipeline calls, never a poller. Provisa dials another
    organization's git host with that organization's credential, and doing that on a timer would
    mean an unbounded number of authenticated calls nobody asked for; it would also make the answer
    to "what does origin/main point at" depend on when the last tick happened rather than on when
    somebody last asked.
    """
    actor = await _guard(request, org_id)
    remote = await _remote_of(org_id)
    try:
        fetched = await _remotely(
            org_id, env_ci.fetch, org_id, remote, user_id=_caller_user_id(request)
        )
    except _remote_failures() as exc:
        raise ApiError(400, "environments.remote_unreadable", str(exc), org=org_id) from exc
    await _audit(org_id, actor, "environment.repo_fetch", "-", {"branches": sorted(fetched)})
    return {"branches": fetched}


@router.get("/-/repo-integration/remote-branches")
async def list_remote_branches(request: Request, org_id: str) -> dict:
    """What the last fetch found, without dialling anything (REQ-1541).

    An empty answer means no fetch has run yet, not that the remote is empty — the two are told
    apart by running one.
    """
    from provisa.core.env_repo import remote_branches

    await _guard(request, org_id)
    return {"branches": remote_branches(org_id)}


async def _browse(request: Request, org_id: str, ref: str) -> None:
    """Who may read a ref of the org's repository (REQ-1524, REQ-1528).

    A ref that names an environment is guarded exactly as that environment is: its owner or an
    org_admin. A ref that names a SHA belongs to no environment — the sha of a deleted branch is
    still an object in the store — so there is no owner to derive an authority from and it is an
    org_admin act. Deriving it from the name rather than from a grant is what keeps a branch
    readable by the person who made it and by nobody else who was not already an administrator.
    """
    if await get_env(_admin_pool(), org_id, ref) is None:
        await _guard(request, org_id)
    else:
        await _guard_within(request, org_id, ref)


async def _collisions(org_id: str, name: str, base_sha: str | None, sha: str | None) -> list:
    """Which objects the environment and the incoming commit both moved since they parted.

    The refusal's half of REQ-1556. No apply follows it, so this reads on its own connection rather
    than inside one: what it reports is the state the person was refused against. ``base_sha`` is
    None when the two lines share no ancestor, and the empty list then says the question could not
    be asked rather than that nothing collided -- the response says which by carrying ``base``.
    """
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.env_conflicts import against_base
    from provisa.core.env_files import load as load_files
    from provisa.core.env_project import project
    from provisa.core.env_repo import files_at
    from provisa.core.environments import org_schema

    if base_sha is None or sha is None:
        return []
    incoming = load_files(_readable(org_id, sha, lambda: files_at(org_id, sha)))
    db = await _org_tenant_db(org_id)
    async with db.acquire() as conn:
        current = await project(conn, org_schema(org_id, name))
    return against_base(org_id, base_sha, incoming, current)


def _readable(org_id: str, ref: str, read):
    """Run one BROWSE verb, turning a ref this repository cannot resolve into a 404."""
    from provisa.core.env_repo import RepositoryError

    try:
        return read()
    except RepositoryError as exc:
        raise ApiError(404, "environments.unknown_ref", str(exc), org=org_id, ref=ref) from exc


@router.get("/-/repo/branches")
async def list_repo_branches(request: Request, org_id: str) -> dict:
    """Every branch in the org's one repository (REQ-1524).

    Not the same list as ``GET ""``: an environment is a branch, but a branch outlives the
    environment that wrote it — deleting an environment drops its schema and its row, and the ref
    stays, which is what makes an earlier state loadable afterwards. Reading them side by side is
    how a person finds the sha to branch from.
    """
    from provisa.core.env_repo import branches

    await _member(request, org_id, MANAGE_CAPABILITY, SWITCH_CAPABILITY)
    return {"branches": branches(org_id)}


@router.get("/-/repo/history")
async def repo_history(request: Request, org_id: str, ref: str, limit: int = 100) -> dict:
    """``ref``'s commits, newest first — the list DEPLOY picks a sha out of (REQ-1524)."""
    from provisa.core.env_repo import history

    await _browse(request, org_id, ref)
    return {"ref": ref, "commits": _readable(org_id, ref, lambda: history(org_id, ref, limit))}


@router.get("/-/repo/files")
async def repo_files(request: Request, org_id: str, ref: str) -> dict:
    """The PATHS in the tree at ``ref``, sorted, without their contents.

    Paths and text are separate doors because the tree is a whole model: a browser that wanted the
    file list would otherwise download every definition in the org to render a sidebar.
    """
    from provisa.core.env_repo import files_at

    await _browse(request, org_id, ref)
    files = _readable(org_id, ref, lambda: files_at(org_id, ref))
    return {"ref": ref, "paths": sorted(files)}


@router.get("/-/repo/file")
async def repo_file(request: Request, org_id: str, ref: str, path: str) -> dict:
    """One file's text at ``ref``. Two calls at two refs are what a diff view is built from."""
    from provisa.core.env_repo import files_at

    await _browse(request, org_id, ref)
    files = _readable(org_id, ref, lambda: files_at(org_id, ref))
    if path not in files:
        raise ApiError(
            404,
            "environments.unknown_path",
            f"{path!r} is not in the tree at {ref!r}.",
            org=org_id,
            ref=ref,
            path=path,
        )
    return {"ref": ref, "path": path, "text": files[path]}


async def _request_or_404(org_id: str, request_id: int) -> dict:
    row = await env_approvals.get_request(_admin_pool(), org_id, request_id)
    if row is None:
        raise ApiError(
            404,
            "environments.unknown_merge_request",
            f"Organization {org_id!r} has no merge request {request_id}.",
            org=org_id,
            request=request_id,
        )
    return row
