# Copyright (c) 2026 Kenneth Stott
# Canary: 0c93c0cf-7457-4eac-8b65-5dd76bb2a14a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A merge held open until someone other than its requester has read it (REQ-1504).

A merge request is a ROW and not a confirmation dialog. The approver is by definition a different
person from the requester, so they are not present at the moment of the request; an ephemeral
confirmation would force the approval to happen inside the requester's own session, which is the
one arrangement the requirement forbids.

WHAT THE APPROVER READS is the REQ-1490 report as it was produced, stored on the row, together with
the requester's own message. Those two are the review: the report IS the squash, at object
granularity instead of line granularity. It is stored rather than recomputed because staleness is
only decidable against the report the approver actually saw — recomputing at approval time would
silently approve a different merge than the one that was reviewed.

STALENESS IS DERIVED, never written. A request goes stale by something happening ELSEWHERE — the
source environment moving on — and nothing observes that at the moment it happens. Re-planning at
read time and comparing against the stored report is the only version that cannot be wrong; a
``stale`` state column would be a claim that some writer somewhere remembered to update.
"""

# Requirements: REQ-1490, REQ-1496, REQ-1504, REQ-1524, REQ-1539

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from provisa.core.env_copy import MERGE, copy_model, plan_copy
from provisa.core.env_deploy import deploy_tree, plan_deploy
from provisa.core.environments import PROD
from provisa.core.schema_admin import env_merge_requests, environments

if TYPE_CHECKING:
    from provisa.core.database import Database

REQUESTED = "requested"
APPROVED = "approved"
REJECTED = "rejected"
APPLIED = "applied"

#: Derived at read time, never stored: the source has moved on since the report was produced.
STALE = "stale"


class MergeRequestError(Exception):
    """A merge request that cannot proceed, carrying the reason it cannot."""


def _is_deploy(request: dict) -> bool:
    """Whether the row describes a DEPLOY of a tree rather than a merge between environments.

    Read off which source the row names. The table's CHECK makes exactly one of them present, so
    this is a fact about the row rather than a guess.
    """
    return request["source_ref"] is not None


def _tree_at(org_id: str, sha: str) -> dict[str, dict[str, Any]]:
    """The model the pinned commit holds, parsed. Read from the repository each time rather than
    stored on the row: the commit is immutable, so re-reading it cannot produce a different tree,
    and a second copy of a whole model on an approval row would be the same bytes twice."""
    from provisa.core.env_files import load as load_files
    from provisa.core.env_repo import files_at

    return load_files(files_at(org_id, sha))


async def is_protected(admin_db: "Database", org_id: str, name: str, member_count: int) -> bool:
    """Whether a merge into ``name`` waits for an approval (REQ-1504).

    prod is protected once the org has more than one member — a second member is exactly the
    condition that makes "someone other than the requester" a person who exists. A single-member
    org cannot satisfy the rule at all, so applying it there would not protect prod; it would make
    prod unmergeable. Any environment is additionally protected when an org_admin says so.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(environments.c.protected).where(
                environments.c.org_id == org_id, environments.c.name == name
            )
        )
        row = result.fetchone()
    if row is None:
        raise MergeRequestError(f"organization {org_id!r} has no environment {name!r}")
    return bool(row[0]) or (name == PROD and member_count > 1)


async def request_merge(
    admin_db: "Database",
    tenant_db: "Database",
    org_id: str,
    *,
    source_env: str,
    target_env: str,
    requested_by: str,
    message: str = "",
    removals: bool = False,
    retire_source: bool = False,
    retire_remote: bool = False,
) -> dict:
    """Propose a merge and store what it would do, for someone else to read (REQ-1504).

    The direction is the requester's: FROM the environment they hold INTO the protected one.
    Nothing here requires the requester to hold rights over the target — proposing is not writing,
    and the approval is the write.

    ``retire_source`` rides along and is NOT acted on here (REQ-1542). Retiring an environment
    drops schemas and Redis users, which is provisioning rather than approval; the caller that
    applies the approved merge performs it, and this row is the record that it was approved.
    """
    if source_env == target_env:
        raise MergeRequestError(f"an environment cannot be merged into itself ({source_env})")
    report = await plan_copy(
        tenant_db, org_id, source_env, target_env, mode=MERGE, removals=removals
    )
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            env_merge_requests.insert()
            .values(
                org_id=org_id,
                source_env=source_env,
                target_env=target_env,
                requested_by=requested_by,
                message=message,
                report=report.as_dict(),
                removals=removals,
                retire_source=retire_source,
                retire_remote=retire_remote,
                state=REQUESTED,
            )
            .returning(env_merge_requests.c.id)
        )
        request_id = int(result.fetchone()[0])
    fetched = await get_request(admin_db, org_id, request_id)
    assert fetched is not None  # just inserted in this transaction's wake
    return fetched


async def request_deploy(
    admin_db: "Database",
    tenant_db: "Database",
    org_id: str,
    *,
    ref: str,
    sha: str,
    tree: dict[str, dict[str, Any]],
    target_env: str,
    requested_by: str,
    message: str = "",
    seed: bool = False,
) -> dict:
    """Propose a DEPLOY of one tree into a protected environment (REQ-1496, REQ-1504).

    The SHA is pinned here and applied later, never the ref: what an approver reads is the report
    of one tree, and a branch that moves between the request and the decision is a different tree
    that nobody has read. Re-pointing the request at the moved branch would be approving by name.
    """
    report = await plan_deploy(tenant_db, org_id, target_env, tree, ref=sha, seed=seed)
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            env_merge_requests.insert()
            .values(
                org_id=org_id,
                source_ref=ref,
                source_sha=sha,
                target_env=target_env,
                requested_by=requested_by,
                message=message,
                report=report.as_dict(),
                seed=seed,
                state=REQUESTED,
            )
            .returning(env_merge_requests.c.id)
        )
        request_id = int(result.fetchone()[0])
    fetched = await get_request(admin_db, org_id, request_id)
    assert fetched is not None  # just inserted in this transaction's wake
    return fetched


async def list_requests(
    admin_db: "Database", org_id: str, *, open_only: bool = False
) -> list[dict]:
    """Every merge request the org holds, newest first."""
    query = select(env_merge_requests).where(env_merge_requests.c.org_id == org_id)
    if open_only:
        query = query.where(env_merge_requests.c.state.in_([REQUESTED, APPROVED]))
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(query.order_by(env_merge_requests.c.id.desc()))
        return [dict(r._mapping) for r in result.fetchall()]


async def get_request(admin_db: "Database", org_id: str, request_id: int) -> dict | None:
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(env_merge_requests).where(
                env_merge_requests.c.org_id == org_id, env_merge_requests.c.id == request_id
            )
        )
        row = result.fetchone()
        return dict(row._mapping) if row is not None else None


async def effective_state(tenant_db: "Database", org_id: str, request: dict) -> str:
    """The request's state including the one that is derived rather than stored.

    A decided request is never stale: rejection is final, and an applied merge already happened.
    Only a request still waiting can be overtaken by its own source.
    """
    if request["state"] not in (REQUESTED, APPROVED):
        return str(request["state"])
    if _is_deploy(request):
        # The tree is pinned to a sha, so what can have moved is the TARGET. Re-planning against
        # the same tree is what says whether the report still describes the deploy.
        current = await plan_deploy(
            tenant_db,
            org_id,
            request["target_env"],
            _tree_at(org_id, request["source_sha"]),
            ref=request["source_sha"],
            seed=bool(request["seed"]),
        )
    else:
        current = await plan_copy(
            tenant_db,
            org_id,
            request["source_env"],
            request["target_env"],
            mode=MERGE,
            removals=bool(request["removals"]),
        )
    return str(request["state"]) if current.as_dict() == request["report"] else STALE


async def decide(
    admin_db: "Database",
    tenant_db: "Database",
    org_id: str,
    request_id: int,
    *,
    approve: bool,
    decided_by: str,
    note: str | None = None,
) -> dict:
    """Approve or reject a request, and on approval apply exactly what it described (REQ-1504)."""
    request = await get_request(admin_db, org_id, request_id)
    if request is None:
        raise MergeRequestError(f"organization {org_id!r} has no merge request {request_id}")
    if request["state"] != REQUESTED:
        raise MergeRequestError(
            f"merge request {request_id} is {request['state']}, and only a requested merge can be "
            "decided"
        )
    if decided_by == request["requested_by"]:
        raise MergeRequestError(
            "a merge into a protected environment waits for an approval by someone other than the "
            "person who requested it"
        )
    now = datetime.now(UTC)
    if not approve:
        await _set(
            admin_db,
            request_id,
            state=REJECTED,
            decided_by=decided_by,
            decided_at=now,
            decision_note=note,
        )
        result = await get_request(admin_db, org_id, request_id)
        assert result is not None
        return result

    if await effective_state(tenant_db, org_id, request) == STALE:
        moved = request["target_env"] if _is_deploy(request) else request["source_env"]
        raise MergeRequestError(
            f"merge request {request_id} no longer describes the change it would perform — "
            f"{moved!r} has changed since it was requested. Request it again against a report "
            "that describes what would happen now."
        )
    if _is_deploy(request):
        applied = await deploy_tree(
            tenant_db,
            org_id,
            request["target_env"],
            _tree_at(org_id, request["source_sha"]),
            ref=request["source_sha"],
            seed=bool(request["seed"]),
        )
    else:
        applied = await copy_model(
            tenant_db,
            org_id,
            request["source_env"],
            request["target_env"],
            mode=MERGE,
            removals=bool(request["removals"]),
        )
    await _set(
        admin_db,
        request_id,
        state=APPLIED,
        decided_by=decided_by,
        decided_at=now,
        decision_note=note,
        applied_at=now,
        report=applied.as_dict(),
    )
    result = await get_request(admin_db, org_id, request_id)
    assert result is not None
    return result


async def _set(admin_db: "Database", request_id: int, **values: Any) -> None:
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            update(env_merge_requests).where(env_merge_requests.c.id == request_id).values(**values)
        )
