# Copyright (c) 2026 Kenneth Stott
# Canary: 8c5be27a-14d9-4f60-b3e1-90a7d6c4fe25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Retiring an environment: its schemas, its registry row, and optionally its branch (REQ-1542).

WHY THIS IS NOT JUST THE DELETE ENDPOINT'S BODY. A merge that retires the environment it came from
has to do exactly what a delete does -- a half-retired environment holding schemas with no registry
row is worse than either state -- and a merge is decided in one place while a delete is called from
another. One function, called from both, is what keeps them the same act.

WHY THE BRANCH IS OPTIONAL. Deleting an environment through the delete door leaves its branch,
because the branch is the record of what that environment held and a person deleting a schema has
not asked to lose the history. A merge that retires its source is the other case: the work landed
in the target, the feature is over, and leaving the ref behind leaves a branch nobody writes and
nobody reads. So the caller says which one this is, and neither guesses.

WHAT IS NEVER LOST. Deleting a ref deletes a NAME. The commits remain in the object store and
remain reachable by sha, so a retired branch is still browsable and still deployable by anybody who
kept one -- retiring is tidying, not destruction.

WHAT STANDS IN THE WAY (REQ-1918). An environment is retired only when nothing still refers to
it: a membership pinned to it, an invitation that can still seat or deploy a redeemer from it, an
environment branched from it. Each one blocks and is named; nothing is removed. The inventory
below says which, and says the one exception: an environment minted for a visitor takes the
memberships pinned to it WITH it, because a visitor's account exists only inside that environment.
"""

# Requirements: REQ-1542, REQ-1524, REQ-1488, REQ-1487, REQ-1620, REQ-1622, REQ-1918, REQ-1596

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, or_, select

from provisa.core.env_repo import delete_branch
from provisa.core.env_source_files import discard_file_sources
from provisa.core.env_store import forget_env
from provisa.core.environments import PROD
from provisa.core.org_invite import SANDBOX_ENV_PREFIX
from provisa.core.schema_admin import (
    environments,
    org_invites,
    user_org_memberships,
    user_profiles,
)

if TYPE_CHECKING:
    from provisa.core.db import Database


class RetirementError(Exception):
    """An environment that may not be retired was named."""


#: The names of environments minted for ONE visitor: an invitation's per-visitor environment
#: (``org_invite.sandbox_env_name``) and the sandbox org's per-user one (``invite_env``).
VISITOR_ENV_PREFIXES = (SANDBOX_ENV_PREFIX, "ephemeral_")

PART = "part"
DEPENDENT = "dependent"


def is_visitor_environment(name: str) -> bool:
    """Whether ``name`` is an environment minted for one visitor (REQ-1595, REQ-1602)."""
    return name.startswith(VISITOR_ENV_PREFIXES)


@dataclass(frozen=True)
class EnvReference:
    """One column of the platform plane that names an environment of an org, and its standing:
    a DEPENDENT blocks the environment's retirement, a PART goes with it. ``visitor`` is the
    standing when the environment was minted for a visitor, ``otherwise`` for every other."""

    table: str
    column: str
    kind: str
    visitor: str
    otherwise: str


#: THE INVENTORY of what refers to an environment (REQ-1918). Plain data; ``env_dependents``
#: and ``_remove_parts`` read it and nothing else decides.
ENVIRONMENT_REFERENCES: tuple[EnvReference, ...] = (
    # A membership pinned to the environment (REQ-1596) is served by it and by no other. For an
    # ordinary environment that is a person who would be left with nowhere to be served: it
    # blocks. For a visitor's environment the account exists only inside it, so it is a PART.
    EnvReference(
        "user_org_memberships", "env_name", "membership", visitor=PART, otherwise=DEPENDENT
    ),
    # An invitation that names the environment seats its redeemers in it (shared) or deploys
    # each visitor's environment from it (per visitor): while it can still be redeemed, it blocks.
    EnvReference("org_invites", "env_name", "invitation", visitor=DEPENDENT, otherwise=DEPENDENT),
    # An environment created from this one resolves its inherited bindings through it (REQ-1529,
    # REQ-1942).
    EnvReference("environments", "parent", "environment", visitor=DEPENDENT, otherwise=DEPENDENT),
)


@dataclass(frozen=True)
class EnvDependent:
    """One thing that blocks an environment's retirement, as a refusal reports it."""

    kind: str
    id: Any
    name: str
    via: str

    def as_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "id": self.id, "name": self.name, "via": [self.via]}


class EnvironmentInUse(RetirementError):  # REQ-1918
    """An environment other things still refer to. ``dependents`` lists each; nothing was removed."""

    def __init__(self, org_id: str, name: str, dependents: list[EnvDependent]) -> None:
        self.org_id = org_id
        self.env = name
        self.dependents = dependents
        named = ", ".join(f"{d.kind} {d.name!r}" for d in dependents)
        super().__init__(f"Environment {name!r} is still referred to by: {named}")


def kinds_and_counts(dependents: list[EnvDependent]) -> dict[str, int]:
    """How many of each kind block the environment: what a log line and the environments page
    say about an expired environment that is kept, without naming anybody."""
    counts: dict[str, int] = {}
    for dependent in dependents:
        counts[dependent.kind] = counts.get(dependent.kind, 0) + 1
    return counts


def _standing(reference: EnvReference, name: str) -> str:
    return reference.visitor if is_visitor_environment(name) else reference.otherwise


async def env_dependents(admin_db: "Database", org_id: str, name: str) -> list[EnvDependent]:
    """What blocks retiring ``name``: every DEPENDENT reference the inventory lists."""
    found: list[EnvDependent] = []
    now = datetime.now(tz=timezone.utc)
    async with admin_db.acquire() as conn:
        for reference in ENVIRONMENT_REFERENCES:
            if _standing(reference, name) != DEPENDENT:
                continue
            via = f"{reference.table}.{reference.column}"
            if reference.table == "user_org_memberships":
                rows = await conn.execute_core(
                    select(user_org_memberships.c.user_id).where(
                        user_org_memberships.c.org_id == org_id,
                        user_org_memberships.c.env_name == name,
                    )
                )
                found.extend(EnvDependent(reference.kind, r[0], r[0], via) for r in rows.fetchall())
            elif reference.table == "org_invites":
                # Only an invitation that can still be redeemed: one that has expired or is used
                # up seats and deploys nobody.
                rows = await conn.execute_core(
                    select(
                        org_invites.c.token, org_invites.c.email, org_invites.c.env_policy
                    ).where(
                        org_invites.c.org_id == org_id,
                        org_invites.c.env_name == name,
                        org_invites.c.expires_at > now,
                        or_(
                            org_invites.c.max_uses.is_(None),
                            org_invites.c.uses < org_invites.c.max_uses,
                        ),
                    )
                )
                found.extend(
                    EnvDependent(
                        reference.kind,
                        # The token IS the invitation's secret: it is named by whom it is
                        # addressed to, or by its kind, never by the token.
                        None,
                        r[1] or f"{r[2]} link",
                        via,
                    )
                    for r in rows.fetchall()
                )
            else:
                rows = await conn.execute_core(
                    select(environments.c.name).where(
                        environments.c.org_id == org_id, environments.c.parent == name
                    )
                )
                found.extend(EnvDependent(reference.kind, r[0], r[0], via) for r in rows.fetchall())
    return found


async def _remove_parts(admin_db: "Database", org_id: str, name: str) -> list[str]:
    """Remove what goes WITH the environment: for a visitor's environment, the memberships pinned
    to it, their tokens for the org, and the profile of each person left with no membership
    anywhere (the visitor's account existed only here). Returns the user ids removed from the org.
    Their role assignments were inside the environment's own schema, which is already dropped."""
    from provisa.auth.pat import PersonalAccessTokenStore

    gone: list[str] = []
    for reference in ENVIRONMENT_REFERENCES:
        if _standing(reference, name) != PART:
            continue
        assert reference.table == "user_org_memberships", reference
        async with admin_db.acquire() as conn:
            rows = await conn.execute_core(
                select(user_org_memberships.c.user_id).where(
                    user_org_memberships.c.org_id == org_id,
                    user_org_memberships.c.env_name == name,
                )
            )
            pinned = sorted(r[0] for r in rows.fetchall())
        for user_id in pinned:
            await PersonalAccessTokenStore(admin_db).revoke_all_for_user_in_org(
                user_id=user_id, org_id=org_id
            )
            async with admin_db.acquire() as conn:
                await conn.execute_core(
                    delete(user_org_memberships).where(
                        user_org_memberships.c.user_id == user_id,
                        user_org_memberships.c.org_id == org_id,
                        user_org_memberships.c.env_name == name,
                    )
                )
                left = (
                    await conn.execute_core(
                        select(func.count())
                        .select_from(user_org_memberships)
                        .where(user_org_memberships.c.user_id == user_id)
                    )
                ).scalar_one()
                if left == 0:
                    await conn.execute_core(
                        delete(user_profiles).where(user_profiles.c.user_id == user_id)
                    )
            gone.append(user_id)
    return gone


async def retire_environment(
    pool: "Database",
    admin_db: "Database",
    org_id: str,
    name: str,
    *,
    drop_branch: bool,
) -> dict:
    """Drop ``name``'s stores and registry row, and its branch when asked. Returns what was done.

    ``prod`` is refused here rather than at each caller, because REQ-1487 makes it exist from the
    organization's creation: an org without a prod environment is not a state this platform has.

    REQ-1918: refused (:class:`EnvironmentInUse`) while anything still refers to the environment,
    naming each; nothing is removed. Here rather than at each caller for the same reason as prod:
    the delete door, a merge that retires its source and the expiry sweep are one act.
    """
    if name == PROD:
        raise RetirementError(
            f"{PROD!r} exists from the organization's creation and cannot be retired; delete the "
            "organization to remove it."
        )
    blocking = await env_dependents(admin_db, org_id, name)
    if blocking:
        raise EnvironmentInUse(org_id, name, blocking)
    from provisa.core.org_provisioning import deprovision_org

    from provisa.core.redis_location import redis_url

    await deprovision_org(pool, org_id, redis_url=redis_url(), env=name)
    # REQ-1620: the schemas are not everything the environment owned. An environment created with
    # its bindings carried was given its own copies of every file-backed source, and those live on
    # disk rather than in a schema -- so this is the door that removes them, before the registry row
    # that names the environment is forgotten.
    files_discarded = discard_file_sources(org_id, name)
    store_schema_dropped = await _drop_store_schema(org_id, name)
    members_removed = await _remove_parts(admin_db, org_id, name)
    await forget_env(admin_db, org_id, name)
    dropped = delete_branch(org_id, name) if drop_branch else False
    return {
        "retired": name,
        "branch_deleted": dropped,
        "files_discarded": files_discarded,
        "store_schema_dropped": store_schema_dropped,
        "members_removed": members_removed,
    }


async def _drop_store_schema(org_id: str, name: str) -> str | None:
    """Remove the replicas schema ``name`` wrote in the materialization store (REQ-1622).

    The third thing an environment owns, after its schemas and its files, and the one furthest from
    this module: the store is a different DSN from the tenant pool ``deprovision_org`` ran against,
    so dropping the environment's tenant schemas never reached it and its replicas outlived it.

    Resolved with the org bound, because the store DSN is per-org (an org's BYO store outranks the
    platform's, REQ-1048) and ``materialize_store()`` reads that binding. ``drop_env_store`` drops
    only the replicas schema a non-prod environment's existence created, so prod's replicas are
    out of reach from here.
    """
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.federation.store_scope import drop_env_store

    token = set_current_org(org_id)
    try:
        dsn = state.federation_engine.engine.materialize_store()
        return await drop_env_store(dsn, org_id, name)
    finally:
        reset_current_org(token)
