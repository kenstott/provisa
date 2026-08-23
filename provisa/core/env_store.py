# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1f0a9e-6b52-4e77-9d3a-2f8c1b7e4a05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registry rows recording which environments an org holds (REQ-1488, REQ-1504, REQ-1523).

The row is deliberately thin. An environment's MODEL is its schema (REQ-1488), so nothing here
duplicates it: this table holds only what the schema cannot — the countable unit a plan ceiling is
read against, the expiry that reaps it, whether a merge into it waits for an approval, and whether
its repository projection has fallen behind.

These rows live on the PLATFORM registry, beside ``orgs``, not in the org's own schema. An
environment's row must be readable to decide which schema to route a request to, which is a
decision taken before any environment schema has been opened; a row inside one of them could only
be read by first choosing one.
"""

# Requirements: REQ-1487, REQ-1488, REQ-1504, REQ-1523, REQ-1524

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update

from provisa.core.environments import PROD, EnvironmentNameError, validate_env_name
from provisa.core.schema_admin import environments

if TYPE_CHECKING:
    from provisa.core.database import Database


class EnvironmentLimitError(Exception):
    """The org's plan admits no further environment. Carries the numbers the refusal names."""

    def __init__(self, held: int, limit: int, plan: str) -> None:
        super().__init__(
            f"This organization holds {held} of the {limit} environments its {plan} plan admits. "
            f"Change the plan on the Billing page, or delete an environment."
        )
        self.held, self.limit, self.plan = held, limit, plan


async def list_envs(db: "Database", org_id: str) -> list[dict]:
    """Every environment the org holds, prod first and the rest by name.

    prod leads because REQ-1487 makes it the environment a request naming none is served by, so it
    is the one a reader is looking for when they are not looking for a particular one.
    """
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(environments)
            .where(environments.c.org_id == org_id)
            .order_by((environments.c.name != PROD), environments.c.name)
        )
        return [dict(r._mapping) for r in result.fetchall()]


async def get_env(db: "Database", org_id: str, name: str) -> dict | None:
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(environments).where(environments.c.org_id == org_id, environments.c.name == name)
        )
        row = result.fetchone()
        return dict(row._mapping) if row is not None else None


async def count_envs(db: "Database", org_id: str) -> int:
    """How many environments the org holds — the number a plan's ceiling is read against.

    prod counts. It is a schema like any other (REQ-1488), and a plan admitting one environment
    admits the one every org already has.
    """
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(func.count()).select_from(environments).where(environments.c.org_id == org_id)
        )
        row = result.fetchone()
        assert row is not None  # COUNT over an empty table is a row holding 0, never no row
        return int(row[0])


async def ensure_prod(db: "Database", org_id: str, created_by: str | None = None) -> None:
    """Write the org's ``prod`` row, if it has none.

    REQ-1487 gives prod to the org at its creation rather than by a load, so this is the one row
    REQ-1488's create-by-loading rule does not write. Called from org provisioning, beside the
    schema, the role and the stores. Idempotent: provisioning is.
    """
    async with db.acquire() as conn:
        await conn.upsert(
            environments,
            {"org_id": org_id, "name": PROD, "created_by": created_by},
            index_elements=["org_id", "name"],
            update_columns=[],
        )


async def reserve_env(
    state: Any,
    db: "Database",
    org_id: str,
    name: str,
    created_by: str | None = None,
    expires_at: datetime | None = None,
    branched_from: str | None = None,
) -> None:
    """Validate ``name``, check the plan ceiling, and write the row — before anything is provisioned.

    REQ-1488 makes creation implicit in a load, and this is the part of that act which must happen
    FIRST: the name is refused against this org's own id (REQ-1523) and the ceiling is enforced
    where the environment is created, so no schema is built for an environment that was never
    admissible. The caller provisions after this returns and loads the model into it.

    ``branched_from`` names the base this environment resolves its bindings through (REQ-1529), and
    is None for a base. It is recorded here rather than after provisioning because a branch that
    exists without it would, for the length of that window, be an environment holding a model and
    reaching nothing.
    """
    from provisa.core.commerce import environment_limit_for_org

    validate_env_name(org_id, name)  # raises EnvironmentNameError; refuses prod
    if await get_env(db, org_id, name) is not None:
        raise EnvironmentNameError(f"organization {org_id!r} already has an environment {name!r}")
    limit = await environment_limit_for_org(state, org_id)
    if limit is not None:
        max_envs, plan = limit
        held = await count_envs(db, org_id)
        if held >= max_envs:
            raise EnvironmentLimitError(held, max_envs, plan)
    async with db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(
                org_id=org_id,
                name=name,
                created_by=created_by,
                expires_at=expires_at,
                branched_from=branched_from,
            )
        )


async def forget_env(db: "Database", org_id: str, name: str) -> None:
    """Drop the registry row for a deleted environment. prod is refused (REQ-1487)."""
    if name == PROD:
        raise EnvironmentNameError(
            f"{PROD!r} cannot be deleted; delete the organization to remove it"
        )
    async with db.acquire() as conn:
        await conn.execute_core(
            delete(environments).where(environments.c.org_id == org_id, environments.c.name == name)
        )


async def set_expiry(db: "Database", org_id: str, name: str, expires_at: datetime | None) -> None:
    """Set or clear an environment's expiry (REQ-1523).

    ``None`` clears it, which means permanent — an environment is never reaped for being idle,
    because a quiet pre-prod is not an abandoned one. prod can carry none.
    """
    if name == PROD and expires_at is not None:
        raise EnvironmentNameError(f"{PROD!r} cannot expire")
    await _set(db, org_id, name, expires_at=expires_at)


async def set_protected(db: "Database", org_id: str, name: str, protected: bool) -> None:
    """Whether a merge into this environment waits for someone else's approval (REQ-1504)."""
    await _set(db, org_id, name, protected=protected)


async def set_drifted(db: "Database", org_id: str, name: str, drifted: bool) -> None:
    """Whether the environment's repository projection has fallen behind its model (REQ-1524).

    Set when a write-through commit did not land — the change it observed is NOT undone by it —
    and cleared by the rebuild that re-serializes every carried class.
    """
    await _set(db, org_id, name, drifted=drifted)


async def set_position(
    db: "Database", org_id: str, name: str, *, deployed_sha: str | None, redo_sha: str | None
) -> None:
    """Where the environment now is in its own history, and what a redo can step back toward.

    REQ-1543: both are written together because they are one fact. An undo moves the position back
    and records the top it departed from; a deploy or a write-through moves the position forward
    and passes ``redo_sha=None``, which is not a missing value but the statement that the future
    the undo left is no longer the one the environment is heading for.
    """
    await _set(db, org_id, name, deployed_sha=deployed_sha, redo_sha=redo_sha)


async def set_origin(db: "Database", org_id: str, name: str, origin_sha: str) -> None:
    """Record where the environment's own line begins (REQ-1543).

    Written once, by the creation, against the sha the branch was seeded at: everything at or below
    it is the history of the environment this one was created from, and an undo stops there rather
    than stepping onto a tree this environment never held.
    """
    await _set(db, org_id, name, origin_sha=origin_sha)


async def _set(db: "Database", org_id: str, name: str, **values: Any) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            update(environments)
            .where(environments.c.org_id == org_id, environments.c.name == name)
            .values(**values)
        )
