# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7a19c6-58d2-4b0e-9c31-a7e5d2b40f81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reaping the environments whose expiry has passed (REQ-1523).

WHY THIS EXISTS SEPARATELY FROM THE EXPIRY COLUMN. ``environments.expires_at`` has been written
since REQ-1523 -- at creation through ``reserve_env`` and afterwards through ``set_expiry`` -- and
nothing read it back. An expiry that is recorded and never fires is not a weaker guarantee than the
requirement's, it is a different one: the org that was told its environment would be deleted keeps
paying for the schema and the store indefinitely.

WHY IT SWEEPS EVERY ORG AT ONCE. The row lives on the PLATFORM registry beside ``orgs`` (see
``provisa.core.env_store``), so one query over ``environments`` finds every expired environment on
the deployment. Reaping per-org would need a list of orgs to iterate, which is the same query with
a step in front of it.

WHY ONE FAILURE DOES NOT END THE SWEEP. Retirement drops schemas and stores through
``deprovision_org``, and an org whose store is unreachable would otherwise stop every environment
queued behind it from ever being reaped -- the sweep would fail at the same place on every tick.
Each environment is therefore attempted, and the failures are raised TOGETHER at the end: nothing
is swallowed, and nothing is blocked by the one in front of it.

WHY THE SWEEP IS NOT THE WHOLE ENFORCEMENT. It runs on a schedule, so between two ticks an expired
environment still has its schemas. ``provisa.api.env_routing.select_environment`` refuses to serve
one whose expiry has passed, which is what makes the deadline the moment it says it is rather than
the moment the next tick happens to arrive.
"""

# Requirements: REQ-1523, REQ-1488, REQ-1487

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import select

from provisa.core.env_retire import retire_environment
from provisa.core.schema_admin import environments

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)


class AuditFn(Protocol):
    """Records one retirement in the org's own trail."""

    async def __call__(self, org_id: str, name: str, outcome: dict) -> None: ...


class ReapError(Exception):
    """One or more expired environments could not be retired. Carries each failure."""

    def __init__(self, failures: list[tuple[str, str, BaseException]]) -> None:
        named = ", ".join(f"{org_id}/{name}: {exc}" for org_id, name, exc in failures)
        super().__init__(f"{len(failures)} expired environment(s) could not be retired: {named}")
        self.failures = failures


def utcnow() -> datetime:
    """The instant an expiry is measured against. One place, so a test can name another."""
    return datetime.now(timezone.utc)


async def expired_envs(admin_db: "Database", now: datetime) -> list[tuple[str, str]]:
    """``(org_id, name)`` for every environment whose expiry has passed at ``now``.

    A NULL expiry is permanent (REQ-1523) and a NULL never satisfies ``<=``, so the absence of an
    expiry needs no clause of its own here. Oldest first: an environment that has been expired
    longest is the one whose deletion is most overdue.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(environments.c.org_id, environments.c.name)
            .where(environments.c.expires_at <= now)
            .order_by(environments.c.expires_at)
        )
        return [(row[0], row[1]) for row in result.fetchall()]


async def reap_expired(
    pool: "Database",
    admin_db: "Database",
    *,
    now: datetime | None = None,
    audit: AuditFn | None = None,
) -> list[dict]:
    """Retire every environment whose expiry has passed. Returns what each retirement did.

    The branch goes with the environment. An expiry is a statement that the work is over on a date
    chosen when the environment was created (REQ-1523), which is the same case
    ``retire_environment`` documents for a merge that retires its source -- not the delete-door case
    where a person removing a schema has not asked to lose the history.

    ``audit``, when given, records each retirement in the org's own trail; it is a parameter rather
    than an import because the trail is written through the API's org-scoped tenant database and
    this module is core.
    """
    at = utcnow() if now is None else now
    outcomes: list[dict] = []
    failures: list[tuple[str, str, BaseException]] = []
    for org_id, name in await expired_envs(admin_db, at):
        try:
            outcome = await retire_environment(pool, admin_db, org_id, name, drop_branch=True)
        except Exception as exc:  # re-raised together below; see the module docstring
            log.exception("reaping expired environment %s/%s failed", org_id, name)
            failures.append((org_id, name, exc))
            continue
        log.info("reaped expired environment %s/%s", org_id, name)
        outcomes.append({"org_id": org_id, **outcome})
        if audit is not None:
            await audit(org_id, name, outcome)
    if failures:
        raise ReapError(failures)
    return outcomes
