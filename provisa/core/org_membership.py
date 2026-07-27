# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared split-plane org-membership grants (REQ-1266).

Org ownership is recorded in two planes: the admin plane holds the
``user_org_memberships`` row (which orgs a user belongs to), the tenant plane
holds the ``user_role_assignments`` row (what the user may do inside a specific
org's schema). Both the invite-redeem path and self-service org creation grant
the same pair, so it lives here once.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.core.schema_org import user_role_assignments

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)


def email_matches_rule(email: str | None, rule: str | None) -> bool:
    """Whether ``email`` satisfies an org's ``email_rule`` regex (REQ-1268).

    A NULL rule imposes no restriction (any email matches, including a missing one). A non-NULL rule
    requires a non-empty email that the regex finds within — an authenticated identity with no email
    can never satisfy a rule. The rule is validated as compilable at org-creation time, so a stored
    rule compiles here; re.error is still guarded so a hand-edited bad rule fails closed (no match).
    """
    if rule is None:
        return True
    if not email:
        return False
    try:
        return re.search(rule, email) is not None
    except re.error:
        return False


async def grant_membership(admin_db: "Database", user_id: str, org_id: str) -> None:
    """Record the admin-plane membership row (user belongs to org). Idempotent.

    The membership is the org-ownership fact the middleware's active-org gate reads; granting it
    synchronously lets a creator own their org the instant it is registered, before the tenant
    schema finishes provisioning.
    """
    async with admin_db.acquire() as conn:
        await conn.upsert(
            user_org_memberships,
            {"user_id": user_id, "org_id": org_id},
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )


async def grant_org_role(tenant_db: "Database", user_id: str, role_id: str) -> None:
    """Record the tenant-plane role assignment inside the org's schema. Idempotent.

    ``tenant_db`` MUST be scoped (search_path) to the target org's schema — the assignment lands
    in whatever ``org_<id>`` schema this Database points at. The role row it references must already
    exist in that schema (schema.sql seeds ``org_admin``), so for a freshly created org this runs
    only after the schema is provisioned.
    """
    async with tenant_db.acquire() as conn:
        await conn.upsert(
            user_role_assignments,
            {"user_id": user_id, "role_id": role_id, "domain_id": "*"},
            index_elements=["user_id", "role_id", "domain_id"],
            update_columns=[],
        )


async def grant_org_admin(
    admin_db: "Database", tenant_db: "Database", user_id: str, org_id: str
) -> None:
    """Make ``user_id`` the org_admin of ``org_id``: membership (admin plane) + org_admin role
    assignment (tenant plane, scoped to the org's schema)."""
    await grant_membership(admin_db, user_id, org_id)
    await grant_org_role(tenant_db, user_id, "org_admin")


async def resolve_auto_join_orgs(
    admin_db: "Database", email: str | None
) -> list[tuple[str, str]]:
    """Return ``(org_id, auto_join_role)`` for every auto-join org whose email rule ``email`` matches.

    REQ-1269. Reads the admin plane only (no tenant-schema access): the caller grants the tenant
    role once it has bound the org's runtime. An auto_join org with no auto_join_role is skipped —
    membership without a role would leave the user with no capabilities in the org.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.email_rule, orgs.c.auto_join_role).where(
                orgs.c.auto_join == True  # noqa: E712 — SQLAlchemy boolean column comparison
            )
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
    return [
        (r["id"], r["auto_join_role"])
        for r in rows
        if r["auto_join_role"] and email_matches_rule(email, r["email_rule"])
    ]


def notify_org_ready(org_id: str, user_id: str) -> None:  # REQ-1266
    """Notify the creator that their org finished provisioning.

    In-app notification is the poll endpoint reading ``orgs.provisioning_state``; this seam is the
    future email hook (a provider integration replaces the log line). Kept synchronous and
    side-effect-light so the provisioning task can call it inline after flipping to ``ready``.
    """
    log.info("org %s provisioned and ready — notifying creator %s", org_id, user_id)
