# Copyright (c) 2026 Kenneth Stott
# Canary: 7c5d11cc-e8ea-4bf0-9a17-442663290f92
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

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import delete, insert, select, update

from provisa.core.schema_admin import (
    AWAITING_CHECKOUT,
    org_auto_join_optouts,
    orgs,
    user_org_memberships,
)
from provisa.core.schema_org import admin_audit_log, user_role_assignments
from provisa.security.rights import ORG_ADMIN_ROLE

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


# REQ-1477: consumer mailbox providers. An address at one of these says nothing about who the
# person works for, so a rule that matches one is not an identity claim — it would hand membership
# of someone's org to any stranger who signs up with the right provider.
PUBLIC_EMAIL_DOMAINS = frozenset(
    {
        "aol.com",
        "gmail.com",
        "googlemail.com",
        "hotmail.co.uk",
        "hotmail.com",
        "icloud.com",
        "live.com",
        "mail.com",
        "me.com",
        "msn.com",
        "outlook.com",
        "pm.me",
        "proton.me",
        "protonmail.com",
        "yahoo.co.uk",
        "yahoo.com",
        "ymail.com",
        "yandex.com",
        "zoho.com",
    }
)


def public_domain_matched_by(rule: str | None) -> str | None:  # REQ-1477
    """The first consumer-mailbox domain an auto-join ``rule`` would admit, or None.

    Probed by matching the rule against a sample address at each domain rather than by reading the
    regex, so a rule reaches the same verdict here as it will at sign-in. A NULL rule matches every
    address, so it reports the first domain in the set: auto-join with no rule admits consumer
    mailboxes just as surely as one naming them.
    """
    for domain in sorted(PUBLIC_EMAIL_DOMAINS):
        if email_matches_rule(f"someone@{domain}", rule):
            return domain
    return None


def bindable_memberships(user_id: str):  # REQ-1476
    """The orgs ``user_id`` belongs to AND can work in, as a select of (org_id, org_name).

    Every session-binding path reads membership through this one statement so a reservation — an org
    that exists only as a claimed id awaiting its subscription — is never bound, never offered as an
    active org and never reported as a membership.
    """
    return (
        select(
            user_org_memberships.c.org_id,
            orgs.c.name.label("org_name"),
            user_org_memberships.c.joined_via,
            user_org_memberships.c.acknowledged_at,
        )
        .select_from(user_org_memberships.join(orgs, orgs.c.id == user_org_memberships.c.org_id))
        .where(
            user_org_memberships.c.user_id == user_id,
            orgs.c.provisioning_state != AWAITING_CHECKOUT,
        )
    )


# REQ-1478: how a membership came about. CREATED and INVITE are acts the user performed; AUTO_JOIN
# and ADMIN happen to them, which is what the sign-in notice exists to explain.
JOINED_VIA_CREATED = "created"
JOINED_VIA_INVITE = "invite"
JOINED_VIA_AUTO_JOIN = "auto_join"
JOINED_VIA_ADMIN = "admin"


def membership_values(user_id: str, org_id: str, joined_via: str) -> dict:  # REQ-1478
    """The membership row to upsert, for the paths that write it on their own transaction's
    connection rather than through ``grant_membership``.

    A membership created by the user's own act needs no announcement, so it is born acknowledged;
    one that happened to them is left unacknowledged until they see the notice.
    """
    row: dict[str, object] = {"user_id": user_id, "org_id": org_id, "joined_via": joined_via}
    if joined_via == JOINED_VIA_CREATED:
        row["acknowledged_at"] = datetime.now(timezone.utc)
    return row


async def grant_membership(
    admin_db: "Database", user_id: str, org_id: str, *, joined_via: str
) -> None:
    """Record the admin-plane membership row (user belongs to org). Idempotent.

    The membership is the org-ownership fact the middleware's active-org gate reads; granting it
    synchronously lets a creator own their org the instant it is registered, before the tenant
    schema finishes provisioning.

    ``joined_via`` is not updated on a re-grant: the first way in is the one that needs explaining,
    and re-announcing an org the member already acknowledged would be noise.
    """
    async with admin_db.acquire() as conn:
        await conn.upsert(
            user_org_memberships,
            membership_values(user_id, org_id, joined_via),
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )


async def acknowledge_membership(  # REQ-1478
    admin_db: "Database", user_id: str, org_id: str
) -> None:
    """Mark that ``user_id`` has been shown how they came to be in ``org_id``."""
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            update(user_org_memberships)
            .where(
                user_org_memberships.c.user_id == user_id,
                user_org_memberships.c.org_id == org_id,
            )
            .values(acknowledged_at=datetime.now(timezone.utc))
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
    admin_db: "Database", tenant_db: "Database", user_id: str, org_id: str, *, joined_via: str
) -> None:
    """Make ``user_id`` the org_admin of ``org_id``: membership (admin plane) + org_admin role
    assignment (tenant plane, scoped to the org's schema)."""
    await grant_membership(admin_db, user_id, org_id, joined_via=joined_via)
    await grant_org_role(tenant_db, user_id, "org_admin")


async def resolve_auto_join_orgs(
    admin_db: "Database", email: str | None, user_id: str
) -> list[tuple[str, str]]:
    """Return ``(org_id, auto_join_role)`` for every auto-join org whose email rule ``email`` matches.

    REQ-1269. Reads the admin plane only (no tenant-schema access): the caller grants the tenant
    role once it has bound the org's runtime. An auto_join org with no auto_join_role is skipped —
    membership without a role would leave the user with no capabilities in the org.

    REQ-1306: an org the user has deliberately left is excluded for that user. The opt-out row is
    what makes departure stick against a rule that still matches their address.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.email_rule, orgs.c.auto_join_role).where(
                orgs.c.auto_join == True,  # noqa: E712 — SQLAlchemy boolean column comparison
                # REQ-1476: a reservation has no schema and no roles, so joining someone to it
                # would grant a role in a schema that does not exist.
                orgs.c.provisioning_state != AWAITING_CHECKOUT,
            )
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
        opted_out_result = await conn.execute_core(
            select(org_auto_join_optouts.c.org_id).where(org_auto_join_optouts.c.user_id == user_id)
        )
        opted_out = {r[0] for r in opted_out_result.fetchall()}
    joined = []
    for r in rows:
        if not r["auto_join_role"] or r["id"] in opted_out:
            continue
        if not email_matches_rule(email, r["email_rule"]):
            continue
        # REQ-1477: refused at the point the policy is set, and again here — a rule written before
        # that rule existed, or edited straight into the table, must not admit a consumer mailbox.
        domain = public_domain_matched_by(r["email_rule"])
        if domain is not None:
            log.warning("org %s auto-joins %s addresses; refusing the join", r["id"], domain)
            continue
        joined.append((r["id"], r["auto_join_role"]))
    return joined


async def suppress_auto_join(admin_db: "Database", user_id: str, org_id: str) -> None:  # REQ-1306
    """Record that ``user_id`` has deliberately left ``org_id`` and must not be auto-rejoined."""
    async with admin_db.acquire() as conn:
        await conn.upsert(
            org_auto_join_optouts,
            {"user_id": user_id, "org_id": org_id},
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )


async def clear_auto_join_suppression(  # REQ-1306
    admin_db: "Database", user_id: str, org_id: str
) -> None:
    """Drop the opt-out row, if any. An invitation or an explicit add is an affirmative act by the
    org and outranks the user's earlier departure; a rule match is not."""
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            delete(org_auto_join_optouts).where(
                org_auto_join_optouts.c.user_id == user_id,
                org_auto_join_optouts.c.org_id == org_id,
            )
        )


async def org_admin_user_ids(tenant_db: "Database") -> set[str]:  # REQ-1302
    """Every user holding org_admin in the schema ``tenant_db`` is scoped to.

    The tenant plane is the authority on who administers an org — a control-plane membership row
    says only that the person belongs to it.
    """
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(user_role_assignments.c.user_id).where(
                user_role_assignments.c.role_id == ORG_ADMIN_ROLE
            )
        )
        return {r[0] for r in result.fetchall()}


class LastOrgAdminError(Exception):  # REQ-1302
    """Raised when an act would leave an org with zero org_admins."""

    def __init__(self, org_id: str, user_id: str):
        self.org_id = org_id
        self.user_id = user_id
        super().__init__(
            f"{user_id} is the last org_admin of {org_id}. Promote another org_admin first, or "
            f"delete the organization."
        )


async def assert_not_last_org_admin(  # REQ-1302
    tenant_db: "Database", user_id: str, org_id: str
) -> None:
    """Raise LastOrgAdminError if removing ``user_id``'s org_admin authority would orphan the org.

    Applies identically to revocation, offboarding and voluntary departure — all three are ordinary
    administrative acts. Org deletion is not: it is a confirmed destruction (REQ-1300) and bypasses
    this check deliberately.
    """
    admins = await org_admin_user_ids(tenant_db)
    if user_id in admins and len(admins) == 1:
        raise LastOrgAdminError(org_id, user_id)


async def record_admin_action(  # REQ-1303, REQ-1308
    tenant_db: "Database", *, action: str, actor_id: str, subject_id: str, detail: dict
) -> None:
    """Append an entry to the org's administrative trail. ``tenant_db`` must be scoped to that org's
    schema — the entry belongs to the org the act was performed against, so a platform_admin's
    intervention is visible there afterward rather than only on the platform."""
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            insert(admin_audit_log).values(
                action=action, actor_id=actor_id, subject_id=subject_id, detail=detail
            )
        )


async def remove_from_org(  # REQ-1305
    admin_db: "Database", tenant_db: "Database", user_id: str, org_id: str
) -> bool:
    """Remove ``user_id`` from ``org_id`` in BOTH planes. Returns whether a membership row existed.

    The tenant-plane assignments must go with the membership: leaving them behind means re-adding
    the person silently restores every privilege they previously held. ``tenant_db`` must be scoped
    to ``org_<org_id>``.

    REQ-1263: the user's personal access tokens for this org are revoked with the membership. A PAT
    outlives any session, so leaving one alive would let a removed member keep reading the org until
    the token's own expiry.
    """
    from provisa.auth.pat import PersonalAccessTokenStore

    await PersonalAccessTokenStore(admin_db).revoke_all_for_user_in_org(
        user_id=user_id, org_id=org_id
    )
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            delete(user_role_assignments).where(user_role_assignments.c.user_id == user_id)
        )
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            delete(user_org_memberships).where(
                user_org_memberships.c.user_id == user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
    return (result.rowcount or 0) > 0


def tombstone_id(user_id: str) -> str:  # REQ-1312
    """The opaque token replacing a deleted user's id in rows that must stay referentially intact.

    Derived by hash rather than randomly so every reference to one deleted account collapses to the
    same token — the record still shows that one person did these things, without saying who. It is
    one-way: the digest cannot be turned back into the id.
    """
    return "deleted-user-" + hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:16]


def notify_org_ready(org_id: str, user_id: str) -> None:  # REQ-1266
    """Notify the creator that their org finished provisioning.

    In-app notification is the poll endpoint reading ``orgs.provisioning_state``; this seam is the
    future email hook (a provider integration replaces the log line). Kept synchronous and
    side-effect-light so the provisioning task can call it inline after flipping to ``ready``.
    """
    log.info("org %s provisioned and ready — notifying creator %s", org_id, user_id)


# REQ-1308: a user may not change their own role, on ANY protocol. Self-elevation is the whole
# reason role assignment is an administrative act; a user who can grant themselves a role has no
# role. Enforced server-side at every mutation site rather than hidden in a UI, because the UI is
# not the only client.
SELF_ROLE_CHANGE_MESSAGE = (
    "A user cannot change their own role. Ask another administrator (REQ-1308)."
)


def is_self_role_change(actor_id: str | None, subject_id: str) -> bool:
    """Whether an actor is attempting to alter their own role assignment.

    ``actor_id`` is None in dev/no-auth mode, where there is no authenticated identity to protect
    and every request is already the operator's own — the rule does not apply.
    """
    return actor_id is not None and actor_id == subject_id
