# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7d1a52-8c4b-4f09-a6e2-5b1c9d0f7a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The scheduled entitlement expiry run (REQ-2219, EXPIRY RULES END EVERY CAPABILITY THEY NAME).

Access ends at a rule's date on every request (the mask and the list-scoped reach read the rules
as of now); each org's run (:func:`run_org`, on the scheduler) records it. Per expiry rule Provisa
holds:

- ahead of its date, by ``entitlements.expiring_soon_days`` (the event's own setting), an
  ``entitlements.expiring_soon`` event;
- once its date has passed, an ``entitlements.expired`` event.

Each event is an entry in the org's administrative trail, once per rule and date: a changed date
is a new one. Where mail is configured, as for role grants (``provisa.auth.role_grants``), the
person and the org's admins are told of an expiring-soon rule, and the admins of an expired one.
"""

# Requirements: REQ-2219

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import admin_audit_log
from provisa.entitlements import store
from provisa.entitlements.lists import ExpiryRule, HeldEntitlements

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Database

log = logging.getLogger(__name__)

JOB_ID = "entitlements:expiry_run"
RUN_MINUTES = 15
SETTING = "entitlements.expiring_soon_days"
EXPIRING_SOON = "entitlements.expiring_soon"
EXPIRED = "entitlements.expired"
_MAIL_KIND = "entitlement_expiry"


def expiring_soon_days() -> int:
    """How many days ahead of a rule's date its expiring-soon event is emitted."""
    from provisa.core import settings_registry

    return int(settings_registry.value(SETTING))


@dataclass(frozen=True)
class Due:
    """One rule an event is due for, on ``principal``'s grant."""

    principal: str
    rule: ExpiryRule


@dataclass(frozen=True)
class Run:
    expiring: list[Due]
    expired: list[Due]


def _event_key(action: str, rule_key: str | None, ends_at: str) -> tuple[str, str | None, str]:
    return action, rule_key, ends_at


def due(
    held: Mapping[str, HeldEntitlements],
    emitted: set[tuple[str, str | None, str]],
    now: datetime,
    within: timedelta,
) -> Run:
    """The rules of ``held`` an event is due for at ``now``: expired, once their date has passed;
    expiring soon, once it is ``within`` ahead. ``emitted`` holds the events already emitted, by
    action, rule key and date, which are not emitted again."""
    expiring: list[Due] = []
    expired: list[Due] = []
    for principal, holding in sorted(held.items()):
        for rule in holding.expiries:
            when = rule.ends_at.isoformat()
            if rule.ends_at <= now:
                if _event_key(EXPIRED, rule.rule_key, when) not in emitted:
                    expired.append(Due(principal, rule))
            elif rule.ends_at <= now + within:
                if _event_key(EXPIRING_SOON, rule.rule_key, when) not in emitted:
                    expiring.append(Due(principal, rule))
    return Run(expiring, expired)


async def _emitted(conn: Any) -> set[tuple[str, str | None, str]]:
    rows = await conn.execute_core(
        select(admin_audit_log.c.action, admin_audit_log.c.detail).where(
            admin_audit_log.c.action.in_((EXPIRING_SOON, EXPIRED))
        )
    )
    return {
        _event_key(action, detail.get("rule_key"), detail.get("ends_at", ""))
        for action, detail in rows.fetchall()
    }


def _detail(d: Due) -> dict:
    return {
        "rule_key": d.rule.rule_key,
        "role_id": d.rule.role_id,
        "ends_at": d.rule.ends_at.isoformat(),
        "ends": {capability: list(ids) for capability, ids in d.rule.ends.items()},
        "reason": d.rule.reason,
    }


async def run(db: "Database", now: datetime) -> Run:
    """Emit ``db``'s due events at ``now`` into its administrative trail; what was due."""
    from provisa.core.org_membership import record_admin_action

    async with db.acquire() as conn:
        held = await store.load_held(conn)
        emitted = await _emitted(conn)
    outcome = due(held, emitted, now, timedelta(days=expiring_soon_days()))
    for action, events in ((EXPIRING_SOON, outcome.expiring), (EXPIRED, outcome.expired)):
        for d in events:
            await record_admin_action(
                db, action=action, actor_id="platform", subject_id=d.principal, detail=_detail(d)
            )
    return outcome


def _what(rule: ExpiryRule) -> str:
    if not rule.ends:
        return f"the role {rule.role_id}"
    named = "; ".join(f"{c}: {', '.join(ids)}" for c, ids in sorted(rule.ends.items()))
    return f"{named} in the role {rule.role_id}"


def _when(at: datetime) -> str:
    return at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _notice(d: Due, org_id: str, *, holder: bool) -> tuple[str, str]:
    who = "Your" if holder else f"{d.principal}'s"
    return (
        f"{who} access in {org_id} ends {_when(d.rule.ends_at)}",
        f"{who} access to {_what(d.rule)} in the organization {org_id} ends "
        f"{_when(d.rule.ends_at)}.",
    )


def _ended(d: Due, org_id: str) -> tuple[str, str]:
    return (
        f"{d.principal}'s access in {org_id} ended",
        f"{d.principal}'s access to {_what(d.rule)} in the organization {org_id} ended "
        f"{_when(d.rule.ends_at)}.",
    )


async def _tell(db: "Database", org_id: str, outcome: Run) -> None:
    from provisa.auth.role_grants import emails_of, send_notice
    from provisa.core.org_membership import org_admin_user_ids

    admins = await org_admin_user_ids(db)
    async with db.acquire() as conn:
        emails = await emails_of(conn, [*admins, *(d.principal for d in outcome.expiring)])
    admin_emails = sorted({emails[a] for a in admins if a in emails})
    for d in outcome.expiring:
        if d.principal in emails:
            subject, body = _notice(d, org_id, holder=True)
            await send_notice(org_id, emails[d.principal], subject, body, kind=_MAIL_KIND)
        for to in admin_emails:
            subject, body = _notice(d, org_id, holder=False)
            await send_notice(org_id, to, subject, body, kind=_MAIL_KIND)
    for d in outcome.expired:
        for to in admin_emails:
            subject, body = _ended(d, org_id)
            await send_notice(org_id, to, subject, body, kind=_MAIL_KIND)


async def run_org(org_id: str) -> Run:
    """``org_id``'s expiry run, by the scheduler. Notices go wherever mail is configured
    (``mail.provider``); where it is not, the trail entry is the record."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        db = state.model_db
        assert db is not None, "the org's model store is bound with its runtime"
        assert state.config is not None, "the deployment's configuration is loaded"
        outcome = await run(db, datetime.now(UTC))
        if state.config.mail.provider:
            await _tell(db, org_id, outcome)
    finally:
        reset_current_org(token)
    if outcome.expiring or outcome.expired:
        log.info(
            "org %s: %d entitlement expiry rule(s) expiring soon, %d expired",
            org_id,
            len(outcome.expiring),
            len(outcome.expired),
        )
    return outcome


def run_job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Run ``org_id``'s entitlement expiry every RUN_MINUTES. A job already held is kept."""
    from apscheduler.triggers.interval import IntervalTrigger

    job_id = run_job_id(org_id)
    if scheduler.get_job(job_id) is not None:
        return
    scheduler.add_job(
        run_org,
        trigger=IntervalTrigger(minutes=RUN_MINUTES),
        args=[org_id],
        id=job_id,
        name=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
