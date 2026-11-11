# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7d1a52-8c4b-4f09-a6e2-5b1c9d0f7a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org's one expiry run (REQ-2077, REQ-2219: EXPIRY IS ON THE GRANT AND ON ITS ENTRIES).

Expiry belongs to a role grant and to each entitlement entry of it. Access ends at either's date on
every request: an expired grant is not held (``provisa.auth.role_grants.in_force``), and the mask
and the list-scoped reach read an entry's rule as of now. The run (:func:`run_org`, on the
scheduler) records it, and one setting, ``auth.role_grant_notice_days``, is the expiring-soon
threshold for both:

- the grants: the sweep (``provisa.auth.role_grants.sweep``) deletes the expired and the unused
  into the trail and finds those ending within the threshold;
- the entries: per expiry rule, an ``entitlements.expiring_soon`` event ahead of its date by the
  threshold, and an ``entitlements.expired`` event once it has passed, each an entry in the org's
  administrative trail once per rule and date (a changed date is a new one).

Where mail is configured, one notice per person and role names the role when it is ending and each
entry that is: to the person and the org's admins ahead of the end, and to the admins after it.
"""

# Requirements: REQ-2077, REQ-2219

from __future__ import annotations

import logging
from collections.abc import Container, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.auth.role_grants import Grant, Sweep, notice_window, sweep
from provisa.core.schema_org import admin_audit_log
from provisa.entitlements import store
from provisa.entitlements.lists import ExpiryRule, HeldEntitlements

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Database

log = logging.getLogger(__name__)

JOB_ID = "entitlements:expiry_run"
RUN_MINUTES = 15
EXPIRING_SOON = "entitlements.expiring_soon"
EXPIRED = "entitlements.expired"
_MAIL_KIND = "role_grant"


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
    emitted: Container[tuple[str, str | None, str]],
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
    outcome = due(held, emitted, now, notice_window())
    for action, events in ((EXPIRING_SOON, outcome.expiring), (EXPIRED, outcome.expired)):
        for d in events:
            await record_admin_action(
                db, action=action, actor_id="platform", subject_id=d.principal, detail=_detail(d)
            )
    return outcome


@dataclass(frozen=True)
class Ending:
    """What of one person's grant of one role ends: the role, when ``grants`` (its grants, by
    domain) end, and each entry in ``entries``."""

    principal: str
    role_id: str
    grants: tuple[tuple[Grant, str | None], ...]  # each with why it was removed, once it was
    entries: tuple[ExpiryRule, ...]


def endings(grants: Iterable[tuple[Grant, str | None]], entries: Iterable[Due]) -> list[Ending]:
    """``grants`` and ``entries`` by person and role: one notice each."""
    by: dict[tuple[str, str], tuple[list, list]] = {}
    for g, why in grants:
        by.setdefault((g.user_id, g.role_id), ([], []))[0].append((g, why))
    for d in entries:
        by.setdefault((d.principal, d.rule.role_id), ([], []))[1].append(d.rule)
    return [
        Ending(principal, role_id, tuple(gs), tuple(sorted(es, key=lambda r: r.ends_at)))
        for (principal, role_id), (gs, es) in sorted(by.items())
    ]


def _when(at: datetime) -> str:
    return at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _entry(rule: ExpiryRule) -> str:
    return "; ".join(f"{c}: {', '.join(ids)}" for c, ids in sorted(rule.ends.items()))


def _lines(e: Ending, org_id: str, *, ended: bool) -> list[str]:
    lines = []
    for g, why in e.grants:
        scope = "" if g.domain_id == "*" else f" in the domain {g.domain_id}"
        if ended:
            lines.append(f"The role {e.role_id}{scope} in {org_id} was removed: {why}.")
        else:
            lines.append(f"The role {e.role_id}{scope} in {org_id} ends {_when(g.at)}.")
    verb = "ended" if ended else "ends"
    lines.extend(
        f"{_entry(r)} in the role {e.role_id} {verb} {_when(r.ends_at)}." for r in e.entries
    )
    return lines


def _notice(e: Ending, org_id: str, *, holder: bool) -> tuple[str, str]:
    """The notice of what of ``e`` is ending (``holder``: to its holder)."""
    who = "Your" if holder else f"{e.principal}'s"
    if e.grants:
        at = min(g.at for g, _why in e.grants)
        subject = f"{who} role {e.role_id} in {org_id} ends {_when(at)}"
    else:
        at = e.entries[0].ends_at
        subject = f"{who} access in the role {e.role_id} in {org_id} ends {_when(at)}"
    return subject, "\n".join(_lines(e, org_id, ended=False))


def _ended(e: Ending, org_id: str) -> tuple[str, str]:
    """The notice to the admins of what of ``e`` ended."""
    if e.grants:
        subject = f"{e.principal}'s role {e.role_id} in {org_id} was removed"
    else:
        subject = f"{e.principal}'s access in the role {e.role_id} in {org_id} ended"
    return subject, "\n".join(_lines(e, org_id, ended=True))


@dataclass(frozen=True)
class Outcome:
    grants: Sweep
    entries: Run


async def _tell(db: "Database", org_id: str, outcome: Outcome) -> None:
    from provisa.auth.role_grants import emails_of, send_notice
    from provisa.core.org_membership import org_admin_user_ids

    g = outcome.grants
    soon = endings([(x, None) for x in (*g.expiring, *g.going_unused)], outcome.entries.expiring)
    gone = endings(
        [(x, "the grant expired") for x in g.expired]
        + [(x, "the grant went unused") for x in g.inactive],
        outcome.entries.expired,
    )
    admins = await org_admin_user_ids(db)
    async with db.acquire() as conn:
        emails = await emails_of(conn, [*admins, *(e.principal for e in soon)])
    admin_emails = sorted({emails[a] for a in admins if a in emails})
    for e in soon:
        if e.principal in emails:
            subject, body = _notice(e, org_id, holder=True)
            await send_notice(org_id, emails[e.principal], subject, body, kind=_MAIL_KIND)
        for to in admin_emails:
            subject, body = _notice(e, org_id, holder=False)
            await send_notice(org_id, to, subject, body, kind=_MAIL_KIND)
    for e in gone:
        for to in admin_emails:
            subject, body = _ended(e, org_id)
            await send_notice(org_id, to, subject, body, kind=_MAIL_KIND)


async def run_org(org_id: str) -> Outcome:
    """``org_id``'s expiry run, by the scheduler: its grants swept, its entries' events emitted,
    and notices sent wherever mail is configured (``mail.provider``), a single-org deployment
    included; where it is not, the trail entry is the record and no grant is marked as told."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        db = state.model_db
        assert db is not None, "the org's model store is bound with its runtime"
        assert state.config is not None, "the deployment's configuration is loaded"
        notify = bool(state.config.mail.provider)
        outcome = Outcome(await sweep(db, notify=notify), await run(db, datetime.now(UTC)))
        if notify:
            await _tell(db, org_id, outcome)
    finally:
        reset_current_org(token)
    g, e = outcome.grants, outcome.entries
    if g.expired or g.inactive or e.expiring or e.expired:
        log.info(
            "org %s: %d expired and %d unused role grant(s) removed; "
            "%d entitlement entr(ies) expiring soon, %d expired",
            org_id,
            len(g.expired),
            len(g.inactive),
            len(e.expiring),
            len(e.expired),
        )
    return outcome


def run_job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Run ``org_id``'s expiry every RUN_MINUTES. A job already held is kept."""
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
