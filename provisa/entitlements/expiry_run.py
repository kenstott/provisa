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

Expiry belongs to a role grant and to each entitlement entry of it. The run (:func:`run_org`, on
the scheduler) acts on it, and one setting, ``auth.role_grant_notice_days``, is the expiring-soon
threshold for both:

- the grants: the sweep (``provisa.auth.role_grants.sweep``) deletes the expired and the unused
  into the trail and finds those ending within the threshold;
- the entries: per expiry rule, an ``entitlements.expiring_soon`` event ahead of its date by the
  threshold, and an ``entitlements.expired`` event once it has passed, each an entry in the org's
  administrative trail once per rule and date (a changed date is a new one).

AN EXPIRY IS A CANCEL (REQ-2219, amended 2026-11-11 and 2026-11-19). Every expiring-soon and
expired event, and for each expired grant or entry "cancel this grant, expired", goes to the org's
outbound hook (``provisa.core.offer_hooks``) where one is configured. Access Provisa holds is
removed at once by the run itself, as the matching ordinary removal attributed to the expiry: an
expired grant is deleted (the sweep), and an entry that ends identifiers of the list it was set
with has them removed from that list by an appended row, and its rule closed. Access the identity
provider holds (an entry over a list its sign-in sends) only goes to the hook; with no hook it is
recorded as the event only. A send the hook refuses leaves an entry's event unrecorded, so the
next run sends it again; the run then fails naming it (:class:`HookFailed`).

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
from provisa.core import offer_hooks
from provisa.core.offer_hooks import EXPIRY, Expiring, HookFailed, HookTarget
from provisa.core.schema_org import admin_audit_log, user_role_assignments
from provisa.entitlements import store
from provisa.entitlements.lists import ExpiryRule, HeldEntitlements, IdpLists

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Database

log = logging.getLogger(__name__)

JOB_ID = "entitlements:expiry_run"
RUN_MINUTES = 15
EXPIRING_SOON = "entitlements.expiring_soon"
EXPIRED = "entitlements.expired"
GRANT_EXPIRING_SOON = "role.expiring_soon"
#: Who holds what an expiry ends, as its trail entry records it.
HELD_BY_PROVISA = "provisa"
HELD_BY_IDP = "idp"
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
    # The expired entries the run removed itself (Provisa holds them), and each send the hook
    # refused, which is sent again on the next run.
    removed: tuple[Due, ...] = ()
    failed: tuple[str, ...] = ()


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


def _detail(d: Due, held_by: str) -> dict:
    return {
        "rule_key": d.rule.rule_key,
        "role_id": d.rule.role_id,
        "ends_at": d.rule.ends_at.isoformat(),
        "ends": {capability: list(ids) for capability, ids in d.rule.ends.items()},
        "reason": d.rule.reason,
        "held_by": held_by,
    }


def held_by_idp(d: Due, idp: Mapping[str, IdpLists]) -> bool:
    """Whether the identity provider holds what ``d`` ends: the list the rule was set with is one
    the person's sign-in sends for the role, which no Provisa rule touches."""
    kind = d.rule.list_kind
    return kind is not None and store.idp_supersedes(idp, d.principal, d.rule.role_id, kind)


def _expiring_entry(d: Due) -> Expiring:
    assert d.rule.rule_key is not None, "a stored rule has its key"
    return Expiring(
        d.principal,
        d.rule.role_id,
        d.rule.ends_at,
        rule=d.rule.rule_key,
        ends=d.rule.ends,
        reason=d.rule.reason,
    )


async def _send(hook: HookTarget | None, bodies: Iterable[dict], failed: list[str]) -> bool:
    """Post ``bodies`` to ``hook`` in order (none with no hook); whether every one was taken. A
    refusal is added to ``failed`` and stops the rest."""
    if hook is None:
        return True
    for body in bodies:
        try:
            await offer_hooks.deliver(hook, body)
        except HookFailed as refused:
            failed.append(f"{body['kind']} {body['id']}: {refused}")
            return False
    return True


def _ends_its_list(rule: ExpiryRule) -> bool:
    """Whether ``rule`` ends identifiers of the list it was set with, which an ordinary removal
    takes out of that list. A rule naming capabilities, or set with no list, ends no list's
    identifiers, and stays as it is."""
    return rule.list_kind is not None and set(rule.ends) == {"*"}


def _expired_reason(rule: ExpiryRule) -> str:
    return f"expired: {rule.reason}" if rule.reason else "expired"


async def _remove_identifiers(db: "Database", d: Due) -> None:
    """Take what ``d`` ends out of the lists of the kind it was set with -- the role's own, and
    each owner capability's row of that kind (REQ-2219: an owner row expires like any other) --
    by an appended row attributed to the expiry: the matching ordinary removal. Nothing is
    appended once they are gone."""
    rule = d.rule
    assert rule.list_kind is not None
    ended = set(rule.ends["*"])
    async with db.acquire() as conn:
        async with conn.transaction():
            held = await store.held_lists(conn, d.principal, rule.role_id, rule.list_kind)
            for capability, listed in sorted(held.items(), key=lambda kv: kv[0] or ""):
                if not ended & set(listed):
                    continue
                await store.set_list(
                    conn,
                    d.principal,
                    rule.role_id,
                    rule.list_kind,
                    [i for i in listed if i not in ended],
                    set_by=EXPIRY,
                    reason=_expired_reason(rule),
                    capability=capability,
                )


async def _close(db: "Database", d: Due) -> None:
    """Close ``d``'s rule, attributed to the expiry, once what it ended is gone and told."""
    async with db.acquire() as conn:
        await store.withdraw_expiry(
            conn, d.rule, d.principal, set_by=EXPIRY, reason=_expired_reason(d.rule)
        )


async def run(db: "Database", org_id: str, now: datetime, hook: HookTarget | None) -> Run:
    """Act on ``db``'s due entries at ``now``, ``db`` holding the org ``org_id``: each event goes to
    ``hook`` (None: the org configures none) and into the administrative trail, an expired entry
    also as its cancel, and an expired one Provisa holds is removed at once. What was due."""
    from provisa.core.org_membership import record_admin_action

    async with db.acquire() as conn:
        held = await store.load_held(conn)
        idp = await store.load_idp(conn)
        emitted = await _emitted(conn)
    outcome = due(held, emitted, now, notice_window())
    failed: list[str] = []
    removed: list[Due] = []
    for action, events in ((EXPIRING_SOON, outcome.expiring), (EXPIRED, outcome.expired)):
        for d in events:
            by_idp = held_by_idp(d, idp)
            removes = action == EXPIRED and not by_idp and _ends_its_list(d.rule)
            if removes:
                await _remove_identifiers(db, d)
            what = _expiring_entry(d)
            if action == EXPIRED:
                bodies = [
                    offer_hooks.expiry_event("expired", what),
                    offer_hooks.expiry_cancel(what),
                ]
            else:
                bodies = [offer_hooks.expiry_event("expiring_soon", what)]
            if not await _send(hook, bodies, failed):
                continue  # its rule stays open, so the next run sends it again
            await record_admin_action(
                db,
                org_id=org_id,
                action=action,
                actor_id="platform",
                subject_id=d.principal,
                detail=_detail(d, HELD_BY_IDP if by_idp else HELD_BY_PROVISA),
            )
            if removes:
                await _close(db, d)
                removed.append(d)
    if outcome.expired:
        # REQ-2219: an owner or steward entry that has expired names its object no longer, whether
        # Provisa held it (removed above) or the identity provider did.
        from provisa.entitlements.owners import write_object_owners

        async with db.acquire() as conn:
            await write_object_owners(conn)
    return Run(outcome.expiring, outcome.expired, tuple(removed), tuple(failed))


def _grant(g: Grant) -> Expiring:
    return Expiring(g.user_id, g.role_id, g.at, domain=g.domain_id)


async def _grants_expiring(conn: Any, now: datetime, within: timedelta) -> list[Grant]:
    rows = await conn.execute_core(
        select(
            user_role_assignments.c.user_id,
            user_role_assignments.c.role_id,
            user_role_assignments.c.domain_id,
            user_role_assignments.c.expires_at,
        ).where(
            user_role_assignments.c.expires_at > now,
            user_role_assignments.c.expires_at <= now + within,
        )
    )
    return [Grant(r.user_id, r.role_id, r.domain_id, r.expires_at) for r in rows.fetchall()]


async def _grants_told(conn: Any) -> set[tuple[str, str, str, str]]:
    rows = await conn.execute_core(
        select(admin_audit_log.c.subject_id, admin_audit_log.c.detail).where(
            admin_audit_log.c.action == GRANT_EXPIRING_SOON
        )
    )
    return {
        (subject, detail["role_id"], detail["domain_id"], detail["expires_at"])
        for subject, detail in rows.fetchall()
    }


async def announce_grants(
    db: "Database", org_id: str, swept: Sweep, now: datetime, hook: HookTarget | None
) -> tuple[str, ...]:
    """The grants' share of the run, ``db`` holding the org ``org_id``: each grant ending within
    the threshold gets one ``role.expiring_soon`` event, to ``hook`` and into the trail, and each
    grant the sweep removed as expired (``swept``) goes to ``hook`` as the expired event and its
    cancel. Each send the hook refused."""
    from provisa.core.org_membership import record_admin_action

    failed: list[str] = []
    async with db.acquire() as conn:
        ending = await _grants_expiring(conn, now, notice_window())
        told = await _grants_told(conn)
    for g in ending:
        if (g.user_id, g.role_id, g.domain_id, g.at.isoformat()) in told:
            continue
        if not await _send(hook, [offer_hooks.expiry_event("expiring_soon", _grant(g))], failed):
            continue
        await record_admin_action(
            db,
            org_id=org_id,
            action=GRANT_EXPIRING_SOON,
            actor_id="platform",
            subject_id=g.user_id,
            detail={"role_id": g.role_id, "domain_id": g.domain_id, "expires_at": g.at.isoformat()},
        )
    for g in swept.expired:
        what = _grant(g)
        await _send(
            hook,
            [offer_hooks.expiry_event("expired", what), offer_hooks.expiry_cancel(what)],
            failed,
        )
    return tuple(failed)


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
    """``org_id``'s expiry run, by the scheduler: its grants swept, its entries acted on, each
    expiry sent to the org's outbound hook where one is configured, and notices sent wherever mail
    is configured (``mail.provider``), a single-org deployment included; where it is not, the
    trail entry is the record and no grant is marked as told. Ends in :class:`HookFailed` naming
    each send the hook refused, once the rest of the run is done."""
    from provisa.api.app import state
    from provisa.core.org_settings import resolve_org_config
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        db = state.model_db
        assert db is not None, "the org's model store is bound with its runtime"
        assert state.config is not None, "the deployment's configuration is loaded"
        notify = bool(state.config.mail.provider)
        hook = offer_hooks.target_of(await resolve_org_config(db))
        now = datetime.now(UTC)
        swept = await sweep(db, org_id=org_id, notify=notify)
        failed = await announce_grants(db, org_id, swept, now, hook)
        outcome = Outcome(swept, await run(db, org_id, now, hook))
        if outcome.entries.expired:
            async with db.acquire() as conn:
                state.held_entitlements = await store.load_held(conn)
        if notify:
            await _tell(db, org_id, outcome)
    finally:
        reset_current_org(token)
    failed = (*failed, *outcome.entries.failed)
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
    if failed:
        raise HookFailed(f"org {org_id}: the request hook refused " + "; ".join(failed))
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
