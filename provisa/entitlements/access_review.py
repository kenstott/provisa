# Copyright (c) 2026 Kenneth Stott
# Canary: 2c6f0e84-71b9-4d3a-9e52-8a0d6b1f4c27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Access reviews (REQ-2266).

A review is a snapshot of the grants Provisa holds in force (``user_role_assignments``), taken on
demand (:func:`start`) or on the org's schedule (:func:`run_org`): one row per member, role and
domain, with that grant's entitlement lists and its last use. Rows are grouped by the domain whose
owner reviews them: a domain's owner is whoever ``domain_steward`` reaches it for
(``provisa.entitlements.runtime.reaches_domain``). The owner confirms or revokes each row once
(:func:`decide`); the decision keeps who made it, when and why. A revocation deletes the grant in
the same transaction, so it holds from the next request.

A role reached only through the identity provider's claims or a role mapping is the provider's to
review: Provisa holds no grant of it to revoke (REQ-2077). The review lists it read-only with its
source (:data:`IDP`, from ``user_idp_roles``); a decision on it is refused (:func:`decide`).

A scheduled review is posted to the org's signed webhook (:func:`post_scheduled`): the
``access_review.webhook`` setting names a URL and a secret, the post is signed as the offer hook
signs its requests (``provisa.core.offer_hooks``), and its outcome, a failure with its text, stays
on the review. An on-demand review is not posted.

The schedule is the org's ``access_review.cron`` (a five-field cron expression in UTC; unset, no
scheduled review). The scheduler checks it every :data:`RUN_MINUTES` and starts a review when a
cron time has passed since the last scheduled one.
"""

# Requirements: REQ-2266

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update

from provisa.auth.role_grants import in_force
from provisa.core.schema_org import (
    access_review_rows,
    access_reviews,
    domains,
    user_directory,
    user_idp_roles,
    user_role_assignments,
)
from provisa.entitlements.lists import HeldEntitlements

if TYPE_CHECKING:
    from provisa.core.offer_hooks import HookTarget
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Connection, Database

log = logging.getLogger(__name__)

#: Who started a review the schedule started.
SCHEDULE = "schedule"
#: Where a row's role is held: a grant Provisa holds, or the identity provider (read-only).
PROVISA = "provisa"
IDP = "identity_provider"
DELIVERED = "delivered"
FAILED = "failed"
CONFIRMED = "confirmed"
REVOKED = "revoked"
DECISIONS = (CONFIRMED, REVOKED)

#: The org settings block holding the schedule: ``{"cron": "<five-field cron, UTC>"}``.
SETTINGS_KEY = "access_review"
CRON_FIELD = "cron"
#: ``{"url": ..., "secret": ...}`` inside the block: where a scheduled review is posted.
WEBHOOK_FIELD = "webhook"

JOB_ID = "access_review"
RUN_MINUTES = 15

_r = access_review_rows
_g = user_role_assignments


class ReviewRefused(ValueError):
    """A decision or a schedule the review cannot take: ``code`` names why."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


@dataclass(frozen=True)
class Review:
    id: int
    started_by: str
    started_at: datetime
    rows: int
    decided: int
    webhook_status: str | None = None
    webhook_error: str | None = None


def _lists_of(held: HeldEntitlements | None, role_id: str) -> list[dict[str, Any]]:
    """The entitlement lists ``held`` carries for ``role_id``'s grant, as the row keeps them."""
    if held is None:
        return []
    return [
        {"kind": h.kind, "capability": h.capability, "identifiers": list(h.identifiers)}
        for h in held.lists
        if h.role_id == role_id
    ]


async def start(conn: "Connection", started_by: str, held: Mapping[str, HeldEntitlements]) -> int:
    """Take a review of every grant in force now, with ``held`` (the store's lists by person,
    ``provisa.entitlements.store.load_held``); returns its id."""
    result = await conn.execute_core(
        insert(access_reviews).values(started_by=started_by).returning(access_reviews.c.id)
    )
    review_id = int(result.scalar_one())
    grants = (
        await conn.execute_core(
            select(_g.c.user_id, _g.c.role_id, _g.c.domain_id, _g.c.expires_at, _g.c.last_used_at)
            .where(in_force())
            .order_by(_g.c.domain_id, _g.c.user_id, _g.c.role_id)
        )
    ).fetchall()
    held_here = {(g.user_id, g.role_id, g.domain_id) for g in grants}
    # What the identity provider's claims or role mapping give and Provisa holds no grant of.
    provider = (
        await conn.execute_core(
            select(
                user_idp_roles.c.user_id, user_idp_roles.c.role_id, user_idp_roles.c.domain_id
            ).order_by(
                user_idp_roles.c.domain_id, user_idp_roles.c.user_id, user_idp_roles.c.role_id
            )
        )
    ).fetchall()
    rows = [
        {
            "review_id": review_id,
            "user_id": g.user_id,
            "role_id": g.role_id,
            "domain_id": g.domain_id,
            "source": PROVISA,
            "entitlements": _lists_of(held.get(g.user_id), g.role_id),
            "expires_at": g.expires_at,
            "last_used_at": g.last_used_at,
        }
        for g in grants
    ] + [
        {
            "review_id": review_id,
            "user_id": p.user_id,
            "role_id": p.role_id,
            "domain_id": p.domain_id,
            "source": IDP,
            "entitlements": _lists_of(held.get(p.user_id), p.role_id),
            "expires_at": None,
            "last_used_at": None,
        }
        for p in provider
        if (p.user_id, p.role_id, p.domain_id) not in held_here
    ]
    await conn.execute_core_many(insert(_r), rows)
    return review_id


async def reviews(conn: "Connection") -> list[Review]:
    """Every review, newest first, with how many of its rows are decided."""
    decided = func.count(_r.c.decision)
    rows = (
        await conn.execute_core(
            select(
                access_reviews.c.id,
                access_reviews.c.started_by,
                access_reviews.c.started_at,
                access_reviews.c.webhook_status,
                access_reviews.c.webhook_error,
                func.count(_r.c.id).label("rows"),
                decided.label("decided"),
            )
            .select_from(access_reviews.outerjoin(_r, _r.c.review_id == access_reviews.c.id))
            .group_by(access_reviews.c.id)
            .order_by(access_reviews.c.started_at.desc(), access_reviews.c.id.desc())
        )
    ).fetchall()
    return [
        Review(
            r.id, r.started_by, r.started_at, r.rows, r.decided, r.webhook_status, r.webhook_error
        )
        for r in rows
    ]


async def review_exists(conn: "Connection", review_id: int) -> bool:
    found = await conn.execute_core(
        select(access_reviews.c.id).where(access_reviews.c.id == review_id)
    )
    return found.fetchone() is not None


async def rows_of(conn: "Connection", review_id: int) -> list[dict[str, Any]]:
    """``review_id``'s rows, by domain, member and role, each with the member's name and email."""
    u = user_directory
    result = await conn.execute_core(
        select(
            _r.c.id,
            _r.c.user_id,
            u.c.display_name,
            u.c.email,
            _r.c.role_id,
            _r.c.domain_id,
            _r.c.source,
            _r.c.entitlements,
            _r.c.expires_at,
            _r.c.last_used_at,
            _r.c.decision,
            _r.c.decided_by,
            _r.c.decided_at,
            _r.c.reason,
        )
        .select_from(_r.outerjoin(u, u.c.user_id == _r.c.user_id))
        .where(_r.c.review_id == review_id)
        .order_by(_r.c.domain_id, _r.c.user_id, _r.c.role_id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def domain_spellings(conn: "Connection") -> dict[str, tuple[str, ...]]:
    """Each domain's id, its SQL name, and its ancestors' (REQ-1363: a domains list naming a
    parent covers the domains under it): what a ``domain_steward`` list is matched against. The
    whole-org grant domain ``*`` is spelled ``*`` alone, which only a list reaching every domain
    reaches."""
    from provisa.compiler.naming import domain_to_sql_name

    rows = (await conn.execute_core(select(domains.c.id, domains.c.parent_domain_id))).fetchall()
    parent = {r.id: r.parent_domain_id for r in rows}
    out: dict[str, tuple[str, ...]] = {"*": ("*",)}
    for domain_id in parent:
        names: list[str] = []
        seen: set[str] = set()
        at: str | None = domain_id
        while at is not None and at not in seen:
            seen.add(at)
            names.extend((at, domain_to_sql_name(at)))
            at = parent.get(at)
        out[domain_id] = tuple(dict.fromkeys(n for n in names if n))
    return out


async def row(conn: "Connection", row_id: int) -> dict[str, Any] | None:
    result = await conn.execute_core(select(_r).where(_r.c.id == row_id))
    found = result.fetchone()
    return None if found is None else dict(found._mapping)


async def decide(
    conn: "Connection", row_id: int, decision: str, decided_by: str, reason: str
) -> dict[str, Any]:
    """Record ``decision`` on the row, by ``decided_by`` now, for ``reason``; a revocation deletes
    the grant it names in the same transaction. A row is decided once. Returns the decided row and
    ``revoked_grant``: whether a grant was deleted (False when it had already gone)."""
    if decision not in DECISIONS:
        raise ReviewRefused(
            "access_review.decision_invalid",
            f"A decision is one of {', '.join(DECISIONS)}",
            decision=decision,
        )
    why = reason.strip()
    if not why:
        raise ReviewRefused("access_review.reason_required", "Say why")
    updated = await conn.execute_core(
        update(_r)
        .where(_r.c.id == row_id, _r.c.decision.is_(None), _r.c.source == PROVISA)
        .values(decision=decision, decided_by=decided_by, decided_at=func.now(), reason=why)
        .returning(_r)
    )
    decided = updated.fetchone()
    if decided is None:
        found = await row(conn, row_id)
        if found is None:
            raise ReviewRefused("access_review.row_not_found", "No such review row", row=row_id)
        if found["source"] != PROVISA:
            raise ReviewRefused(
                "access_review.read_only",
                "This role is held through the identity provider; Provisa holds no grant of it "
                "to confirm or revoke",
                row=row_id,
            )
        raise ReviewRefused(
            "access_review.already_decided", "This row is already decided", row=row_id
        )
    out = dict(decided._mapping)
    out["revoked_grant"] = False
    if decision == REVOKED:
        gone = await conn.execute_core(
            delete(_g)
            .where(
                _g.c.user_id == out["user_id"],
                _g.c.role_id == out["role_id"],
                _g.c.domain_id == out["domain_id"],
            )
            .returning(_g.c.id)
        )
        out["revoked_grant"] = gone.fetchone() is not None
    return out


EXPORT_COLUMNS = (
    "domain_id",
    "user_id",
    "display_name",
    "email",
    "role_id",
    "source",
    "entitlements",
    "expires_at",
    "last_used_at",
    "decision",
    "decided_by",
    "decided_at",
    "reason",
)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return "; ".join(
            f"{e['kind']}{'/' + e['capability'] if e.get('capability') else ''}: "
            + ", ".join(e["identifiers"])
            for e in value
        )
    return str(value)


def export_csv(rows: Iterable[Mapping[str, Any]]) -> str:
    """The review as CSV: one line per row, its decision beside it."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(EXPORT_COLUMNS)
    for r in rows:
        writer.writerow([_cell(r.get(c)) for c in EXPORT_COLUMNS])
    return out.getvalue()


def check_cron(cron: str) -> str:
    """``cron`` as stored, refused unless it is a five-field cron expression."""
    from provisa.core.cron import cron_trigger

    text = cron.strip()
    try:
        cron_trigger(text, timezone=UTC)
    except ValueError as exc:
        raise ReviewRefused(
            "access_review.cron_invalid",
            f"Not a five-field cron expression: {exc}",
            cron=cron,
        ) from None
    return text


def cron_of(overrides: Mapping[str, Any]) -> str | None:
    """The org's schedule from its settings rows; None when it has set none."""
    block = overrides.get(SETTINGS_KEY) or {}
    return block.get(CRON_FIELD)


def due(cron: str, last: datetime | None, now: datetime) -> bool:
    """Whether a cron time of ``cron`` has passed since ``last`` (the last scheduled review), or,
    with none yet, within the last check interval."""
    from provisa.core.cron import cron_trigger

    since = last if last is not None else now - timedelta(minutes=RUN_MINUTES)
    fire = cron_trigger(cron, timezone=UTC).get_next_fire_time(None, since)
    return fire is not None and since < fire <= now


async def last_scheduled(conn: "Connection") -> datetime | None:
    result = await conn.execute_core(
        select(func.max(access_reviews.c.started_at)).where(access_reviews.c.started_by == SCHEDULE)
    )
    return result.scalar_one()


def check_webhook(hook: Mapping[str, Any]) -> None:
    """Refuse by name a webhook whose URL is not http(s) or that has no secret."""
    from provisa.core.offer_hooks import HookTarget, checked

    url = str(hook.get("url") or "")
    if not str(hook.get("secret") or ""):
        raise ReviewRefused(
            "access_review.webhook_secret_required",
            "A webhook needs the secret scheduled reviews are signed with",
        )
    try:
        checked(HookTarget(url=url, secret=str(hook["secret"])))
    except ValueError as exc:
        raise ReviewRefused("access_review.webhook_invalid", str(exc), url=url) from None


def webhook_of(overrides: Mapping[str, Any]) -> "HookTarget | None":
    """The org's webhook from its settings rows; None when it has set none. One missing its URL
    or its secret is refused by name (ValueError)."""
    from provisa.core.offer_hooks import HookTarget, checked

    hook = (overrides.get(SETTINGS_KEY) or {}).get(WEBHOOK_FIELD)
    if not hook:
        return None
    return checked(HookTarget(url=str(hook.get("url") or ""), secret=str(hook.get("secret") or "")))


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def webhook_body(
    review_id: int, started_at: datetime, rows: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """What the webhook receives: the review, its rows and its id, which is the same on every
    send of it so a repeat is harmless."""
    import uuid

    return {
        "id": uuid.uuid5(uuid.NAMESPACE_URL, f"provisa-access-review:{review_id}").hex,
        "kind": "access_review",
        "review_id": review_id,
        "started_at": _iso(started_at),
        "rows": [{c: _iso(r.get(c)) for c in EXPORT_COLUMNS} for r in rows],
    }


async def post_scheduled(
    db: "Database", review_id: int, overrides: Mapping[str, Any]
) -> str | None:
    """Post the scheduled review ``review_id`` to the org's webhook, signed; None when the org
    has set none. The outcome stays on the review: :data:`DELIVERED`, or :data:`FAILED` with the
    failure's text, which the review list shows. Returns the outcome."""
    from provisa.core.offer_hooks import HookFailed, deliver

    try:
        target = webhook_of(overrides)
    except ValueError as exc:
        status, error = FAILED, str(exc)
        target = None
    else:
        status, error = DELIVERED, None
    if target is None and status == DELIVERED:
        return None
    async with db.acquire() as conn:
        if target is not None:
            started = (
                await conn.execute_core(
                    select(access_reviews.c.started_at).where(access_reviews.c.id == review_id)
                )
            ).scalar_one()
            body = webhook_body(review_id, started, await rows_of(conn, review_id))
            try:
                await deliver(target, body)
            except HookFailed as exc:
                status, error = FAILED, str(exc)
        await conn.execute_core(
            update(access_reviews)
            .where(access_reviews.c.id == review_id)
            .values(webhook_status=status, webhook_error=error, webhook_at=func.now())
        )
    return status


async def run_scheduled(db: "Database", overrides: Mapping[str, Any], now: datetime) -> int | None:
    """Start the scheduled review when one is due; its id, or None when none is."""
    from provisa.entitlements import store

    cron = cron_of(overrides)
    if cron is None:
        return None
    async with db.acquire() as conn:
        async with conn.transaction():
            if not due(cron, await last_scheduled(conn), now):
                return None
            return await start(conn, SCHEDULE, await store.load_held(conn))


async def run_org(org_id: str) -> int | None:
    """``org_id``'s scheduled review check, by the scheduler."""
    from provisa.api.app import state
    from provisa.core.org_membership import record_admin_action
    from provisa.core.org_settings import read_org_overrides
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        db = state.model_db
        assert db is not None, "the org's model store is bound with its runtime"
        overrides = await read_org_overrides(db)
        review_id = await run_scheduled(db, overrides, datetime.now(UTC))
        if review_id is not None:
            if await post_scheduled(db, review_id, overrides) == FAILED:
                await record_admin_action(
                    db,
                    org_id=org_id,
                    action="access_review.webhook_failed",
                    actor_id=SCHEDULE,
                    subject_id=str(review_id),
                    detail={"review_id": review_id},
                )
                log.error(
                    "org %s: access review %d was not posted to its webhook", org_id, review_id
                )
            await record_admin_action(
                db,
                org_id=org_id,
                action="access_review.started",
                actor_id=SCHEDULE,
                subject_id=str(review_id),
                detail={"review_id": review_id},
            )
            log.info("org %s: scheduled access review %d started", org_id, review_id)
        return review_id
    finally:
        reset_current_org(token)


def run_job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Check ``org_id``'s review schedule every RUN_MINUTES. A job already held is kept."""
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
