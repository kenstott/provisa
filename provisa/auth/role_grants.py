# Copyright (c) 2026 Kenneth Stott
# Canary: cabbce3c-e8ec-4219-a0ed-7e0c63c17bb8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Time-limited role grants and the removal of unused ones (REQ-2077).

Only the grants Provisa holds (``user_role_assignments``) expire or are removed. A role that comes
from an identity provider's claims or a role mapping is the provider's to time-limit; derived
roles and meta-roles are made per request and never expire.

- A grant with ``expires_at`` is absent from the held roles from that instant, on every request
  (:func:`in_force`, read where the held roles are resolved).
- ``last_used_at`` is the last day a request depended on the grant (:func:`used_roles`), written
  at most once a day by the audit writer (:func:`mark_used`).
- A role's ``inactivity_days``: a grant of it unused for longer is removed.
- A grant given again keeps the latest of its expiries, and no expiry is later than any date
  (:func:`regrant`). A rule that ends the whole grant is the grant's own expiry: it sets or
  shortens ``expires_at`` (:func:`end_grant`).
- The sweep (:func:`sweep`) deletes expired and unused grants into the org's administrative trail
  (``role.expired``, ``role.removed_inactive``) and finds the ones ending within
  ``auth.role_grant_notice_days``, the one expiring-soon setting for a grant and its entries. It
  runs in each org's one expiry run, which tells of them (``provisa.entitlements.expiry_run``).
"""

# Requirements: REQ-2077

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, case, delete, func, literal, null, or_, select, update

from provisa.core.schema_org import roles, user_directory, user_role_assignments

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

NOTICE_SETTING = "auth.role_grant_notice_days"

_t = user_role_assignments


def in_force() -> Any:
    """The grants held now: no expiry, or one still ahead."""
    return or_(_t.c.expires_at.is_(None), _t.c.expires_at > func.now())


def _latest(held: Any, given: Any) -> Any:
    """The later of two expiries, where no expiry (NULL) is later than any date."""
    return case(
        (or_(held.is_(None), given.is_(None)), null()),
        else_=func.greatest(held, given),
    )


async def regrant(
    conn: Any, user_id: str, role_id: str, domain_id: str, expires_at: datetime | None
) -> None:
    """Grant ``role_id`` in ``domain_id`` to ``user_id`` until ``expires_at`` (None: no end). A
    person holds a role once: given again, the grant keeps the latest of its expiries, and its
    holder is told again before an end it moved."""
    given = literal(expires_at, DateTime(timezone=True))
    latest = _latest(_t.c.expires_at, given)
    await conn.upsert(
        _t,
        {
            "user_id": user_id,
            "role_id": role_id,
            "domain_id": domain_id,
            "expires_at": expires_at,
            "expiry_notified_at": None,
        },
        index_elements=["user_id", "role_id", "domain_id"],
        update_columns=[],
        set_extra={
            "expires_at": latest,
            "expiry_notified_at": case(
                (latest.is_distinct_from(_t.c.expires_at), null()),
                else_=_t.c.expiry_notified_at,
            ),
        },
    )


async def end_grant(conn: Any, user_id: str, role_id: str, ends_at: datetime) -> int:
    """End ``user_id``'s grants of ``role_id`` at ``ends_at``: a rule that ends a whole grant is
    the grant's own expiry. It sets the expiry, or shortens one that is later, and never extends
    one. Returns how many grants it holds; 0 when Provisa holds none of the role for them."""
    result = await conn.execute_core(
        update(_t)
        .where(_t.c.user_id == user_id, _t.c.role_id == role_id, in_force())
        .values(
            expires_at=func.least(func.coalesce(_t.c.expires_at, ends_at), ends_at),
            expiry_notified_at=case(
                (or_(_t.c.expires_at.is_(None), _t.c.expires_at > ends_at), null()),
                else_=_t.c.expiry_notified_at,
            ),
        )
        .returning(_t.c.id)
    )
    return len(result.fetchall())


@dataclass(frozen=True)
class HeldGrant:
    """A grant Provisa holds now: its role, domain and expiry (None: no end)."""

    role_id: str
    domain_id: str
    expires_at: datetime | None


async def held_grants(conn: Any, user_id: str) -> list[HeldGrant]:
    """``user_id``'s grants in force, by role and domain."""
    rows = await conn.execute_core(
        select(_t.c.role_id, _t.c.domain_id, _t.c.expires_at)
        .where(_t.c.user_id == user_id, in_force())
        .order_by(_t.c.role_id, _t.c.domain_id)
    )
    return [HeldGrant(r.role_id, r.domain_id, r.expires_at) for r in rows.fetchall()]


async def grant_ended(conn: Any, user_id: str, role_id: str) -> bool:
    """Whether ``user_id``'s grants of ``role_id`` have all expired, and the sweep has not yet
    removed them."""
    rows = await conn.execute_core(
        select(_t.c.expires_at, in_force().label("held")).where(
            _t.c.user_id == user_id, _t.c.role_id == role_id
        )
    )
    held = [r.held for r in rows.fetchall()]
    return bool(held) and not any(held)


def grant_end(grants: Iterable[HeldGrant], role_id: str) -> tuple[bool, datetime | None]:
    """Whether ``grants`` hold ``role_id``, and when the role ends: the latest of its grants'
    expiries, None when one has no end."""
    ends = [g.expires_at for g in grants if g.role_id == role_id]
    if not ends:
        return False, None
    return True, None if None in ends else max(e for e in ends if e is not None)


async def holder_emails(conn: Any) -> dict[str, tuple[str, ...]]:
    """Each role's holders' email addresses, by role id (REQ-2118): the members holding it now,
    directly, whose directory entry has an address. A data product's owners in a catalog that
    names owners by address are its owner role's holders, from here."""
    rows = (
        await conn.execute_core(
            select(_t.c.role_id, user_directory.c.email)
            .join(user_directory, user_directory.c.user_id == _t.c.user_id)
            .where(in_force(), user_directory.c.email.is_not(None), user_directory.c.email != "")
            .distinct()
            .order_by(_t.c.role_id, user_directory.c.email)
        )
    ).fetchall()
    out: dict[str, list[str]] = {}
    for role_id, email in rows:
        out.setdefault(role_id, []).append(email)
    return {role_id: tuple(emails) for role_id, emails in out.items()}


def used_roles(role_id: str, table_ids: Iterable[int], state: Any) -> tuple[str, ...]:
    """The roles a served statement depended on. A single acting role served it. A meta-role's
    members are those granted a table the statement read: acting as several roles (or all of
    them) is not a use of each."""
    from provisa.security.meta_role import is_meta_role_id

    if not is_meta_role_id(role_id):
        return (role_id,)
    from provisa.api.role_surface import built_surface

    touched = set(table_ids)
    surfaces = state.surfaces
    return tuple(
        m
        for m in state.meta_roles[role_id]
        if m in surfaces
        and any(tm.table_id in touched for tm in built_surface(surfaces[m]).context.tables.values())
    )


def used_roles_of(
    role_id: str, table_ids: "list[int] | Callable[[], tuple[int, ...]]", state: Any
) -> Callable[[], tuple[str, ...]]:
    """:func:`used_roles`, resolved on the audit writer's thread where ``table_ids`` is resolved,
    against the model the statement was governed under."""
    from types import SimpleNamespace

    from provisa.security.meta_role import is_meta_role_id

    if not is_meta_role_id(role_id):
        return lambda: (role_id,)
    at = SimpleNamespace(surfaces=state.surfaces, meta_roles=state.meta_roles)

    def resolve() -> tuple[str, ...]:
        return used_roles(role_id, table_ids() if callable(table_ids) else table_ids, at)

    return resolve


async def mark_used(db: "Database", user_id: str, role_ids: Iterable[str]) -> None:
    """Record that ``user_id``'s grants of ``role_ids`` were used today: once a day per grant."""
    ids = list(role_ids)
    if not ids:
        return
    async with db.acquire() as conn:
        await conn.execute_core(
            update(_t)
            .where(
                _t.c.user_id == user_id,
                _t.c.role_id.in_(ids),
                in_force(),
                or_(
                    _t.c.last_used_at.is_(None),
                    _t.c.last_used_at < func.date_trunc("day", func.now()),
                ),
            )
            .values(last_used_at=func.now())
        )


def _last_activity() -> Any:
    # max(last_used_at, created_at): Postgres' greatest() passes over a NULL last use.
    return func.greatest(_t.c.last_used_at, _t.c.created_at)


def _inactive_after() -> Any:
    """When a grant of a role with ``inactivity_days`` is removed for lack of use."""
    return _last_activity() + func.make_interval(0, 0, 0, roles.c.inactivity_days)


@dataclass(frozen=True)
class Grant:
    user_id: str
    role_id: str
    domain_id: str
    at: datetime  # when it expires, or is removed for lack of use


def _grant(row: Any) -> Grant:
    return Grant(row.user_id, row.role_id, row.domain_id, row.at)


def _detail(g: Grant, when: str) -> dict[str, str]:
    return {"role_id": g.role_id, "domain_id": g.domain_id, when: g.at.isoformat()}


async def _expired(conn: Any) -> list[Grant]:
    result = await conn.execute_core(
        delete(_t)
        .where(_t.c.expires_at <= func.now())
        .returning(_t.c.user_id, _t.c.role_id, _t.c.domain_id, _t.c.expires_at.label("at"))
    )
    return [_grant(r) for r in result.fetchall()]


async def _inactive(conn: Any) -> list[Grant]:
    due = (
        select(_t.c.id, _inactive_after().label("at"))
        .select_from(_t.join(roles, roles.c.id == _t.c.role_id))
        .where(roles.c.inactivity_days.is_not(None), _inactive_after() <= func.now())
    ).subquery()
    result = await conn.execute_core(
        delete(_t)
        .where(_t.c.id == due.c.id)
        .returning(_t.c.user_id, _t.c.role_id, _t.c.domain_id, due.c.at)
    )
    return [_grant(r) for r in result.fetchall()]


async def _expiring(conn: Any, within: timedelta) -> list[Grant]:
    """Grants that expire within ``within`` whose holder has not been told."""
    result = await conn.execute_core(
        update(_t)
        .where(
            _t.c.expires_at.is_not(None),
            _t.c.expires_at <= func.now() + within,
            _t.c.expiry_notified_at.is_(None),
        )
        .values(expiry_notified_at=func.now())
        .returning(_t.c.user_id, _t.c.role_id, _t.c.domain_id, _t.c.expires_at.label("at"))
    )
    return [_grant(r) for r in result.fetchall()]


async def _going_unused(conn: Any, within: timedelta) -> list[Grant]:
    """Grants removed for lack of use within ``within`` whose holder has not been told since the
    grant was last used."""
    due = (
        select(_t.c.id, _inactive_after().label("at"))
        .select_from(_t.join(roles, roles.c.id == _t.c.role_id))
        .where(
            roles.c.inactivity_days.is_not(None),
            _inactive_after() <= func.now() + within,
            or_(
                _t.c.inactive_notified_at.is_(None),
                _t.c.inactive_notified_at < _last_activity(),
            ),
            # One that expires first is told of its expiry instead.
            or_(_t.c.expires_at.is_(None), _t.c.expires_at > _inactive_after()),
        )
    ).subquery()
    result = await conn.execute_core(
        update(_t)
        .where(_t.c.id == due.c.id)
        .values(inactive_notified_at=func.now())
        .returning(_t.c.user_id, _t.c.role_id, _t.c.domain_id, due.c.at)
    )
    return [_grant(r) for r in result.fetchall()]


async def emails_of(conn: Any, user_ids: Iterable[str]) -> dict[str, str]:
    """Each of ``user_ids`` with an address in the directory, and that address."""
    ids = list(set(user_ids))
    if not ids:
        return {}
    result = await conn.execute_core(
        select(user_directory.c.user_id, user_directory.c.email).where(
            user_directory.c.user_id.in_(ids), user_directory.c.email.is_not(None)
        )
    )
    return {r.user_id: r.email for r in result.fetchall()}


@dataclass(frozen=True)
class Sweep:
    expired: list[Grant]
    inactive: list[Grant]
    expiring: list[Grant]
    going_unused: list[Grant]


def notice_window() -> timedelta:
    from provisa.core import settings_registry

    return timedelta(days=int(settings_registry.value(NOTICE_SETTING)))


async def sweep(db: "Database", *, notify: bool) -> Sweep:
    """Remove ``db``'s expired and unused grants and find the ones to give notice of. With
    ``notify`` False (no mail is configured) no grant is marked as told."""
    from provisa.core.org_membership import record_admin_action

    async with db.acquire() as conn:
        async with conn.transaction():
            expired = await _expired(conn)
            inactive = await _inactive(conn)
    for g in expired:
        await record_admin_action(
            db,
            action="role.expired",
            actor_id="platform",
            subject_id=g.user_id,
            detail=_detail(g, "expires_at"),
        )
    for g in inactive:
        await record_admin_action(
            db,
            action="role.removed_inactive",
            actor_id="platform",
            subject_id=g.user_id,
            detail=_detail(g, "unused_until"),
        )
    expiring: list[Grant] = []
    going_unused: list[Grant] = []
    if notify:
        within = notice_window()
        async with db.acquire() as conn:
            expiring = await _expiring(conn, within)
            going_unused = await _going_unused(conn, within)
    return Sweep(expired, inactive, expiring, going_unused)


async def send_notice(org_id: str, to: str, subject: str, body: str, *, kind: str) -> None:
    """One notice through the org mail channel, recorded as every send is (REQ-1576) under
    ``kind``. A failed send is logged and recorded, never raised: what it tells of has happened."""
    from provisa.api.app import state
    from provisa.core.mail import MailMessage, email_sender
    from provisa.core.mail_stats import MailAttempt, record

    assert state.config is not None and state.admin_db is not None
    provider = state.config.mail.provider
    try:
        email_sender(state.config.mail).send(MailMessage(to=to, subject=subject, body=body))
    except Exception as exc:  # recorded and logged below; the next grant's notice still goes
        log.error("%s notice to %s for org %s not delivered: %s", kind, to, org_id, exc)
        await record(
            state.admin_db,
            MailAttempt(
                provider=provider,
                kind=kind,
                recipient=to,
                succeeded=False,
                org_id=org_id,
                error=str(exc),
            ),
        )
        return
    await record(
        state.admin_db,
        MailAttempt(provider=provider, kind=kind, recipient=to, succeeded=True, org_id=org_id),
    )
