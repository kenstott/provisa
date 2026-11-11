# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8c2e17-9d63-4f0a-8e51-6a2d0c7f3b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What accepting an offer grants (REQ-2180, WHAT ACCEPTING GRANTS).

Accepting assigns the offer's role to the requester with entitlement entries built from the
product -- its datasets (or a sample's views), its data quality results and its profiles as the
offer includes them -- so no one writes an entitlement list by hand. Masks, row rules and anything
else applied on read come from the role, defined up front.

THE DURATION. An offer's duration dates what accepting adds. A requester who does not hold the
role is granted it with the entries, both ending after the duration; one who holds it has the
entries added to that grant, each ending after the duration. Without a duration nothing accepted
expires.

EXPIRY IS THE LATEST OF ITS GRANTS. A person holds a role once: a role granted again ends at the
latest of the expiries that granted it, no expiry being later than any date. The entries are added
through the entitlement store (``store.add_to_list``), whose rules are its own.

A CANCEL IS THE MATCHING REMOVAL. Cancelling an accepted bid removes what its grant alone added:
the entries it added, with the expiry they were added with, and the role when the grant made it.
It does not bring an extended date back.
"""

# Requirements: REQ-2180, REQ-2219, REQ-2077

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, update

from provisa.core.models import DataProductOffer
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused
from provisa.core.schema_org import user_role_assignments
from provisa.entitlements import store
from provisa.entitlements.index import WILDCARD

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The entitlement list an offer's entries are added to: they are tables and views.
TABLES = "tables"


async def entries(conn: "Connection", product_id: str, offer: DataProductOffer) -> list[str]:
    """The entitlement entries accepting ``offer`` adds, as the list names them: the product's
    datasets, or a sample's views alone (never the tables beneath them); with the data quality
    results the product owns on them where the offer includes them, and their profiles where it
    includes those. A synthetic offer is refused: no read serves a role the product's synthetic
    data, and its datasets' entries would give real rows."""
    datasets = await offers.dataset_ids(conn, product_id)
    if offer.data_level == "synthetic":
        await _require_profilers(conn, datasets)
        raise OfferRefused(
            "synthetic_not_served",
            "a synthetic offer cannot be accepted yet: no read serves a role the product's "
            "synthetic data, and granting its datasets would give real records",
            status=409,
        )
    tables: list[int] = list(offer.sample_views) if offer.data_level == "sample" else datasets
    for dataset in datasets:
        if offer.dq_results:
            tables += await offers.quality_tables(conn, product_id, dataset)
        if offer.profiles:
            tables += await offers.profile_tables(conn, dataset)
    names = await offers.sql_names(conn, tables)
    return list(dict.fromkeys(names[t] for t in tables))


async def _require_profilers(conn: "Connection", datasets: Sequence[int]) -> None:
    missing = await offers.without_profiler(conn, datasets)
    if missing:
        raise OfferRefused(
            "synthetic_needs_profiler",
            f"synthetic data is generated from each dataset's profile, and "
            f"{', '.join(missing)} has no profiler",
            status=409,
            datasets=", ".join(missing),
        )


@dataclass(frozen=True)
class Held:
    """The requester's grant of a role now: its assignment's row, None when they do not hold it,
    and its expiry (None: no end)."""

    assignment_id: int | None
    expires_at: datetime | None


async def held_role(conn: "Connection", principal: str, role_id: str, now: datetime) -> Held:
    """``principal``'s grant of ``role_id`` in force at ``now``: the one ending latest, a grant
    with no end before any other."""
    rows = (
        await conn.execute_core(
            select(user_role_assignments.c.id, user_role_assignments.c.expires_at).where(
                user_role_assignments.c.user_id == principal,
                user_role_assignments.c.role_id == role_id,
            )
        )
    ).fetchall()
    live = [r for r in rows if r.expires_at is None or _aware(r.expires_at) > now]
    if not live:
        return Held(None, None)
    best = min(live, key=lambda r: (r.expires_at is not None, -_ts(r.expires_at)))
    return Held(int(best.id), None if best.expires_at is None else _aware(best.expires_at))


def _aware(at: datetime) -> datetime:
    """A stored instant as UTC: SQLite returns it naive."""
    from datetime import UTC

    return at if at.tzinfo is not None else at.replace(tzinfo=UTC)


def _ts(at: datetime | None) -> float:
    return 0.0 if at is None else _aware(at).timestamp()


def latest(a: datetime | None, b: datetime | None) -> datetime | None:
    """The later of two expiries, None (no end) being later than any date."""
    if a is None or b is None:
        return None
    return max(a, b)


def until(offer: DataProductOffer, now: datetime) -> datetime | None:
    """When what accepting ``offer`` at ``now`` adds ends: None without a duration."""
    return None if offer.duration_days is None else now + timedelta(days=offer.duration_days)


@dataclass(frozen=True)
class Extension:
    """What accepting would do to a grant the requester holds: the role's expiry now and after."""

    role_id: str
    before: datetime | None
    after: datetime | None

    def statement(self) -> str:
        """In words, as the offer's summary states it before a bid."""
        return f"extends your {self.role_id} role from {_day(self.before)} to {_day(self.after)}"


def _day(at: datetime | None) -> str:
    return "no end date" if at is None else at.strftime("%-d %b %Y")


async def extension(
    conn: "Connection", principal: str, offer: DataProductOffer, now: datetime
) -> Extension | None:
    """The extension accepting ``offer`` would make to ``principal``'s grant of its role, or None
    when it would make none: they do not hold the role, or it already ends no earlier."""
    held = await held_role(conn, principal, offer.role_id, now)
    if held.assignment_id is None or held.expires_at is None:
        return None
    after = latest(held.expires_at, until(offer, now))
    if after is not None and after <= held.expires_at:
        return None
    return Extension(offer.role_id, held.expires_at, after)


async def grant(
    conn: "Connection",
    *,
    principal: str,
    offer: DataProductOffer,
    domain_id: str,
    identifiers: Sequence[str],
    role_defaults: Mapping[str, Sequence[str]],
    set_by: str,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    """Grant ``principal`` what accepting ``offer`` gives: its role, or the role they hold
    re-granted at the latest expiry, with ``identifiers`` added to their tables list ending with
    the offer's duration. A person who did not hold the role is granted it narrowed to the
    entries; one who did keeps their list, the role's default where Provisa holds none
    (``role_defaults``), and has the entries added to it. Refused where their identity provider
    sends the list: its list wins and no write reaches it. What was granted, as a bid records it.
    The caller holds the transaction."""
    idp = await store.load_idp(conn, principal)
    if store.idp_supersedes(idp, principal, offer.role_id, TABLES):
        raise OfferRefused(
            "idp_supersedes",
            f"{principal}'s identity provider sends their {TABLES} list for role "
            f"{offer.role_id!r}; it wins, and accepting cannot add to it",
            status=409,
            role=offer.role_id,
        )
    ends = until(offer, now)
    held = await held_role(conn, principal, offer.role_id, now)
    if held.assignment_id is None:
        expires_at = ends
        await conn.upsert(
            user_role_assignments,
            {
                "user_id": principal,
                "role_id": offer.role_id,
                "domain_id": domain_id,
                "expires_at": expires_at,
                "expiry_notified_at": None,
            },
            index_elements=["user_id", "role_id", "domain_id"],
            update_columns=["expires_at", "expiry_notified_at"],
        )
        base: Sequence[str] = ()
    else:
        expires_at = latest(held.expires_at, ends)
        if expires_at != held.expires_at:
            await conn.execute_core(
                update(user_role_assignments)
                .where(user_role_assignments.c.id == held.assignment_id)
                .values(expires_at=expires_at, expiry_notified_at=None)
            )
        base = role_defaults.get(TABLES, [WILDCARD])
    added = await store.add_to_list(
        conn,
        principal,
        offer.role_id,
        TABLES,
        identifiers,
        base=base,
        capabilities=None,
        until=ends,
        set_by=set_by,
        reason=reason,
    )
    return {
        "role_id": offer.role_id,
        "role_held": held.assignment_id is not None,
        "role_expires_before": _iso(held.expires_at),
        "role_expires_at": _iso(expires_at),
        "kind": TABLES,
        "entries": list(identifiers),
        "added": list(added),
        "entries_until": _iso(ends),
    }


def _iso(at: datetime | None) -> str | None:
    return None if at is None else at.isoformat()


async def revoke(
    conn: "Connection", *, principal: str, granted: Mapping[str, Any], set_by: str, reason: str
) -> None:
    """Remove what one accepted bid granted ``principal`` (``granted``, as :func:`grant` returned
    it): the entries it added to their tables list for the role, with the expiry they were added
    with, and the role itself when they did not hold it before. The caller holds the
    transaction."""
    role_id = str(granted["role_id"])
    added = tuple(granted["added"])
    held = (await store.load_held(conn)).get(principal)
    if added and held is not None:
        listed = next(
            (
                h.identifiers
                for h in held.lists
                if h.role_id == role_id and h.kind == TABLES and h.capability is None
            ),
            None,
        )
        if listed is not None:
            kept = [i for i in listed if i not in added]
            if len(kept) != len(listed):
                await store.set_list(
                    conn, principal, role_id, TABLES, kept, set_by=set_by, reason=reason
                )
        for rule in held.expiries:
            if (
                rule.role_id == role_id
                and rule.list_kind == TABLES
                and set().union(*rule.ends.values()) == set(added)
            ):
                await store.withdraw_expiry(conn, rule, principal, set_by=set_by, reason=reason)
    if not granted["role_held"]:
        await conn.execute_core(
            delete(user_role_assignments).where(
                user_role_assignments.c.user_id == principal,
                user_role_assignments.c.role_id == role_id,
            )
        )
