# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1d6b0e-8a52-4c71-9e0d-5b7c2a94e613
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Grants waiting on a typed email (REQ-2219, ONE API, THREE WAYS TO USE IT).

A person not yet seen is granted by typed email; the grant is shown as pending and attaches when a
sign-in token's email matches. Attaching is the write API's own write (:func:`store.write_list`),
and where that sign-in's identity provider sends the list for the role and kind, the grant is
refused there as the write API refuses it.

Append-only, keyed by (email, role_id, kind): the last row is the grant's state.
"""

# Requirements: REQ-2219

from __future__ import annotations

import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import insert, select

from provisa.core.schema_org import entitlement_pending
from provisa.entitlements import store
from provisa.entitlements.lists import IdpLists

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.core.database import Connection

ENDED = "ended"
ATTACHED = "attached"
REFUSED = "refused"


def normalize_email(email: str) -> str:
    """An email as grants are keyed and matched by: trimmed, lower-cased."""
    return email.strip().lower()


def _lock_key(email: str) -> int:
    return zlib.crc32(f"entitlement_pending:{normalize_email(email)}".encode())


@dataclass(frozen=True)
class PendingGrant:
    email: str
    role_id: str
    kind: str
    identifiers: tuple[str, ...]
    expires_at: "datetime | None"
    set_by: str
    reason: str


async def _append(
    conn: "Connection",
    grant: PendingGrant,
    *,
    outcome: str | None = None,
    principal: str | None = None,
) -> None:
    await conn.execute_core(
        insert(entitlement_pending).values(
            email=grant.email,
            role_id=grant.role_id,
            kind=grant.kind,
            identifiers=list(grant.identifiers),
            expires_at=grant.expires_at,
            set_by=grant.set_by,
            reason=grant.reason,
            outcome=outcome,
            principal=principal,
        )
    )


async def set_pending(
    conn: "Connection",
    email: str,
    role_id: str,
    kind: str,
    identifiers: Iterable[str],
    *,
    expires_at: "datetime | None",
    set_by: str,
    reason: str,
) -> PendingGrant:
    """Grant ``email``'s list for ``role_id`` and ``kind``, replacing what was waiting there."""
    grant = PendingGrant(
        normalize_email(email), role_id, kind, tuple(identifiers), expires_at, set_by, reason
    )
    await _append(conn, grant)
    return grant


async def load_pending(conn: "Connection", email: str | None = None) -> list[PendingGrant]:
    """The grants waiting now, oldest first; ``email`` reads one address's."""
    query = select(entitlement_pending)
    if email is not None:
        query = query.where(entitlement_pending.c.email == normalize_email(email))
    rows = await conn.execute_core(
        query.order_by(entitlement_pending.c.valid_from, entitlement_pending.c.id)
    )
    last: dict[tuple[str, str, str], PendingGrant | None] = {}
    for row in rows.fetchall():
        key = (row.email, row.role_id, row.kind)
        last.pop(key, None)  # keep the order of each key's latest row
        last[key] = (
            None
            if row.outcome is not None
            else PendingGrant(
                row.email,
                row.role_id,
                row.kind,
                tuple(row.identifiers),
                row.expires_at,
                row.set_by,
                row.reason,
            )
        )
    return [grant for grant in last.values() if grant is not None]


async def end_pending(
    conn: "Connection", email: str, role_id: str, kind: str, *, set_by: str, reason: str
) -> PendingGrant | None:
    """End the grant waiting on ``email`` for ``role_id`` and ``kind``; None when none waits."""
    waiting = [g for g in await load_pending(conn, email) if (g.role_id, g.kind) == (role_id, kind)]
    if not waiting:
        return None
    grant = waiting[0]
    await _append(
        conn, PendingGrant(grant.email, role_id, kind, (), None, set_by, reason), outcome=ENDED
    )
    return grant


async def attach(
    conn: "Connection", principal: str, email: str, idp: IdpLists
) -> list[tuple[PendingGrant, str]]:
    """Attach the grants waiting on ``email`` to ``principal``, whose sign-in token carried that
    email and ``idp``'s lists. Each becomes the write API's write, as set by whoever granted it,
    or is refused where ``idp`` sends that role and kind's list. The caller holds the
    transaction; returns each grant with what became of it. Two sign-ins at once attach once:
    the second waits on the first, and finds nothing waiting."""
    await conn.advisory_xact_lock(_lock_key(email))
    out: list[tuple[PendingGrant, str]] = []
    for grant in await load_pending(conn, email):
        if store.idp_supersedes({principal: idp}, principal, grant.role_id, grant.kind):
            outcome = REFUSED
        else:
            outcome = ATTACHED
            await store.write_list(
                conn,
                principal,
                grant.role_id,
                grant.kind,
                grant.identifiers,
                expires_at=grant.expires_at,
                set_by=grant.set_by,
                reason=grant.reason,
            )
        await _append(conn, grant, outcome=outcome, principal=principal)
        out.append((grant, outcome))
    return out
