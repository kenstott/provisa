# Copyright (c) 2026 Kenneth Stott
# Canary: bf036d5e-ee0f-41b6-a93a-098ac3f2aac9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""IS MAIL ACTUALLY GOING OUT (REQ-1576).

A configured transport tells an operator nothing about delivery. The failures that matter -- a
rejected API key, an expired relay credential, a sender domain that was never verified -- happen at
send time and, until now, were visible only in the server log of whoever had a shell on the node,
or not at all: an invitation nobody receives looks exactly like an invitation nobody sent.

So every attempt is recorded, failures included, and the Email settings page answers "is mail
working" out of the record rather than out of the config. Recording NEVER breaks the send it
observes -- a registry the writer cannot reach must not turn a delivered message into a failed one
-- so ``record`` swallows its own storage error and logs it, which is the one place in this module
where that is the correct behaviour rather than the silent handling the codebase forbids.
"""

# Requirements: REQ-1576

# complexity-gate: allow-ble=1 reason="recording an attempt must not fail the attempt: a registry
# error here would turn a delivered message into a reported failure"

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, func, select

from provisa.core.schema_admin import mail_events

log = logging.getLogger(__name__)

#: How many recent attempts the surface lists. Enough to see a pattern, short enough to read.
RECENT_LIMIT = 20


@dataclass(frozen=True)
class MailAttempt:
    provider: str
    kind: str
    recipient: str
    succeeded: bool
    org_id: str | None = None
    error: str | None = None
    requested_by: str | None = None


async def record(db, attempt: MailAttempt) -> None:
    """Write one attempt to the registry. Never raises."""
    try:
        async with db.acquire() as conn:
            await conn.execute_core(
                mail_events.insert().values(
                    provider=attempt.provider,
                    kind=attempt.kind,
                    recipient=attempt.recipient,
                    org_id=attempt.org_id,
                    succeeded=attempt.succeeded,
                    error=attempt.error,
                    requested_by=attempt.requested_by,
                )
            )
    except Exception as exc:  # see the module docstring: never fail the send being recorded
        log.warning("mail attempt to %s could not be recorded: %s", attempt.recipient, exc)


async def stats(db) -> dict[str, Any]:
    """The basic counts a platform_admin reads to answer "is mail working".

    Attempted / delivered / failed over the last day and the last week, the last success, the last
    failure with the transport's own message, and the most recent attempts in order.
    """
    now = datetime.now(timezone.utc)
    async with db.acquire() as conn:
        windows = {}
        for name, since in (("day", now - timedelta(days=1)), ("week", now - timedelta(days=7))):
            row = (
                (
                    await conn.execute_core(
                        select(
                            func.count().label("attempted"),
                            func.count().filter(mail_events.c.succeeded).label("delivered"),
                            func.count().filter(~mail_events.c.succeeded).label("failed"),
                        ).where(mail_events.c.sent_at >= since)
                    )
                )
                .mappings()
                .one()
            )
            windows[name] = dict(row)
        total = (
            (
                await conn.execute_core(
                    select(
                        func.count().label("attempted"),
                        func.count().filter(mail_events.c.succeeded).label("delivered"),
                        func.count().filter(~mail_events.c.succeeded).label("failed"),
                    )
                )
            )
            .mappings()
            .one()
        )
        last_success = await _last(conn, succeeded=True)
        last_failure = await _last(conn, succeeded=False)
        recent = [
            _event(row)
            for row in (
                await conn.execute_core(
                    select(mail_events)
                    .order_by(desc(mail_events.c.sent_at), desc(mail_events.c.id))
                    .limit(RECENT_LIMIT)
                )
            ).mappings()
        ]
    return {
        "total": dict(total),
        "windows": windows,
        "last_success": last_success,
        "last_failure": last_failure,
        "recent": recent,
    }


async def _last(conn, *, succeeded: bool) -> dict | None:
    row = (
        (
            await conn.execute_core(
                select(mail_events)
                .where(mail_events.c.succeeded == succeeded)
                .order_by(desc(mail_events.c.sent_at), desc(mail_events.c.id))
                .limit(1)
            )
        )
        .mappings()
        .first()
    )
    return _event(row) if row is not None else None


def _event(row) -> dict:
    return {
        "sent_at": row["sent_at"].isoformat() if row["sent_at"] is not None else None,
        "provider": row["provider"],
        "kind": row["kind"],
        "recipient": row["recipient"],
        "org_id": row["org_id"],
        "succeeded": row["succeeded"],
        "error": row["error"],
        "requested_by": row["requested_by"],
    }
