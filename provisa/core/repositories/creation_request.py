# Copyright (c) 2026 Kenneth Stott
# Canary: 6e5e36bd-9c58-47fc-8571-5146b591ed87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Creation-request queue repository (REQ-434/063).

A governed create attempted by a user lacking the authority is persisted here as a
pending request; a rights-holder later executes or rejects it.
"""

# Requirements: REQ-063, REQ-434

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from provisa.core.schema_org import creation_requests

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def create(  # REQ-063, REQ-434
    conn: "Connection",
    request_type: str,
    capability: str,
    payload: dict,
    requested_by: str | None,
) -> int:
    """Persist a pending creation request; return its id."""
    rid = await conn.insert_returning(
        creation_requests,
        {
            "request_type": request_type,
            "capability": capability,
            "payload": payload,
            "requested_by": requested_by,
        },
        returning="id",
    )
    assert rid is not None
    return int(rid)


async def latest_status(conn: "Connection", request_type: str, name: str) -> str | None:  # REQ-209
    """Status of the most-recent request for (request_type, payload name), or None if none exist.

    Mirrors the exposure gate in app_loaders._load_tracked_functions_and_webhooks: an entity is
    approved when its latest request is 'executed'.
    """
    row = await conn.fetchrow(
        "SELECT status FROM creation_requests "
        "WHERE request_type = $1 AND payload->>'name' = $2 "
        "ORDER BY id DESC LIMIT 1",
        request_type,
        name,
    )
    return None if row is None else row["status"]


async def ensure_executed(
    conn: "Connection", request_type: str, name: str, resolved_by: str | None
) -> None:  # REQ-209
    """Idempotently pre-approve a config-declared entity so the REQ-209 exposure gate passes.

    The config file is the trusted source of truth (DB functions load ungated), so a webhook it
    declares is approved without a steward round-trip. No-op when the latest request is already
    'executed' — keeps startup idempotent across restarts (no unbounded row growth).
    """
    if await latest_status(conn, request_type, name) == "executed":
        return
    await conn.insert_returning(
        creation_requests,
        {
            "request_type": request_type,
            "capability": f"{request_type}_registration",
            "payload": {"name": name},
            "requested_by": resolved_by,
            "status": "executed",
            "resolved_by": resolved_by,
            "resolved_at": func.now(),
        },
        returning="id",
    )


async def list_pending(conn: "Connection") -> list[dict]:  # REQ-063, REQ-434
    """Return all pending requests, oldest first."""
    result = await conn.execute_core(
        select(creation_requests)
        .where(creation_requests.c.status == "pending")
        .order_by(creation_requests.c.created_at)
    )
    return [_row_to_dict(r) for r in result.fetchall()]


async def get(conn: "Connection", request_id: int) -> dict | None:  # REQ-480
    result = await conn.execute_core(
        select(creation_requests).where(creation_requests.c.id == request_id)
    )
    row = result.fetchone()
    return _row_to_dict(row) if row is not None else None


async def mark_executed(
    conn: "Connection", request_id: int, resolved_by: str | None
) -> bool:  # REQ-063, REQ-434
    result = await conn.execute_core(
        update(creation_requests)
        .where(
            creation_requests.c.id == request_id,
            creation_requests.c.status == "pending",
        )
        .values(status="executed", resolved_by=resolved_by, resolved_at=func.now())
    )
    return (result.rowcount or 0) > 0


async def mark_rejected(  # REQ-063, REQ-434
    conn: "Connection", request_id: int, reason: str, resolved_by: str | None
) -> bool:
    result = await conn.execute_core(
        update(creation_requests)
        .where(
            creation_requests.c.id == request_id,
            creation_requests.c.status == "pending",
        )
        .values(
            status="rejected",
            rejection_reason=reason,
            resolved_by=resolved_by,
            resolved_at=func.now(),
        )
    )
    return (result.rowcount or 0) > 0


async def add_approval(
    conn: "Connection", request_id: int, approver: str
) -> dict | None:  # REQ-480
    """Append an approval entry. Returns updated row or None if not found/already resolved."""
    entry = {"approver": approver, "approved_at": "now"}
    async with conn.transaction():
        result = await conn.execute_core(
            select(creation_requests).where(
                creation_requests.c.id == request_id,
                creation_requests.c.status == "pending",
            )
        )
        row = result.fetchone()
        if row is None:
            return None
        approvals = list(row._mapping["approvals"] or [])
        approvals.append(entry)
        await conn.execute_core(
            update(creation_requests)
            .where(creation_requests.c.id == request_id)
            .values(approvals=approvals)
        )
        result = await conn.execute_core(
            select(creation_requests).where(creation_requests.c.id == request_id)
        )
        updated = result.fetchone()
    return _row_to_dict(updated) if updated is not None else None


async def list_all(  # REQ-480
    conn: "Connection",
    status: str | None = None,
    request_type: str | None = None,
) -> list[dict]:
    """Return all requests, newest first, with optional filters."""
    stmt = select(creation_requests)
    if status:
        stmt = stmt.where(creation_requests.c.status == status)
    if request_type:
        stmt = stmt.where(creation_requests.c.request_type == request_type)
    stmt = stmt.order_by(creation_requests.c.created_at.desc())
    result = await conn.execute_core(stmt)
    return [_row_to_dict(r) for r in result.fetchall()]


def _row_to_dict(row) -> dict:
    return dict(row._mapping)
