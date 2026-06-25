# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Creation-request queue repository (REQ-434/063).

A governed create attempted by a user lacking the authority is persisted here as a
pending request; a rights-holder later executes or rejects it.
"""

# Requirements: REQ-063, REQ-434

from __future__ import annotations

import json

import asyncpg


async def create(  # REQ-063, REQ-434
    conn: asyncpg.Connection,
    request_type: str,
    capability: str,
    payload: dict,
    requested_by: str | None,
) -> int:
    """Persist a pending creation request; return its id."""
    rid = await conn.fetchval(
        """
        INSERT INTO creation_requests (request_type, capability, payload, requested_by)
        VALUES ($1, $2, $3::jsonb, $4)
        RETURNING id
        """,
        request_type,
        capability,
        json.dumps(payload),
        requested_by,
    )
    assert rid is not None
    return int(rid)


async def list_pending(conn: asyncpg.Connection) -> list[dict]:  # REQ-063, REQ-434
    """Return all pending requests, oldest first."""
    rows = await conn.fetch(
        "SELECT * FROM creation_requests WHERE status = 'pending' ORDER BY created_at"
    )
    return [_row_to_dict(r) for r in rows]


async def get(conn: asyncpg.Connection, request_id: int) -> dict | None:  # REQ-480
    row = await conn.fetchrow("SELECT * FROM creation_requests WHERE id = $1", request_id)
    return _row_to_dict(row) if row else None


async def mark_executed(
    conn: asyncpg.Connection, request_id: int, resolved_by: str | None
) -> bool:  # REQ-063, REQ-434
    result = await conn.execute(
        "UPDATE creation_requests SET status = 'executed', resolved_by = $2, resolved_at = NOW() "
        "WHERE id = $1 AND status = 'pending'",
        request_id,
        resolved_by,
    )
    return result == "UPDATE 1"


async def mark_rejected(  # REQ-063, REQ-434
    conn: asyncpg.Connection, request_id: int, reason: str, resolved_by: str | None
) -> bool:
    result = await conn.execute(
        "UPDATE creation_requests SET status = 'rejected', rejection_reason = $2, "
        "resolved_by = $3, resolved_at = NOW() WHERE id = $1 AND status = 'pending'",
        request_id,
        reason,
        resolved_by,
    )
    return result == "UPDATE 1"


async def add_approval(
    conn: asyncpg.Connection, request_id: int, approver: str
) -> dict | None:  # REQ-480
    """Append an approval entry. Returns updated row or None if not found/already resolved."""
    entry = json.dumps({"approver": approver, "approved_at": "now"})
    row = await conn.fetchrow(
        """
        UPDATE creation_requests
        SET approvals = approvals || $2::jsonb
        WHERE id = $1 AND status = 'pending'
        RETURNING *
        """,
        request_id,
        f"[{entry}]",
    )
    return _row_to_dict(row) if row else None


async def list_all(  # REQ-480
    conn: asyncpg.Connection,
    status: str | None = None,
    request_type: str | None = None,
) -> list[dict]:
    """Return all requests, newest first, with optional filters."""
    conditions = []
    params: list = []
    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    if request_type:
        params.append(request_type)
        conditions.append(f"request_type = ${len(params)}")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await conn.fetch(
        f"SELECT * FROM creation_requests {where} ORDER BY created_at DESC",
        *params,
    )
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row) -> dict:
    d = dict(row)
    payload = d.get("payload")
    if isinstance(payload, str):
        d["payload"] = json.loads(payload)
    return d
