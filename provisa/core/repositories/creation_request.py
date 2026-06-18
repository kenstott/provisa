# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Creation-request queue repository (REQ-434/063).

A governed create attempted by a user lacking the authority is persisted here as a
pending request; a rights-holder later executes or rejects it.
"""

from __future__ import annotations

import json

import asyncpg


async def create(
    conn: asyncpg.Connection,
    request_type: str,
    capability: str,
    payload: dict,
    requested_by: str | None,
) -> int:
    """Persist a pending creation request; return its id."""
    return await conn.fetchval(
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


async def list_pending(conn: asyncpg.Connection) -> list[dict]:
    """Return all pending requests, oldest first."""
    rows = await conn.fetch(
        "SELECT * FROM creation_requests WHERE status = 'pending' ORDER BY created_at"
    )
    return [_row_to_dict(r) for r in rows]


async def get(conn: asyncpg.Connection, request_id: int) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM creation_requests WHERE id = $1", request_id)
    return _row_to_dict(row) if row else None


async def mark_executed(conn: asyncpg.Connection, request_id: int, resolved_by: str | None) -> bool:
    result = await conn.execute(
        "UPDATE creation_requests SET status = 'executed', resolved_by = $2, resolved_at = NOW() "
        "WHERE id = $1 AND status = 'pending'",
        request_id,
        resolved_by,
    )
    return result == "UPDATE 1"


async def mark_rejected(
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


def _row_to_dict(row) -> dict:
    d = dict(row)
    payload = d.get("payload")
    if isinstance(payload, str):
        d["payload"] = json.loads(payload)
    return d
