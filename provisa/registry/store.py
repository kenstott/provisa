# Copyright (c) 2026 Kenneth Stott
# Canary: c8570acb-d33d-42de-85fc-b3a29183de80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Persisted query storage in PG (REQ-022, REQ-023, REQ-024, REQ-026)."""

from __future__ import annotations

import json
import uuid

import asyncpg


async def submit(
    conn: asyncpg.Connection,
    query_text: str,
    compiled_sql: str,
    target_tables: list[int],
    developer_id: str,
    parameter_schema: dict | None = None,
    permitted_outputs: list[str] | None = None,
) -> int:
    """Submit a query for approval. Returns the query ID."""
    row = await conn.fetchrow(
        """
        INSERT INTO persisted_queries
            (query_text, compiled_sql, target_tables, parameter_schema,
             permitted_outputs, developer_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """,
        query_text,
        compiled_sql,
        target_tables,
        json.dumps(parameter_schema) if parameter_schema else None,
        permitted_outputs or ["json"],
        developer_id,
    )
    query_id = row["id"]
    await _log(conn, query_id, "submitted", developer_id)
    return query_id


async def approve(
    conn: asyncpg.Connection,
    query_id: int,
    approver_id: str,
    routing_hint: str | None = None,
    cache_ttl: int | None = None,
    visible_to: list[str] | None = None,
) -> str:
    """Approve a query. Assigns a stable ID. Returns the stable ID."""
    stable_id = str(uuid.uuid4())
    await conn.execute(
        """
        UPDATE persisted_queries
        SET status = 'approved', stable_id = $1, approved_by = $2,
            approved_at = NOW(), routing_hint = $3, cache_ttl = $4,
            visible_to = $5, updated_at = NOW()
        WHERE id = $6
        """,
        stable_id, approver_id, routing_hint, cache_ttl, visible_to or [], query_id,
    )
    await _log(conn, query_id, "approved", approver_id)
    return stable_id


async def deprecate(
    conn: asyncpg.Connection,
    query_id: int,
    actor_id: str,
    replacement_stable_id: str | None = None,
) -> None:
    """Deprecate a query, optionally pointing to a replacement."""
    await conn.execute(
        """
        UPDATE persisted_queries
        SET status = 'deprecated', deprecated_by = $1, updated_at = NOW()
        WHERE id = $2
        """,
        replacement_stable_id, query_id,
    )
    await _log(conn, query_id, "deprecated", actor_id,
               reason=f"replacement: {replacement_stable_id}" if replacement_stable_id else None)


async def flag_for_review(
    conn: asyncpg.Connection,
    query_id: int,
    reason: str,
) -> None:
    """Flag an approved query for re-review (REQ-025)."""
    await conn.execute(
        """
        UPDATE persisted_queries
        SET status = 'flagged', updated_at = NOW()
        WHERE id = $1
        """,
        query_id,
    )
    await _log(conn, query_id, "flagged", "system", reason=reason)


async def get_by_id(conn: asyncpg.Connection, query_id: int) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM persisted_queries WHERE id = $1", query_id)
    return dict(row) if row else None


async def get_by_stable_id(conn: asyncpg.Connection, stable_id: str) -> dict | None:
    row = await conn.fetchrow(
        "SELECT * FROM persisted_queries WHERE stable_id = $1", stable_id,
    )
    return dict(row) if row else None


async def list_pending(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM persisted_queries WHERE status = 'pending' ORDER BY created_at",
    )
    return [dict(r) for r in rows]


async def list_approved(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM persisted_queries WHERE status = 'approved' ORDER BY approved_at",
    )
    return [dict(r) for r in rows]


async def get_log(conn: asyncpg.Connection, query_id: int) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM approval_log WHERE query_id = $1 ORDER BY created_at",
        query_id,
    )
    return [dict(r) for r in rows]


async def _log(
    conn: asyncpg.Connection,
    query_id: int,
    action: str,
    actor_id: str,
    reason: str | None = None,
) -> None:
    await conn.execute(
        "INSERT INTO approval_log (query_id, action, actor_id, reason) VALUES ($1, $2, $3, $4)",
        query_id, action, actor_id, reason,
    )
