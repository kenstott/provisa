# Copyright (c) 2025 Kenneth Stott
# Canary: 6a559b37-323f-4083-b516-6bd16af803e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PG repository for API endpoint candidates (Phase U)."""

from __future__ import annotations

import json

import asyncpg

from provisa.api_source.models import ApiColumn, ApiEndpoint, ApiEndpointCandidate


async def store_candidates(
    conn: asyncpg.Connection,
    source_id: str,
    candidates: list[ApiEndpointCandidate],
) -> list[int]:
    """Store discovered candidates. Returns list of inserted IDs."""
    ids: list[int] = []
    for c in candidates:
        columns_json = json.dumps([col.model_dump() for col in c.columns])
        row = await conn.fetchrow(
            """
            INSERT INTO api_endpoint_candidates (source_id, path, method, table_name, columns, status)
            VALUES ($1, $2, $3, $4, $5::jsonb, 'discovered')
            ON CONFLICT (source_id, path, method) DO UPDATE
                SET table_name = EXCLUDED.table_name, columns = EXCLUDED.columns, status = 'discovered'
            RETURNING id
            """,
            source_id, c.path, c.method, c.table_name, columns_json,
        )
        ids.append(row["id"])
    return ids


async def list_candidates(
    conn: asyncpg.Connection,
    source_id: str | None = None,
) -> list[ApiEndpointCandidate]:
    """List pending (discovered) candidates."""
    if source_id:
        rows = await conn.fetch(
            "SELECT * FROM api_endpoint_candidates WHERE source_id = $1 AND status = 'discovered' ORDER BY id",
            source_id,
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM api_endpoint_candidates WHERE status = 'discovered' ORDER BY id",
        )
    return [
        ApiEndpointCandidate(
            id=r["id"],
            source_id=r["source_id"],
            path=r["path"],
            method=r["method"],
            table_name=r["table_name"],
            columns=[ApiColumn(**c) for c in json.loads(r["columns"])],
            status=r["status"],
        )
        for r in rows
    ]


async def accept_candidate(
    conn: asyncpg.Connection,
    candidate_id: int,
    overrides: dict | None = None,
) -> ApiEndpoint:
    """Accept a candidate: register it as an endpoint."""
    row = await conn.fetchrow(
        "SELECT * FROM api_endpoint_candidates WHERE id = $1", candidate_id,
    )
    if row is None:
        raise ValueError(f"Candidate {candidate_id} not found")
    if row["status"] != "discovered":
        raise ValueError(f"Candidate {candidate_id} status is {row['status']!r}, not 'discovered'")

    overrides = overrides or {}
    table_name = overrides.get("table_name", row["table_name"])
    columns = row["columns"]
    ttl = overrides.get("ttl", 300)
    response_root = overrides.get("response_root")

    # Insert endpoint
    ep_row = await conn.fetchrow(
        """
        INSERT INTO api_endpoints (source_id, path, method, table_name, columns, ttl, response_root)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
        RETURNING id
        """,
        row["source_id"], row["path"], row["method"],
        table_name, columns, ttl, response_root,
    )

    # Update candidate status
    await conn.execute(
        "UPDATE api_endpoint_candidates SET status = 'registered' WHERE id = $1",
        candidate_id,
    )

    columns_parsed = [ApiColumn(**c) for c in json.loads(columns)]
    return ApiEndpoint(
        id=ep_row["id"],
        source_id=row["source_id"],
        path=row["path"],
        method=row["method"],
        table_name=table_name,
        columns=columns_parsed,
        ttl=ttl,
        response_root=response_root,
    )


async def reject_candidate(
    conn: asyncpg.Connection,
    candidate_id: int,
) -> None:
    """Reject a candidate."""
    result = await conn.execute(
        "UPDATE api_endpoint_candidates SET status = 'rejected' WHERE id = $1 AND status = 'discovered'",
        candidate_id,
    )
    if result == "UPDATE 0":
        raise ValueError(f"Candidate {candidate_id} not found or not in 'discovered' status")
