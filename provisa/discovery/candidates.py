# Copyright (c) 2025 Kenneth Stott
# Canary: 448f4c23-521e-49c2-96da-248f1e140020
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Repository for relationship_candidates table."""

from __future__ import annotations

import uuid

import asyncpg

from provisa.discovery.analyzer import RelationshipCandidate


async def store_candidates(
    conn: asyncpg.Connection,
    candidates: list[RelationshipCandidate],
    scope: str,
) -> list[int]:
    """Store candidates with dedup via ON CONFLICT. Returns list of IDs."""
    ids: list[int] = []
    for c in candidates:
        row_id = await conn.fetchval(
            """
            INSERT INTO relationship_candidates
                (source_table_id, target_table_id, source_column, target_column,
                 cardinality, confidence, reasoning, scope)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (source_table_id, source_column, target_table_id, target_column)
            DO UPDATE SET
                cardinality = EXCLUDED.cardinality,
                confidence = EXCLUDED.confidence,
                reasoning = EXCLUDED.reasoning,
                scope = EXCLUDED.scope,
                status = 'suggested'
            RETURNING id
            """,
            c.source_table_id,
            c.target_table_id,
            c.source_column,
            c.target_column,
            c.cardinality,
            c.confidence,
            c.reasoning,
            scope,
        )
        ids.append(row_id)
    return ids


async def list_pending(conn: asyncpg.Connection) -> list[dict]:
    """List all candidates with status='suggested'."""
    rows = await conn.fetch(
        "SELECT * FROM relationship_candidates WHERE status = 'suggested' ORDER BY confidence DESC"
    )
    return [dict(r) for r in rows]


async def accept(conn: asyncpg.Connection, candidate_id: int) -> dict:
    """Accept a candidate: mark accepted and create a relationship."""
    row = await conn.fetchrow(
        "UPDATE relationship_candidates SET status = 'accepted' "
        "WHERE id = $1 AND status = 'suggested' RETURNING *",
        candidate_id,
    )
    if row is None:
        raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")

    rel_id = f"disc-{uuid.uuid4().hex[:12]}"
    await conn.execute(
        """
        INSERT INTO relationships (id, source_table_id, target_table_id,
                                   source_column, target_column, cardinality)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        rel_id,
        row["source_table_id"],
        row["target_table_id"],
        row["source_column"],
        row["target_column"],
        row["cardinality"],
    )

    return {
        "relationship_id": rel_id,
        "source_table_id": row["source_table_id"],
        "target_table_id": row["target_table_id"],
        "source_column": row["source_column"],
        "target_column": row["target_column"],
        "cardinality": row["cardinality"],
    }


async def reject(conn: asyncpg.Connection, candidate_id: int, reason: str) -> None:
    """Reject a candidate with a reason."""
    result = await conn.execute(
        "UPDATE relationship_candidates SET status = 'rejected', rejection_reason = $2 "
        "WHERE id = $1 AND status = 'suggested'",
        candidate_id,
        reason,
    )
    if result == "UPDATE 0":
        raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")
