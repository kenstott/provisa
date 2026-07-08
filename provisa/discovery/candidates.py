# Copyright (c) 2026 Kenneth Stott
# Canary: 448f4c23-521e-49c2-96da-248f1e140020
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Repository for relationship_candidates table — via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-018, REQ-019, REQ-020, REQ-167, REQ-612

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, insert, select, update

from provisa.core.schema_org import relationship_candidates, relationships
from provisa.discovery.analyzer import RelationshipCandidate

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def store_candidates(
    conn: "Connection",
    candidates: list[RelationshipCandidate],
    scope: str,
) -> list[int]:  # REQ-018, REQ-167, REQ-612
    """Store candidates with dedup via ON CONFLICT. Returns list of IDs."""
    ids: list[int] = []
    for c in candidates:
        row_id = await conn.upsert_returning(
            relationship_candidates,
            {
                "source_table_id": c.source_table_id,
                "target_table_id": c.target_table_id,
                "source_column": c.source_column,
                "target_column": c.target_column,
                "cardinality": c.cardinality,
                "confidence": c.confidence,
                "reasoning": c.reasoning,
                "suggested_name": c.suggested_name or None,
                "scope": scope,
            },
            index_elements=[
                "source_table_id",
                "source_column",
                "target_table_id",
                "target_column",
            ],
            returning="id",
            update_columns=[
                "cardinality",
                "confidence",
                "reasoning",
                "suggested_name",
                "scope",
            ],
            set_extra={"status": "suggested"},
        )
        assert row_id is not None
        ids.append(row_id)
    return ids


async def list_pending(conn: "Connection") -> list[dict]:  # REQ-018, REQ-167
    """List all candidates with status='suggested'."""
    result = await conn.execute_core(
        select(relationship_candidates)
        .where(relationship_candidates.c.status == "suggested")
        .order_by(relationship_candidates.c.confidence.desc())
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def accept(
    conn: "Connection", candidate_id: int, rel_id: str | None = None
) -> dict:  # REQ-019, REQ-020
    """Accept a candidate: mark accepted and create a relationship."""
    async with conn.transaction():
        result = await conn.execute_core(
            select(relationship_candidates).where(
                relationship_candidates.c.id == candidate_id,
                relationship_candidates.c.status == "suggested",
            )
        )
        found = result.fetchone()
        if found is None:
            raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")
        row = dict(found._mapping)

        await conn.execute_core(
            update(relationship_candidates)
            .where(
                relationship_candidates.c.id == candidate_id,
                relationship_candidates.c.status == "suggested",
            )
            .values(status="accepted")
        )

        rel_id = rel_id or row.get("suggested_name") or f"disc-{uuid.uuid4().hex[:12]}"
        await conn.execute_core(
            insert(relationships).values(
                id=rel_id,
                source_table_id=row["source_table_id"],
                target_table_id=row["target_table_id"],
                source_column=row["source_column"],
                target_column=row["target_column"],
                cardinality=row["cardinality"],
            )
        )

    return {
        "relationship_id": rel_id,
        "source_table_id": row["source_table_id"],
        "target_table_id": row["target_table_id"],
        "source_column": row["source_column"],
        "target_column": row["target_column"],
        "cardinality": row["cardinality"],
    }


async def clear_rejections(conn: "Connection") -> int:  # REQ-167
    """Delete all rejected candidates. Returns count deleted."""
    result = await conn.execute_core(
        _delete(relationship_candidates).where(relationship_candidates.c.status == "rejected")
    )
    return result.rowcount or 0


async def reject(conn: "Connection", candidate_id: int, reason: str) -> None:  # REQ-167
    """Reject a candidate with a reason."""
    result = await conn.execute_core(
        update(relationship_candidates)
        .where(
            relationship_candidates.c.id == candidate_id,
            relationship_candidates.c.status == "suggested",
        )
        .values(status="rejected", rejection_reason=reason)
    )
    if (result.rowcount or 0) == 0:
        raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")
