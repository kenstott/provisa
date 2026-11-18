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
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, select, update

from provisa.core.schema_org import registered_tables, relationship_candidates
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


async def list_pending(conn: "Connection", state: Any) -> list[dict]:  # REQ-018, REQ-167
    """List all candidates with status='suggested'. ``reduced_information`` (REQ-413, REQ-1986):
    one of its tables requires a parameter and is not kept for good, so it is never read and no
    values were compared; said from the model as it is now (``state``: which tables are kept for
    good), beside the note its reasoning carries."""
    from provisa.discovery.gather import tables_never_read

    result = await conn.execute_core(
        select(relationship_candidates)
        .where(relationship_candidates.c.status == "suggested")
        .order_by(relationship_candidates.c.confidence.desc())
    )
    found = [dict(r._mapping) for r in result.fetchall()]
    ends = {c["source_table_id"] for c in found} | {c["target_table_id"] for c in found}
    never_read = await tables_never_read(state, conn, sorted(ends)) if ends else set()
    for c in found:
        c["reduced_information"] = bool({c["source_table_id"], c["target_table_id"]} & never_read)
    return found


async def accept(
    conn: "Connection", candidate_id: int, rel_id: str | None = None
) -> dict:  # REQ-019, REQ-020, REQ-413
    """Accept a candidate: mark it accepted and register its relationship through the existing
    derivation (:func:`provisa.core.repositories.relationship.upsert`), which marks the referring
    column a foreign key. No column is marked a primary or alternate key (REQ-413)."""
    from provisa.core.models import Cardinality, Relationship
    from provisa.core import relationship_naming
    from provisa.core.repositories import relationship as relationship_repo

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

        tables = {
            int(r.id): r
            for r in (
                await conn.execute_core(
                    select(
                        registered_tables.c.id,
                        registered_tables.c.alias,
                        registered_tables.c.table_name,
                    ).where(
                        registered_tables.c.id.in_([row["source_table_id"], row["target_table_id"]])
                    )
                )
            ).fetchall()
        }
        source, target = tables[int(row["source_table_id"])], tables[int(row["target_table_id"])]
        suggested = row.get("suggested_name") or f"disc-{uuid.uuid4().hex[:12]}"
        # REQ-415: a generated name is told apart from the names already registered; one a person
        # typed is never changed, and refused when taken. The run's result sends each proposal
        # with the name it shows, its suggested name: that name is the generated one, not typed.
        (named,) = await relationship_naming.unique_names(
            conn,
            [
                relationship_naming.Generated(
                    rel_id=rel_id or suggested,
                    id_typed=bool(rel_id) and rel_id != row.get("suggested_name"),
                    source_table_id=int(row["source_table_id"]),
                    target_table_id=int(row["target_table_id"]),
                    source_column=row["source_column"],
                    target_column=row["target_column"],
                    cardinality=row["cardinality"],
                    target_name=target.alias or target.table_name,
                    target_table=target.table_name,
                )
            ],
        )
        name = named.rel_id
        await conn.execute_core(
            update(relationship_candidates)
            .where(
                relationship_candidates.c.id == candidate_id,
                relationship_candidates.c.status == "suggested",
            )
            .values(status="accepted")
        )
        await relationship_repo.upsert(
            conn,
            Relationship(
                id=name,
                source_table_id=source.alias or source.table_name,
                target_table_id=target.alias or target.table_name,
                source_column=row["source_column"],
                target_column=row["target_column"],
                cardinality=Cardinality(row["cardinality"]),
                alias=named.alias,
                graphql_alias=named.graphql_alias,
            ),
            derive_target_key=False,
        )

    return {
        "relationship_id": name,
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


async def mark_accepted(conn: "Connection", candidate_id: int) -> None:  # REQ-413
    """Mark a many-to-many candidate accepted: accepting it made or joined a shared dimension
    (:mod:`provisa.discovery.dimension`), whose relationships end at the dimension, so none is
    registered for the pair itself."""
    result = await conn.execute_core(
        update(relationship_candidates)
        .where(
            relationship_candidates.c.id == candidate_id,
            relationship_candidates.c.status == "suggested",
        )
        .values(status="accepted")
    )
    if (result.rowcount or 0) == 0:
        raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")


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


async def withdraw(conn: "Connection", ids: list[int]) -> int:  # REQ-413
    """Remove suggestions a discovery run stored and no longer shows (a later answer of the same
    run turned the pair the other way); one a person has decided is left as it is."""
    if not ids:
        return 0
    result = await conn.execute_core(
        _delete(relationship_candidates).where(
            relationship_candidates.c.id.in_(ids),
            relationship_candidates.c.status == "suggested",
        )
    )
    return result.rowcount or 0
