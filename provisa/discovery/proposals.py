# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7c5b9e-4a13-4f68-8e0b-6c1f9a3d7e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every proposal a discovery run read, kept with the run (REQ-413).

A run shows its suggestions; everything else it read is left out, each for one reason
(:data:`REASONS`), and none is dropped: the run's rows hold every foreign key it found and every
element of every answer, with the confidence and reasoning given and why it is not shown. A
reviewer reads the left-out proposals under the run's suggestions and may accept one that names two
compared columns (:data:`ACCEPTABLE`), as he accepts a suggestion.
"""

# Requirements: REQ-413

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update

from provisa.core.schema_org import discovery_run_proposals as rows_t
from provisa.discovery import candidates as candidates_repo
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.discovery.direction import Kind, Proposal

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: Why an element of an answer made no proposal: it lacks a field, or a field is not of its
#: type, or its cardinality is neither many-to-one nor one-to-many.
INVALID_SHAPE = "invalid_shape"
#: It names a table or column the request did not compare.
UNKNOWN_COLUMN = "unknown_column"
#: Its two columns are not compared: not both of the request's type group, or their classes do
#: not meet.
NOT_COMPARABLE = "not_comparable"
#: The model's own confidence is under the threshold.
BELOW_THRESHOLD = "below_threshold"
#: The model's confidence met the threshold and a second scorer's did not.
SCORED_BELOW_THRESHOLD = "scored_below_threshold"

#: Every reason a proposal is left out for: those of an answer's reading, then the removals of
#: :class:`provisa.discovery.direction.Removed`.
REASONS: tuple[str, ...] = (
    INVALID_SHAPE,
    UNKNOWN_COLUMN,
    NOT_COMPARABLE,
    BELOW_THRESHOLD,
    SCORED_BELOW_THRESHOLD,
    "same_column",
    "primary_key_to_primary_key",
    "both_repeating",
    "existing",
    "pending",
    "rejected",
    "matches_foreign_key",
    "out_of_scope",
    "weaker",
)

#: The reasons a reviewer may overrule by accepting the proposal. Not these: one that names no two
#: compared columns; a column to itself; one already registered, already waiting among the
#: suggestions, or matching a foreign key that is itself suggested.
ACCEPTABLE: frozenset[str] = frozenset(
    {
        NOT_COMPARABLE,
        BELOW_THRESHOLD,
        SCORED_BELOW_THRESHOLD,
        "primary_key_to_primary_key",
        "both_repeating",
        "rejected",
        "out_of_scope",
        "weaker",
    }
)


@dataclass(frozen=True)
class Recorded:
    """One proposal as a run read it. ``left_out`` None: it is among the run's suggestions.
    ``proposal`` None: the element named no two compared columns, and ``answer`` is the element
    as the model wrote it. ``scored``: the confidence a second scorer gave it."""

    kind: Kind
    left_out: str | None
    proposal: Proposal | None = None
    answer: str | None = None
    scored: float | None = None


def _values(run_id: int, r: Recorded) -> dict[str, Any]:
    p = r.proposal
    return {
        "run_id": run_id,
        "kind": r.kind,
        "source_table_id": None if p is None else p.start[0],
        "source_column": None if p is None else p.start[1],
        "target_table_id": None if p is None else p.end[0],
        "target_column": None if p is None else p.end[1],
        "confidence": None if p is None else p.confidence,
        "scored": r.scored,
        "reasoning": None if p is None else p.reasoning,
        "suggested_name": None if p is None else (p.suggested_name or None),
        "left_out": r.left_out,
        "answer": r.answer,
    }


async def record(conn: "Connection", run_id: int, read: list[Recorded]) -> None:
    """The run's rows made equal to ``read``: every proposal it has read so far. A row a reviewer
    accepted is kept as it is."""
    async with conn.transaction():
        await conn.execute_core(
            delete(rows_t).where(rows_t.c.run_id == run_id, rows_t.c.accepted_as.is_(None))
        )
        await conn.execute_core_many(insert(rows_t), [_values(run_id, r) for r in read])


def _view(row: Any) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "run_id": int(row.run_id),
        "kind": row.kind,
        "source_table_id": row.source_table_id,
        "source_column": row.source_column,
        "target_table_id": row.target_table_id,
        "target_column": row.target_column,
        "cardinality": "many-to-one",
        "confidence": row.confidence,
        "scored": row.scored,
        "reasoning": row.reasoning,
        "suggested_name": row.suggested_name,
        "reason": row.left_out,
        "answer": row.answer,
        "acceptable": row.left_out in ACCEPTABLE,
    }


async def left_out(conn: "Connection", run_id: int) -> list[dict[str, Any]]:
    """The run's proposals that are not among its suggestions and that no reviewer has accepted,
    the most confident first, those with no confidence last."""
    found = (
        await conn.execute_core(
            select(rows_t)
            .where(
                rows_t.c.run_id == run_id,
                rows_t.c.left_out.is_not(None),
                rows_t.c.accepted_as.is_(None),
            )
            .order_by(rows_t.c.id)
        )
    ).fetchall()
    listed = [_view(r) for r in found]
    return sorted(listed, key=lambda r: (r["confidence"] is None, -(r["confidence"] or 0.0)))


class NotAcceptable(ValueError):
    """A left-out proposal that cannot be accepted, and why."""


async def accept(conn: "Connection", proposal_id: int, name: str | None) -> dict[str, Any]:
    """Accept a left-out proposal: it becomes a suggestion of its run and is accepted as one
    (:func:`provisa.discovery.candidates.accept`), in one transaction."""
    async with conn.transaction():
        row = (await conn.execute_core(select(rows_t).where(rows_t.c.id == proposal_id))).fetchone()
        if row is None or row.left_out is None or row.accepted_as is not None:
            raise NotAcceptable(f"Proposal {proposal_id} not found among the left-out proposals")
        if row.left_out not in ACCEPTABLE:
            raise NotAcceptable(
                f"Proposal {proposal_id} cannot be accepted: it was left out as {row.left_out}"
            )
        (candidate_id,) = await candidates_repo.store_candidates(
            conn,
            [
                RelationshipCandidate(
                    source_table_id=int(row.source_table_id),
                    source_column=str(row.source_column),
                    target_table_id=int(row.target_table_id),
                    target_column=str(row.target_column),
                    cardinality="many-to-one",
                    confidence=float(row.confidence),
                    reasoning=row.reasoning or "",
                    suggested_name=row.suggested_name or "",
                )
            ],
            f"run:{int(row.run_id)}",
        )
        accepted = await candidates_repo.accept(conn, candidate_id, name)
        await conn.execute_core(
            update(rows_t)
            .where(rows_t.c.id == proposal_id)
            .values(accepted_as=accepted["relationship_id"])
        )
    return accepted
