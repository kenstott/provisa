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

A run discards nothing: its rows hold every foreign key it found and every element of every
answer, with the confidence and reasoning given. A proposal that is not among the run's
suggestions carries why (:data:`REASONS`). The run's result lists them all in one table; a
reviewer accepts those he ticks and marks those he does not want recommended again
(:func:`apply`), and what he leaves alone stays undecided: nothing is recorded against it.
One that names no two compared columns, or that repeats what is already there
(:data:`ACCEPTABLE`), is listed with its reason and cannot be decided.
"""

# Requirements: REQ-413

from __future__ import annotations

import json
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

#: Every reason a proposal is left out for: those of an answer's reading, then the removals of
#: :class:`provisa.discovery.direction.Removed`.
REASONS: tuple[str, ...] = (
    INVALID_SHAPE,
    UNKNOWN_COLUMN,
    NOT_COMPARABLE,
    BELOW_THRESHOLD,
    "same_column",
    "primary_key_to_primary_key",
    "both_repeating",
    "existing",
    "pending",
    "rejected",
    "matches_foreign_key",
    "out_of_scope",
    "weaker",
    "many_to_many",
)

#: The reasons a proposal left out for can still be decided by a reviewer. Not these: one that
#: names no two compared columns; two columns that are not compared; a column to itself; one
#: already registered; one repeating a foreign key or a stronger proposal listed beside it.
ACCEPTABLE: frozenset[str] = frozenset(
    {
        BELOW_THRESHOLD,
        "primary_key_to_primary_key",
        "both_repeating",
        "pending",
        "rejected",
        "out_of_scope",
        "many_to_many",
    }
)

#: The reason a proposal marked not to be recommended again is rejected with.
NOT_AGAIN = "Not to be recommended again"


@dataclass(frozen=True)
class Recorded:
    """One proposal as a run read it. ``left_out`` None: it is among the run's suggestions.
    ``proposal`` None: the element named no two compared columns, and ``answer`` is the element
    as the model wrote it. A second scorer's confidence is the proposal's ``scored``."""

    kind: Kind
    left_out: str | None
    proposal: Proposal | None = None
    answer: str | None = None


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
        "scored": None if p is None else p.scored,
        "score": None if p is None else p.score,
        "cardinality": None if p is None else p.cardinality,
        "measure": None if p is None or p.measure is None else json.dumps(p.measure.as_dict()),
        "reasoning": None if p is None else p.reasoning,
        "suggested_name": None if p is None else (p.suggested_name or None),
        "left_out": r.left_out,
        "answer": r.answer,
    }


async def record(conn: "Connection", run_id: int, read: list[Recorded]) -> None:
    """The run's rows made equal to ``read``: every proposal it has read so far. A row a reviewer
    decided is kept as it is."""
    async with conn.transaction():
        await conn.execute_core(
            delete(rows_t).where(rows_t.c.run_id == run_id, rows_t.c.decision.is_(None))
        )
        await conn.execute_core_many(insert(rows_t), [_values(run_id, r) for r in read])


def decidable(left_out: str | None) -> bool:
    """Whether a reviewer may accept the proposal or mark it not to be recommended again."""
    return left_out is None or left_out in ACCEPTABLE


#: The measured cardinality no relationship has: neither column's values are distinct, so none
#: starts at one and ends at the other's key (a relationship is many-to-one, one-to-many or
#: one-to-one; tables sharing values join through a table holding each value once).
MANY_TO_MANY = "many-to-many"


def acceptable(left_out: str | None, cardinality: str | None) -> bool:
    """Whether a reviewer may accept the proposal: it can be decided, and its measured
    cardinality is one a relationship has."""
    return decidable(left_out) and cardinality != MANY_TO_MANY


def _view(row: Any, never_read: set[int]) -> dict[str, Any]:
    ends = {row.source_table_id, row.target_table_id} - {None}
    return {
        "id": int(row.id),
        "run_id": int(row.run_id),
        "kind": row.kind,
        "source_table_id": row.source_table_id,
        "source_column": row.source_column,
        "target_table_id": row.target_table_id,
        "target_column": row.target_column,
        "cardinality": row.cardinality,
        # The score the row is judged by: the measure's where values were compared, else the
        # model's confidence, which is shown beside it.
        "score": row.confidence if row.score is None else row.score,
        "measured": row.score is not None,
        "measure": None if row.measure is None else json.loads(row.measure),
        "confidence": row.confidence,
        "scored": row.scored,
        "reasoning": row.reasoning,
        "suggested_name": row.suggested_name,
        "reason": row.left_out,
        "answer": row.answer,
        "decidable": decidable(row.left_out),
        "acceptable": acceptable(row.left_out, row.cardinality),
        # REQ-1986: one of its tables is never read, so no values were compared.
        "reduced_information": bool(ends & never_read),
    }


async def listed(conn: "Connection", state: Any, run_id: int) -> list[dict[str, Any]]:
    """Every proposal of the run no reviewer has decided, the highest score first (the measure's,
    else the model's), those with none last. A second scorer's score is shown beside it and does
    not order (REQ-1789)."""
    from provisa.discovery.gather import tables_never_read

    found = (
        await conn.execute_core(
            select(rows_t)
            .where(rows_t.c.run_id == run_id, rows_t.c.decision.is_(None))
            .order_by(rows_t.c.id)
        )
    ).fetchall()
    ends = {r.source_table_id for r in found} | {r.target_table_id for r in found}
    ends.discard(None)
    never_read = await tables_never_read(state, conn, sorted(ends)) if ends else set()

    rows = [_view(r, never_read) for r in found]
    return sorted(rows, key=lambda r: (r["score"] is None, -(r["score"] or 0.0)))


class NotDecidable(ValueError):
    """A proposal that cannot be accepted or marked, and why."""


async def _candidate_of(
    conn: "Connection", run_id: int, proposal_id: int, *, accepting: bool
) -> int:
    """The proposal as a suggestion of its run: the stored suggestion it already is, or one
    stored now, with its measured cardinality and score."""
    row = (
        await conn.execute_core(
            select(rows_t).where(rows_t.c.id == proposal_id, rows_t.c.run_id == run_id)
        )
    ).fetchone()
    if row is None or row.decision is not None:
        raise NotDecidable(f"Proposal {proposal_id} is not among the run's undecided proposals")
    if not decidable(row.left_out):
        raise NotDecidable(f"Proposal {proposal_id} cannot be decided: it is {row.left_out}")
    if accepting and not acceptable(row.left_out, row.cardinality):
        raise NotDecidable(
            f"Proposal {proposal_id} cannot be accepted: neither column's values are distinct, "
            "so no relationship starts at one and ends at the other's key"
        )
    (candidate_id,) = await candidates_repo.store_candidates(
        conn,
        [
            RelationshipCandidate(
                source_table_id=int(row.source_table_id),
                source_column=str(row.source_column),
                target_table_id=int(row.target_table_id),
                target_column=str(row.target_column),
                cardinality=str(row.cardinality),
                confidence=float(row.confidence if row.score is None else row.score),
                reasoning=row.reasoning or "",
                suggested_name=row.suggested_name or "",
            )
        ],
        f"run:{run_id}",
    )
    return candidate_id


async def accept(conn: "Connection", run_id: int, proposal_id: int, name: str | None) -> str:
    """Accept one proposal of the run as a suggestion is accepted
    (:func:`provisa.discovery.candidates.accept`); the relationship it became."""
    async with conn.transaction():
        candidate_id = await _candidate_of(conn, run_id, proposal_id, accepting=True)
        accepted = await candidates_repo.accept(conn, candidate_id, name)
        await conn.execute_core(
            update(rows_t)
            .where(rows_t.c.id == proposal_id)
            .values(decision="accepted", accepted_as=accepted["relationship_id"])
        )
    return str(accepted["relationship_id"])


async def not_again(conn: "Connection", run_id: int, proposal_id: int) -> None:
    """Mark one proposal of the run not to be recommended again: it is rejected, as a suggestion
    is (:func:`provisa.discovery.candidates.reject`), so later runs leave it out."""
    async with conn.transaction():
        candidate_id = await _candidate_of(conn, run_id, proposal_id, accepting=False)
        await candidates_repo.reject(conn, candidate_id, NOT_AGAIN)
        await conn.execute_core(
            update(rows_t).where(rows_t.c.id == proposal_id).values(decision="rejected")
        )
