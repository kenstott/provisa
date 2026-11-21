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
suggestions carries why (:data:`REASONS`). The run's result lists them in one table, leaving out
only a pair already registered and one marked not to be recommended again (:data:`NOT_LISTED`,
amended 2026-11-18); a reviewer accepts those he ticks and marks those he does not want
recommended again (:func:`apply`), and what he leaves alone stays undecided: nothing is recorded
against it. One that names no two compared columns (:data:`ACCEPTABLE`) is listed with its reason
and cannot be decided.
"""

# Requirements: REQ-413, REQ-1586

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update

from provisa.core.join_through import VIA_CARDINALITY, Hop, Through, existing, qualify
from provisa.core.schema_org import discovery_run_proposals as rows_t
from provisa.core.schema_org import relationship_candidates as candidates_t
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
    "existing",
    "rejected",
    "out_of_scope",
)

#: The reasons a proposal is not listed for (REQ-413, amended 2026-11-18): its pair is already
#: registered, or marked not to be recommended again.
NOT_LISTED: frozenset[str] = frozenset({"existing", "rejected"})

#: The reasons a proposal left out for can still be decided by a reviewer. Not these: one that
#: names no two compared columns; two columns that are not compared; a column to itself; one
#: already registered; one already decided (rejected: a decided pair keeps its decision).
ACCEPTABLE: frozenset[str] = frozenset({BELOW_THRESHOLD, "out_of_scope"})

#: The reason a proposal marked not to be recommended again is rejected with.
NOT_AGAIN = "Not to be recommended again"


@dataclass(frozen=True)
class Recorded:
    """One proposal as a run read it. ``left_out`` None: it is among the run's suggestions.
    ``proposal`` None: the element named no two compared columns, and ``answer`` is the element
    as the model wrote it. A second scorer's confidence is the proposal's ``scored``. ``through``
    set: the suggestion joins two ends through a junction (:func:`junctions`)."""

    kind: Kind
    left_out: str | None
    proposal: Proposal | None = None
    answer: str | None = None
    #: A junction suggestion (REQ-1586): the relationship two of the run's suggestions make
    #: through the table both refer from, as confident as the less confident leg. ``proposal``
    #: is then None.
    through: Through | None = None
    confidence: float | None = None


def _values(run_id: int, r: Recorded) -> dict[str, Any]:
    if r.through is not None:
        return _junction_values(run_id, r)
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
        "via_table_id": None,
        "via_source_column": None,
        "via_target_column": None,
    }


def _junction_values(run_id: int, r: Recorded) -> dict[str, Any]:
    t = r.through
    assert t is not None
    return {
        "run_id": run_id,
        "kind": r.kind,
        "source_table_id": t.source_table_id,
        "source_column": t.source_column,
        "target_table_id": t.target_table_id,
        "target_column": t.target_column,
        "confidence": r.confidence,
        "scored": None,
        "score": None,
        "cardinality": VIA_CARDINALITY,
        "measure": None,
        "reasoning": None,
        "suggested_name": None,
        "left_out": r.left_out,
        "answer": None,
        "via_table_id": t.via_table_id,
        "via_source_column": t.via_source_column,
        "via_target_column": t.via_target_column,
    }


def _hop(p: Proposal) -> Hop:
    return Hop(
        rel_id="",
        source_table_id=p.start[0],
        source_column=p.start[1],
        target_table_id=p.end[0],
        target_column=p.end[1],
        cardinality=p.cardinality,
    )


#: The proposals a junction's legs are: the run's suggestions, and those left out only because
#: they are suggested by an earlier run or registered already. The junction is found either way.
_LEGS: frozenset[str | None] = frozenset({None, "pending", "existing"})


def junctions(read: list[Recorded]) -> list[Recorded]:
    """Each relationship two of the run's suggestions make through a junction both refer from
    (REQ-1586): J.a -> A and J.b -> B make A -> J -> B, proposed as one suggestion beside its two
    legs. A shared dimension is proposed as a many-to-many pair instead (REQ-413)."""
    legs = [r for r in read if r.left_out in _LEGS and r.proposal is not None]
    out: list[Recorded] = []
    for i, first in enumerate(legs):
        for second in legs[i + 1 :]:
            assert first.proposal is not None and second.proposal is not None
            through = qualify(_hop(first.proposal), _hop(second.proposal))
            if through is not None and through.shape == "junction":
                confidence = min(first.proposal.rank, second.proposal.rank)
                out.append(Recorded(first.kind, None, through=through, confidence=confidence))
    return out


async def _junction_left_out(conn: "Connection", through: Through) -> str | None:
    """Why a junction suggestion is left out: its two ends are joined through the junction already,
    or its pair was accepted or marked not to be recommended again."""
    if await existing(conn, through) is not None:
        return "existing"
    rc = candidates_t
    held = (
        await conn.execute_core(
            select(rc.c.status).where(
                rc.c.source_table_id == through.source_table_id,
                rc.c.source_column == through.source_column,
                rc.c.target_table_id == through.target_table_id,
                rc.c.target_column == through.target_column,
            )
        )
    ).scalar_one_or_none()
    return "rejected" if held == "rejected" else "existing" if held == "accepted" else None


async def record(conn: "Connection", run_id: int, read: list[Recorded]) -> None:
    """The run's rows made equal to ``read``: every proposal it has read so far. A row a reviewer
    decided is kept as it is. A junction both of whose legs are among its suggestions is proposed
    as one suggestion besides (:func:`junctions`)."""
    joined = [
        replace(j, left_out=await _junction_left_out(conn, j.through))
        for j in junctions(read)
        if j.through is not None
    ]
    async with conn.transaction():
        await conn.execute_core(
            delete(rows_t).where(rows_t.c.run_id == run_id, rows_t.c.decision.is_(None))
        )
        await conn.execute_core_many(insert(rows_t), [_values(run_id, r) for r in (*read, *joined)])


def decidable(left_out: str | None) -> bool:
    """Whether a reviewer may accept the proposal or mark it not to be recommended again."""
    return left_out is None or left_out in ACCEPTABLE


#: The measured cardinality no relationship has: neither column's values are distinct, so none
#: starts at one and ends at the other's key. Such a pair is accepted as a shared dimension
#: (:mod:`provisa.discovery.dimension`): a table holding each value once, which each side joins.
MANY_TO_MANY = "many-to-many"


@dataclass(frozen=True)
class Shared:
    """The shared dimension a many-to-many proposal joins on accepting it. ``name`` None: its
    two sides already join two different dimensions, so it cannot be accepted."""

    name: str | None
    reused: bool


def acceptable(left_out: str | None, shared: Shared | None) -> bool:
    """Whether a reviewer may accept the proposal: it can be decided, and a many-to-many pair
    (``shared``) names the dimension it joins."""
    return decidable(left_out) and (shared is None or shared.name is not None)


def _view(row: Any, never_read: set[int], shared: Shared | None) -> dict[str, Any]:
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
        "acceptable": acceptable(row.left_out, shared),
        # REQ-1986: one of its tables is never read, so no values were compared.
        "reduced_information": bool(ends & never_read),
        # REQ-413 (amended 2026-11-16): only a pair in the band under the threshold is measured,
        # so a suggestion with a measure was admitted on key values matching in the data.
        "admitted_on_key_values": row.left_out is None and row.measure is not None,
        # A many-to-many pair: the shared dimension accepting it joins, and whether it is one
        # already registered (a second pair on the same values reuses it).
        "dimension": None
        if shared is None or shared.name is None
        else {"name": shared.name, "reused": shared.reused},
        # REQ-1586: a junction suggestion, the junction its two ends are joined through.
        "via": None
        if row.via_table_id is None
        else {
            "table_id": int(row.via_table_id),
            "source_column": row.via_source_column,
            "target_column": row.via_target_column,
        },
    }


async def listed(conn: "Connection", state: Any, run_id: int) -> list[dict[str, Any]]:
    """Every proposal of the run no reviewer has decided, but those already registered or marked
    not to be recommended again (:data:`NOT_LISTED`), the highest score first (the measure's, else
    the model's), those with none last. A second scorer's score is shown beside it and does not
    order (REQ-1789)."""
    from provisa.discovery.gather import tables_never_read

    found = (
        await conn.execute_core(
            select(rows_t)
            .where(
                rows_t.c.run_id == run_id,
                rows_t.c.decision.is_(None),
                rows_t.c.left_out.is_(None) | rows_t.c.left_out.notin_(sorted(NOT_LISTED)),
            )
            .order_by(rows_t.c.id)
        )
    ).fetchall()
    ends = {r.source_table_id for r in found} | {r.target_table_id for r in found}
    ends.discard(None)
    never_read = await tables_never_read(state, conn, sorted(ends)) if ends else set()

    rows = [_view(r, never_read, await _shared(conn, r)) for r in found]
    return sorted(rows, key=lambda r: (r["score"] is None, -(r["score"] or 0.0)))


async def _shared(conn: "Connection", row: Any) -> Shared | None:
    """The shared dimension a decidable many-to-many proposal joins; None for any other."""
    from provisa.discovery import dimension

    if row.cardinality != MANY_TO_MANY or not decidable(row.left_out):
        return None
    try:
        name, reused = await dimension.named(
            conn,
            (int(row.source_table_id), str(row.source_column)),
            (int(row.target_table_id), str(row.target_column)),
            row.suggested_name,
        )
    except dimension.TwoDimensions:
        return Shared(None, False)
    return Shared(name, reused)


class NotDecidable(ValueError):
    """A proposal that cannot be accepted or marked, and why."""


async def undecided(conn: "Connection", run_id: int, proposal_id: int) -> Any:
    """The proposal's row, refused by name when it cannot be decided: not among the run's
    undecided proposals, left out for a reason no reviewer decides, or its pair already decided."""
    row = (
        await conn.execute_core(
            select(rows_t).where(rows_t.c.id == proposal_id, rows_t.c.run_id == run_id)
        )
    ).fetchone()
    if row is None or row.decision is not None:
        raise NotDecidable(f"Proposal {proposal_id} is not among the run's undecided proposals")
    if not decidable(row.left_out):
        raise NotDecidable(f"Proposal {proposal_id} cannot be decided: it is {row.left_out}")
    # REQ-413 (amended 2026-11-16): a pair once accepted or marked not to be recommended again
    # keeps that decision; storing the proposal would reopen it as pending.
    rc = candidates_t
    held = (
        await conn.execute_core(
            select(rc.c.status).where(
                rc.c.source_table_id == row.source_table_id,
                rc.c.source_column == row.source_column,
                rc.c.target_table_id == row.target_table_id,
                rc.c.target_column == row.target_column,
            )
        )
    ).scalar_one_or_none()
    if held is not None and held != "suggested":
        raise NotDecidable(f"Proposal {proposal_id} cannot be decided: its pair is already {held}")
    return row


async def _candidate_of(conn: "Connection", run_id: int, proposal_id: int) -> int:
    """The proposal as a suggestion of its run: the stored suggestion it already is, or one
    stored now, with its measured cardinality and score."""
    row = await undecided(conn, run_id, proposal_id)
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
        candidate_id = await _candidate_of(conn, run_id, proposal_id)
        accepted = await candidates_repo.accept(conn, candidate_id, name)
        await conn.execute_core(
            update(rows_t)
            .where(rows_t.c.id == proposal_id)
            .values(decision="accepted", accepted_as=accepted["relationship_id"])
        )
    return str(accepted["relationship_id"])


async def shared_pair(conn: "Connection", run_id: int, proposal_id: int) -> bool:
    """Whether the run's proposal is a many-to-many pair, accepted as a shared dimension."""
    cardinality = (
        await conn.execute_core(
            select(rows_t.c.cardinality).where(
                rows_t.c.id == proposal_id, rows_t.c.run_id == run_id
            )
        )
    ).scalar_one_or_none()
    return cardinality == MANY_TO_MANY


async def junction_of(conn: "Connection", run_id: int, proposal_id: int) -> Through | None:
    """The relationship a junction suggestion of the run joins through its junction (REQ-1586);
    None for any other proposal."""
    row = (
        await conn.execute_core(
            select(rows_t).where(rows_t.c.id == proposal_id, rows_t.c.run_id == run_id)
        )
    ).fetchone()
    if row is None or row.via_table_id is None:
        return None
    return Through(
        shape="junction",
        via_table_id=int(row.via_table_id),
        source_table_id=int(row.source_table_id),
        source_column=str(row.source_column),
        target_table_id=int(row.target_table_id),
        target_column=str(row.target_column),
        via_source_column=str(row.via_source_column),
        via_target_column=str(row.via_target_column),
    )


async def accepted_as(conn: "Connection", run_id: int, proposal_id: int, made: str) -> None:
    """Record a proposal accepted as what accepting it made, ``made``: the shared dimension a
    many-to-many pair made or joined (:mod:`provisa.discovery.dimension`), or the relationship a
    junction suggestion made (REQ-1586). Its pair is accepted, so a later run never reopens it,
    and the run's row is decided."""
    async with conn.transaction():
        candidate_id = await _candidate_of(conn, run_id, proposal_id)
        await candidates_repo.mark_accepted(conn, candidate_id)
        await conn.execute_core(
            update(rows_t)
            .where(rows_t.c.id == proposal_id)
            .values(decision="accepted", accepted_as=made)
        )


async def not_again(conn: "Connection", run_id: int, proposal_id: int) -> None:
    """Mark one proposal of the run not to be recommended again: it is rejected, as a suggestion
    is (:func:`provisa.discovery.candidates.reject`), so later runs leave it out."""
    async with conn.transaction():
        candidate_id = await _candidate_of(conn, run_id, proposal_id)
        await candidates_repo.reject(conn, candidate_id, NOT_AGAIN)
        await conn.execute_core(
            update(rows_t).where(rows_t.c.id == proposal_id).values(decision="rejected")
        )
