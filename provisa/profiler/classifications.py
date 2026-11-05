# Copyright (c) 2026 Kenneth Stott
# Canary: 8d54bcd7-d0a4-4714-9c33-7ac376f763a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Classification suggestions and the steward's decisions on them (REQ-2147).

A run tests each column's sampled values against the enabled kinds (``classify``) and records each
kind a column reached the threshold of as a ``classifications`` result row, with the decision in
force at the run's time. The decisions are stored in ``profiler_classifications``: an accepted
suggestion has linked the column to the kind's glossary Property term (a key column's meaning is its
Class term, so it is not moved) and, for a sensitive kind, marked it pii; a rejected one is not
suggested again for that column and kind. Withdrawing a decision makes the suggestion new again; it
does not undo what accepting did, which the glossary and the tags own from then on.

An org admin may disable a kind: the org's ``classification`` setting lists the disabled kinds
(``provisa.core.org_settings``), and a run tests none of them.
"""

# Requirements: REQ-2147

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, select

from provisa.profiler.classify import Kind, Match, catalogue, kind_of

DECISION_STATUSES: tuple[str, ...] = ("accepted", "rejected")
SETTING_KEY = "classification"  # the org setting holding {"disabled": [kind ids]}
KEY_COLUMN_NOTE = "a key column's meaning is its Class term"


# -- enabled kinds ---------------------------------------------------------------------------------


def disabled_of(overrides: dict[str, Any]) -> frozenset[str]:
    """The kinds the org disabled, from its setting overrides; unknown ids are refused by name."""
    setting = overrides.get(SETTING_KEY)
    if setting is None:
        return frozenset()
    disabled = frozenset(setting["disabled"])
    unknown = disabled - {k.id for k in catalogue()}
    if unknown:
        raise ValueError(
            f"the org's classification setting disables unknown kinds {sorted(unknown)}"
        )
    return disabled


def setting_for(disabled: set[str]) -> dict[str, Any] | None:
    """The org setting that disables ``disabled``; None (the row removed) when none is."""
    unknown = disabled - {k.id for k in catalogue()}
    if unknown:
        raise ValueError(f"unknown classification kinds {sorted(unknown)}")
    return {"disabled": sorted(disabled)} if disabled else None


async def enabled_kinds(model_db: Any) -> tuple[Kind, ...]:
    from provisa.core.org_settings import read_org_overrides

    disabled = disabled_of(await read_org_overrides(model_db))
    return tuple(k for k in catalogue() if k.id not in disabled)


# -- decisions -------------------------------------------------------------------------------------


async def decisions(conn: Any, table_id: int) -> dict[tuple[str, str], dict]:
    """Every decision on the table's columns, by (physical column, kind)."""
    from provisa.core.schema_org import profiler_classifications as pc

    result = await conn.execute_core(select(pc).where(pc.c.table_id == table_id))
    return {(r.physical_column, r.kind): dict(r._mapping) for r in result.fetchall()}


@dataclass(frozen=True)
class Suggestion:
    column: str  # as the org admin reads it
    physical: str
    match: Match


def result_rows(
    table_name: str, suggestions: list[Suggestion], decided: dict[tuple[str, str], dict]
) -> list[dict]:
    """``classifications`` result rows (without the run key)."""
    out = []
    for s in suggestions:
        decision = decided.get((s.physical, s.match.kind.id))
        out.append(
            {
                "column_name": s.column,
                "physical_column": s.physical,
                "kind": s.match.kind.id,
                "sensitive": s.match.kind.sensitive,
                "values_tested": s.match.tested,
                "values_passed": s.match.passed,
                "share": s.match.share,
                "threshold": s.match.kind.threshold,
                "evidence": s.match.evidence(table_name, s.column),
                "status": "suggested" if decision is None else decision["status"],
            }
        )
    return out


async def record(
    conn: Any,
    table_id: int,
    physical: str,
    kind_id: str,
    *,
    status: str,
    evidence: str,
    run_id: str,
    by: str | None,
) -> None:
    from provisa.core.schema_org import profiler_classifications as pc

    if status not in DECISION_STATUSES:
        raise ValueError(f"a decision is one of {DECISION_STATUSES}, got {status!r}")
    kind_of(kind_id)  # an unknown kind is refused by name
    values = {
        "table_id": table_id,
        "physical_column": physical,
        "kind": kind_id,
        "status": status,
        "evidence": evidence,
        "run_id": run_id,
        "decided_by": by,
    }
    await conn.upsert(
        pc,
        values,
        index_elements=["table_id", "physical_column", "kind"],
        update_columns=["status", "evidence", "run_id", "decided_by"],
    )


async def forget(conn: Any, table_id: int, physical: str, kind_id: str) -> bool:
    """Withdraw a decision: the suggestion is new again. False when there is none."""
    from provisa.core.schema_org import profiler_classifications as pc

    result = await conn.execute_core(
        delete(pc).where(
            (pc.c.table_id == table_id)
            & (pc.c.physical_column == physical)
            & (pc.c.kind == kind_id)
        )
    )
    return bool(result.rowcount)


# -- accepting -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Accepted:
    term: str | None  # the Property term the column now means; None for a key column
    term_note: str | None  # why the column was not linked, where it was not
    pii: bool  # whether the column was marked pii
    mask_preset: str | None  # the mask the kind offers, for a sensitive kind
    tag_changes: Any  # what the term move did to the column's inherited tags (REQ-2145)


class PiiRefused(Exception):
    """The pii tag could not be put on the column; ``result`` is the tag rules' refusal."""

    def __init__(self, result: Any) -> None:
        super().__init__(result.message)
        self.result = result


async def accept_effects(
    conn: Any,
    table_id: int,
    physical: str,
    kind: Kind,
    evidence: str,
    *,
    confirm_tag_loss: tuple[str, ...] = (),
    carry_tags: bool = False,
) -> Accepted:
    """Link the column to the kind's Property term and, for a sensitive kind, mark it pii, in the
    caller's transaction. A key column keeps its Class term (REQ-2141) and is marked pii alone.
    Raises the glossary's and the tags' own refusals."""
    from provisa.api.admin.schema_mutation import store_tag_assignment
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.term_tags import inherited_snapshot, tag_changes_of_moves

    term: str | None = None
    note: str | None = None
    changes: Any = None
    if await glossary_repo.is_key_column(conn, table_id, physical):
        note = KEY_COLUMN_NOTE
    else:
        term_id = await glossary_repo._find_or_create_term(conn, kind.term)  # noqa: SLF001
        before = await inherited_snapshot(conn, {table_id})
        moved = await glossary_repo.move_ref(conn, table_id, physical, term_id)
        if moved is None:
            raise ValueError(f"column {physical!r} of table {table_id} has no glossary ref")
        changes = await tag_changes_of_moves(
            conn,
            before,
            {table_id},
            confirm_tag_loss=confirm_tag_loss,
            carry_tags=carry_tags,
        )
        term = kind.term
    if kind.sensitive:
        tag_row = await tag_repo.get(conn, "pii")
        assert tag_row is not None, "pii is a system tag"
        refused = await store_tag_assignment(
            conn,
            TagAssignment(
                tag_id="pii",
                object_type="column",
                table_id=table_id,
                column_name=physical,
                reason=evidence,
            ),
            tag_row,
        )
        if refused is not None:
            raise PiiRefused(refused)
    return Accepted(term, note, kind.sensitive, kind.mask_preset, changes)
