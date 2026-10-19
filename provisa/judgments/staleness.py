# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What flags a judgment (REQ-2094): its evidence and decided time against the model and the
profiles now. Thresholds are the profiler's own.

| Finding | Category | Flagged when |
|---|---|---|
| ``profile_drift`` | dated by data | the latest run against the evidence run flags drift on the decided column |
| ``type_changed`` | dated by data | plausible type, family or data type differs from the evidence digest |
| ``column_added`` | dated by data | the table has a column the evidence did not (table-level judgments, fakes) |
| ``schema_changed`` | dated by data | the decided column is gone, or retyped at registration |
| ``decider_left`` | dated by data | the decider is no longer a member of the org |
| ``basis_changed`` | dated by context | a cited basis was declared changed after the decision |
| ``review_due`` | dated by context | the review-by date is today or past |
| ``expired`` | dated by context | a tag assignment's ``expires_on`` has passed |
| ``undecided`` | undecided | a column added to a table that has a sensitive tag or a fake |
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from provisa.judgments.findings import (
    DATED_BY_CONTEXT,
    DATED_BY_DATA,
    FAKE,
    ROW_RULE,
    TAG,
    UNDECIDED,
    WRITE_GRANT,
)
from provisa.judgments.inventory import Judgment

PROFILE_DRIFT = "profile_drift"
TYPE_CHANGED = "type_changed"
COLUMN_ADDED = "column_added"
SCHEMA_CHANGED = "schema_changed"
DECIDER_LEFT = "decider_left"
BASIS_CHANGED = "basis_changed"
REVIEW_DUE = "review_due"
EXPIRED = "expired"
UNDECIDED_COLUMN = "undecided"
FINDINGS = (
    PROFILE_DRIFT,
    TYPE_CHANGED,
    COLUMN_ADDED,
    SCHEMA_CHANGED,
    DECIDER_LEFT,
    BASIS_CHANGED,
    REVIEW_DUE,
    EXPIRED,
)

#: The digest facts a type change is read from.
TYPE_FACTS = ("plausible_type", "family", "data_type")


@dataclass(frozen=True)
class Found:
    category: str
    finding: str
    observed: dict[str, Any]


@dataclass
class TableNow:
    """What a table is now: its registered columns and types, and its latest run's digest."""

    columns: dict[str, str | None]  # registered column -> data type
    run_id: str | None
    facts: dict[str, dict[str, Any]]  # the latest run's per-column digest
    drifting: dict[str, set[str]]  # evidence run id -> the columns drifting against it


def _decided_at(why: dict[str, Any]) -> dt.datetime | None:
    at = why.get("decided_at")
    if not at:
        return None
    parsed = dt.datetime.fromisoformat(at) if isinstance(at, str) else at
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _column_level(j: Judgment) -> bool:
    return j.column is not None


def _table_level(j: Judgment) -> bool:
    return j.judgment in (ROW_RULE, WRITE_GRANT, FAKE) or (
        j.judgment == TAG and j.extra.get("object_kind") == "table"
    )


def check(
    j: Judgment,
    now: TableNow | None,
    *,
    today: dt.date,
    members: set[str],
    bases: dict[str, dt.datetime | None],
) -> list[Found]:
    """Every finding that holds for ``j``. ``now`` is its table as it is (None for a judgment on
    no table), ``members`` the org's members, ``bases`` each basis's declared change time."""
    out: list[Found] = []
    why = j.why
    evidence = why.get("evidence") or {}
    decided = _decided_at(why)
    if now is not None and _column_level(j):
        assert j.column is not None
        if j.column not in now.columns:
            out.append(Found(DATED_BY_DATA, SCHEMA_CHANGED, {"column": j.column, "now": "removed"}))
        else:
            then = (evidence.get("column") or {}).get("data_type")
            registered = now.columns[j.column]
            if then and registered and then != registered:
                out.append(
                    Found(
                        DATED_BY_DATA,
                        SCHEMA_CHANGED,
                        {"column": j.column, "then": then, "now": registered},
                    )
                )
            digest = evidence.get("column") or {}
            current = now.facts.get(j.column) or {}
            if digest and current:
                changed = {
                    f: {"then": digest.get(f), "now": current.get(f)}
                    for f in TYPE_FACTS
                    if digest.get(f) != current.get(f)
                }
                if changed:
                    out.append(Found(DATED_BY_DATA, TYPE_CHANGED, changed))
            run = evidence.get("run_id")
            if run and now.run_id and run != now.run_id and j.column in now.drifting.get(run, ()):
                out.append(
                    Found(
                        DATED_BY_DATA,
                        PROFILE_DRIFT,
                        {"column": j.column, "then": run, "now": now.run_id},
                    )
                )
    if now is not None and _table_level(j) and evidence.get("columns") is not None:
        added = sorted(set(now.columns) - set(evidence["columns"]))
        if added:
            out.append(Found(DATED_BY_DATA, COLUMN_ADDED, {"columns": added}))
    by = why.get("decided_by")
    if by and by not in members:
        out.append(Found(DATED_BY_DATA, DECIDER_LEFT, {"decided_by": by}))
    for basis in why.get("bases") or ():
        changed_at = bases.get(basis)
        if changed_at is not None and (decided is None or changed_at > decided):
            out.append(
                Found(
                    DATED_BY_CONTEXT,
                    BASIS_CHANGED,
                    {"basis": basis, "changed_at": changed_at.isoformat()},
                )
            )
    review_on = why.get("review_on")
    if review_on and dt.date.fromisoformat(review_on) <= today:
        out.append(Found(DATED_BY_CONTEXT, REVIEW_DUE, {"review_on": review_on}))
    if j.expires_on and dt.date.fromisoformat(j.expires_on) < today:
        out.append(Found(DATED_BY_CONTEXT, EXPIRED, {"expires_on": j.expires_on}))
    return out


def undecided_columns(
    table_judgments: list[Judgment], now: TableNow
) -> list[tuple[str, dict[str, Any]]]:
    """The columns added to a table that has a sensitive tag or a fake, since the evidence its
    judgments were made on: each is undecided. Read from the table's fakes and sensitive tags."""
    holds = [
        j for j in table_judgments if j.judgment == FAKE or (j.judgment == TAG and j.sensitive)
    ]
    if not holds:
        return []
    known: set[str] = set()
    seen_any = False
    for j in holds:
        cols = (j.why.get("evidence") or {}).get("columns")
        if cols is not None:
            known |= set(cols)
            seen_any = True
    if not seen_any:
        return []
    decided = {j.column for j in table_judgments if j.column is not None}
    return [(c, {"column": c}) for c in sorted(set(now.columns) - known - decided)]


__all__ = ["FINDINGS", "UNDECIDED", "UNDECIDED_COLUMN", "Found", "TableNow", "check"]
