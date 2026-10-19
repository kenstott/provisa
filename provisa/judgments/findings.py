# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The review queue (REQ-2094): ``judgment_findings``, one row per (object, judgment, finding
kind), written only through :func:`record_finding` -- the staleness checks, the adequacy checks,
the posture checks and the refusals that are findings at heart all write through it, so the
queue has one producer contract.

A finding seen again refreshes ``last_seen``; one seen again after it was resolved is open again.
A run that no longer sees an open finding it produces clears it (:func:`clear_unseen`).
"""

# Requirements: REQ-2094

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, select, update

from provisa.core.schema_org import judgment_findings as jf

_jf = jf

WRONG = "wrong"
DATED_BY_DATA = "dated_by_data"
DATED_BY_CONTEXT = "dated_by_context"
UNDECIDED = "undecided"
WORTH_A_LOOK = "worth_a_look"
#: The queue's order.
CATEGORIES = (WRONG, DATED_BY_DATA, DATED_BY_CONTEXT, UNDECIDED, WORTH_A_LOOK)

TAG = "tag"
FAKE = "fake"
MASK = "mask"
ROW_RULE = "row_rule"
WRITE_GRANT = "write_grant"
POSTURE = "posture"
JUDGMENTS = (TAG, FAKE, MASK, ROW_RULE, WRITE_GRANT, POSTURE)

#: How a finding no longer seen is resolved.
CLEARED = "cleared"


@dataclass(frozen=True)
class Key:
    object_type: str
    object_key: str
    judgment: str
    finding: str


async def record_finding(
    conn: Any,
    *,
    object_type: str,
    object_key: str,
    judgment: str,
    category: str,
    finding: str,
    observed: Mapping[str, Any] | None,
    at: Any,
    table: Any = None,
) -> Key:
    """Record that ``finding`` holds for ``judgment`` on the object, as of ``at``: a new row, a
    refreshed ``last_seen`` (with what is observed now), or a resolved finding opened again.
    ``table``: another environment's ``judgment_findings`` (default: the connection's)."""
    jf = _jf if table is None else table
    if category not in CATEGORIES:
        raise ValueError(f"finding category {category!r} is not one of {CATEGORIES}")
    if judgment not in JUDGMENTS:
        raise ValueError(f"judgment {judgment!r} is not one of {JUDGMENTS}")
    key = Key(object_type, object_key, judgment, finding)
    where = and_(
        jf.c.object_type == object_type,
        jf.c.object_key == object_key,
        jf.c.judgment == judgment,
        jf.c.finding == finding,
    )
    row = (await conn.execute_core(select(jf.c.id, jf.c.resolved_at).where(where))).fetchone()
    if row is None:
        await conn.execute_core(
            jf.insert().values(
                object_type=object_type,
                object_key=object_key,
                judgment=judgment,
                category=category,
                finding=finding,
                observed=dict(observed) if observed is not None else None,
                first_seen=at,
                last_seen=at,
            )
        )
        return key
    values: dict[str, Any] = {
        "category": category,
        "observed": dict(observed) if observed is not None else None,
        "last_seen": at,
    }
    if row[1] is not None:  # resolved, and seen again: open again from now
        values.update(first_seen=at, resolved_at=None, resolved_by=None, resolution=None)
    await conn.execute_core(update(jf).where(jf.c.id == row[0]).values(values))
    return key


async def clear_unseen(
    conn: Any,
    seen: Iterable[Key],
    *,
    judgments: Iterable[str],
    findings: Iterable[str],
    at: Any,
    within: set[tuple[str, str]] | None = None,
) -> int:
    """Resolve, as cleared, every open finding of ``judgments`` and ``findings`` (the kinds one
    producer writes) that this run did not see; with ``within``, only those on the (object type,
    object key) pairs the run looked at. Returns how many."""
    seen_keys = set(seen)
    rows = (
        await conn.execute_core(
            select(jf.c.id, jf.c.object_type, jf.c.object_key, jf.c.judgment, jf.c.finding).where(
                jf.c.resolved_at.is_(None),
                jf.c.judgment.in_(sorted(set(judgments))),
                jf.c.finding.in_(sorted(set(findings))),
            )
        )
    ).fetchall()
    gone = [
        r[0]
        for r in rows
        if Key(r[1], r[2], r[3], r[4]) not in seen_keys
        and (within is None or (r[1], r[2]) in within)
    ]
    if gone:
        await conn.execute_core(
            update(jf)
            .where(jf.c.id.in_(gone))
            .values(resolved_at=at, resolved_by=None, resolution=CLEARED)
        )
    return len(gone)


async def resolve(conn: Any, finding_id: int, *, by: str | None, resolution: str, at: Any) -> bool:
    """Resolve one open finding: reaffirmed, changed or (a posture finding) dismissed with a
    note. False when there is no such open finding."""
    result = await conn.execute_core(
        update(jf)
        .where(jf.c.id == finding_id, jf.c.resolved_at.is_(None))
        .values(resolved_at=at, resolved_by=by, resolution=resolution)
    )
    return (result.rowcount or 0) > 0


async def open_findings(conn: Any) -> list[dict[str, Any]]:
    """Every open finding, in the queue's order: by category, then by first seen."""
    rows = (await conn.execute_core(select(jf).where(jf.c.resolved_at.is_(None)))).fetchall()
    order = {c: i for i, c in enumerate(CATEGORIES)}
    found = [dict(r._mapping) for r in rows]
    return sorted(found, key=lambda f: (order[f["category"]], f["first_seen"], f["id"]))
