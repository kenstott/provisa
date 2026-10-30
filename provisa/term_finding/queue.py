# Copyright (c) 2026 Kenneth Stott
# Canary: 7e0d4a9b-31c8-4f65-a2b7-e96f1c08d4a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The review queue in the org's control plane (REQ-2179).

``glossary_term_scans`` and ``glossary_term_drafts`` (schema_org.py): a scan of a library
document, and each candidate term it proposes as a draft until the steward accepts, merges or
rejects it. Nothing is re-reviewed automatically: a scan is made only when someone asks for one,
and a draft changes only when the steward decides on it.
"""

# Requirements: REQ-2179

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, insert, select, update

from provisa.core.schema_org import glossary_term_drafts as drafts_t
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.term_finding.scan import Candidate

if TYPE_CHECKING:
    from collections.abc import Sequence

    from provisa.core.database import Connection

RUNNING = "running"
DONE = "done"
FAILED = "failed"

DRAFT = "draft"
ACCEPTED = "accepted"
MERGED = "merged"
REJECTED = "rejected"

NEW = "new"
MERGE = "merge"


@dataclass(frozen=True)
class ScanRow:
    id: int
    document_id: int | None
    document_title: str
    document_source: str
    domains: list[str]
    status: str
    error: str | None
    summary: dict[str, Any] | None
    started_by: str | None
    started_at: datetime
    finished_at: datetime | None


@dataclass(frozen=True)
class DraftRow:
    id: int
    scan_id: int
    ordinal: int
    name: str
    category: str
    sense: str
    definition: str | None
    definition_kind: str
    proposal: str
    match_term_id: int | None
    match_kind: str | None
    payload: dict[str, Any]
    status: str
    term_id: int | None
    decided_by: str | None
    decided_at: datetime | None


_SCAN_COLUMNS = (
    scans_t.c.id,
    scans_t.c.document_id,
    scans_t.c.document_title,
    scans_t.c.document_source,
    scans_t.c.domains,
    scans_t.c.status,
    scans_t.c.error,
    scans_t.c.summary,
    scans_t.c.started_by,
    scans_t.c.started_at,
    scans_t.c.finished_at,
)
_DRAFT_COLUMNS = (
    drafts_t.c.id,
    drafts_t.c.scan_id,
    drafts_t.c.ordinal,
    drafts_t.c.name,
    drafts_t.c.category,
    drafts_t.c.sense,
    drafts_t.c.definition,
    drafts_t.c.definition_kind,
    drafts_t.c.proposal,
    drafts_t.c.match_term_id,
    drafts_t.c.match_kind,
    drafts_t.c.payload,
    drafts_t.c.status,
    drafts_t.c.term_id,
    drafts_t.c.decided_by,
    drafts_t.c.decided_at,
)


def _scan(row: Any) -> ScanRow:
    values = list(row)
    values[4] = json.loads(values[4])
    values[7] = None if values[7] is None else json.loads(values[7])
    return ScanRow(*values)


def _draft(row: Any) -> DraftRow:
    values = list(row)
    values[11] = json.loads(values[11])
    return DraftRow(*values)


async def start_scan(
    conn: Connection,
    *,
    document_id: int,
    document_title: str,
    document_source: str,
    domains: Sequence[str],
    started_by: str | None,
) -> int:
    result = await conn.execute_core(
        insert(scans_t)
        .values(
            document_id=document_id,
            document_title=document_title,
            document_source=document_source,
            domains=json.dumps(sorted(domains)),
            status=RUNNING,
            started_by=started_by,
        )
        .returning(scans_t.c.id)
    )
    return int(result.fetchone()[0])


def _payload(c: Candidate) -> str:
    return json.dumps(
        {
            "passages": c.passages,
            "variants": c.variants,
            "conflicts": c.conflicts,
            "homonyms": c.homonyms,
            "matches": c.matches,
        }
    )


async def finish_scan(
    conn: Connection, scan_id: int, candidates: Sequence[Candidate], summary: dict[str, Any]
) -> None:
    """The scan done: its candidates queued as drafts, in the order the document mentions them."""
    if candidates:
        await conn.execute_core_many(
            insert(drafts_t),
            [
                {
                    "scan_id": scan_id,
                    "ordinal": i,
                    "name": c.name,
                    "category": c.category,
                    "sense": c.sense,
                    "definition": c.definition,
                    "definition_kind": c.definition_kind,
                    "proposal": MERGE if c.match_term_id is not None else NEW,
                    "match_term_id": c.match_term_id,
                    "match_kind": c.match_kind,
                    "payload": _payload(c),
                    "status": DRAFT,
                }
                for i, c in enumerate(candidates)
            ],
        )
    await conn.execute_core(
        update(scans_t)
        .where(scans_t.c.id == scan_id)
        .values(status=DONE, summary=json.dumps(summary), finished_at=func.now())
    )


async def fail_scan(conn: Connection, scan_id: int, error: str) -> None:
    await conn.execute_core(
        update(scans_t)
        .where(scans_t.c.id == scan_id)
        .values(status=FAILED, error=error, finished_at=func.now())
    )


async def scans(conn: Connection, *, document_id: int | None = None) -> list[ScanRow]:
    """The scans (of ``document_id``), newest first."""
    query = select(*_SCAN_COLUMNS).order_by(scans_t.c.started_at.desc(), scans_t.c.id.desc())
    if document_id is not None:
        query = query.where(scans_t.c.document_id == document_id)
    return [_scan(r) for r in (await conn.execute_core(query)).fetchall()]


async def scan(conn: Connection, scan_id: int) -> ScanRow | None:
    row = (
        await conn.execute_core(select(*_SCAN_COLUMNS).where(scans_t.c.id == scan_id))
    ).fetchone()
    return None if row is None else _scan(row)


async def draft_counts(conn: Connection) -> dict[int, dict[str, int]]:
    """Each scan's drafts counted by status."""
    rows = (
        await conn.execute_core(
            select(drafts_t.c.scan_id, drafts_t.c.status, func.count()).group_by(
                drafts_t.c.scan_id, drafts_t.c.status
            )
        )
    ).fetchall()
    counts: dict[int, dict[str, int]] = {}
    for scan_id, status, n in rows:
        counts.setdefault(scan_id, {})[status] = int(n)
    return counts


async def drafts(conn: Connection, scan_id: int) -> list[DraftRow]:
    rows = (
        await conn.execute_core(
            select(*_DRAFT_COLUMNS)
            .where(drafts_t.c.scan_id == scan_id)
            .order_by(drafts_t.c.ordinal)
        )
    ).fetchall()
    return [_draft(r) for r in rows]


async def draft(conn: Connection, draft_id: int) -> DraftRow | None:
    row = (
        await conn.execute_core(select(*_DRAFT_COLUMNS).where(drafts_t.c.id == draft_id))
    ).fetchone()
    return None if row is None else _draft(row)


async def edit(
    conn: Connection,
    draft_id: int,
    *,
    name: str,
    definition: str | None,
    payload: dict[str, Any],
) -> None:
    await conn.execute_core(
        update(drafts_t)
        .where(drafts_t.c.id == draft_id)
        .values(name=name, definition=definition, payload=json.dumps(payload))
    )


async def decide(
    conn: Connection, draft_id: int, status: str, *, term_id: int | None, by: str | None
) -> None:
    await conn.execute_core(
        update(drafts_t)
        .where(drafts_t.c.id == draft_id)
        .values(status=status, term_id=term_id, decided_by=by, decided_at=func.now())
    )
