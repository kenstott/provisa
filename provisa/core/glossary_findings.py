# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1a8f3c-2e94-4b7a-9c05-8f3e1b6d2a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a curator should look at in the ontology (REQ-2146): findings, by term, each
``{kind, message, params}``. One function per kind, in :data:`FINDERS`; a kind is added by adding
one. A plain draft -- a term not yet approved -- is not a finding: drafts are the ontology's
ordinary state, and the term list already says which they are. A term reaching data only through
drafts is (path_pending), naming the approval that grounds it."""

# Requirements: REQ-2146

from __future__ import annotations

import datetime
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.glossary import GROUNDED_PENDING, NOT_GROUNDED
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import identity
from provisa.core.schema_org import glossary_terms

if TYPE_CHECKING:
    from provisa.core.database import Connection

Finding = dict[str, Any]
#: A finder: the findings of its kind, by term id.
Finder = Callable[["Connection"], Awaitable[dict[int, list[Finding]]]]


def _finding(kind: str, message: str, *, case: str | None = None, **params: Any) -> Finding:
    """A finding; ``code`` names its message in the UI catalogs (serverErrors), one per kind or,
    where a kind has cases, per case."""
    code = f"glossary.finding.{kind}" + (f"_{case}" if case else "")
    return {"kind": kind, "code": code, "message": message, "params": params}


async def unlinked_tables(conn: "Connection") -> dict[int, list[Finding]]:
    """A Class grounded in several tables that same-entity relationships do not all join
    (REQ-2141)."""
    out: dict[int, list[Finding]] = {}
    for term_id in await identity.identity_gap_ids(conn):
        tables = [
            f"{t['source_id']}.{t['schema_name']}.{t['table_name']}"
            for t in await glossary_repo.term_tables(conn, term_id)
        ]
        out[term_id] = [
            _finding(
                "unlinked_tables",
                "the class is grounded in tables that same-entity relationships do not all join",
                tables=", ".join(tables),
            )
        ]
    return out


async def class_and_attribute(conn: "Connection") -> dict[int, list[Finding]]:
    """A Class holding column refs that are not its keys, and a keyed table left without a Class
    because its concept is a Property holding non-key refs (REQ-2141)."""
    conflicts = await glossary_repo.role_conflicts(conn)
    out: dict[int, list[Finding]] = {}
    for t in conflicts["terms"]:
        out.setdefault(t["term_id"], []).append(
            _finding(
                "class_and_attribute",
                "the class also holds columns that are not its keys",
                tables=", ".join(t["tables"]),
                columns=", ".join(t["columns"]),
            )
        )
    for t in conflicts["tables"]:
        out.setdefault(t["term_id"], []).append(
            _finding(
                "class_and_attribute",
                "a table's concept is this term, which holds columns that are not keys, so the "
                "table has no class",
                case="table",
                table=t["table"],
                columns=", ".join(t["columns"]),
            )
        )
    return out


async def tag_wrong_kind(conn: "Connection") -> dict[int, list[Finding]]:
    """A term tag on a term that is not a Property: only a Property passes a tag to its columns
    (REQ-2145). The terms are role_conflicts' tag list."""
    tagged: dict[int, list[dict]] = {}
    for t in (await glossary_repo.role_conflicts(conn))["tags"]:
        tagged.setdefault(t["term_id"], []).append(t)
    return {
        term_id: [
            _finding(
                "tag_wrong_kind",
                "the term carries tags only a property passes to columns",
                tags=", ".join(t["tag_id"] for t in items),
                role=items[0]["role"],
            )
        ]
        for term_id, items in tagged.items()
    }


async def retirement_blocked(conn: "Connection") -> dict[int, list[Finding]]:
    """A deprecated term past its planned retirement that still holds columns or tables, which
    need a successor no date can name (REQ-2144)."""
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.retire_on)
            .where(glossary_terms.c.deprecation_asserted & ~glossary_terms.c.retired)
            .where(glossary_terms.c.retire_on <= datetime.date.today())
        )
    ).fetchall()
    out: dict[int, list[Finding]] = {}
    for r in rows:
        role = await glossary_repo.term_role(conn, r.id)
        if role != "concept":
            out[r.id] = [
                _finding(
                    "retirement_blocked",
                    "the term was due for retirement but still holds data; retire it with a "
                    "successor to take it",
                    retire_on=r.retire_on.isoformat(),
                    role=role,
                )
            ]
    return out


async def no_data(conn: "Connection") -> dict[int, list[Finding]]:
    """A draft Concept that reaches no data: no chain of in-service edges leads it to a term
    holding a column or a table (REQ-1387). An approved one is approved_not_grounded."""
    grounded = await glossary_repo.grounded_ids(conn)
    statuses = await glossary_repo.term_statuses(conn)
    out: dict[int, list[Finding]] = {}
    for term_id, status in statuses.items():
        if status != "draft" or term_id in grounded:
            continue
        if await glossary_repo.term_role(conn, term_id) == "concept":
            out[term_id] = [
                _finding("no_data", "the concept is connected to no term that holds data")
            ]
    return out


async def approved_not_grounded(conn: "Connection") -> dict[int, list[Finding]]:
    """An approved term that reaches no data (REQ-1387): an agent cannot see it."""
    grounding = await glossary_repo.grounding(conn)
    statuses = await glossary_repo.term_statuses(conn)
    return {
        term_id: [
            _finding(
                "approved_not_grounded",
                "the term is approved but reaches no data, so no agent sees it",
            )
        ]
        for term_id, status in statuses.items()
        if status == "approved" and grounding[term_id] == NOT_GROUNDED
    }


async def path_pending(conn: "Connection") -> dict[int, list[Finding]]:
    """A term that reaches data only through terms not yet approved (REQ-1387): Grounded · path
    pending approval, naming the terms whose approval alone grounds it. A pending term no single
    approval grounds names none."""
    grounding = await glossary_repo.grounding(conn)
    unlocking = await glossary_repo.pending_on(conn)
    names = {
        r.id: r.name
        for r in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
    }
    out: dict[int, list[Finding]] = {}
    for term_id, value in grounding.items():
        if value != GROUNDED_PENDING:
            continue
        drafts = sorted(names[d] for d in unlocking.get(term_id, []))
        out[term_id] = [
            _finding(
                "path_pending",
                "Grounded · path pending approval"
                + (f": approving {', '.join(drafts)} grounds it" if drafts else ""),
                case="approve" if drafts else None,
                approve=", ".join(drafts),
            )
        ]
    return out


FINDERS: dict[str, Finder] = {
    "unlinked_tables": unlinked_tables,
    "class_and_attribute": class_and_attribute,
    "tag_wrong_kind": tag_wrong_kind,
    "retirement_blocked": retirement_blocked,
    "no_data": no_data,
    "approved_not_grounded": approved_not_grounded,
    "path_pending": path_pending,
}


async def findings(conn: "Connection") -> dict[int, list[Finding]]:
    """Every finding, by term id, in the order of :data:`FINDERS`."""
    out: dict[int, list[Finding]] = {}
    for finder in FINDERS.values():
        for term_id, found in (await finder(conn)).items():
            out.setdefault(term_id, []).extend(found)
    return out
