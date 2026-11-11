# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b05e9-6a21-4c7f-9e48-b1f27c6d0a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An import and its proposals, kept for review in the org's control plane (REQ-2237).

``glossary_ontology_imports``, ``glossary_ontology_items`` and ``glossary_ontology_alignments``
(schema_org.py). An import is kept as it was read; an item changes only when a steward edits,
excludes or applies it, and an alignment only when a steward decides on it or it is written.
"""

# Requirements: REQ-2237

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update

from provisa.core.schema_org import glossary_ontology_alignments as alignments_t
from provisa.core.schema_org import glossary_ontology_imports as imports_t
from provisa.core.schema_org import glossary_ontology_items as items_t
from provisa.ontology_import.align import Proposal
from provisa.ontology_import.parse import Parsed

if TYPE_CHECKING:
    from provisa.core.database import Connection

PENDING = "pending"
EXCLUDED = "excluded"
APPLIED = "applied"

PROPOSED = "proposed"
APPROVED = "approved"
REJECTED = "rejected"
WRITTEN = "written"


@dataclass(frozen=True)
class ImportRow:
    id: int
    title: str
    file_name: str
    format: str
    domains: list[str]
    summary: dict[str, Any]
    imported_by: str | None
    imported_at: datetime
    applied_by: str | None
    applied_at: datetime | None


@dataclass(frozen=True)
class ItemRow:
    id: int
    import_id: int
    ordinal: int
    name: str
    kind: str
    definition: str | None
    iri: str | None
    #: ``keys`` (its identifiers in the file), ``labels`` (its alternative labels) and
    #: ``statements`` (what the file states about it that a term does not model).
    payload: dict[str, Any]
    status: str
    term_id: int | None


@dataclass(frozen=True)
class AlignmentRow:
    id: int
    import_id: int
    item_id: int
    term_id: int | None
    other_item_id: int | None
    rel_type: str
    item_is_source: bool
    basis: str
    status: str
    use_definition: bool
    decided_by: str | None
    decided_at: datetime | None


_IMPORT_COLUMNS = tuple(imports_t.c[f] for f in ImportRow.__dataclass_fields__)
_ITEM_COLUMNS = tuple(items_t.c[f] for f in ItemRow.__dataclass_fields__)
_ALIGNMENT_COLUMNS = tuple(alignments_t.c[f] for f in AlignmentRow.__dataclass_fields__)


def _import(row: Any) -> ImportRow:
    values = dict(row._mapping)
    values["domains"] = json.loads(values["domains"])
    values["summary"] = json.loads(values["summary"])
    return ImportRow(**values)


def _item(row: Any) -> ItemRow:
    values = dict(row._mapping)
    values["payload"] = json.loads(values["payload"])
    return ItemRow(**values)


def _alignment(row: Any) -> AlignmentRow:
    values = dict(row._mapping)
    values["item_is_source"] = bool(values["item_is_source"])
    values["use_definition"] = bool(values["use_definition"])
    return AlignmentRow(**values)


async def create(
    conn: Connection,
    parsed: Parsed,
    proposals: Sequence[Proposal],
    *,
    file_name: str,
    domains: Sequence[str],
    by: str | None,
) -> int:
    """The import of ``parsed`` with its ``proposals``, every one waiting on a steward."""
    summary = {
        "items": len(parsed.items),
        "statements": len(parsed.edges),
        "kept": sum(len(item.statements) for item in parsed.items),
        "left_out": parsed.left_out,
    }
    if parsed.modules:
        summary["modules"] = parsed.modules
        summary["imports"] = parsed.imports
    result = await conn.execute_core(
        insert(imports_t)
        .values(
            title=parsed.title,
            file_name=file_name,
            format=parsed.format,
            domains=json.dumps(sorted(domains)),
            summary=json.dumps(summary),
            imported_by=by,
        )
        .returning(imports_t.c.id)
    )
    import_id = int(result.fetchone()[0])
    await conn.execute_core_many(
        insert(items_t),
        [
            {
                "import_id": import_id,
                "ordinal": ordinal,
                "name": item.name,
                "kind": item.kind,
                "definition": item.definition,
                "iri": item.iri,
                "payload": json.dumps(
                    {"keys": item.keys, "labels": item.labels, "statements": item.statements}
                ),
                "status": PENDING,
            }
            for ordinal, item in enumerate(parsed.items)
        ],
    )
    ids = {
        ordinal: item_id
        for ordinal, item_id in (
            await conn.execute_core(
                select(items_t.c.ordinal, items_t.c.id).where(items_t.c.import_id == import_id)
            )
        ).fetchall()
    }
    if proposals:
        await conn.execute_core_many(
            insert(alignments_t),
            [
                {
                    "import_id": import_id,
                    "item_id": ids[p.item],
                    "term_id": p.term_id,
                    "other_item_id": None if p.other_item is None else ids[p.other_item],
                    "rel_type": p.rel_type,
                    "item_is_source": p.item_is_source,
                    "basis": p.basis,
                    "status": PROPOSED,
                }
                for p in proposals
            ],
        )
    return import_id


async def imports(conn: Connection) -> list[ImportRow]:
    """Every import, newest first."""
    query = select(*_IMPORT_COLUMNS).order_by(imports_t.c.imported_at.desc(), imports_t.c.id.desc())
    return [_import(r) for r in (await conn.execute_core(query)).fetchall()]


async def import_(conn: Connection, import_id: int) -> ImportRow | None:
    row = (
        await conn.execute_core(select(*_IMPORT_COLUMNS).where(imports_t.c.id == import_id))
    ).fetchone()
    return None if row is None else _import(row)


async def counts(conn: Connection) -> dict[int, dict[str, dict[str, int]]]:
    """Each import's items and alignments counted by status."""
    out: dict[int, dict[str, dict[str, int]]] = {}
    for key, table in (("items", items_t), ("alignments", alignments_t)):
        rows = (
            await conn.execute_core(
                select(table.c.import_id, table.c.status, func.count()).group_by(
                    table.c.import_id, table.c.status
                )
            )
        ).fetchall()
        for import_id, status, n in rows:
            out.setdefault(import_id, {"items": {}, "alignments": {}})[key][status] = int(n)
    return out


async def items(conn: Connection, import_id: int) -> list[ItemRow]:
    rows = (
        await conn.execute_core(
            select(*_ITEM_COLUMNS)
            .where(items_t.c.import_id == import_id)
            .order_by(items_t.c.ordinal)
        )
    ).fetchall()
    return [_item(r) for r in rows]


async def item(conn: Connection, item_id: int) -> ItemRow | None:
    row = (
        await conn.execute_core(select(*_ITEM_COLUMNS).where(items_t.c.id == item_id))
    ).fetchone()
    return None if row is None else _item(row)


async def alignments(conn: Connection, import_id: int) -> list[AlignmentRow]:
    rows = (
        await conn.execute_core(
            select(*_ALIGNMENT_COLUMNS)
            .where(alignments_t.c.import_id == import_id)
            .order_by(alignments_t.c.id)
        )
    ).fetchall()
    return [_alignment(r) for r in rows]


async def alignment(conn: Connection, alignment_id: int) -> AlignmentRow | None:
    row = (
        await conn.execute_core(
            select(*_ALIGNMENT_COLUMNS).where(alignments_t.c.id == alignment_id)
        )
    ).fetchone()
    return None if row is None else _alignment(row)


async def edit_item(
    conn: Connection, item_id: int, *, name: str, definition: str | None, status: str
) -> None:
    await conn.execute_core(
        update(items_t)
        .where(items_t.c.id == item_id)
        .values(name=name, definition=definition, status=status)
    )


async def item_applied(conn: Connection, item_id: int, term_id: int) -> None:
    await conn.execute_core(
        update(items_t).where(items_t.c.id == item_id).values(status=APPLIED, term_id=term_id)
    )


async def add_alignment(
    conn: Connection,
    *,
    import_id: int,
    item_id: int,
    term_id: int | None,
    other_item_id: int | None,
    rel_type: str,
    item_is_source: bool,
    basis: str,
    use_definition: bool,
    by: str | None,
) -> int:
    """An alignment a steward adds: approved as it is added, since adding it is their decision."""
    result = await conn.execute_core(
        insert(alignments_t)
        .values(
            import_id=import_id,
            item_id=item_id,
            term_id=term_id,
            other_item_id=other_item_id,
            rel_type=rel_type,
            item_is_source=item_is_source,
            basis=basis,
            status=APPROVED,
            use_definition=use_definition,
            decided_by=by,
            decided_at=func.now(),
        )
        .returning(alignments_t.c.id)
    )
    return int(result.fetchone()[0])


async def decide(
    conn: Connection, alignment_id: int, status: str, *, use_definition: bool, by: str | None
) -> None:
    await conn.execute_core(
        update(alignments_t)
        .where(alignments_t.c.id == alignment_id)
        .values(status=status, use_definition=use_definition, decided_by=by, decided_at=func.now())
    )


async def written(conn: Connection, alignment_id: int) -> None:
    await conn.execute_core(
        update(alignments_t).where(alignments_t.c.id == alignment_id).values(status=WRITTEN)
    )


async def applied(conn: Connection, import_id: int, by: str | None) -> None:
    await conn.execute_core(
        update(imports_t)
        .where(imports_t.c.id == import_id)
        .values(applied_by=by, applied_at=func.now())
    )


async def remove(conn: Connection, import_id: int) -> bool:
    """Discard the import and its proposals. The terms it made stay: they are the glossary's."""
    result = await conn.execute_core(
        delete(imports_t).where(imports_t.c.id == import_id).returning(imports_t.c.id)
    )
    return result.fetchone() is not None
