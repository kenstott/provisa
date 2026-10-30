# Copyright (c) 2026 Kenneth Stott
# Canary: af742d6f-6cc2-4a3f-b405-0370515b10c2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reference library in the org's control plane (REQ-2185).

``reference_documents``, ``reference_document_domains`` and ``reference_passages``
(schema_org.py). A passage's embedding is held as a JSON array of floats, and the nearest passages
to a question are found by cosine similarity here (:func:`nearest_passages`), the code source
vector collection's pattern; that function is the one place a vector index would replace.
"""

# Requirements: REQ-2185

from __future__ import annotations

import json
import math
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update

from provisa.core.schema_org import (
    reference_document_domains as domains_t,
)
from provisa.core.schema_org import (
    reference_documents as documents_t,
)
from provisa.core.schema_org import (
    reference_passages as passages_t,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

PROPOSED = "proposed"
KEPT = "kept"
STATUSES = (PROPOSED, KEPT)
FILE = "file"
URL = "url"


@dataclass(frozen=True)
class DocumentRow:
    id: int
    title: str
    source_kind: str
    source: str
    media_type: str
    content_sha256: str
    status: str
    embedding_model: str
    added_by: str | None
    added_at: datetime
    refreshed_at: datetime | None
    replaced_at: datetime | None
    domains: tuple[str, ...]
    passages: int


@dataclass(frozen=True)
class StoredPassage:
    """A passage to be written: where it sits, its text and its embedding."""

    ordinal: int
    section: str | None
    page: int | None
    text: str
    vector: Sequence[float]


@dataclass(frozen=True)
class PassageHit:
    """A passage near a question, with its similarity (cosine, higher is nearer)."""

    document_id: int
    ordinal: int
    section: str | None
    page: int | None
    text: str
    score: float


_DOCUMENT_COLUMNS = (
    documents_t.c.id,
    documents_t.c.title,
    documents_t.c.source_kind,
    documents_t.c.source,
    documents_t.c.media_type,
    documents_t.c.content_sha256,
    documents_t.c.status,
    documents_t.c.embedding_model,
    documents_t.c.added_by,
    documents_t.c.added_at,
    documents_t.c.refreshed_at,
    documents_t.c.replaced_at,
)


async def _domains(conn: Connection, ids: Collection[int]) -> dict[int, tuple[str, ...]]:
    if not ids:
        return {}
    result = await conn.execute_core(
        select(domains_t.c.document_id, domains_t.c.domain_id)
        .where(domains_t.c.document_id.in_(list(ids)))
        .order_by(domains_t.c.domain_id)
    )
    held: dict[int, list[str]] = {}
    for document_id, domain_id in result.fetchall():
        held.setdefault(document_id, []).append(domain_id)
    return {k: tuple(v) for k, v in held.items()}


async def _passage_counts(conn: Connection, ids: Collection[int]) -> dict[int, int]:
    if not ids:
        return {}
    result = await conn.execute_core(
        select(passages_t.c.document_id, func.count())
        .where(passages_t.c.document_id.in_(list(ids)))
        .group_by(passages_t.c.document_id)
    )
    return {document_id: int(n) for document_id, n in result.fetchall()}


async def documents(
    conn: Connection, *, ids: Collection[int] | None = None, status: str | None = None
) -> list[DocumentRow]:
    """The library's documents (those of ``ids``, of ``status``), newest first."""
    query = select(*_DOCUMENT_COLUMNS).order_by(documents_t.c.added_at.desc(), documents_t.c.id)
    if ids is not None:
        query = query.where(documents_t.c.id.in_(list(ids)))
    if status is not None:
        query = query.where(documents_t.c.status == status)
    rows = (await conn.execute_core(query)).fetchall()
    found = [r[0] for r in rows]
    scopes = await _domains(conn, found)
    counts = await _passage_counts(conn, found)
    return [
        DocumentRow(*r, domains=scopes.get(r[0], ()), passages=counts.get(r[0], 0)) for r in rows
    ]


async def document(conn: Connection, document_id: int) -> DocumentRow | None:
    found = await documents(conn, ids=[document_id])
    return found[0] if found else None


async def find_url(conn: Connection, url: str) -> int | None:
    """The document added from ``url``, if the library holds one."""
    result = await conn.execute_core(
        select(documents_t.c.id).where(
            (documents_t.c.source_kind == URL) & (documents_t.c.source == url)
        )
    )
    row = result.fetchone()
    return None if row is None else int(row[0])


async def find_content(conn: Connection, sha256: str) -> int | None:
    """The document whose content has digest ``sha256``, if the library holds one."""
    result = await conn.execute_core(
        select(documents_t.c.id).where(documents_t.c.content_sha256 == sha256).limit(1)
    )
    row = result.fetchone()
    return None if row is None else int(row[0])


async def insert_document(
    conn: Connection,
    *,
    title: str,
    source_kind: str,
    source: str,
    media_type: str,
    content_sha256: str,
    status: str,
    embedding_model: str,
    added_by: str | None,
    domains: Collection[str],
    passages: Sequence[StoredPassage],
) -> int:
    result = await conn.execute_core(
        insert(documents_t)
        .values(
            title=title,
            source_kind=source_kind,
            source=source,
            media_type=media_type,
            content_sha256=content_sha256,
            status=status,
            embedding_model=embedding_model,
            added_by=added_by,
        )
        .returning(documents_t.c.id)
    )
    document_id = int(result.fetchone()[0])
    await set_domains(conn, document_id, domains)
    await _write_passages(conn, document_id, passages)
    return document_id


async def set_domains(conn: Connection, document_id: int, domains: Collection[str]) -> None:
    await conn.execute_core(delete(domains_t).where(domains_t.c.document_id == document_id))
    if domains:
        await conn.execute_core_many(
            insert(domains_t),
            [{"document_id": document_id, "domain_id": d} for d in sorted(domains)],
        )


async def set_status(conn: Connection, document_id: int, status: str) -> None:
    await conn.execute_core(
        update(documents_t).where(documents_t.c.id == document_id).values(status=status)
    )


async def set_title(conn: Connection, document_id: int, title: str) -> None:
    await conn.execute_core(
        update(documents_t).where(documents_t.c.id == document_id).values(title=title)
    )


async def _write_passages(
    conn: Connection, document_id: int, passages: Sequence[StoredPassage]
) -> None:
    if passages:
        await conn.execute_core_many(
            insert(passages_t),
            [
                {
                    "document_id": document_id,
                    "ordinal": p.ordinal,
                    "section": p.section,
                    "page": p.page,
                    "text": p.text,
                    "vector": json.dumps(list(p.vector)),
                }
                for p in passages
            ],
        )


async def replace_content(
    conn: Connection,
    document_id: int,
    *,
    media_type: str,
    content_sha256: str,
    embedding_model: str,
    passages: Sequence[StoredPassage],
    source: str | None,
    refreshed: bool,
) -> None:
    """The document's content replaced by ``passages``; earlier content is not kept. Its place,
    status and domains stay. ``refreshed``: fetched again from its URL (else a new version was
    uploaded, named ``source``)."""
    now = func.now()
    values: dict[str, Any] = {
        "media_type": media_type,
        "content_sha256": content_sha256,
        "embedding_model": embedding_model,
    }
    if refreshed:
        values["refreshed_at"] = now
    else:
        values["replaced_at"] = now
    if source is not None:
        values["source"] = source
    await conn.execute_core(
        update(documents_t).where(documents_t.c.id == document_id).values(**values)
    )
    await conn.execute_core(delete(passages_t).where(passages_t.c.document_id == document_id))
    await _write_passages(conn, document_id, passages)


async def replace_vectors(
    conn: Connection,
    document_id: int,
    *,
    embedding_model: str,
    vectors: Sequence[Sequence[float]],
) -> None:
    """The document's passages re-embedded: ``vectors`` are the new embeddings of its passages in
    ordinal order, made by ``embedding_model``. Text, sections and everything else stay."""
    held = [o for o, _, _, _ in await passages(conn, document_id)]
    if len(vectors) != len(held):
        raise ValueError(f"{len(vectors)} embeddings for {len(held)} passages")
    for ordinal, vector in zip(held, vectors, strict=True):
        await conn.execute_core(
            update(passages_t)
            .where((passages_t.c.document_id == document_id) & (passages_t.c.ordinal == ordinal))
            .values(vector=json.dumps(list(vector)))
        )
    await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id)
        .values(embedding_model=embedding_model)
    )


async def remove(conn: Connection, document_id: int) -> bool:
    """Remove the document with its domains and passages. False when there is none."""
    result = await conn.execute_core(
        delete(documents_t).where(documents_t.c.id == document_id).returning(documents_t.c.id)
    )
    return result.fetchone() is not None


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return 0.0 if norm == 0 else dot / norm


async def nearest_passages(
    conn: Connection, query: Sequence[float], document_ids: Collection[int], k: int
) -> list[PassageHit]:
    """The ``k`` passages of ``document_ids`` nearest ``query``, nearest first."""
    if not document_ids:
        return []
    result = await conn.execute_core(
        select(
            passages_t.c.document_id,
            passages_t.c.ordinal,
            passages_t.c.section,
            passages_t.c.page,
            passages_t.c.text,
            passages_t.c.vector,
        ).where(passages_t.c.document_id.in_(list(document_ids)))
    )
    hits: list[PassageHit] = []
    for document_id, ordinal, section, page, text, vector in result.fetchall():
        embedded = json.loads(vector)
        if len(embedded) != len(query):
            raise ValueError(
                f"library document {document_id}'s passages have {len(embedded)} dimensions; "
                f"the question's have {len(query)}"
            )
        hits.append(PassageHit(document_id, ordinal, section, page, text, _cosine(query, embedded)))
    hits.sort(key=lambda h: (-h.score, h.document_id, h.ordinal))
    return hits[:k]


async def passages(
    conn: Connection, document_id: int
) -> list[tuple[int, str | None, int | None, str]]:
    """The document's passages in order: ordinal, section, page, text."""
    result = await conn.execute_core(
        select(passages_t.c.ordinal, passages_t.c.section, passages_t.c.page, passages_t.c.text)
        .where(passages_t.c.document_id == document_id)
        .order_by(passages_t.c.ordinal)
    )
    return [tuple(r) for r in result.fetchall()]  # type: ignore[misc]
