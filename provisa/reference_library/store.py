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
(schema_org.py). A passage's embedding is a pgvector column of any dimension (provisa.core.pgvector)
under one partial HNSW index per dimension in use, ``reference_passages_vector_<n>`` over
``vector::vector(n)`` where ``vector_dims(vector) = n``: an org's model decides the dimension, and a
re-embed with another model brings its own. Writing passages of a dimension creates its index; a
re-embed or a removal that leaves none of a dimension drops it. The passages nearest a question are
found by one query ordered by cosine distance over that index (:func:`nearest_passages`); there is
no unindexed search.
"""

# Requirements: REQ-2185

from __future__ import annotations

import zlib
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, text, update

from provisa.core import pgvector

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
        dimensions = _dimensions([p.vector for p in passages])
        await conn.execute_core_many(
            insert(passages_t),
            [
                {
                    "document_id": document_id,
                    "ordinal": p.ordinal,
                    "section": p.section,
                    "page": p.page,
                    "text": p.text,
                    "vector": list(p.vector),
                }
                for p in passages
            ],
        )
        await _index(conn, dimensions, None)


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
    await _drop_unused_indexes(conn, None)


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
    dimensions = _dimensions(vectors) if vectors else None
    for ordinal, vector in zip(held, vectors, strict=True):
        await conn.execute_core(
            update(passages_t)
            .where((passages_t.c.document_id == document_id) & (passages_t.c.ordinal == ordinal))
            .values(vector=list(vector))
        )
    await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id)
        .values(embedding_model=embedding_model)
    )
    if dimensions is not None:
        await _index(conn, dimensions, None)
    await _drop_unused_indexes(conn, None)


async def remove(conn: Connection, document_id: int) -> bool:
    """Remove the document with its domains and passages. False when there is none."""
    result = await conn.execute_core(
        delete(documents_t).where(documents_t.c.id == document_id).returning(documents_t.c.id)
    )
    if result.fetchone() is None:
        return False
    await _drop_unused_indexes(conn, None)
    return True


# -- the vector index ---------------------------------------------------------------------------

_INDEX_PREFIX = "reference_passages_vector_"
_V = pgvector.SCHEMA


def _dimensions(vectors: Sequence[Sequence[float]]) -> int:
    """The one dimension of ``vectors``, refused when they differ or pgvector cannot index it."""
    found = {len(v) for v in vectors}
    if len(found) != 1:
        raise ValueError(
            f"one document's embeddings have {sorted(found)} dimensions; one is needed"
        )
    (dimensions,) = found
    _indexable(dimensions)
    return dimensions


def _indexable(dimensions: int) -> None:
    if not 0 < dimensions <= pgvector.MAX_INDEXED_DIMENSIONS:
        raise ValueError(
            f"the embeddings have {dimensions} dimensions; pgvector indexes vectors of at most "
            f"{pgvector.MAX_INDEXED_DIMENSIONS}, so the library cannot hold them"
        )


def _table(schema: str | None) -> str:
    return f'"{schema}".reference_passages' if schema else "reference_passages"


def _in_schema(schema: str | None) -> str:
    return "schemaname = :schema" if schema else "schemaname = current_schema()"


async def _indexed(conn: Connection, schema: str | None) -> set[int]:
    """The dimensions ``schema`` (None: the connection's) has a vector index for."""
    rows = await conn.execute_core(
        text(
            f"SELECT indexname FROM pg_indexes WHERE {_in_schema(schema)} "
            f"AND tablename = 'reference_passages' AND indexname LIKE '{_INDEX_PREFIX}%'"
        ).bindparams(**({"schema": schema} if schema else {}))
    )
    return {int(r[0].removeprefix(_INDEX_PREFIX)) for r in rows.fetchall()}


async def _held_dimensions(conn: Connection, schema: str | None) -> set[int]:
    rows = await conn.execute_core(
        text(f"SELECT DISTINCT {_V}.vector_dims(vector) FROM {_table(schema)}")
    )
    return {int(r[0]) for r in rows.fetchall()}


async def _index(conn: Connection, dimensions: int, schema: str | None) -> None:
    """The partial HNSW index of ``dimensions``, created if it is not there. Under a lock on the
    schema's library, so two first writes of a dimension do not both create it."""
    if dimensions in await _indexed(conn, schema):
        return
    async with conn.transaction():
        await conn.execute_core(
            text("SELECT pg_advisory_xact_lock(:key)").bindparams(key=await _lock_key(conn, schema))
        )
        await conn.execute_core(
            text(
                f"CREATE INDEX IF NOT EXISTS {_INDEX_PREFIX}{int(dimensions)} ON {_table(schema)} "
                f"USING hnsw ((vector::{_V}.vector({int(dimensions)})) {_V}.vector_cosine_ops) "
                f"WHERE {_V}.vector_dims(vector) = {int(dimensions)}"
            )
        )


async def _drop_unused_indexes(conn: Connection, schema: str | None) -> None:
    """Drop the index of each dimension no passage has any more."""
    indexed = await _indexed(conn, schema)
    if not indexed:
        return
    unused = indexed - await _held_dimensions(conn, schema)
    prefix = f'"{schema}".' if schema else ""
    for dimensions in sorted(unused):
        await conn.execute_core(text(f"DROP INDEX IF EXISTS {prefix}{_INDEX_PREFIX}{dimensions}"))


async def _lock_key(conn: Connection, schema: str | None) -> int:
    if schema is None:
        schema = (await conn.execute_core(text("SELECT current_schema()"))).scalar()
    return zlib.crc32(f"{schema}.reference_passages.vector".encode())


async def index_vectors(conn: Connection, schema: str | None = None) -> None:
    """``schema``'s (None: the connection's) vector indexes made to fit the passages it holds, for
    passages written other than through this module: a demo's carried library
    (provisa.term_finding.carry) and a sandbox's copy of its template's (env_copy)."""
    for dimensions in sorted(await _held_dimensions(conn, schema)):
        _indexable(dimensions)
        await _index(conn, dimensions, schema)
    await _drop_unused_indexes(conn, schema)


# -- governed retrieval's nearest passages ------------------------------------------------------


def nearest_sql(
    query: Sequence[float], document_ids: Collection[int], k: int
) -> tuple[str, dict[str, Any]]:
    """The search for the ``k`` passages of ``document_ids`` nearest ``query``, and its
    parameters. The dimension is written into the SQL, not bound: the planner matches the
    partial index's predicate and expression only to a constant."""
    n = int(len(query))
    sql = (
        "SELECT document_id, ordinal, section, page, text, 1 - distance AS score FROM ("
        "SELECT document_id, ordinal, section, page, text, "
        f"(vector::{_V}.vector({n})) OPERATOR({_V}.<=>) CAST(:query AS {_V}.vector({n})) "
        "AS distance FROM reference_passages "
        f"WHERE {_V}.vector_dims(vector) = {n} AND document_id = ANY(:ids) "
        "ORDER BY distance LIMIT :k"
        ") nearest ORDER BY distance, document_id, ordinal"
    )
    return sql, {"query": pgvector.literal(query), "ids": sorted(document_ids), "k": k}


async def nearest_passages(
    conn: Connection, query: Sequence[float], document_ids: Collection[int], k: int
) -> list[PassageHit]:
    """The ``k`` passages of ``document_ids`` nearest ``query`` by cosine distance, nearest first,
    found through the index of the question's dimension. A document of another dimension is
    refused by name rather than left out."""
    if not document_ids:
        return []
    # A document's passages are written together, by one model: its first passage's dimension
    # is the document's, read through the (document_id, ordinal) key, one row per document.
    other = (
        await conn.execute_core(
            text(
                f"SELECT d.id, {_V}.vector_dims(p.vector) "
                "FROM unnest(CAST(:ids AS integer[])) AS d(id) CROSS JOIN LATERAL ("
                "SELECT vector FROM reference_passages WHERE document_id = d.id "
                "ORDER BY ordinal LIMIT 1) AS p "
                f"WHERE {_V}.vector_dims(p.vector) <> :n ORDER BY d.id LIMIT 1"
            ).bindparams(ids=sorted(document_ids), n=len(query))
        )
    ).fetchone()
    if other is not None:
        raise ValueError(
            f"library document {other[0]}'s passages have {other[1]} dimensions; "
            f"the question's have {len(query)}"
        )
    sql, params = nearest_sql(query, document_ids, k)
    async with conn.transaction():
        # Filtered by document, an HNSW scan would stop at its candidate list (hnsw.ef_search)
        # and return fewer than k; iterative scanning goes on until k are found, in order.
        await conn.execute_core(text("SET LOCAL hnsw.iterative_scan = strict_order"))
        rows = (await conn.execute_core(text(sql).bindparams(**params))).fetchall()
    return [PassageHit(*row) for row in rows]


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
