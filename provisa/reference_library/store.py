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
no unindexed search. pgvector indexes a ``vector`` to 2000 dimensions; from 2001 to 4000 the index is
over ``vector::halfvec(n)`` (half precision) while the column stays full precision, and above 4000
the library refuses the embeddings by name.

A passage's neighbours, for governed retrieval: the passages just before and after it in its
document, read through the (document_id, ordinal) key (:func:`adjacent_passages`), and the passages
nearest it by embedding, the same indexed search seeded with its own vector (:func:`near_passage`).
"""

# Requirements: REQ-2185

from __future__ import annotations

import zlib
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import Select, delete, func, insert, or_, select, text, update

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
#: A document's index status (ADD NOW, INDEX IN THE BACKGROUND): being read and indexed, usable, or
#: stopped with a reason and a retry.
INDEXING = "indexing"
READY = "ready"
FAILED = "failed"


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
    index_status: str
    index_error: str | None
    index_step: str | None
    index_done: int | None
    index_total: int | None
    terms_note: str | None
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
    documents_t.c.index_status,
    documents_t.c.index_error,
    documents_t.c.index_step,
    documents_t.c.index_done,
    documents_t.c.index_total,
    documents_t.c.terms_note,
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
    conn: Connection,
    *,
    ids: Collection[int] | None = None,
    status: str | None = None,
    ready: bool = False,
) -> list[DocumentRow]:
    """The library's documents (those of ``ids``, of ``status``, only the ``ready`` ones), newest
    first."""
    query = select(*_DOCUMENT_COLUMNS).order_by(documents_t.c.added_at.desc(), documents_t.c.id)
    if ready:
        query = query.where(documents_t.c.index_status == READY)
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


async def insert_pending(
    conn: Connection,
    *,
    title: str,
    source_kind: str,
    source: str,
    media_type: str,
    content_sha256: str,
    pending: bytes | None,
    status: str,
    added_by: str | None,
    domains: Collection[str],
    launch: str,
) -> int:
    """A document that exists from now on, with no passages yet: its background job reads and
    indexes it. A URL document's digest and media type are empty until the job has fetched it."""
    result = await conn.execute_core(
        insert(documents_t)
        .values(
            title=title,
            source_kind=source_kind,
            source=source,
            media_type=media_type,
            content_sha256=content_sha256,
            status=status,
            embedding_model="",
            added_by=added_by,
            index_status=INDEXING,
            index_launch=launch,
            index_progress_at=func.now(),
            pending_content=pending,
        )
        .returning(documents_t.c.id)
    )
    document_id = int(result.fetchone()[0])
    await set_domains(conn, document_id, domains)
    return document_id


async def pending_content(conn: Connection, document_id: int) -> bytes | None:
    """The uploaded bytes a document not yet ready is waiting to have read."""
    result = await conn.execute_core(
        select(documents_t.c.pending_content).where(documents_t.c.id == document_id)
    )
    row = result.fetchone()
    return None if row is None or row[0] is None else bytes(row[0])


async def record_index_progress(
    conn: Connection,
    document_id: int,
    *,
    step: str,
    done: int | None = None,
    total: int | None = None,
) -> bool:
    """What the job is doing, if the document is still indexing. False when it no longer is (it was
    removed or given up on), and the job stops there."""
    result = await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id, documents_t.c.index_status == INDEXING)
        .values(index_step=step, index_done=done, index_total=total, index_progress_at=func.now())
        .returning(documents_t.c.id)
    )
    return result.fetchone() is not None


async def finish_indexing(
    conn: Connection,
    document_id: int,
    *,
    title: str,
    media_type: str,
    content_sha256: str,
    embedding_model: str,
    passages: Sequence[StoredPassage],
) -> bool:
    """The document made ready with ``passages``, its uploaded bytes dropped. False when it is no
    longer indexing, and nothing is written."""
    result = await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id, documents_t.c.index_status == INDEXING)
        .values(
            title=title,
            media_type=media_type,
            content_sha256=content_sha256,
            embedding_model=embedding_model,
            index_status=READY,
            index_error=None,
            index_step=None,
            index_done=None,
            index_total=None,
            pending_content=None,
        )
        .returning(documents_t.c.id)
    )
    if result.fetchone() is None:
        return False
    await _write_passages(conn, document_id, passages)
    return True


async def fail_indexing(conn: Connection, document_id: int, reason: str) -> None:
    """The document's job has stopped, for ``reason``; a retry can run it again."""
    await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id, documents_t.c.index_status == INDEXING)
        .values(index_status=FAILED, index_error=reason, index_step=None)
    )


async def begin_retry(conn: Connection, document_id: int, launch: str) -> bool:
    """A failed document back to indexing, in ``launch``. False when it is not a failed one."""
    result = await conn.execute_core(
        update(documents_t)
        .where(documents_t.c.id == document_id, documents_t.c.index_status == FAILED)
        .values(
            index_status=INDEXING,
            index_error=None,
            index_step=None,
            index_done=None,
            index_total=None,
            index_launch=launch,
            index_progress_at=func.now(),
            terms_note=None,
        )
        .returning(documents_t.c.id)
    )
    return result.fetchone() is not None


async def fail_indexing_left_behind(conn: Connection, launch: str, reason: str) -> int:
    """Fail the documents indexing in an earlier launch, whose server is gone; how many."""
    result = await conn.execute_core(
        update(documents_t)
        .where(
            documents_t.c.index_status == INDEXING,
            or_(documents_t.c.index_launch.is_(None), documents_t.c.index_launch != launch),
        )
        .values(index_status=FAILED, index_error=reason, index_step=None)
        .returning(documents_t.c.id)
    )
    return len(result.fetchall())


async def fail_indexing_stalled(conn: Connection, after: timedelta, reason: str) -> int:
    """Fail the documents whose job has not moved on for ``after``; how many."""
    result = await conn.execute_core(
        update(documents_t)
        .where(
            documents_t.c.index_status == INDEXING,
            documents_t.c.index_progress_at < datetime.now(UTC) - after,
        )
        .values(index_status=FAILED, index_error=reason, index_step=None)
        .returning(documents_t.c.id)
    )
    return len(result.fetchall())


async def set_terms_note(conn: Connection, document_id: int, note: str | None) -> None:
    """Why the document's terms were not looked for; None when they were."""
    await conn.execute_core(
        update(documents_t).where(documents_t.c.id == document_id).values(terms_note=note)
    )


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
        dimensions = dimensions_of([p.vector for p in passages])
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
        await index(conn, dimensions, None, PASSAGES)


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
    await drop_unused_indexes(conn, None, PASSAGES)


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
    dimensions = dimensions_of(vectors) if vectors else None
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
        await index(conn, dimensions, None, PASSAGES)
    await drop_unused_indexes(conn, None, PASSAGES)


async def remove(conn: Connection, document_id: int) -> bool:
    """Remove the document with its domains and passages. False when there is none."""
    result = await conn.execute_core(
        delete(documents_t).where(documents_t.c.id == document_id).returning(documents_t.c.id)
    )
    if result.fetchone() is None:
        return False
    await drop_unused_indexes(conn, None, PASSAGES)
    return True


# -- the vector index ---------------------------------------------------------------------------

#: The tables whose ``vector`` column is held under one partial index per dimension: the passages,
#: and the glossary terms' embeddings governed retrieval seeds its walk with (terms.py).
PASSAGES = "reference_passages"
TERM_VECTORS = "glossary_term_vectors"
_V = pgvector.SCHEMA


def _prefix(table: str) -> str:
    return f"{table}_vector_"


def dimensions_of(vectors: Sequence[Sequence[float]]) -> int:
    """The one dimension of ``vectors``, refused when they differ or pgvector cannot index it."""
    found = {len(v) for v in vectors}
    if len(found) != 1:
        raise ValueError(f"embeddings of {sorted(found)} dimensions given together; one is needed")
    (dimensions,) = found
    _indexable(dimensions)
    return dimensions


#: The most dimensions pgvector's HNSW index takes for a ``halfvec``.
_MAX_HALF_DIMENSIONS = 4000


def _half(dimensions: int) -> bool:
    """Whether ``dimensions`` are indexed as ``halfvec``: more than a ``vector`` index takes."""
    return dimensions > pgvector.MAX_INDEXED_DIMENSIONS


def vector_type(dimensions: int) -> str:
    """The type the index of ``dimensions`` is over, and a search casts to."""
    return f"{_V}.{'halfvec' if _half(dimensions) else 'vector'}({int(dimensions)})"


def _ops(dimensions: int) -> str:
    return f"{_V}.{'halfvec' if _half(dimensions) else 'vector'}_cosine_ops"


def _indexable(dimensions: int) -> None:
    if not 0 < dimensions <= _MAX_HALF_DIMENSIONS:
        raise ValueError(
            f"the embeddings have {dimensions} dimensions; the library indexes embeddings of at "
            f"most {_MAX_HALF_DIMENSIONS}, so it cannot hold them"
        )


def _table(schema: str | None, table: str) -> str:
    return f'"{schema}".{table}' if schema else table


def _in_schema(schema: str | None) -> str:
    return "schemaname = :schema" if schema else "schemaname = current_schema()"


async def _indexed(conn: Connection, schema: str | None, table: str) -> set[int]:
    """The dimensions ``schema`` (None: the connection's) has a vector index of ``table`` for."""
    rows = await conn.execute_core(
        text(
            f"SELECT indexname FROM pg_indexes WHERE {_in_schema(schema)} "
            f"AND tablename = '{table}' AND indexname LIKE '{_prefix(table)}%'"
        ).bindparams(**({"schema": schema} if schema else {}))
    )
    return {int(r[0].removeprefix(_prefix(table))) for r in rows.fetchall()}


async def _held_dimensions(conn: Connection, schema: str | None, table: str) -> set[int]:
    rows = await conn.execute_core(
        text(f"SELECT DISTINCT {_V}.vector_dims(vector) FROM {_table(schema, table)}")
    )
    return {int(r[0]) for r in rows.fetchall()}


async def index(conn: Connection, dimensions: int, schema: str | None, table: str) -> None:
    """``table``'s partial HNSW index of ``dimensions``, created if it is not there. Under a lock
    on the schema's ``table``, so two first writes of a dimension do not both create it."""
    if dimensions in await _indexed(conn, schema, table):
        return
    async with conn.transaction():
        await conn.execute_core(
            text("SELECT pg_advisory_xact_lock(:key)").bindparams(
                key=await _lock_key(conn, schema, table)
            )
        )
        await conn.execute_core(
            text(
                f"CREATE INDEX IF NOT EXISTS {_prefix(table)}{int(dimensions)} "
                f"ON {_table(schema, table)} "
                f"USING hnsw ((vector::{vector_type(dimensions)}) {_ops(dimensions)}) "
                f"WHERE {_V}.vector_dims(vector) = {int(dimensions)}"
            )
        )


async def drop_unused_indexes(conn: Connection, schema: str | None, table: str) -> None:
    """Drop ``table``'s index of each dimension no row of it has any more."""
    indexed = await _indexed(conn, schema, table)
    if not indexed:
        return
    unused = indexed - await _held_dimensions(conn, schema, table)
    prefix = f'"{schema}".' if schema else ""
    for dimensions in sorted(unused):
        await conn.execute_core(text(f"DROP INDEX IF EXISTS {prefix}{_prefix(table)}{dimensions}"))


async def _lock_key(conn: Connection, schema: str | None, table: str) -> int:
    if schema is None:
        schema = (await conn.execute_core(text("SELECT current_schema()"))).scalar()
    return zlib.crc32(f"{schema}.{table}.vector".encode())


async def index_vectors(conn: Connection, schema: str | None = None) -> None:
    """``schema``'s (None: the connection's) vector indexes made to fit the passages and term
    embeddings it holds, for rows written other than through the library: a demo's carried
    library (provisa.term_finding.carry) and a sandbox's copy of its template's (env_copy)."""
    for table in (PASSAGES, TERM_VECTORS):
        for dimensions in sorted(await _held_dimensions(conn, schema, table)):
            _indexable(dimensions)
            await index(conn, dimensions, schema, table)
        await drop_unused_indexes(conn, schema, table)


# -- governed retrieval's nearest passages ------------------------------------------------------


def _nearest_sql(n: int, operand: str, where: str = "") -> str:
    """The indexed cosine search over ``n``-dimension passages of ``:ids`` for the ``:k`` nearest
    ``operand`` (an expression of the index's type), each also meeting ``where``."""
    kind = vector_type(n)
    return (
        "SELECT document_id, ordinal, section, page, text, 1 - distance AS score FROM ("
        "SELECT document_id, ordinal, section, page, text, "
        f"(vector::{kind}) OPERATOR({_V}.<=>) {operand} "
        "AS distance FROM reference_passages "
        f"WHERE {_V}.vector_dims(vector) = {int(n)} AND document_id = ANY(:ids){where} "
        "ORDER BY distance LIMIT :k"
        ") nearest ORDER BY distance, document_id, ordinal"
    )


def nearest_sql(
    query: Sequence[float], document_ids: Collection[int], k: int
) -> tuple[str, dict[str, Any]]:
    """The search for the ``k`` passages of ``document_ids`` nearest ``query``, and its
    parameters. The dimension is written into the SQL, not bound: the planner matches the
    partial index's predicate and expression only to a constant. From 2001 dimensions the index is
    over ``halfvec``, so the column and the question are cast to it and the order, and the score,
    come from the half-precision distance (the order is that of the half-precision index)."""
    n = int(len(query))
    sql = _nearest_sql(n, f"CAST(:query AS {vector_type(n)})")
    return sql, {"query": pgvector.literal(query), "ids": sorted(document_ids), "k": k}


def near_passage_sql(
    document_id: int, ordinal: int, dimensions: int, document_ids: Collection[int], k: int
) -> tuple[str, dict[str, Any]]:
    """:func:`nearest_sql` seeded with a passage's own embedding (read by its key in the same
    statement) rather than a question's, the passage itself left out."""
    seed = "document_id = :seed_document AND ordinal = :seed_ordinal"
    sql = _nearest_sql(
        dimensions,
        f"(SELECT vector::{vector_type(dimensions)} FROM reference_passages WHERE {seed})",
        f" AND NOT ({seed})",
    )
    return sql, {
        "seed_document": document_id,
        "seed_ordinal": ordinal,
        "ids": sorted(document_ids),
        "k": k,
    }


async def _refuse_other_dimensions(
    conn: Connection, document_ids: Collection[int], n: int, whose: str
) -> None:
    """A document of ``document_ids`` whose passages are not of ``n`` dimensions is refused by
    name rather than left out of a search."""
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
            ).bindparams(ids=sorted(document_ids), n=n)
        )
    ).fetchone()
    if other is not None:
        raise ValueError(
            f"library document {other[0]}'s passages have {other[1]} dimensions; {whose} have {n}"
        )


async def _search(conn: Connection, sql: str, params: dict[str, Any]) -> list[PassageHit]:
    async with conn.transaction():
        # Filtered by document, an HNSW scan would stop at its candidate list (hnsw.ef_search)
        # and return fewer than k; iterative scanning goes on until k are found, in order.
        await conn.execute_core(text("SET LOCAL hnsw.iterative_scan = strict_order"))
        rows = (await conn.execute_core(text(sql).bindparams(**params))).fetchall()
    return [PassageHit(*row) for row in rows]


async def nearest_passages(
    conn: Connection, query: Sequence[float], document_ids: Collection[int], k: int
) -> list[PassageHit]:
    """The ``k`` passages of ``document_ids`` nearest ``query`` by cosine distance, nearest first,
    found through the index of the question's dimension. A document of another dimension is
    refused by name rather than left out."""
    if not document_ids:
        return []
    await _refuse_other_dimensions(conn, document_ids, len(query), "the question's")
    return await _search(conn, *nearest_sql(query, document_ids, k))


# -- a passage's neighbours ---------------------------------------------------------------------


@dataclass(frozen=True)
class Passage:
    """A passage where it sits in its document."""

    document_id: int
    ordinal: int
    section: str | None
    page: int | None
    text: str


def _within(document_id: int, document_ids: Collection[int]) -> None:
    if document_id not in document_ids:
        raise ValueError(
            f"library document {document_id} is not among the documents being searched"
        )


async def adjacent_passages(
    conn: Connection, document_id: int, ordinal: int, document_ids: Collection[int], n: int = 1
) -> list[Passage]:
    """The passages up to ``n`` before and after passage ``ordinal`` in its document, in order,
    read through the (document_id, ordinal) key. The document must be one of ``document_ids``,
    those being searched."""
    if n <= 0:
        raise ValueError("n must be a positive integer")
    _within(document_id, document_ids)
    result = await conn.execute_core(adjacent_query(document_id, ordinal, n))
    return [Passage(*row) for row in result.fetchall()]


def adjacent_query(document_id: int, ordinal: int, n: int) -> Select:
    """:func:`adjacent_passages`' query."""
    return (
        select(
            passages_t.c.document_id,
            passages_t.c.ordinal,
            passages_t.c.section,
            passages_t.c.page,
            passages_t.c.text,
        )
        .where(
            (passages_t.c.document_id == document_id)
            & passages_t.c.ordinal.between(ordinal - n, ordinal + n)
            & (passages_t.c.ordinal != ordinal)
        )
        .order_by(passages_t.c.ordinal)
    )


async def near_passage(
    conn: Connection, document_id: int, ordinal: int, document_ids: Collection[int], k: int
) -> list[PassageHit]:
    """The ``k`` passages of ``document_ids`` nearest passage ``ordinal`` of ``document_id`` by
    cosine distance (the score is the similarity to that passage), nearest first, the passage
    itself left out: :func:`nearest_passages` seeded with the passage's embedding. Its document
    must be one of ``document_ids``; a document of another dimension is refused by name."""
    _within(document_id, document_ids)
    dimensions = (
        await conn.execute_core(
            text(
                f"SELECT {_V}.vector_dims(vector) FROM reference_passages "
                "WHERE document_id = :d AND ordinal = :o"
            ).bindparams(d=document_id, o=ordinal)
        )
    ).scalar()
    if dimensions is None:
        raise ValueError(f"library document {document_id} has no passage {ordinal}")
    await _refuse_other_dimensions(conn, document_ids, int(dimensions), "the passage's")
    return await _search(
        conn, *near_passage_sql(document_id, ordinal, int(dimensions), document_ids, k)
    )


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


# -- the passages a glossary term cites ---------------------------------------------------------


async def scored_passages(
    conn: Connection, query: Sequence[float], keys: Collection[tuple[int, int]]
) -> list[PassageHit]:
    """The passages of ``keys`` ((document id, ordinal) pairs), each with its similarity to
    ``query``, nearest first: read through the (document_id, ordinal) key, not searched, so every
    one of them is returned. Each must be of the question's dimension."""
    if not keys:
        return []
    n = int(len(query))
    kind = vector_type(n)
    documents, ordinals = zip(*sorted(set(keys)), strict=True)
    rows = (
        await conn.execute_core(
            text(
                "SELECT p.document_id, p.ordinal, p.section, p.page, p.text, "
                f"1 - ((p.vector::{kind}) OPERATOR({_V}.<=>) CAST(:query AS {kind})) AS score "
                "FROM unnest(CAST(:documents AS integer[]), CAST(:ordinals AS integer[])) "
                "AS k(document_id, ordinal) JOIN reference_passages p "
                "ON p.document_id = k.document_id AND p.ordinal = k.ordinal "
                f"WHERE {_V}.vector_dims(p.vector) = {n} "
                "ORDER BY score DESC, p.document_id, p.ordinal"
            ).bindparams(
                query=pgvector.literal(query), documents=list(documents), ordinals=list(ordinals)
            )
        )
    ).fetchall()
    return [PassageHit(*row) for row in rows]
