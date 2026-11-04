# Copyright (c) 2026 Kenneth Stott
# Canary: 1ce8666b-e132-4302-ab53-a48547411f86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The glossary's terms as governed retrieval reads them (REQ-2185, GRAPH RETRIEVAL).

Each in-service term's embedding -- its name and definition, embedded by the org's vector model --
is held in ``glossary_term_vectors`` under one partial HNSW index per dimension, as a passage's is
(store.py). Its ``digest`` is of the model and the text embedded: :func:`refresh` embeds again
each term whose name or definition changed, or every term when the model changed, and removes the
embedding of a term no longer in service, before any search reads them. :func:`nearest_terms` is
the indexed cosine search for the terms nearest a question.

A term cites a library passage when a scan of the passage's document found the term there and a
steward accepted the draft as the term or merged it into the term (REQ-2179): the draft's source
passages are the term's citations (:func:`citations`). A draft still awaiting review cites nothing.
"""

# Requirements: REQ-2185

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import delete, select, text

from provisa.core import pgvector
from provisa.core.schema_org import glossary_term_drafts as drafts_t
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.core.schema_org import glossary_term_vectors as vectors_t
from provisa.core.schema_org import glossary_terms as terms_t
from provisa.reference_library import store
from provisa.reference_library.library import Embedder

if TYPE_CHECKING:
    from provisa.core.database import Connection

_V = pgvector.SCHEMA

#: The review decisions after which a draft's passages are its term's citations.
CITING = ("accepted", "merged")


@dataclass(frozen=True)
class Term:
    """An in-service glossary term: its name, definition and status."""

    id: int
    name: str
    definition: str | None
    status: str


def embedded_text(term: Term) -> str:
    """What a term's embedding is of: its name, and its definition beneath it."""
    return term.name if not term.definition else f"{term.name}\n{term.definition}"


def digest(model: str, term: Term) -> str:
    return hashlib.sha256(f"{model}\n{embedded_text(term)}".encode()).hexdigest()


async def in_service(conn: Connection) -> dict[int, Term]:
    """The glossary's in-service terms (draft or approved), by id: a deprecated or retired term
    seeds nothing and conducts nothing, as it conducts no grounding (core.glossary._conducting)."""
    from provisa.core.glossary import term_status

    rows = (
        await conn.execute_core(
            select(
                terms_t.c.id,
                terms_t.c.name,
                terms_t.c.definition,
                terms_t.c.approved,
                terms_t.c.retired,
                terms_t.c.deprecated,
                terms_t.c.deprecation_asserted,
            )
        )
    ).fetchall()
    out: dict[int, Term] = {}
    for r in rows:
        status = term_status(r._mapping)
        if status in ("draft", "approved"):
            out[r.id] = Term(r.id, r.name, r.definition, status)
    return out


async def refresh(conn: Connection, terms: Mapping[int, Term], embedder: Embedder) -> int:
    """Every term of ``terms`` (the in-service terms) holding the embedding of its current name
    and definition by ``embedder``'s model: a term that has none, or one of other text or another
    model, is embedded again; the embedding of a term not among ``terms`` is removed. Returns how
    many were embedded."""
    held = {
        r.term_id: r.digest
        for r in (
            await conn.execute_core(select(vectors_t.c.term_id, vectors_t.c.digest))
        ).fetchall()
    }
    gone = sorted(set(held) - set(terms))
    if gone:
        await conn.execute_core(delete(vectors_t).where(vectors_t.c.term_id.in_(gone)))
    stale = [
        t
        for t in sorted(terms.values(), key=lambda t: t.id)
        if held.get(t.id) != digest(embedder.model, t)
    ]
    if stale:
        vectors = await embedder.embed([embedded_text(t) for t in stale])
        if len(vectors) != len(stale):
            raise ValueError(f"{len(vectors)} embeddings for {len(stale)} glossary terms")
        dimensions = store.dimensions_of(vectors)
        async with conn.transaction():
            for term, vector in zip(stale, vectors, strict=True):
                await conn.upsert(
                    vectors_t,
                    {
                        "term_id": term.id,
                        "embedding_model": embedder.model,
                        "digest": digest(embedder.model, term),
                        "vector": list(vector),
                    },
                    index_elements=["term_id"],
                    update_columns=["embedding_model", "digest", "vector"],
                )
        await store.index(conn, dimensions, None, store.TERM_VECTORS)
    if gone or stale:
        await store.drop_unused_indexes(conn, None, store.TERM_VECTORS)
    return len(stale)


def nearest_terms_sql(
    query: Sequence[float], term_ids: Collection[int], k: int
) -> tuple[str, dict[str, object]]:
    """The indexed cosine search for the ``k`` terms of ``term_ids`` nearest ``query``, and its
    parameters; the dimension is written into the SQL, as a passage search's is (store.py)."""
    n = int(len(query))
    kind = store.vector_type(n)
    sql = (
        "SELECT term_id, 1 - distance AS score FROM ("
        f"SELECT term_id, (vector::{kind}) OPERATOR({_V}.<=>) CAST(:query AS {kind}) AS distance "
        f"FROM glossary_term_vectors WHERE {_V}.vector_dims(vector) = {n} "
        "AND term_id = ANY(:ids) ORDER BY distance LIMIT :k"
        ") nearest ORDER BY distance, term_id"
    )
    return sql, {"query": pgvector.literal(query), "ids": sorted(term_ids), "k": k}


async def nearest_terms(
    conn: Connection, query: Sequence[float], term_ids: Collection[int], k: int
) -> list[tuple[int, float]]:
    """The ``k`` terms of ``term_ids`` nearest ``query`` by cosine distance, nearest first, each
    with its similarity, found through the index of the question's dimension."""
    if not term_ids:
        return []
    sql, params = nearest_terms_sql(query, term_ids, k)
    async with conn.transaction():
        # As a passage search (store._search): iterative scanning goes on until k are found.
        await conn.execute_core(text("SET LOCAL hnsw.iterative_scan = strict_order"))
        rows = (await conn.execute_core(text(sql).bindparams(**params))).fetchall()
    return [(int(r[0]), float(r[1])) for r in rows]


async def citations(
    conn: Connection, document_ids: Collection[int]
) -> dict[int, list[tuple[int, int]]]:
    """Each term's citations among ``document_ids``: the (document id, passage ordinal) of each
    passage a scan of one of them found the term in, where the draft was accepted as the term or
    merged into it, in document and passage order."""
    if not document_ids:
        return {}
    rows = (
        await conn.execute_core(
            select(drafts_t.c.term_id, scans_t.c.document_id, drafts_t.c.payload)
            .join(scans_t, drafts_t.c.scan_id == scans_t.c.id)
            .where(
                drafts_t.c.status.in_(CITING),
                drafts_t.c.term_id.is_not(None),
                scans_t.c.document_id.in_(sorted(document_ids)),
            )
        )
    ).fetchall()
    out: dict[int, set[tuple[int, int]]] = {}
    for term_id, document_id, payload in rows:
        for passage in json.loads(payload).get("passages", []):
            out.setdefault(int(term_id), set()).add((int(document_id), int(passage["ordinal"])))
    return {t: sorted(cited) for t, cited in out.items()}
