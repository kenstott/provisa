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
passages are the term's citations (:func:`citations`). A draft still awaiting review cites nothing;
one proposing a merge into a term is shown on the term's panel as proposed (:func:`term_citations`).
A steward also picks a document or a section of one for a term on its panel: that citation is kept
as a tag's is (provisa.reference_library.citations, kind ``term``).
"""

# Requirements: REQ-2185

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, delete, or_, select, text, update

from provisa.core import pgvector
from provisa.core.schema_org import glossary_term_drafts as drafts_t
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.core.schema_org import glossary_term_vectors as vectors_t
from provisa.core.schema_org import glossary_terms as terms_t
from provisa.core.schema_org import reference_documents as documents_t
from provisa.reference_library import citations as cites
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


#: A citation whose quoted words are still in the document, wherever they now are.
CURRENT = "current"
#: A citation whose quoted words the document no longer holds: flagged for review.
DRIFTED = "drifted"


@dataclass(frozen=True)
class CitedBy:
    """A term's citation of a passage of a document: ``ordinal``, ``section`` and ``page`` say
    where the words are now, or where they were when ``state`` is :data:`DRIFTED`."""

    term_id: int
    term: str
    ordinal: int
    section: str | None
    page: int | None
    quote: str
    state: str


def _spaced(text: str) -> str:
    return " ".join(text.split()).casefold()


def same_text(quote: str, passage: str) -> bool:
    """Whether ``passage`` still holds the words of ``quote``, however they are spaced."""
    return _spaced(quote) in _spaced(passage)


def _placed(
    held: Sequence[tuple[int, str | None, int | None, str]], cited: Mapping[str, Any]
) -> tuple[int, str | None, int | None, str]:
    """Where a cited passage's words are now (ordinal, section, page) and the citation's state. A
    quote is looked for where it was cited and then everywhere in the document, so a passage that
    only moved follows silently; one whose words are gone is flagged, at where it was."""
    quote = cited.get("quote") or ""
    ordinal = int(cited["ordinal"])
    at_place = [p for p in held if p[0] == ordinal and same_text(quote, p[3])]
    anywhere = at_place or [p for p in held if same_text(quote, p[3])]
    if anywhere:
        now = anywhere[0]
        return now[0], now[1], now[2], CURRENT
    return ordinal, cited.get("section"), cited.get("page"), DRIFTED


def _first_ordinal(
    held: Sequence[tuple[int, str | None, int | None, str]], c: cites.Citation
) -> int:
    """The ordinal of the first passage of the section a picked citation names (the document's
    first for the whole of it), so it sorts among the passages scans cited."""
    for ordinal, section, page, _text in held:
        if c.section_heading is not None and section == c.section_heading:
            return ordinal
        if c.section_heading is None and c.section_page is not None and page == c.section_page:
            return ordinal
        if c.section_heading is None and c.section_page is None:
            return ordinal
    return 0


async def cited_by(conn: Connection, document_id: int) -> list[CitedBy]:
    """The terms citing the document, each citation with its state: the passages scans found
    the term in (:func:`_placed`), and the sections stewards picked for it on its panel, whose
    section's changed or vanished text flags them as drifted. In term order, then passage order."""
    rows = (
        await conn.execute_core(
            select(terms_t.c.id, terms_t.c.name, drafts_t.c.payload)
            .join(drafts_t, drafts_t.c.term_id == terms_t.c.id)
            .join(scans_t, drafts_t.c.scan_id == scans_t.c.id)
            .where(drafts_t.c.status.in_(CITING), scans_t.c.document_id == document_id)
        )
    ).fetchall()
    picked = [c for c in await cites.citing(conn, document_id) if c.citer_kind == cites.TERM]
    if not rows and not picked:
        return []
    held = await store.passages(conn, document_id)
    seen: set[tuple[int, str]] = set()
    found: list[CitedBy] = []
    for term_id, name, payload in rows:
        for cited in json.loads(payload).get("passages", []):
            quote = cited.get("quote") or ""
            if (int(term_id), _spaced(quote)) in seen:
                continue
            seen.add((int(term_id), _spaced(quote)))
            ordinal, section, page, state = _placed(held, cited)
            found.append(CitedBy(int(term_id), name, ordinal, section, page, quote, state))
    if picked:
        names = dict(
            (
                await conn.execute_core(
                    select(terms_t.c.id, terms_t.c.name).where(
                        terms_t.c.id.in_(sorted({int(c.object_key) for c in picked}))
                    )
                )
            ).fetchall()
        )
        for c in picked:
            term_id = int(c.object_key)
            found.append(
                CitedBy(
                    term_id,
                    names[term_id],
                    _first_ordinal(held, c),
                    c.section_heading,
                    c.section_page,
                    "",
                    CURRENT if c.state == cites.CURRENT else DRIFTED,
                )
            )
    return sorted(found, key=lambda c: (c.term.casefold(), c.ordinal))


#: A term's citation that is its: a scan's passage of an accepted or merged draft, or a section a
#: steward picked.
ACCEPTED = "accepted"
#: A passage a scan's draft still awaiting review proposes merging into the term.
PROPOSED = "proposed"
#: Found by a scan of the document (a draft's passage).
SCAN = "scan"
#: Picked for the term on its panel (reference_citations, kind ``term``).
PICKED = "picked"


@dataclass(frozen=True)
class TermCitation:
    """One document passage or section a term cites, or a draft proposes it cite.

    ``state`` is :data:`CURRENT`, :data:`DRIFTED` or, for a picked section whose document or
    section is gone, ``broken`` (cites.BROKEN). A scan's citation names its ``draft_id``,
    passage ``ordinal`` and ``quote``; a picked one its ``cite_key``. ``domains`` are the scan's
    for a scan's citation (who may read the draft), empty for a picked one."""

    origin: str
    status: str
    document_id: int | None
    document_title: str
    section_heading: str | None
    section_page: int | None
    state: str
    quote: str | None
    ordinal: int | None
    draft_id: int | None
    cite_key: str | None
    cited_by: str | None
    domains: tuple[str, ...]

    @property
    def key(self) -> str:
        """The citation's name in the term's list."""
        if self.origin == SCAN:
            return f"scan:{self.draft_id}:{self.ordinal}:{_spaced(self.quote or '')}"
        return f"picked:{self.cite_key}"


async def term_citations(conn: Connection, term_id: int) -> list[TermCitation]:
    """What the term cites: the passages its accepted or merged drafts were found in, and the
    passages of drafts awaiting review that propose merging into it (:data:`PROPOSED`), each read
    against its document as held now (:func:`_placed`); and the sections picked for it on its
    panel, each with its state. In document title order, then passage order."""
    rows = (
        await conn.execute_core(
            select(
                drafts_t.c.id,
                drafts_t.c.status,
                drafts_t.c.payload,
                drafts_t.c.decided_by,
                scans_t.c.document_id,
                documents_t.c.title,
                scans_t.c.domains,
            )
            .join(scans_t, drafts_t.c.scan_id == scans_t.c.id)
            .join(documents_t, documents_t.c.id == scans_t.c.document_id)
            .where(
                or_(
                    and_(drafts_t.c.term_id == term_id, drafts_t.c.status.in_(CITING)),
                    and_(
                        drafts_t.c.match_term_id == term_id,
                        drafts_t.c.status == "draft",
                        drafts_t.c.proposal == "merge",
                    ),
                ),
            )
            .order_by(drafts_t.c.id)
        )
    ).fetchall()
    held: dict[int, list[tuple[int, str | None, int | None, str]]] = {}
    found: list[TermCitation] = []
    seen: set[tuple[str, int, str]] = set()
    for draft_id, status, payload, decided_by, document_id, title, domains in rows:
        if document_id not in held:
            held[document_id] = await store.passages(conn, document_id)
        status_of = ACCEPTED if status in CITING else PROPOSED
        for cited in json.loads(payload).get("passages", []):
            quote = cited.get("quote") or ""
            ordinal, section, page, state = _placed(held[document_id], cited)
            if (status_of, document_id, _spaced(quote)) in seen:
                continue
            seen.add((status_of, document_id, _spaced(quote)))
            found.append(
                TermCitation(
                    origin=SCAN,
                    status=status_of,
                    document_id=int(document_id),
                    document_title=title,
                    section_heading=section or None,
                    section_page=page,
                    state=state,
                    quote=quote,
                    ordinal=ordinal,
                    draft_id=int(draft_id),
                    cite_key=None,
                    cited_by=decided_by if status_of == ACCEPTED else None,
                    domains=tuple(json.loads(domains)),
                )
            )
    for c in await cites.of_object(conn, cites.TERM, cites.term_key(term_id)):
        found.append(
            TermCitation(
                origin=PICKED,
                status=ACCEPTED,
                document_id=c.document_id,
                document_title=c.document_title,
                section_heading=c.section_heading,
                section_page=c.section_page,
                state=c.state,
                quote=None,
                ordinal=None,
                draft_id=None,
                cite_key=cites.citation_key(c),
                cited_by=c.cited_by,
                domains=(),
            )
        )
    return sorted(
        found,
        key=lambda c: (c.status != ACCEPTED, c.document_title.casefold(), c.ordinal or 0),
    )


async def drop_passage(conn: Connection, term_id: int, draft_id: int, quote: str) -> bool:
    """Take a scan's citation off the term: the passage quoting ``quote`` leaves the passages of
    the term's accepted or merged draft ``draft_id``. False when that draft is not the term's or
    cites no such passage."""
    row = (
        await conn.execute_core(
            select(drafts_t.c.payload).where(
                drafts_t.c.id == draft_id,
                drafts_t.c.term_id == term_id,
                drafts_t.c.status.in_(CITING),
            )
        )
    ).fetchone()
    if row is None:
        return False
    payload = json.loads(row[0])
    passages = payload.get("passages", [])
    kept = [p for p in passages if _spaced(p.get("quote") or "") != _spaced(quote)]
    if len(kept) == len(passages):
        return False
    payload["passages"] = kept
    await conn.execute_core(
        update(drafts_t).where(drafts_t.c.id == draft_id).values(payload=json.dumps(payload))
    )
    return True
