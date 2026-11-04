# Copyright (c) 2026 Kenneth Stott
# Canary: e473d793-c4b4-4b9d-9ae2-49ec6886e490
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The passages of the reference library that answer a question, for a role (REQ-2185).

What an assistant acting for a role draws on: Polly, the MCP ``search_reference_library`` tool,
and the ontology when it drafts definitions and proposes relationships. A role draws on a kept
document whose domains it reads, and on every ``*`` document -- the rule a glossary term is read
by (provisa.core.glossary.readable_term). Each passage comes with what citing it takes: the
document, its section or page, and the quoted text.

:func:`retrieve` is the passages nearest the question; :func:`retrieve_with_neighbours`, what an
assistant is given, adds their neighbours within one passage budget, each passage labelled with
how it was reached (``reached``) so the assistant can weigh it and cite only what it used. Where
the passages are held and how the nearest and the neighbours are found is store.py's.
"""

# Requirements: REQ-2185

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from provisa.reference_library import store
from provisa.reference_library.library import Embedder

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from provisa.core.database import Connection


class LibraryModelChanged(ValueError):
    """Library documents embedded by a model other than the org's vector model now."""


#: ``reached`` of a passage found by its nearness to the question.
NEAREST_QUESTION = "nearest the question"

#: The passages nearest a passage by embedding that it brings as neighbours, besides the passage
#: just before and the one just after it.
NEAR_PER_PASSAGE = 2


@dataclass(frozen=True)
class Citation:
    """A passage with what citing it takes, and how it was reached: ``reached`` is
    :data:`NEAREST_QUESTION`, or names the passage it neighbours ("after passage 3 in 'Client
    policy'", "near passage 3 in 'Client policy'"). ``ordinal`` is its place in its document, the
    passage number a neighbour's label names; ``score`` its similarity to the question, None for
    a neighbour."""

    document_id: int
    document_title: str
    source: str
    section: str | None
    page: int | None
    quote: str
    score: float | None
    ordinal: int
    reached: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def readable(allowed: AbstractSet[str] | None, domains: AbstractSet[str]) -> bool:
    """May a role whose domains are ``allowed`` (None: every domain) draw on a document of
    ``domains``? A ``*`` document is the whole org's."""
    from provisa.core.glossary import readable_term

    return readable_term(allowed, domains)


async def retrieve(
    conn: Connection,
    question: str,
    allowed: AbstractSet[str] | None,
    embedder: Embedder,
    *,
    k: int = 5,
) -> list[Citation]:
    """The ``k`` passages nearest ``question`` among the kept documents a role whose domains are
    ``allowed`` (None: every domain) may read, nearest first."""
    _check(question, k)
    documents = await _readable_documents(conn, allowed, embedder)
    return await _nearest_question(conn, question, documents, embedder, k)


async def retrieve_with_neighbours(
    conn: Connection,
    question: str,
    allowed: AbstractSet[str] | None,
    embedder: Embedder,
    *,
    k: int = 5,
    budget: int | None = None,
) -> list[Citation]:
    """The ``k`` passages nearest ``question`` (:func:`retrieve`), then their neighbours, within
    ``budget`` passages (default ``2 * k``). The passages nearest the question come first and are
    never displaced; neighbours fill the rest of the budget in this order: for each passage
    nearest the question, nearest first, the passage just after it in its document, the one just
    before, and the :data:`NEAR_PER_PASSAGE` passages nearest it by embedding. A passage already
    given is not given again. Only the documents the role may read are drawn on, neighbours'
    included."""
    _check(question, k)
    if budget is None:
        budget = 2 * k
    if budget < k:
        raise ValueError(f"a budget of {budget} passages cannot hold the {k} nearest the question")
    documents = await _readable_documents(conn, allowed, embedder)
    hits = await _nearest_question(conn, question, documents, embedder, k)
    # J607 (GRAPH RETRIEVAL): the glossary walk's candidates join between the passages nearest the
    # question and the neighbours, in priority order, and are seeds of neighbours as well.
    seeds = hits
    return await _fill(budget, hits, _neighbours(conn, seeds, documents))


def _check(question: str, k: int) -> None:
    if not question or not question.strip():
        raise ValueError("a question to search the reference library with is required")
    if k <= 0:
        raise ValueError("k must be a positive integer")


async def _readable_documents(
    conn: Connection, allowed: AbstractSet[str] | None, embedder: Embedder
) -> dict[int, store.DocumentRow]:
    """The kept documents a role whose domains are ``allowed`` may read, by id; refused by name
    when one was embedded by another model than ``embedder``'s."""
    documents = {
        d.id: d
        for d in await store.documents(conn, status=store.KEPT)
        if readable(allowed, frozenset(d.domains))
    }
    stale = sorted(d.title for d in documents.values() if d.embedding_model != embedder.model)
    if stale:
        raise LibraryModelChanged(
            f"reference library documents {', '.join(repr(t) for t in stale)} were embedded by "
            f"another vector model than the org's {embedder.model!r}: re-embed them on the "
            "Admin Reference library page"
        )
    return documents


async def _nearest_question(
    conn: Connection,
    question: str,
    documents: Mapping[int, store.DocumentRow],
    embedder: Embedder,
    k: int,
) -> list[Citation]:
    if not documents:
        return []
    query = (await embedder.embed([question.strip()]))[0]
    hits = await store.nearest_passages(conn, query, documents, k)
    return [_citation(documents, h, round(h.score, 4), NEAREST_QUESTION) for h in hits]


def _citation(
    documents: Mapping[int, store.DocumentRow],
    passage: store.Passage | store.PassageHit,
    score: float | None,
    reached: str,
) -> Citation:
    document = documents[passage.document_id]
    return Citation(
        document_id=passage.document_id,
        document_title=document.title,
        source=document.source,
        section=passage.section,
        page=passage.page,
        quote=passage.text,
        score=score,
        ordinal=passage.ordinal,
        reached=reached,
    )


def _named(citation: Citation) -> str:
    """How a neighbour's label names the passage it neighbours."""
    return f"passage {citation.ordinal} in {citation.document_title!r}"


async def _neighbours(
    conn: Connection, seeds: Iterable[Citation], documents: Mapping[int, store.DocumentRow]
) -> AsyncGenerator[Citation, None]:
    """The neighbours of ``seeds``, in :func:`retrieve_with_neighbours`' order, each fetched only
    when the budget still has room for it."""
    for seed in seeds:
        beside = await store.adjacent_passages(conn, seed.document_id, seed.ordinal, documents)
        after = [p for p in beside if p.ordinal > seed.ordinal]
        before = [p for p in beside if p.ordinal < seed.ordinal]
        for passage in after:
            yield _citation(documents, passage, None, f"after {_named(seed)}")
        for passage in before:
            yield _citation(documents, passage, None, f"before {_named(seed)}")
        near = await store.near_passage(
            conn, seed.document_id, seed.ordinal, documents, NEAR_PER_PASSAGE
        )
        for hit in near:
            yield _citation(documents, hit, None, f"near {_named(seed)}")


async def _fill(
    budget: int, first: Sequence[Citation], candidates: AsyncGenerator[Citation, None]
) -> list[Citation]:
    """``first``, then ``candidates`` in order until ``budget`` passages are held, a passage
    already held skipped."""
    held = {(c.document_id, c.ordinal): c for c in first}
    if len(held) < budget:
        async for candidate in candidates:
            held.setdefault((candidate.document_id, candidate.ordinal), candidate)
            if len(held) >= budget:
                break
    await candidates.aclose()
    return list(held.values())


async def org_embedder(state: Any) -> Embedder:
    """The org's vector model (the first enabled one its AI Models settings list), embedding what
    it is given redacted as everything Provisa sends out is (provisa.security.redaction)."""
    from provisa.api.mcp.tools import resolve_embedding_model
    from provisa.core.request_context import current_org
    from provisa.security.redaction import redact
    from provisa.vector.providers import get_provider

    model = await resolve_embedding_model(state, purpose="the reference library")
    provider = get_provider(model.provider)
    org_id = current_org.get()

    async def embed(texts: list[str]) -> list[list[float]]:
        return await provider.embed([redact(t, org_id) for t in texts], model)

    return Embedder(model=model.id, embed=embed)


def role_domains(state: Any, role: str) -> frozenset[str] | None:
    """The domains ``role`` reads (None: every domain), as the glossary's agents read them."""
    from provisa.core.env_authority import domains_within
    from provisa.security.rights import domain_access_for_claims

    return domains_within(sorted(domain_access_for_claims([role], state.roles)))


async def search_for_role(state: Any, role: str, question: str, k: int = 5) -> list[dict]:
    """:func:`retrieve_with_neighbours` for ``role`` in the bound org, as an assistant's tool
    answers: the ``k`` passages nearest the question and up to as many again of neighbours."""
    embedder = await org_embedder(state)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        found = await retrieve_with_neighbours(
            conn, question, role_domains(state, role), embedder, k=k
        )
    return [c.as_dict() for c in found]
