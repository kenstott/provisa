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

:func:`retrieve` is the interface; where the passages are held and how the nearest are found is
store.py's (store.nearest_passages).
"""

# Requirements: REQ-2185

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from provisa.reference_library import store
from provisa.reference_library.library import Embedder

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet

    from provisa.core.database import Connection


class LibraryModelChanged(ValueError):
    """Library documents embedded by a model other than the org's vector model now."""


@dataclass(frozen=True)
class Citation:
    """A passage with what citing it takes."""

    document_id: int
    document_title: str
    source: str
    section: str | None
    page: int | None
    quote: str
    score: float

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
    if not question or not question.strip():
        raise ValueError("a question to search the reference library with is required")
    if k <= 0:
        raise ValueError("k must be a positive integer")
    documents = {
        d.id: d
        for d in await store.documents(conn, status=store.KEPT)
        if readable(allowed, frozenset(d.domains))
    }
    if not documents:
        return []
    stale = sorted(d.title for d in documents.values() if d.embedding_model != embedder.model)
    if stale:
        raise LibraryModelChanged(
            f"reference library documents {', '.join(repr(t) for t in stale)} were embedded by "
            f"another vector model than the org's {embedder.model!r}: refresh them or upload "
            "them again"
        )
    query = (await embedder.embed([question.strip()]))[0]
    hits = await store.nearest_passages(conn, query, documents, k)
    return [
        Citation(
            document_id=h.document_id,
            document_title=documents[h.document_id].title,
            source=documents[h.document_id].source,
            section=h.section,
            page=h.page,
            quote=h.text,
            score=round(h.score, 4),
        )
        for h in hits
    ]


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
    """:func:`retrieve` for ``role`` in the bound org, as an assistant's tool answers."""
    embedder = await org_embedder(state)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        found = await retrieve(conn, question, role_domains(state, role), embedder, k=k)
    return [c.as_dict() for c in found]
