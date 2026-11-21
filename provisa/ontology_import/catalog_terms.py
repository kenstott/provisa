# Copyright (c) 2026 Kenneth Stott
# Canary: 7d41c0b2-5e9a-4f3c-a8d6-2b17e94f0c55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Terms sourced from a catalog's business glossary (REQ-2247, TERMS SOURCED FROM CATALOGS).

A catalog term (``CatalogTerm``, read by a ``CatalogReader``'s ``list_glossary``) becomes a Provisa term
in one of two ways, the steward's choice:

* ``create_drafts``: a draft term with the catalog's definition, a KIND_OF edge to each broader
  term that exists in Provisa or is created in the same call, and its provenance (the catalog and
  the time). A draft is not approved, so it goes through normal review (REQ-2144) and the Glossary
  shows it only once approved.
* ``merge``: the catalog term is recorded on a chosen existing term as its provenance, and its
  definition fills the term's only when the term has none, as an ontology import's SAME_AS does.

Before either, ``matches`` proposes the existing terms the catalog term may duplicate, by the
ontology import's alignment (``align.propose``), so the steward can merge instead of duplicating.
"""

# Requirements: REQ-2247, REQ-2144

from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.api.catalog_import.reader import CatalogTerm
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_terms
from provisa.ontology_import import align
from provisa.ontology_import.parse import CLASS, CONCEPT, PROPERTY, Item

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _role(term: dict) -> str:
    if term["table_count"]:
        return CLASS
    return PROPERTY if term["ref_count"] else CONCEPT


async def terms_in_service(conn: Connection, *, domains: frozenset[str] | None) -> list[align.Term]:
    """The terms a catalog term is compared with: those readable in ``domains`` (None: every
    term) that are neither deprecated nor retired."""
    return [
        align.Term(t["id"], t["name"], _role(t), t["iri"])
        for t in await glossary_repo.list_terms(conn, domains=domains)
        if t["status"] not in ("deprecated", "retired")
    ]


def matches(term: CatalogTerm, terms: Sequence[align.Term]) -> list[align.Proposal]:
    """The existing terms ``term`` may duplicate, strongest first: the ontology import's
    alignments of one concept with the glossary's terms in service."""
    item = Item(name=term.name, kind=CONCEPT, keys=[], definition=term.definition)
    return align.propose([item], [], terms)


async def _term_id(conn: Connection, name: str) -> int | None:
    row = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.name == name.strip().lower())
        )
    ).fetchone()
    return None if row is None else row.id


def _broader_first(terms: Sequence[CatalogTerm]) -> list[CatalogTerm]:
    """``terms`` ordered so each comes after the broader terms of the same call it names."""
    by_name = {t.name.strip().lower(): t for t in terms}
    ordered: list[CatalogTerm] = []
    placed: set[str] = set()
    visiting: set[str] = set()

    def place(term: CatalogTerm) -> None:
        key = term.name.strip().lower()
        if key in placed:
            return
        if key in visiting:
            raise ValueError(f"catalog term {term.name!r} is broader than itself")
        visiting.add(key)
        for name in term.broader:
            parent = by_name.get(name.strip().lower())
            if parent is not None:
                place(parent)
        visiting.discard(key)
        placed.add(key)
        ordered.append(term)

    for term in terms:
        place(term)
    return ordered


async def create_drafts(
    conn: Connection,
    terms: Sequence[CatalogTerm],
    *,
    catalog: str,
    at: datetime,
    domains: Collection[str],
) -> dict[str, int]:
    """Create a draft term for each catalog term, by name; each its definition, its KIND_OF
    edges to the broader terms that exist in Provisa or are created here, and its provenance.

    A name a term already holds is refused (``create_abstract_term``): the steward merges
    instead. A broader term in neither place is left unlinked; it is a term of its own to accept.
    """
    created: dict[str, int] = {}
    for term in _broader_first(terms):
        term_id = await glossary_repo.create_abstract_term(
            conn, term.name, definition=term.definition, domains=set(domains)
        )
        created[term.name] = term_id
        await glossary_repo.add_term_source(
            conn, term_id, catalog=catalog, catalog_ref=term.ref, at=at
        )
        for name in term.broader:
            broader_id = await _term_id(conn, name)
            if broader_id is not None:
                await glossary_repo.add_edge(conn, term_id, broader_id, "KIND_OF")
    return created


async def merge(
    conn: Connection, term: CatalogTerm, into: int, *, catalog: str, at: datetime
) -> None:
    """Merge the catalog term into the existing term ``into``: its provenance is recorded there,
    and its definition fills the term's only when the term has none."""
    found = await glossary_repo.get_term(conn, into)
    if found is None:
        raise glossary_repo.TermNotFound(f"term {into} not found")
    if term.definition and not (found["definition"] or "").strip():
        await glossary_repo.set_definition(conn, into, term.definition)
    await glossary_repo.add_term_source(conn, into, catalog=catalog, catalog_ref=term.ref, at=at)
