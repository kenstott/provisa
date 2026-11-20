# Copyright (c) 2026 Kenneth Stott
# Canary: 9a0c6e24-71b3-4f85-b2d9-5e83a1c7f046
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247 (TERMS SOURCED FROM CATALOGS): a catalog term becomes a draft Provisa term, or merges.

Against a real control plane: the ontology import's alignment proposes the existing terms a
catalog term may duplicate; creating yields a draft (REQ-2144) with the catalog's definition, a
KIND_OF edge to its broader term and its provenance; merging records the provenance on the chosen
term and makes no second term.
"""

# Requirements: REQ-2247, REQ-2144

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import insert

from provisa.api.metadata_export.provider import CatalogTerm
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import domains
from provisa.ontology_import import align, catalog_terms

AT = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)


@asynccontextmanager
async def _conn(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
    async with control_plane.org().acquire() as conn:
        yield conn


async def _by_name(conn, name: str) -> dict | None:
    return next((t for t in await glossary_repo.list_terms(conn) if t["name"] == name), None)


def test_matches_proposes_the_aligned_existing_terms():
    terms = [
        align.Term(1, "customer", "concept", None),
        align.Term(2, "enterprise customer", "concept", None),
        align.Term(3, "region", "concept", None),
    ]
    found = catalog_terms.matches(CatalogTerm("Customers", "A buyer.", (), "om-1"), terms)
    assert [(p.term_id, p.rel_type, p.basis) for p in found] == [
        (1, align.SAME_AS, align.NAME),
        (2, "KIND_OF", align.HEAD_NOUN),
    ]


@pytest.mark.asyncio
async def test_matches_reads_the_terms_in_service(control_plane):
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "customer", definition="x", domains=set())
        terms = await catalog_terms.terms_in_service(conn, domains=None)
        found = catalog_terms.matches(CatalogTerm("Customer", None, (), "r"), terms)
        assert [(t.name, p.rel_type) for p in found for t in terms if t.id == p.term_id] == [
            ("customer", align.SAME_AS)
        ]


@pytest.mark.asyncio
async def test_create_yields_a_draft_with_definition_broader_and_provenance(control_plane):
    async with _conn(control_plane) as conn:
        existing = await glossary_repo.create_abstract_term(
            conn, "party", definition="Anyone.", domains=set()
        )
        created = await catalog_terms.create_drafts(
            conn,
            [
                # Listed narrower first: the broader term of the same call is created before it.
                CatalogTerm("Enterprise Customer", "A large buyer.", ("Customer",), "om-2"),
                CatalogTerm("Customer", "A buyer.", ("Party",), "om-1"),
            ],
            catalog="om",
            at=AT,
            domains=set(),
        )
        assert set(created) == {"Enterprise Customer", "Customer"}
        enterprise = await _by_name(conn, "enterprise customer")
        customer = await _by_name(conn, "customer")
        assert enterprise is not None and customer is not None
        assert enterprise["status"] == "draft" and customer["status"] == "draft"
        assert enterprise["definition"] == "A large buyer."
        edges = set(await glossary_repo.term_edges(conn))
        assert (enterprise["id"], customer["id"], "KIND_OF") in edges
        assert (customer["id"], existing, "KIND_OF") in edges
        assert await glossary_repo.term_sources(conn, enterprise["id"]) == [
            {"catalog": "om", "catalog_ref": "om-2", "sourced_at": AT}
        ]


@pytest.mark.asyncio
async def test_a_broader_term_in_neither_place_is_left_unlinked(control_plane):
    async with _conn(control_plane) as conn:
        created = await catalog_terms.create_drafts(
            conn,
            [CatalogTerm("Region", "An area.", ("Geography",), "r")],
            catalog="om",
            at=AT,
            domains=set(),
        )
        assert [e for e in await glossary_repo.term_edges(conn) if created["Region"] in e[:2]] == []
        assert await _by_name(conn, "geography") is None


@pytest.mark.asyncio
async def test_create_of_a_name_already_held_is_refused(control_plane):
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "customer", definition="x", domains=set())
        with pytest.raises(ValueError, match="already exists"):
            await catalog_terms.create_drafts(
                conn,
                [CatalogTerm("Customer", "A buyer.", (), "om-1")],
                catalog="om",
                at=AT,
                domains=set(),
            )


@pytest.mark.asyncio
async def test_merge_adds_the_provenance_without_a_duplicate(control_plane):
    async with _conn(control_plane) as conn:
        term_id = await glossary_repo.create_abstract_term(
            conn, "customer", definition=None, domains=set()
        )
        before = len(await glossary_repo.list_terms(conn))
        await catalog_terms.merge(
            conn, CatalogTerm("Customers", "A buyer.", (), "om-1"), term_id, catalog="om", at=AT
        )
        assert len(await glossary_repo.list_terms(conn)) == before
        merged = await glossary_repo.get_term(conn, term_id)
        assert merged is not None and merged["definition"] == "A buyer."
        assert await glossary_repo.term_sources(conn, term_id) == [
            {"catalog": "om", "catalog_ref": "om-1", "sourced_at": AT}
        ]


@pytest.mark.asyncio
async def test_merge_keeps_a_definition_the_term_holds(control_plane):
    async with _conn(control_plane) as conn:
        term_id = await glossary_repo.create_abstract_term(
            conn, "customer", definition="Ours.", domains=set()
        )
        await catalog_terms.merge(
            conn, CatalogTerm("Customer", "Theirs.", (), "om-1"), term_id, catalog="om", at=AT
        )
        merged = await glossary_repo.get_term(conn, term_id)
        assert merged is not None and merged["definition"] == "Ours."
