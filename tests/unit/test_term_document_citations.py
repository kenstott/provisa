# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8b2d41-7c9a-4f13-b6e0-a2d4c71f3e95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A term's documents (REQ-2208, REQ-2179): the term panel reads the library documents the term
cites -- the passages its accepted drafts were found in, the passages of drafts awaiting review
that propose merging into it, and the sections picked for it -- among what the caller may read;
a curator who may change the term picks a section for it, confirms a drifted one by citing it
again, and takes one off, through the review queue's right and the term's own curation gate.

The library: a sales policy and an hr policy, each with a section "Scope" and a section "Terms".
The glossary: a term ``revenue`` declared in sales.
"""

# Requirements: REQ-2208, REQ-2179

from __future__ import annotations

import json

import pytest
from sqlalchemy import insert, update

import provisa.api.app as appmod
from provisa.api.admin import reference_library_router as router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_drafts,
    glossary_term_scans,
    reference_passages,
)
from provisa.reference_library import store
from tests.unit.gate_identity import grant

pytestmark = pytest.mark.asyncio

SCOPE = "Scope"
TERMS = "Terms"


def _caller(monkeypatch, domain_access: list[str], *capabilities: str):
    """A caller holding ``capabilities`` in ``domain_access``."""
    _info, request = grant(monkeypatch, *capabilities)
    appmod.state.roles["test_role"]["domain_access"] = domain_access
    appmod.state.roles["test_role"]["own_domain_access"] = domain_access
    request.state.active_org_id = "default"
    return request


def _reader(monkeypatch, domain_access: list[str] | None = None):
    return _caller(monkeypatch, domain_access or ["sales"], "glossary_read")


def _curator(monkeypatch, domain_access: list[str] | None = None):
    return _caller(monkeypatch, domain_access or ["sales"], "glossary_read", "glossary_rw")


@pytest.fixture
async def library(control_plane, monkeypatch):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", description=""))
        await conn.execute_core(insert(domains).values(id="hr", description=""))
        ids = {}
        for domain in ("sales", "hr"):
            ids[domain] = await store.insert_document(
                conn,
                title=f"{domain} policy",
                source=f"https://example.com/{domain}",
                source_kind=store.URL,
                media_type="text/markdown",
                content_sha256=domain[0] * 64,
                status=store.KEPT,
                embedding_model="m",
                added_by="org_admin",
                domains=[domain],
                passages=[
                    store.StoredPassage(
                        ordinal=0, section=SCOPE, page=1, text=f"{domain} scope.", vector=[0.1, 0.9]
                    ),
                    store.StoredPassage(
                        ordinal=1,
                        section=TERMS,
                        page=2,
                        text=f"Revenue is what {domain} earns.",
                        vector=[0.9, 0.1],
                    ),
                ],
            )
        ids["revenue"] = await glossary_repo.create_abstract_term(
            conn, "revenue", definition="What is earned.", domains={"sales"}
        )
    monkeypatch.setattr(router, "_pool", lambda: db)
    return db, ids


async def _draft(
    conn, term_id: int, document_id: int, quote: str, *, status: str, domains: list[str]
) -> int:
    """A scan of ``document_id`` (working in ``domains``) found ``quote`` at its passage 1, in a
    draft ``status``: accepted into the term, or awaiting review proposing a merge into it."""
    scan = await conn.execute_core(
        insert(glossary_term_scans)
        .values(
            document_id=document_id,
            document_title="scanned title",
            document_source="scanned",
            domains=json.dumps(domains),
            status="done",
        )
        .returning(glossary_term_scans.c.id)
    )
    draft = await conn.execute_core(
        insert(glossary_term_drafts)
        .values(
            scan_id=int(scan.fetchone()[0]),
            ordinal=0,
            name="revenue",
            category="concept",
            sense="",
            definition_kind="missing",
            proposal="merge",
            payload=json.dumps(
                {"passages": [{"ordinal": 1, "section": TERMS, "page": 2, "quote": quote}]}
            ),
            status=status,
            term_id=term_id if status in ("accepted", "merged") else None,
            match_term_id=term_id,
        )
        .returning(glossary_term_drafts.c.id)
    )
    return int(draft.fetchone()[0])


def _summary(found: dict) -> list[tuple]:
    return [
        (c["origin"], c["status"], c["document_title"], c["section_heading"], c["state"])
        for c in found["citations"]
    ]


async def test_the_term_lists_what_it_cites_and_what_review_proposes(library, monkeypatch):
    db, ids = library
    async with db.acquire() as conn:
        accepted = await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue is what sales earns.",
            status="merged",
            domains=["sales"],
        )
        proposed = await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue is what sales earns.",
            status="draft",
            domains=["sales"],
        )
    await router.cite_for_term(
        ids["revenue"],
        {"document_id": ids["sales"], "section_heading": SCOPE},
        _curator(monkeypatch),
    )
    found = await router.term_citations(ids["revenue"], _reader(monkeypatch))
    assert _summary(found) == [
        ("picked", "accepted", "sales policy", SCOPE, "current"),
        ("scan", "accepted", "sales policy", TERMS, "current"),
        ("scan", "proposed", "sales policy", TERMS, "current"),
    ]
    picked, scan, review = found["citations"]
    assert (scan["draft_id"], scan["quote"], scan["ordinal"]) == (
        accepted,
        "Revenue is what sales earns.",
        1,
    )
    assert picked["cite_key"] == f"section:{ids['sales']}:heading:{SCOPE}"
    assert picked["cited_by"] == "test-user" and picked["needs_review"] is False
    assert review["draft_id"] == proposed and review["cited_by"] is None
    assert len({c["key"] for c in found["citations"]}) == 3


async def test_a_quote_the_document_no_longer_holds_is_drifted(library, monkeypatch):
    db, ids = library
    async with db.acquire() as conn:
        await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue was never defined here.",
            status="accepted",
            domains=["sales"],
        )
    found = await router.term_citations(ids["revenue"], _reader(monkeypatch))
    [cited] = found["citations"]
    assert (cited["state"], cited["needs_review"]) == ("drifted", True)


async def test_a_picked_section_whose_text_changed_is_confirmed_by_citing_it_again(
    library, monkeypatch
):
    db, ids = library
    place = {"document_id": ids["sales"], "section_heading": SCOPE}
    await router.cite_for_term(ids["revenue"], place, _curator(monkeypatch))
    async with db.acquire() as conn:
        await conn.execute_core(
            update(reference_passages)
            .where(
                reference_passages.c.document_id == ids["sales"],
                reference_passages.c.section == SCOPE,
            )
            .values(text="sales scope, revised.")
        )
    [drifted] = (await router.term_citations(ids["revenue"], _reader(monkeypatch)))["citations"]
    assert drifted["state"] == "drifted"
    again = await router.cite_for_term(ids["revenue"], place, _curator(monkeypatch))
    assert again["state"] == "current"
    [current] = (await router.term_citations(ids["revenue"], _reader(monkeypatch)))["citations"]
    assert current["state"] == "current"


async def test_what_the_caller_may_not_read_is_not_listed(library, monkeypatch):
    db, ids = library
    async with db.acquire() as conn:
        # Accepted from a scan of the hr policy: the document is hr's.
        await _draft(
            conn,
            ids["revenue"],
            ids["hr"],
            "Revenue is what hr earns.",
            status="accepted",
            domains=["hr"],
        )
        # A scan of the sales policy that worked in hr: its draft is hr's to review.
        await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue is what sales earns.",
            status="draft",
            domains=["hr"],
        )
    assert (await router.term_citations(ids["revenue"], _reader(monkeypatch)))["citations"] == []
    both = await router.term_citations(ids["revenue"], _reader(monkeypatch, ["sales", "hr"]))
    assert len(both["citations"]) == 2
    with pytest.raises(ApiError) as outside:
        await router.term_citations(ids["revenue"], _reader(monkeypatch, ["hr"]))
    assert outside.value.status_code == 403


async def test_citing_asks_the_curation_right_and_the_terms_curation_gate(library, monkeypatch):
    _db, ids = library
    place = {"document_id": ids["sales"], "section_heading": SCOPE}
    with pytest.raises(ApiError) as reader:
        await router.cite_for_term(ids["revenue"], place, _reader(monkeypatch))
    assert reader.value.status_code == 403
    # Curating in hr does not reach a term of sales: it reads it, and may not change it.
    with pytest.raises(ApiError) as elsewhere:
        await router.cite_for_term(ids["revenue"], place, _curator(monkeypatch, ["hr"]))
    assert elsewhere.value.status_code == 403
    with pytest.raises(ApiError) as unread:
        await router.cite_for_term(
            ids["revenue"],
            {"document_id": ids["hr"], "section_heading": SCOPE},
            _curator(monkeypatch),
        )
    assert unread.value.status_code == 404
    with pytest.raises(ApiError) as missing:
        await router.cite_for_term(
            ids["revenue"],
            {"document_id": ids["sales"], "section_heading": "Nowhere"},
            _curator(monkeypatch),
        )
    assert missing.value.status_code == 400
    with pytest.raises(ApiError) as no_term:
        await router.cite_for_term(10_000, place, _curator(monkeypatch))
    assert no_term.value.status_code == 404
    assert (await router.term_citations(ids["revenue"], _reader(monkeypatch)))["citations"] == []


async def test_a_citation_is_taken_off_by_a_curator(library, monkeypatch):
    db, ids = library
    async with db.acquire() as conn:
        accepted = await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue is what sales earns.",
            status="accepted",
            domains=["sales"],
        )
    picked = await router.cite_for_term(
        ids["revenue"], {"document_id": ids["sales"], "section_page": 1}, _curator(monkeypatch)
    )
    with pytest.raises(ApiError) as reader:
        await router.uncite_for_term(
            ids["revenue"], _reader(monkeypatch), cite_key=picked["cite_key"]
        )
    assert reader.value.status_code == 403
    gone = await router.uncite_for_term(
        ids["revenue"], _curator(monkeypatch), cite_key=picked["cite_key"]
    )
    assert gone == {"removed": True}
    await router.uncite_for_term(
        ids["revenue"],
        _curator(monkeypatch),
        draft_id=accepted,
        quote="Revenue is   what sales earns.",
    )
    assert (await router.term_citations(ids["revenue"], _reader(monkeypatch)))["citations"] == []
    with pytest.raises(ApiError) as again:
        await router.uncite_for_term(
            ids["revenue"], _curator(monkeypatch), cite_key=picked["cite_key"]
        )
    assert again.value.status_code == 404
    with pytest.raises(ApiError) as unnamed:
        await router.uncite_for_term(ids["revenue"], _curator(monkeypatch))
    assert unnamed.value.status_code == 400


async def test_a_proposed_draft_is_not_taken_off_here(library, monkeypatch):
    db, ids = library
    async with db.acquire() as conn:
        proposed = await _draft(
            conn,
            ids["revenue"],
            ids["sales"],
            "Revenue is what sales earns.",
            status="draft",
            domains=["sales"],
        )
    with pytest.raises(ApiError) as refused:
        await router.uncite_for_term(
            ids["revenue"],
            _curator(monkeypatch),
            draft_id=proposed,
            quote="Revenue is what sales earns.",
        )
    assert refused.value.status_code == 404


async def test_the_document_lists_a_term_that_picked_it(library, monkeypatch):
    _db, ids = library
    await router.cite_for_term(
        ids["revenue"],
        {"document_id": ids["sales"], "section_heading": TERMS},
        _curator(monkeypatch),
    )
    cited = await router.cited_by(ids["sales"], _reader(monkeypatch))
    assert [(c["term"], c["section"], c["ordinal"], c["state"]) for c in cited["terms"]] == [
        ("revenue", TERMS, 1, "current")
    ]
    # A term's citation is no tag's: the tag lists stay empty.
    assert (cited["tags"], cited["tag_assignments"]) == ([], [])
