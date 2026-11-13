# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a6c21-9d4e-4b7a-b0e5-62c1d9f4a7e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tag's definition, and each assignment of a tag, cite reference library sections (REQ-2208).

A citation keeps the cited section's heading, a hash of its text and the document's title and
source. When a new version of the document revises the section, exactly the definition or the one
assignment citing it is flagged for review; a section that is gone is broken. A tag may require
each assignment to cite something: one that does not is a gap, never refused. A library section
lists the tags and assignments citing it.

The library: one policy document with the sections "Personal data", "Retention" and a page-only
passage (page 3, no heading).
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.core.models import Tag, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import reference_citations, sources
from provisa.reference_library import citations, store
from provisa.reference_library.citations import BROKEN, CURRENT, DRIFTED, Place

pytestmark = pytest.mark.asyncio

PERSONAL = "Personal data"
RETENTION = "Retention"
PERSONAL_TEXT = ["Personal data is any information about a person.", "It includes their email."]
RETENTION_TEXT = "Records are kept for seven years."


def _passages(personal: list[str], retention: str | None = RETENTION_TEXT):
    texts: list[tuple[str | None, int | None, str]] = [(PERSONAL, 1, t) for t in personal]
    if retention is not None:
        texts.append((RETENTION, 2, retention))
    texts.append((None, 3, "Appendix: contacts."))
    return [
        store.StoredPassage(ordinal=i, section=s, page=p, text=t, vector=[0.1, 0.9])
        for i, (s, p, t) in enumerate(texts)
    ]


@pytest.fixture
async def library(control_plane):
    """The org's model holding a source ``pg``, a user tag ``approved`` whose assignments require
    a citation, and the policy document."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await tag_repo.upsert(
            conn,
            Tag(
                id="approved",
                description="Approved for use",
                applies_to=["source", "section"],
                requires_citation=True,
            ),
        )
        document = await store.insert_document(
            conn,
            title="Privacy policy",
            source="https://example.com/privacy",
            source_kind=store.URL,
            media_type="text/markdown",
            content_sha256="a" * 64,
            status=store.KEPT,
            embedding_model="m",
            added_by="org_admin",
            domains=["*"],
            passages=_passages(PERSONAL_TEXT),
        )
    return db, document


SOURCE = TagAssignment(tag_id="approved", object_type="source", source_id="pg")


def _section(document: int, heading: str) -> TagAssignment:
    return TagAssignment(
        tag_id="approved", object_type="section", document_id=document, section_heading=heading
    )


async def _revise(conn, document: int, **passages) -> None:
    """A new version of the document, as a refresh brings one in: its content replaced in place."""
    await store.replace_content(
        conn,
        document,
        media_type="text/markdown",
        content_sha256="b" * 64,
        embedding_model="m",
        passages=_passages(**passages),
        source=None,
        refreshed=True,
    )


async def test_a_definition_and_an_assignment_citation_are_read_back(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.assign(conn, SOURCE)
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), "steward")
        await tag_repo.cite_assignment(conn, SOURCE, Place(document, RETENTION), "steward")
        defined = await tag_repo.definition_citations(conn)
        assigned = await tag_repo.assignment_citations(conn)
    [definition] = defined["pii"]
    assert (definition.document_title, definition.document_source) == (
        "Privacy policy",
        "https://example.com/privacy",
    )
    assert (definition.section_heading, definition.cited_by, definition.state) == (
        PERSONAL,
        "steward",
        CURRENT,
    )
    assert definition.text_sha256 == citations.text_hash("\n".join(PERSONAL_TEXT))
    [cited] = assigned[("approved", "source:pg")]
    assert (cited.document_title, cited.section_heading, cited.state) == (
        "Privacy policy",
        RETENTION,
        CURRENT,
    )
    assert cited.text_sha256 == citations.text_hash(RETENTION_TEXT)


async def test_a_page_or_the_whole_document_may_be_cited(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.cite_definition(conn, "approved", Place(document, section_page=3), None)
        await tag_repo.cite_definition(conn, "approved", Place(document), None)
        cited = (await tag_repo.definition_citations(conn))["approved"]
    assert {(c.section_heading, c.section_page) for c in cited} == {(None, 3), (None, None)}
    assert {c.state for c in cited} == {CURRENT}


async def test_a_section_the_document_does_not_hold_is_refused(library):
    db, document = library
    async with db.acquire() as conn:
        with pytest.raises(citations.CitationRefused, match="no section at heading 'Nope'"):
            await tag_repo.cite_definition(conn, "pii", Place(document, "Nope"), None)
        with pytest.raises(tag_repo.CitationTargetNotFound):
            await tag_repo.cite_assignment(conn, SOURCE, Place(document, PERSONAL), None)
        with pytest.raises(tag_repo.CitationTargetNotFound):
            await tag_repo.cite_definition(conn, "no-such-tag", Place(document, PERSONAL), None)


async def test_a_revised_section_flags_exactly_the_definition_or_assignment_citing_it(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.assign(conn, SOURCE)
        await tag_repo.assign(conn, _section(document, RETENTION))
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), None)
        await tag_repo.cite_definition(conn, "approved", Place(document, RETENTION), None)
        await tag_repo.cite_assignment(conn, SOURCE, Place(document, PERSONAL), None)
        await tag_repo.cite_assignment(
            conn, _section(document, RETENTION), Place(document, RETENTION), None
        )
        # Only the whitespace changes: the text is the same, nothing is flagged.
        await _revise(
            conn,
            document,
            personal=[
                "  Personal data is any information about a person.",
                "It includes   their email.",
            ],
        )
        assert {
            c.state for cs in (await tag_repo.definition_citations(conn)).values() for c in cs
        } == {CURRENT}
        await _revise(conn, document, personal=[*PERSONAL_TEXT, "And their address."])
        defined = await tag_repo.definition_citations(conn)
        assigned = await tag_repo.assignment_citations(conn)
    assert [c.state for c in defined["pii"]] == [DRIFTED]
    assert [c.state for c in defined["approved"]] == [CURRENT]
    assert [c.state for c in assigned[("approved", "source:pg")]] == [DRIFTED]
    section_key = f"section:{document}:heading:{RETENTION}"
    assert [c.state for c in assigned[("approved", section_key)]] == [CURRENT]
    assert defined["pii"][0].needs_review and not defined["approved"][0].needs_review


async def test_citing_a_revised_section_again_confirms_it(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), "first")
        await _revise(conn, document, personal=["Personal data is anything about a person."])
        assert [c.state for c in (await tag_repo.definition_citations(conn))["pii"]] == [DRIFTED]
        again = await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), "reviewer")
    assert (again.state, again.cited_by) == (CURRENT, "reviewer")
    assert again.text_sha256 == citations.text_hash("Personal data is anything about a person.")


async def test_a_section_gone_or_a_document_removed_breaks_the_citation(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), None)
        await tag_repo.cite_definition(conn, "approved", Place(document, RETENTION), None)
        await _revise(conn, document, personal=PERSONAL_TEXT, retention=None)
        defined = await tag_repo.definition_citations(conn)
        assert [c.state for c in defined["approved"]] == [BROKEN]
        assert [c.state for c in defined["pii"]] == [CURRENT]
        from provisa.reference_library import library as library_mod

        await library_mod.remove(conn, document)
        defined = await tag_repo.definition_citations(conn)
    # The citation outlives the document, naming it by its title and source.
    [kept] = defined["pii"]
    assert (kept.state, kept.document_id, kept.document_title) == (BROKEN, None, "Privacy policy")


async def test_a_tag_requiring_citations_reports_an_uncited_assignment_as_a_gap(library):
    from provisa.api.admin.schema_mutation import store_tag_assignment

    db, document = library
    async with db.acquire() as conn:
        tag_row = await tag_repo.get(conn, "approved")
        assert tag_row is not None and tag_row["requires_citation"] is True
        # The assignment is stored, not refused, though it cites nothing.
        assert await store_tag_assignment(conn, SOURCE, tag_row) is None
        assert [r["object_key"] for r in await tag_repo.list_assignments(conn)] == ["source:pg"]
        assert tag_repo.citation_gap(tag_row, []) is True
        cited = await tag_repo.cite_assignment(conn, SOURCE, Place(document, PERSONAL), None)
        assert tag_repo.citation_gap(tag_row, [cited]) is False
        # A tag that does not require one has no gap.
        assert tag_repo.citation_gap(await tag_repo.get(conn, "pii"), []) is False


async def test_a_section_lists_the_tags_and_assignments_citing_it(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.assign(conn, SOURCE)
        await tag_repo.assign(conn, _section(document, RETENTION))
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), None)
        await tag_repo.cite_assignment(conn, SOURCE, Place(document, PERSONAL), None)
        await tag_repo.cite_assignment(
            conn, _section(document, RETENTION), Place(document, RETENTION), None
        )
        personal = await citations.citing(conn, document, section_heading=PERSONAL)
        whole = await citations.citing(conn, document)
    assert [(c.citer_kind, c.tag_id, c.object_key) for c in personal] == [
        ("tag", "pii", ""),
        ("tag_assignment", "approved", "source:pg"),
    ]
    assert len(whole) == 3


async def test_an_assignment_or_a_tag_taken_away_takes_its_citations(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.assign(conn, SOURCE)
        await tag_repo.cite_assignment(conn, SOURCE, Place(document, PERSONAL), None)
        await tag_repo.cite_definition(conn, "approved", Place(document, PERSONAL), None)
        assert await tag_repo.unassign(conn, "approved", "source:pg", "source")
        assert await tag_repo.assignment_citations(conn) == {}
        assert await tag_repo.delete(conn, "approved")
        left = (await conn.execute_core(select(reference_citations))).fetchall()
    assert left == []


async def test_a_citation_is_taken_off_by_its_place(library):
    db, document = library
    async with db.acquire() as conn:
        await tag_repo.cite_definition(conn, "pii", Place(document, PERSONAL), None)
        assert not await tag_repo.uncite_definition(conn, "pii", Place(document, RETENTION))
        assert await tag_repo.uncite_definition(conn, "pii", Place(document, PERSONAL))
        assert await tag_repo.definition_citations(conn) == {}
