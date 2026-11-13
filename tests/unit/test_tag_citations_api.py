# Copyright (c) 2026 Kenneth Stott
# Canary: 9c2d7e14-6b3a-4f58-a1d0-e7b4c8f25a91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin API of tag citations (REQ-2208): citing a library section from a tag's definition asks
the right that edits the definition, and from an assignment the right that edits the assignment;
the tags and assignments read back their citations, the flag and the gap; a library document
lists what cites it among what its reader may read.

The library: a sales policy and an hr policy, each with a section "Scope". The model: a source
``pg`` open to every domain, and a user tag ``approved``.
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, update

import provisa.api.app as appmod
from provisa.api.admin import reference_library_router as router
from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.schema_query import Query
from provisa.api.admin.types import LibraryCitationInput, TagAssignmentInput, TagInput
from provisa.core.models import Tag, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import domains, reference_citations, sources
from provisa.reference_library import citations, store
from tests.unit.gate_identity import grant

pytestmark = pytest.mark.asyncio

SCOPE = "Scope"


def _scoped(monkeypatch, domain_access: list[str], *capabilities: str):
    """A caller holding ``capabilities`` in ``domain_access``."""
    info, request = grant(monkeypatch, *capabilities)
    appmod.state.roles["test_role"]["domain_access"] = domain_access
    appmod.state.roles["test_role"]["own_domain_access"] = domain_access
    request.state.active_org_id = "default"
    return info, request


@pytest.fixture
async def library(control_plane, monkeypatch):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", description=""))
        await conn.execute_core(insert(domains).values(id="hr", description=""))
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await tag_repo.upsert(
            conn, Tag(id="approved", applies_to=["source", "section"], description="Approved")
        )
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
                    )
                ],
            )

    async def _pool():
        return db

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _nothing)
    return db, ids


async def _nothing() -> None:
    return None


def _cite(document: int) -> LibraryCitationInput:
    return LibraryCitationInput(document_id=document, section_heading=SCOPE)


SOURCE = TagAssignmentInput(tag_id="approved", object_type="source", source_id="pg")


def _on_section(document: int) -> TagAssignmentInput:
    return TagAssignmentInput(
        tag_id="approved", object_type="section", document_id=document, section_heading=SCOPE
    )


async def test_a_definition_citation_asks_the_right_that_edits_the_definition(library, monkeypatch):
    _db, ids = library
    reader, _ = _scoped(monkeypatch, ["*"], "glossary_read")
    with pytest.raises(PermissionError):
        await Mutation().cite_tag_definition(reader, "approved", _cite(ids["sales"]))
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    done = await Mutation().cite_tag_definition(editor, "approved", _cite(ids["sales"]))
    assert (done.success, done.code) == (True, "tag.citation_added")
    [tag] = [t for t in await Query().tags(editor) if t.id == "approved"]
    [cited] = tag.citations
    assert (cited.section_heading, cited.document_title, cited.state) == (
        SCOPE,
        "sales policy",
        "current",
    )
    assert cited.text_sha256 == citations.text_hash("sales scope.")
    assert cited.cited_by == "test-user" and tag.needs_review is False
    # Each grant rebinds the one test role: the caller is granted again before it acts.
    reader, _ = _scoped(monkeypatch, ["*"], "glossary_read")
    with pytest.raises(PermissionError):
        await Mutation().uncite_tag_definition(reader, "approved", _cite(ids["sales"]))
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    gone = await Mutation().uncite_tag_definition(editor, "approved", _cite(ids["sales"]))
    assert (gone.success, gone.code) == (True, "tag.citation_removed")


async def test_an_assignment_citation_asks_the_right_that_edits_the_assignment(
    library, monkeypatch
):
    db, ids = library
    async with db.acquire() as conn:
        await tag_repo.assign(
            conn,
            TagAssignment(
                tag_id="approved",
                object_type="section",
                document_id=ids["hr"],
                section_heading=SCOPE,
            ),
        )
    # A tag on a library section is the library's curator's: a table editor may not cite on it.
    table_editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    with pytest.raises(PermissionError):
        await Mutation().cite_tag_assignment(
            table_editor, _on_section(ids["hr"]), _cite(ids["sales"])
        )
    curator, _ = _scoped(monkeypatch, ["*"], "glossary_rw", "glossary_read")
    done = await Mutation().cite_tag_assignment(
        curator, _on_section(ids["hr"]), _cite(ids["sales"])
    )
    assert done.success, done.message
    [assignment] = await Query().tag_assignments(curator)
    [cited] = assignment.citations
    assert (cited.document_title, cited.section_heading, cited.state) == (
        "sales policy",
        SCOPE,
        "current",
    )
    # The tag on the source is not assigned: nothing to cite on.
    table_editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    missing = await Mutation().cite_tag_assignment(table_editor, SOURCE, _cite(ids["sales"]))
    assert (missing.success, missing.code) == (False, "schema.tag_assignment_not_found")


async def test_a_document_the_caller_does_not_read_cannot_be_cited(library, monkeypatch):
    _db, ids = library
    editor, _ = _scoped(monkeypatch, ["sales"], "table_registration")
    refused = await Mutation().cite_tag_definition(editor, "approved", _cite(ids["hr"]))
    assert (refused.success, refused.code) == (False, "reference_library.not_found")
    assert "hr policy" not in refused.message


async def test_the_flag_is_set_on_the_tag_and_an_uncited_assignment_reads_as_a_gap(
    library, monkeypatch
):
    _db, ids = library
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    saved = await Mutation().update_tag(
        editor,
        TagInput(
            id="approved",
            description="Approved",
            applies_to=["source", "section"],
            requires_citation=True,
        ),
    )
    assert saved.success, saved.message
    [tag] = [t for t in await Query().tags(editor) if t.id == "approved"]
    assert tag.requires_citation is True
    # Assigning it without a citation is not refused.
    assigned = await Mutation().assign_tag(editor, SOURCE)
    assert assigned.success, assigned.message
    [gap] = await Query().tag_assignments(editor)
    assert (gap.object_key, gap.citations, gap.citation_gap) == ("source:pg", [], True)
    await Mutation().cite_tag_assignment(editor, SOURCE, _cite(ids["sales"]))
    [cited] = await Query().tag_assignments(editor)
    assert cited.citation_gap is False and len(cited.citations) == 1


async def test_a_document_lists_what_cites_it_among_what_its_reader_reads(library, monkeypatch):
    db, ids = library
    sales = ids["sales"]
    async with db.acquire() as conn:
        on_hr = TagAssignment(
            tag_id="approved", object_type="section", document_id=ids["hr"], section_heading=SCOPE
        )
        on_pg = TagAssignment(tag_id="approved", object_type="source", source_id="pg")
        for assignment in (on_hr, on_pg):
            await tag_repo.assign(conn, assignment)
            await tag_repo.cite_assignment(conn, assignment, citations.Place(sales, SCOPE), None)
        await tag_repo.cite_definition(conn, "pii", citations.Place(sales, SCOPE), None)
    _info, everyone = _scoped(monkeypatch, ["*"], "glossary_read")
    cited = await router.cited_by(sales, everyone, section_heading=SCOPE)
    assert [t["tag_id"] for t in cited["tags"]] == ["pii"]
    assert sorted(a["object_key"] for a in cited["tag_assignments"]) == [
        f"section:{ids['hr']}:heading:{SCOPE}",
        "source:pg",
    ]
    assert {a["state"] for a in cited["tag_assignments"]} == {"current"}
    # A sales reader reads the sales policy, but not the tag on the hr policy's section.
    _info, seller = _scoped(monkeypatch, ["sales"], "glossary_read")
    cited = await router.cited_by(sales, seller)
    assert [a["object_key"] for a in cited["tag_assignments"]] == ["source:pg"]
    # No section of that name is cited.
    assert (await router.cited_by(sales, seller, section_heading="Other"))["tags"] == []


# One assignment form: the assignment and its citations saved by one assignTag call (REQ-2208).


def _whole(document: int) -> LibraryCitationInput:
    return LibraryCitationInput(document_id=document)


async def _held(db) -> list:
    async with db.acquire() as conn:
        return await tag_repo.list_assignments(conn)


async def _cited_on_source(db) -> list[citations.Citation]:
    async with db.acquire() as conn:
        found = await tag_repo.assignment_citations(conn)
    return found.get(("approved", "source:pg"), [])


async def test_an_assignment_saved_with_two_citations_stores_both(library, monkeypatch):
    db, ids = library
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    done = await Mutation().assign_tag(
        editor, SOURCE, citations=[_cite(ids["sales"]), _whole(ids["hr"])]
    )
    assert done.success, done.message
    [assignment] = await Query().tag_assignments(editor)
    assert sorted((c.document_title, c.section_heading) for c in assignment.citations) == [
        ("hr policy", None),
        ("sales policy", SCOPE),
    ]
    assert {c.state for c in assignment.citations} == {"current"}
    assert assignment.citation_gap is False


async def test_a_citation_the_library_refuses_stores_nothing_not_even_the_assignment(
    library, monkeypatch
):
    db, ids = library
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    no_section = LibraryCitationInput(document_id=ids["sales"], section_heading="Nowhere")
    refused = await Mutation().assign_tag(editor, SOURCE, citations=[_cite(ids["hr"]), no_section])
    assert (refused.success, refused.code) == (False, "reference_library.citation_invalid")
    assert await _held(db) == [] and await _cited_on_source(db) == []
    no_document = await Mutation().assign_tag(
        editor, SOURCE, citations=[_cite(ids["hr"]), _whole(987654)]
    )
    assert (no_document.success, no_document.code) == (False, "reference_library.not_found")
    assert await _held(db) == [] and await _cited_on_source(db) == []


async def test_an_edit_replaces_the_set_and_keeps_an_unchanged_citations_record(
    library, monkeypatch
):
    db, ids = library
    editor, _ = _scoped(monkeypatch, ["*"], "table_registration")
    first = await Mutation().assign_tag(
        editor, SOURCE, citations=[_cite(ids["sales"]), _cite(ids["hr"])]
    )
    assert first.success, first.message
    # The sales section's text changed since it was cited: its record is the old text's hash.
    async with db.acquire() as conn:
        await conn.execute_core(
            update(reference_citations)
            .where(reference_citations.c.document_id == ids["sales"])
            .values(text_sha256="0" * 64)
        )
    edited = await Mutation().assign_tag(
        editor,
        TagAssignmentInput(
            tag_id="approved", object_type="source", source_id="pg", reason="Reviewed"
        ),
        citations=[_cite(ids["sales"]), _whole(ids["hr"])],
    )
    assert edited.success, edited.message
    by_place = {(c.document_title, c.section_heading): c for c in await _cited_on_source(db)}
    assert set(by_place) == {("sales policy", SCOPE), ("hr policy", None)}
    kept = by_place[("sales policy", SCOPE)]
    assert (kept.text_sha256, kept.state) == ("0" * 64, "drifted")
    [assignment] = await _held(db)
    assert assignment["reason"] == "Reviewed"
    # Leaving the list out leaves the citations as they are; an empty list takes them all off.
    untouched = await Mutation().assign_tag(editor, SOURCE)
    assert untouched.success, untouched.message
    assert len(await _cited_on_source(db)) == 2
    cleared = await Mutation().assign_tag(editor, SOURCE, citations=[])
    assert cleared.success, cleared.message
    assert await _cited_on_source(db) == []


async def test_saving_citations_with_an_assignment_asks_only_the_assignments_rights(
    library, monkeypatch
):
    db, ids = library
    reader, _ = _scoped(monkeypatch, ["*"], "glossary_read")
    with pytest.raises(PermissionError):
        await Mutation().assign_tag(reader, SOURCE, citations=[_cite(ids["sales"])])
    assert await _held(db) == []
    # A document the caller does not read is one the library does not hold, for this caller: a
    # sales curator tags the sales policy's section, citing the hr policy too.
    on_sales = _on_section(ids["sales"])
    curator, _ = _scoped(monkeypatch, ["sales"], "glossary_rw", "glossary_read")
    refused = await Mutation().assign_tag(
        curator, on_sales, citations=[_cite(ids["sales"]), _cite(ids["hr"])]
    )
    assert (refused.success, refused.code) == (False, "reference_library.not_found")
    assert await _held(db) == []
    curator, _ = _scoped(monkeypatch, ["sales"], "glossary_rw", "glossary_read")
    done = await Mutation().assign_tag(curator, on_sales, citations=[_cite(ids["sales"])])
    assert done.success, done.message
    async with db.acquire() as conn:
        found = await tag_repo.assignment_citations(conn)
    key = ("approved", f"section:{ids['sales']}:heading:{SCOPE}")
    assert [c.document_title for c in found[key]] == ["sales policy"]
