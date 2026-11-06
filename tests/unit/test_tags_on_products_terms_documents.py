# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f2d71-6c4e-4a95-b0d3-9e17c5a2f846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registry tags on a data product, a glossary term, and a reference library document and its
sections, through the one assignTag / unassignTag surface tables use (REQ-1634, REQ-2145,
REQ-2208).

Each is assigned by whoever curates the object -- a product's editor in its domain, the glossary's
curator, the library's curator in the document's domains -- and read as the object is read
(REQ-2152): a product's tags in its domain, a term's as the term, a document's or a section's as
the document.
"""

# Requirements: REQ-1634, REQ-2145, REQ-2160, REQ-2208, REQ-2152

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.read_rights import DependentsReader, _tag_assignment_readable
from provisa.api.admin.types import TagAssignmentInput
from provisa.core.models import TagAssignment
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import data_products, tags
from provisa.reference_library import store
from tests.unit.test_admin_deletes_need_the_domain import ROLES, _info, plane  # noqa: F401

CURATORS = {
    # a product editor and library and glossary curator in sales, and nothing else
    "sales_curator": {
        "id": "sales_curator",
        "capabilities": ["data_product_rw", "glossary_read", "glossary_rw"],
        "domain_access": ["sales"],
    },
    # a table editor in sales, who curates neither products nor the library
    "sales_tables": {
        "id": "sales_tables",
        "capabilities": ["table_registration"],
        "domain_access": ["sales"],
    },
}

KINDS = ["source", "table", "column", "relationship", "command", "product", "term"]


def _passage(ordinal: int, section: str | None, page: int | None) -> store.StoredPassage:
    return store.StoredPassage(
        ordinal=ordinal, section=section, page=page, text=f"text {ordinal}", vector=[0.1, 0.9]
    )


async def _document(conn, title: str, domains: list[str], passages) -> int:
    return await store.insert_document(
        conn,
        title=title,
        source_kind=store.URL,
        source=f"https://example.com/{title}",
        media_type="text/markdown",
        content_sha256=title.encode().hex().ljust(64, "0")[:64],
        status=store.KEPT,
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
        added_by="org_admin",
        domains=domains,
        passages=passages,
    )


@pytest.fixture
async def library(plane, monkeypatch):  # noqa: F811
    """A tag for every kind of object; a data product in each domain; a term in sales; a policy
    in sales with a headed section, a PDF in sales read by its pages, and a policy in finance."""
    import provisa.api.app as appmod

    async def _refresh() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _refresh)
    monkeypatch.setattr(appmod.state, "roles", {**ROLES, **CURATORS}, raising=False)

    async def _pool():
        return plane

    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    ids: dict[str, int] = {}
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(tags).values(
                id="reviewed", applies_to=[*KINDS, "document", "section"], reason_policy="optional"
            )
        )
        for product_id, domain_id in (("sales_kpis", "sales"), ("ledger", "finance")):
            await conn.execute_core(
                insert(data_products).values(id=product_id, name=product_id, domain_id=domain_id)
            )
        ids["term"] = await glossary_repo.create_abstract_term(conn, "order", domains={"sales"})
        ids["policy"] = await _document(
            conn,
            "privacy",
            ["sales"],
            [_passage(1, "Personal data", None), _passage(2, "Retention", None)],
        )
        ids["pdf"] = await _document(conn, "handbook", ["sales"], [_passage(1, None, 3)])
        ids["finance_policy"] = await _document(
            conn, "ledger-rules", ["finance"], [_passage(1, "Close", None)]
        )
    return ids


def _on(kind: str, **target) -> TagAssignmentInput:
    return TagAssignmentInput(tag_id="reviewed", object_type=kind, **target)


async def _seen(role_id: str) -> list:
    return await schema_query.Query().tag_assignments(_info(role_id))


# --- what each kind of object is keyed by ---------------------------------------------------------


@pytest.mark.parametrize(
    ("target", "key"),
    [
        ({"object_type": "product", "product_id": "sales_kpis"}, "product:sales_kpis"),
        ({"object_type": "document", "document_id": 7}, "document:7"),
        (
            {"object_type": "section", "document_id": 7, "section_heading": "Personal data"},
            "section:7:heading:Personal data",
        ),
        ({"object_type": "section", "document_id": 7, "section_page": 3}, "section:7:page:3"),
    ],
)
def test_each_object_has_its_own_key(target, key):
    assert TagAssignment(tag_id="reviewed", **target).object_key() == key


def test_a_section_with_no_heading_and_no_page_names_no_target():
    model = TagAssignment(tag_id="reviewed", object_type="section", document_id=7)
    result = schema_mutation._assignment_target_problem(model)
    assert result is not None and result.code == "schema.tag_bad_target"


# --- a data product ---------------------------------------------------------------------------------


async def test_a_product_editor_tags_a_product_in_their_domain_and_reads_it_back(library):
    mutation = schema_mutation.Mutation()
    result = await mutation.assign_tag(
        _info("sales_curator"), _on("product", product_id="sales_kpis")
    )
    assert result.success, result.message
    seen = await _seen("sales_curator")
    assert [(a.object_type, a.product_id) for a in seen] == [("product", "sales_kpis")]
    assert (
        await mutation.unassign_tag(_info("sales_curator"), _on("product", product_id="sales_kpis"))
    ).success
    assert await _seen("sales_curator") == []


async def test_a_product_in_another_domain_is_refused(library):
    with pytest.raises(PermissionError):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_curator"), _on("product", product_id="ledger")
        )


async def test_a_product_tag_takes_the_product_editors_right(library):
    with pytest.raises(PermissionError):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_tables"), _on("product", product_id="sales_kpis")
        )


async def test_a_product_that_is_not_there_is_not_found(library):
    result = await schema_mutation.Mutation().assign_tag(
        _info("sales_curator"), _on("product", product_id="nope")
    )
    assert (result.success, result.code) == (False, "schema.tag_target_not_found")


# --- a glossary term --------------------------------------------------------------------------------


async def test_a_glossary_curator_tags_a_term_without_the_table_editors_right(library):
    result = await schema_mutation.Mutation().assign_tag(
        _info("sales_curator"), _on("term", term_id=library["term"])
    )
    assert result.success, result.message
    seen = await _seen("sales_curator")
    assert [(a.object_type, a.term_id) for a in seen] == [("term", library["term"])]


# --- a library document and its sections ------------------------------------------------------------


async def test_a_library_curator_tags_a_document_and_a_section_of_it(library):
    mutation = schema_mutation.Mutation()
    policy = library["policy"]
    for target in (
        _on("document", document_id=policy),
        _on("section", document_id=policy, section_heading="Personal data"),
        _on("section", document_id=library["pdf"], section_page=3),
    ):
        result = await mutation.assign_tag(_info("sales_curator"), target)
        assert result.success, result.message
    seen = {
        (a.object_type, a.document_id, a.section_heading, a.section_page)
        for a in await _seen("sales_curator")
    }
    assert seen == {
        ("document", policy, None, None),
        ("section", policy, "Personal data", None),
        ("section", library["pdf"], None, 3),
    }
    assert (
        await mutation.unassign_tag(
            _info("sales_curator"),
            _on("section", document_id=policy, section_heading="Personal data"),
        )
    ).success
    assert len(await _seen("sales_curator")) == 2


async def test_a_section_the_library_did_not_read_is_not_found(library):
    mutation = schema_mutation.Mutation()
    for target in (
        _on("section", document_id=library["policy"], section_heading="Cookies"),
        _on("section", document_id=library["pdf"], section_page=4),
        _on("document", document_id=987654),
    ):
        result = await mutation.assign_tag(_info("sales_curator"), target)
        assert (result.success, result.code) == (False, "schema.tag_target_not_found")


async def test_a_document_in_another_domain_is_refused(library):
    with pytest.raises(PermissionError):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_curator"), _on("document", document_id=library["finance_policy"])
        )


async def test_a_document_tag_takes_the_library_curators_right(library):
    with pytest.raises(PermissionError):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_tables"), _on("document", document_id=library["policy"])
        )


# --- read as the object is read (REQ-2152) ----------------------------------------------------------


async def test_a_documents_tags_are_read_as_the_document_is(library):
    mutation = schema_mutation.Mutation()
    for document_id in (library["policy"], library["finance_policy"]):
        assert (
            await mutation.assign_tag(_info("everywhere"), _on("document", document_id=document_id))
        ).success
    assert {a.document_id for a in await _seen("sales_curator")} == {library["policy"]}
    assert {a.document_id for a in await _seen("everywhere")} == {
        library["policy"],
        library["finance_policy"],
    }


async def test_a_refusal_names_a_document_tag_only_to_a_reader_of_the_document(
    library,
    plane,  # noqa: F811
):
    reads_sales = DependentsReader(
        column=lambda table_id, column: True,
        metric=lambda row: True,
        term=lambda domains: "sales" in domains,
        command=lambda name: True,
    )
    async with plane.acquire() as conn:
        for document_id, readable in (
            (library["policy"], True),
            (library["finance_policy"], False),
        ):
            row = {"object_type": "document", "document_id": document_id, "table_id": None}
            assert await _tag_assignment_readable(conn, reads_sales, row) is readable
        term_row = {"object_type": "term", "term_id": library["term"], "table_id": None}
        assert await _tag_assignment_readable(conn, reads_sales, term_row)
