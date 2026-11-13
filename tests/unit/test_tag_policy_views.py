# Copyright (c) 2026 Kenneth Stott
# Canary: 4b1f7e2a-9c36-4d58-8e0a-71c5d2f3a964
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2208 (decision 2026-11-13): governed meta views beside the tags report connect tags,
library documents and sections, and objects. tag_citations has one row per citation of a tag's
definition or of an assignment, with its state; section_tags one per tag on a document or section;
policy_coverage one per path from a section (or a whole document) to an object, by a shared tag or
by a citation. Each is read as the tags report is, and a document's rows where the document is.

The model: domains sales and hr; table 1 (sales.orders, columns email and total) and table 2
(hr.staff, column name). The library: a privacy policy read in sales and hr, with the sections
Personal data and Retention, and a handbook read by hr alone. The tags: personal on the Personal
data section and on the columns email and name; retained, whose definition cites Retention, on
both tables; checked on the column total, its assignment citing Personal data; handbook on the
handbook as a whole. The Personal data section is then revised: the assignment's citation drifts.
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, text

from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS
from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS, _output_columns
from provisa.api.app_loaders import _META_TABLES
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context, library_row_predicate
from provisa.core.models import Tag, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import domains, registered_tables, table_columns
from provisa.reference_library import store
from provisa.reference_library.citations import Place
from provisa.security.rights import LIBRARY_ROW_VIEWS, META_DOMAIN_ID

PERSONAL = "Personal data"
RETENTION = "Retention"
VIEWS = ("tag_citations", "section_tags", "policy_coverage")

TAGS = (
    Tag(id="personal", applies_to=["column", "section"]),
    Tag(id="retained", applies_to=["table"]),
    Tag(id="checked", applies_to=["column"]),
    Tag(id="handbook", applies_to=["document"]),
)


def _passages(personal: str):
    texts = [(PERSONAL, 1, personal), (RETENTION, 2, "Records are kept for seven years.")]
    return [
        store.StoredPassage(ordinal=i, section=s, page=p, text=t, vector=[0.1, 0.9])
        for i, (s, p, t) in enumerate(texts)
    ]


async def _document(conn, title: str, readers: list[str]) -> int:
    return await store.insert_document(
        conn,
        title=title,
        source=f"https://example.com/{title}",
        source_kind=store.URL,
        media_type="text/markdown",
        content_sha256="a" * 64,
        status=store.KEPT,
        embedding_model="m",
        added_by="org_admin",
        domains=readers,
        passages=_passages("Personal data is any information about a person."),
    )


async def _model(conn) -> tuple[int, int]:
    """The model and the library: the privacy policy's and the handbook's ids."""
    from provisa.core.schema_org import sources

    await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    for domain in ("sales", "hr"):
        await conn.execute_core(insert(domains).values(id=domain, description=domain))
    for tid, domain, name, columns in (
        (1, "sales", "orders", ("email", "total")),
        (2, "hr", "staff", ("name",)),
    ):
        await conn.execute_core(
            insert(registered_tables).values(
                id=tid, source_id="pg", domain_id=domain, schema_name="public", table_name=name
            )
        )
        for column in columns:
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=tid, domain_id=domain, column_name=column, data_type="varchar"
                )
            )
    return (
        await _document(conn, "privacy", ["sales", "hr"]),
        await _document(conn, "handbook", ["hr"]),
    )


def _column(tag: str, table_id: int, column: str) -> TagAssignment:
    return TagAssignment(tag_id=tag, object_type="column", table_id=table_id, column_name=column)


@pytest.fixture
async def library(control_plane):
    """The model, its tags and citations, the privacy policy's Personal data section then
    revised, and the views made: the database and the two documents' ids."""
    db = control_plane.org()
    async with db.acquire() as conn:
        for name in ("derived_tags", "effective_column_tags", "tag_instances", *VIEWS):
            await conn.execute_core(text(_META_TABLE_VIEWS[name]))
        privacy, handbook = await _model(conn)
        for tag in TAGS:
            await tag_repo.upsert(conn, tag)
        await tag_repo.assign(
            conn,
            TagAssignment(
                tag_id="personal",
                object_type="section",
                document_id=privacy,
                section_heading=PERSONAL,
                reason="defines personal data",
            ),
        )
        await tag_repo.assign(
            conn, TagAssignment(tag_id="handbook", object_type="document", document_id=handbook)
        )
        await tag_repo.assign(conn, _column("personal", 1, "email"))
        await tag_repo.assign(conn, _column("personal", 2, "name"))
        for table_id in (1, 2):
            await tag_repo.assign(
                conn, TagAssignment(tag_id="retained", object_type="table", table_id=table_id)
            )
        await tag_repo.assign(conn, _column("checked", 1, "total"))
        await tag_repo.cite_definition(conn, "retained", Place(privacy, RETENTION), "steward")
        await tag_repo.cite_assignment(
            conn, _column("checked", 1, "total"), Place(privacy, PERSONAL), "steward"
        )
        await store.replace_content(
            conn,
            privacy,
            media_type="text/markdown",
            content_sha256="b" * 64,
            embedding_model="m",
            passages=_passages("Personal data is anything that identifies a person."),
            source=None,
            refreshed=True,
        )
    yield db, privacy, handbook
    await db.close()


async def _all(db, view: str, where: str = "TRUE") -> list[dict]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(text(f"SELECT * FROM {view} WHERE {where}"))
        return [dict(r) for r in rows.mappings().all()]


def _role(role_id: str, *capabilities: str) -> dict:
    return {"id": role_id, "capabilities": list(capabilities)}


async def _read(db, view: str, role: dict, readable_in: frozenset[str] | None) -> list[dict]:
    predicate = library_row_predicate(role, readable_in, view).sql(dialect="postgres")
    return await _all(db, view, predicate)


# --------------------------------------------------------------------------- the relations


def test_the_three_views_are_meta_tables_read_by_the_library_rule():
    for view in VIEWS:
        assert view in _META_TABLE_VIEWS
        assert view in _META_TABLES
    assert set(LIBRARY_ROW_VIEWS) == {(META_DOMAIN_ID, v) for v in VIEWS}


# --------------------------------------------------------------------------- tag_citations


@pytest.mark.asyncio
async def test_tag_citations_has_one_row_per_citation_with_its_state(library):
    db, privacy, _handbook = library
    rows = await _all(db, "tag_citations")
    found = sorted(
        (
            r["citer"],
            r["tag_id"],
            r["object_key"],
            r["document_id"],
            r["section_heading"],
            r["state"],
            r["needs_review"],
        )
        for r in rows
    )
    assert found == [
        ("assignment", "checked", "column:1:total", privacy, PERSONAL, "drifted", True),
        ("definition", "retained", None, privacy, RETENTION, "current", False),
    ]


@pytest.mark.asyncio
async def test_an_assignments_citation_names_its_object_as_the_tags_report_does(library):
    db, _privacy, _handbook = library
    (row,) = await _all(db, "tag_citations", "citer = 'assignment'")
    assert (row["object_kind"], row["object_id"], row["table_id"], row["column_key"]) == (
        "column",
        "1:total",
        1,
        "1:total",
    )
    assert row["document_title"] == "privacy"
    # cited by its heading: the page is not part of the place
    assert row["section_page"] is None
    assert row["cited_by"] == "steward"
    assert row["cited_at"] is not None
    (instance,) = await _all(db, "tag_instances", f"id = '{row['tag_instance_id']}'")
    assert (instance["tag_id"], instance["object_key"]) == ("checked", "column:1:total")


# --------------------------------------------------------------------------- section_tags


@pytest.mark.asyncio
async def test_section_tags_has_one_row_per_tag_on_a_document_or_section(library):
    db, privacy, handbook = library
    rows = await _all(db, "section_tags")
    found = sorted(
        (r["tag_id"], r["document_id"], r["document_title"], r["section_heading"], r["reason"])
        for r in rows
    )
    assert found == [
        ("handbook", handbook, "handbook", None, None),
        ("personal", privacy, "privacy", PERSONAL, "defines personal data"),
    ]
    for row in rows:
        (instance,) = await _all(db, "tag_instances", f"id = '{row['tag_instance_id']}'")
        assert instance["object_key"] == row["place_key"]


# --------------------------------------------------------------------------- policy_coverage


@pytest.mark.asyncio
async def test_policy_coverage_has_one_row_per_path_saying_which(library):
    db, privacy, _handbook = library
    rows = await _all(db, "policy_coverage")
    found = sorted(
        (r["document_id"], r["section_heading"], r["tag_id"], r["link"], r["object_key"])
        for r in rows
    )
    assert found == [
        (privacy, PERSONAL, "checked", "assignment cites it", "column:1:total"),
        (privacy, PERSONAL, "personal", "shared tag", "column:1:email"),
        (privacy, PERSONAL, "personal", "shared tag", "column:2:name"),
        (privacy, RETENTION, "retained", "tag cites it", "table:1"),
        (privacy, RETENTION, "retained", "tag cites it", "table:2"),
    ]


@pytest.mark.asyncio
async def test_a_coverage_row_names_its_object_and_the_citations_state(library):
    db, _privacy, _handbook = library
    rows = {r["object_key"]: r for r in await _all(db, "policy_coverage")}
    total = rows["column:1:total"]
    assert (total["object_kind"], total["domain_id"], total["table_id"], total["column_key"]) == (
        "column",
        "sales",
        1,
        "1:total",
    )
    assert (total["via"], total["citation_state"]) == ("assigned", "drifted")
    assert rows["column:1:email"]["citation_state"] is None
    assert rows["table:2"]["citation_state"] == "current"
    assert rows["table:2"]["column_key"] is None
    for row in rows.values():
        (instance,) = await _all(db, "tag_instances", f"id = '{row['tag_instance_id']}'")
        assert instance["object_key"] == row["object_key"]


@pytest.mark.asyncio
async def test_the_columns_relevant_to_a_document(library):
    db, privacy, _handbook = library
    rows = await _all(
        db,
        "policy_coverage",
        f"document_id = {privacy} AND object_kind = 'column'",
    )
    assert sorted({r["column_key"] for r in rows}) == ["1:email", "1:total", "2:name"]


# --------------------------------------------------------------------------- governance


@pytest.mark.asyncio
async def test_a_whole_catalog_reader_reads_every_row(library):
    db = library[0]
    for view in VIEWS:
        assert len(await _read(db, view, _role("auditor"), None)) == len(await _all(db, view))


@pytest.mark.asyncio
async def test_a_reader_without_the_document_reads_none_of_its_rows(library):
    db, privacy, handbook = library
    # sales does not reach the handbook: its tag is not read.
    tags = await _read(db, "section_tags", _role("clerk"), frozenset({"sales"}))
    assert {r["document_id"] for r in tags} == {privacy}
    # A reader in a domain the policy is not read in reads none of its citations or paths.
    for view in VIEWS:
        rows = await _read(db, view, _role("clerk"), frozenset({"finance"}))
        assert rows == [], view
    assert handbook not in {r["document_id"] for r in tags}


@pytest.mark.asyncio
async def test_a_reader_without_the_object_reads_no_coverage_row_for_it(library):
    db = library[0]
    rows = await _read(db, "policy_coverage", _role("clerk"), frozenset({"sales"}))
    assert sorted(r["object_key"] for r in rows) == [
        "column:1:email",
        "column:1:total",
        "table:1",
    ]
    hr = await _read(db, "policy_coverage", _role("clerk"), frozenset({"hr"}))
    assert sorted(r["object_key"] for r in hr) == ["column:2:name", "table:2"]


@pytest.mark.asyncio
async def test_an_assignments_citation_needs_the_assignment_a_definitions_the_document(library):
    db = library[0]
    hr = await _read(db, "tag_citations", _role("clerk"), frozenset({"hr"}))
    # the assignment is on a sales column; the definition's citation needs the document alone
    assert [r["citer"] for r in hr] == ["definition"]
    sales = await _read(db, "tag_citations", _role("clerk"), frozenset({"sales"}))
    assert sorted(r["citer"] for r in sales) == ["assignment", "definition"]


def test_the_compiler_applies_the_predicate_to_each_view():
    ctx = CompilationContext()
    for tid, name in enumerate(VIEWS, start=100):
        ctx.tables[name] = TableMeta(
            table_id=tid,
            field_name=name,
            type_name=name,
            source_id="provisa-admin",
            catalog_name="main",
            schema_name="public",
            table_name=name,
            domain_id=META_DOMAIN_ID,
        )
    role = {
        "id": "sales",
        "description": "Sales.",
        "capabilities": [],
        "domain_access": ["sales"],
        "acting_chain": ["sales"],
    }

    class _NoRules:
        rules: dict = {}
        write_rules: dict = {}

    gov = build_governance_context("sales", _NoRules(), {}, ctx, [], role)
    for tid, name in enumerate(VIEWS, start=100):
        assert gov.rls_rules[tid] == library_row_predicate(role, frozenset({"sales"}), name)


# --------------------------------------------------------------------------- relationships


_EDGES = {
    # (id): (source, source column, target column, target)
    "meta:tag_citations:tag_instance": ("tag_citations", "tag_instance_id", "id", "tag_instances"),
    "meta:tag_citations:table": ("tag_citations", "table_id", "id", "registered_tables"),
    "meta:tag_citations:column": ("tag_citations", "column_key", "column_key", "table_columns"),
    "meta:section_tags:tag_instance": ("section_tags", "tag_instance_id", "id", "tag_instances"),
    "meta:policy_coverage:tag_instance": (
        "policy_coverage",
        "tag_instance_id",
        "id",
        "tag_instances",
    ),
    "meta:policy_coverage:table": ("policy_coverage", "table_id", "id", "registered_tables"),
    "meta:policy_coverage:column": ("policy_coverage", "column_key", "column_key", "table_columns"),
    "meta:tag_citations:tags": ("tag_citations", "base_tag_id", "id", "tags"),
    "meta:section_tags:tags": ("section_tags", "base_tag_id", "id", "tags"),
    "meta:policy_coverage:tags": ("policy_coverage", "base_tag_id", "id", "tags"),
    "meta:section_tags:policy_coverage": (
        "section_tags",
        "place_key",
        "place_key",
        "policy_coverage",
    ),
    "meta:tag_instances:policy_coverage": (
        "tag_instances",
        "id",
        "tag_instance_id",
        "policy_coverage",
    ),
    "meta:table_columns:policy_coverage": (
        "table_columns",
        "column_key",
        "column_key",
        "policy_coverage",
    ),
    "meta:registered_tables:policy_coverage": (
        "registered_tables",
        "id",
        "table_id",
        "policy_coverage",
    ),
}


def _view_columns(ddl: str) -> list[str]:
    """A view's output columns, its first branch's when it is a union."""
    import sqlglot

    select = sqlglot.parse_one(ddl, read="postgres").expression
    while select.key == "union":
        select = select.this
    return [e.output_name for e in select.expressions]


def test_the_views_relationships_are_seeded_between_meta_tables():
    seeded = {r[0]: (r[2], r[3], r[4], r[7]) for r in META_RELATIONSHIPS}
    for rel_id, edge in _EDGES.items():
        assert seeded.get(rel_id) == edge, rel_id
        src, src_col, tgt_col, tgt = edge
        assert {src, tgt} <= set(_META_TABLES), rel_id
        for table, column in ((src, src_col), (tgt, tgt_col)):
            ddl = _META_TABLE_VIEWS[table]
            assert column in _view_columns(ddl), (rel_id, table, column)


_RELATION = {
    "registered_tables": "registered_tables_meta",
    "table_columns": "table_columns_meta",
    "tags": "tags_meta",
}


@pytest.mark.asyncio
async def test_each_relationship_joins_its_rows(library):
    """Each edge joins on the columns its seeded row names: a coverage row reaches its tag
    instance, its table and its column, and back."""
    db = library[0]
    async with db.acquire() as conn:
        for name in ("registered_tables", "table_columns", "tags"):
            await conn.execute_core(text(_META_TABLE_VIEWS[name]))
        for rel_id, (src, src_col, tgt_col, tgt) in _EDGES.items():
            joined = (
                await conn.execute_core(
                    text(
                        f"SELECT COUNT(*) FROM {_RELATION.get(src, src)} s "
                        f"JOIN {_RELATION.get(tgt, tgt)} t ON t.{tgt_col} = s.{src_col}"
                    )
                )
            ).scalar_one()
            assert joined > 0, rel_id


# --------------------------------------------------------------------------- the catalog


@pytest.mark.parametrize("view", VIEWS)
def test_every_view_and_column_is_described(view):
    assert TABLE_DESCRIPTIONS.get(view)
    described = COLUMN_DESCRIPTIONS.get(view, {})
    columns = _output_columns(_META_TABLE_VIEWS[view])
    assert [c for c in columns if not described.get(c)] == []
