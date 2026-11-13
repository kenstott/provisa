# Copyright (c) 2026 Kenneth Stott
# Canary: e70c5c1a-0062-41b9-b036-557af1f121c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2208 (amended 2026-11-07, THE TAGS REPORT): tags are published as a meta table with one row
per tag on an object, plus one per required tag an object lacks, and a standard report over it.
Each row's citation measures and status are computed when it is read; a reader sees the rows of
the objects it may read.

The model: domains sales and hr, table 1 (sales.orders, columns email and total) and table 2
(hr.staff). The library: one policy document with the sections "Personal data" and "Retention".
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, text

from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS
from provisa.api.app_loaders import _META_TABLES
from provisa.compiler.names_table import NameRow, build_names_table
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.core.models import Tag, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.repositories.model_names import write_names_table
from provisa.core.schema_org import domains, registered_tables, sources, table_columns
from provisa.reference_library import store
from provisa.reference_library.citations import Place
from provisa.security.rights import (
    META_DOMAIN_ID,
    META_ROW_SCOPED_VIEWS,
    OPS_DOMAIN_ID,
    OPS_ROW_SCOPED_VIEWS,
)

PERSONAL = "Personal data"
RETENTION = "Retention"

APPROVED = Tag(id="approved", applies_to=["table"], requires_citation=True)
OWNER_REVIEWED = Tag(id="owner_reviewed", applies_to=["table"], required=True)
SENSITIVITY = Tag(id="sensitivity", applies_to=["column"], param_policy="required")
CHECKED = Tag(id="checked", applies_to=["column"], requires_citation=True)


def _on_table(tag: str, table_id: int) -> TagAssignment:
    return TagAssignment(tag_id=tag, object_type="table", table_id=table_id)


def _on_column(tag: str, column: str, **why) -> TagAssignment:
    return TagAssignment(tag_id=tag, object_type="column", table_id=1, column_name=column, **why)


def _passages(personal: str):
    texts = [(PERSONAL, 1, personal), (RETENTION, 2, "Records are kept for seven years.")]
    return [
        store.StoredPassage(ordinal=i, section=s, page=p, text=t, vector=[0.1, 0.9])
        for i, (s, p, t) in enumerate(texts)
    ]


async def _model(conn) -> int:
    await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    for domain in ("sales", "hr"):
        await conn.execute_core(insert(domains).values(id=domain, description=domain))
    for tid, domain, name in ((1, "sales", "orders"), (2, "hr", "staff")):
        await conn.execute_core(
            insert(registered_tables).values(
                id=tid, source_id="pg", domain_id=domain, schema_name="public", table_name=name
            )
        )
    for column in ("email", "total"):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=1, domain_id="sales", column_name=column, data_type="varchar"
            )
        )
    await write_names_table(
        conn,
        build_names_table(
            [
                NameRow(
                    "table",
                    "1",
                    "sql",
                    "orders",
                    "orders",
                    "pg.public.orders",
                    domain_id="sales",
                    table_id=1,
                    scope=("sql", "sales"),
                )
            ]
        ).rows,
    )
    return await store.insert_document(
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
        passages=_passages("Personal data is any information about a person."),
    )


@pytest.fixture
async def report(control_plane):
    """The model, its tags and citations, the policy's Personal data section then revised, and
    the views made: each report row as a dict, keyed by (tag, object key, via)."""
    db = control_plane.org()
    async with db.acquire() as conn:
        for name in ("derived_tags", "effective_column_tags", "tag_instances"):
            await conn.execute_core(text(_META_TABLE_VIEWS[name]))
        await conn.execute_core(text(_OPS_REPORT_VIEWS["tags_report"]))
        document = await _model(conn)
        for tag in (APPROVED, OWNER_REVIEWED, SENSITIVITY, CHECKED):
            await tag_repo.upsert(conn, tag)
        await tag_repo.assign(conn, _on_table("approved", 1))
        await tag_repo.assign(conn, _on_table("approved", 2))
        await tag_repo.assign(conn, _on_table("owner_reviewed", 2))
        await tag_repo.assign(conn, _on_column("sensitivity:high", "email"))
        await tag_repo.assign(
            conn, _on_column("deprecated", "total", reason="replaced", expires_on="2020-01-01")
        )
        await tag_repo.assign(conn, _on_column("checked", "email"))
        await tag_repo.cite_assignment(
            conn, _on_table("approved", 1), Place(document, RETENTION), "steward"
        )
        await tag_repo.cite_assignment(
            conn, _on_column("checked", "email"), Place(document, PERSONAL), "steward"
        )
        # A new version of the policy revises the Personal data section.
        await store.replace_content(
            conn,
            document,
            media_type="text/markdown",
            content_sha256="b" * 64,
            embedding_model="m",
            passages=_passages("Personal data is anything that identifies a person."),
            source=None,
            refreshed=True,
        )
        rows = (await conn.execute_core(text("SELECT * FROM tags_report"))).mappings().all()
    yield db, {(r["tag_id"], r["object_key"], r["via"]): dict(r) for r in rows}
    await db.close()


def test_the_tags_report_is_a_row_scoped_meta_table_and_a_standard_report():
    assert "tag_instances" in _META_TABLES
    assert META_ROW_SCOPED_VIEWS["tag_instances"] == ("table_id",)
    # A standard report is an ops-domain relation; the Reports list is the ops domain's tables.
    assert "tags_report" in _OPS_REPORT_VIEWS
    assert OPS_ROW_SCOPED_VIEWS["tags_report"] == ("table_id",)


@pytest.mark.asyncio
async def test_one_row_per_tag_on_an_object_and_per_required_tag_missing(report):
    rows = report[1]
    assert sorted(rows) == [
        ("approved", "table:1", "assigned"),
        ("approved", "table:2", "assigned"),
        ("checked", "column:1:email", "assigned"),
        ("deprecated", "column:1:total", "assigned"),
        ("owner_reviewed", "table:1", "missing"),
        ("owner_reviewed", "table:2", "assigned"),
        ("sensitivity:high", "column:1:email", "assigned"),
    ]


@pytest.mark.asyncio
async def test_each_row_names_its_object_and_tag(report):
    rows = report[1]
    approved = rows[("approved", "table:1", "assigned")]
    assert (approved["object_kind"], approved["object_id"], approved["object_name"]) == (
        "table",
        "1",
        "pg.public.orders",
    )
    assert (approved["domain_id"], approved["table_id"]) == ("sales", 1)
    assert (approved["tag_required"], approved["tag_requires_citation"]) == (False, True)
    sensitivity = rows[("sensitivity:high", "column:1:email", "assigned")]
    assert (sensitivity["base_tag_id"], sensitivity["parameter"]) == ("sensitivity", "high")
    assert (sensitivity["domain_id"], sensitivity["table_id"]) == ("sales", 1)
    expired = rows[("deprecated", "column:1:total", "assigned")]
    assert (expired["reason"], expired["expires_on"], expired["tag_is_system"]) == (
        "replaced",
        "2020-01-01",
        True,
    )


@pytest.mark.asyncio
async def test_the_citation_measures_are_computed_when_read(report):
    rows = report[1]
    measures = {
        key: (r["citations"], r["citations_broken"], r["citations_drifted"], r["citation_missing"])
        for key, r in rows.items()
    }
    assert measures[("approved", "table:1", "assigned")] == (1, 0, 0, False)
    assert measures[("approved", "table:2", "assigned")] == (0, 0, 0, True)
    assert measures[("checked", "column:1:email", "assigned")] == (1, 0, 1, False)
    assert measures[("owner_reviewed", "table:1", "missing")] == (0, 0, 0, False)


@pytest.mark.asyncio
async def test_each_row_rolls_up_to_a_status_with_its_reason(report):
    rows = report[1]
    statuses = {key: (r["status"], r["status_reason"]) for key, r in rows.items()}
    assert statuses == {
        ("approved", "table:1", "assigned"): ("clean", None),
        ("approved", "table:2", "assigned"): ("needs attention", "required citation missing"),
        ("checked", "column:1:email", "assigned"): (
            "needs attention",
            "a cited section changed since it was cited",
        ),
        ("deprecated", "column:1:total", "assigned"): ("clean", None),
        ("owner_reviewed", "table:1", "missing"): ("needs attention", "required tag missing"),
        ("owner_reviewed", "table:2", "assigned"): ("clean", None),
        ("sensitivity:high", "column:1:email", "assigned"): ("clean", None),
    }


_INSTANCES_TID, _REPORT_TID = 100, 101


def _governance(role: dict):
    ctx = CompilationContext()
    for tid, name, domain, field in (
        (_INSTANCES_TID, "tag_instances", META_DOMAIN_ID, "tag_instances"),
        # An ops table's field carries its schema's prefix (naming.SYSTEM_SCHEMA_GQL_PREFIX).
        (_REPORT_TID, "tags_report", OPS_DOMAIN_ID, "op_tags_report"),
    ):
        ctx.tables[name] = TableMeta(
            table_id=tid,
            field_name=field,
            type_name=name,
            source_id="provisa-admin",
            catalog_name="main",
            schema_name="public",
            table_name=name,
            domain_id=domain,
        )
    readers = [role["id"]]
    tables = [
        {"id": 1, "domain_id": "sales", "columns": [{"column_name": "id", "visible_to": readers}]},
        {"id": 2, "domain_id": "hr", "columns": [{"column_name": "id", "visible_to": readers}]},
    ]

    class _NoRules:
        rules: dict = {}
        write_rules: dict = {}

    return build_governance_context(
        role["id"], _NoRules(), {}, ctx, tables, {**role, "acting_chain": [role["id"]]}
    )


@pytest.mark.asyncio
async def test_a_reader_sees_no_row_of_an_object_it_may_not_read(report):
    db, rows = report
    gov = _governance(
        {"id": "sales", "description": "Sales.", "capabilities": [], "domain_access": ["sales"]}
    )
    predicates = {tid: gov.rls_rules[tid].sql(dialect="postgres") for tid in (100, 101)}
    assert predicates == {_INSTANCES_TID: "table_id IN (1)", _REPORT_TID: "table_id IN (1)"}
    async with db.acquire() as conn:
        read = (
            await conn.execute_core(
                text(f"SELECT tag_id, object_key FROM tags_report WHERE {predicates[101]}")
            )
        ).fetchall()
    assert sorted(tuple(r) for r in read) == [
        ("approved", "table:1"),
        ("checked", "column:1:email"),
        ("deprecated", "column:1:total"),
        ("owner_reviewed", "table:1"),
        ("sensitivity:high", "column:1:email"),
    ]
    assert len(rows) == 7
