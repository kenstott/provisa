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
per tag on an object, and a standard report over it. Each row's citation measures and status are
computed when it is read; a reader sees a row exactly when the tag admin would show it that
assignment.

The model: domains sales and hr; table 1 (sales.orders, columns email and total) and table 2
(hr.staff); and one object of every other kind in each domain where the kind has one. The library:
a policy document read by the whole org, and a handbook read by hr.
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, text

from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS
from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS
from provisa.api.app_loaders import _META_TABLES
from provisa.compiler.names_table import NameRow, build_names_table
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context, tag_row_predicate
from provisa.core.models import Tag, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.repositories.model_names import write_names_table
from provisa.core.schema_org import (
    data_products,
    domains,
    glossary_term_domains,
    glossary_terms,
    metrics,
    registered_tables,
    relationships,
    rls_rules,
    roles,
    sources,
    table_columns,
    tracked_functions,
)
from provisa.reference_library import store
from provisa.reference_library.citations import Place
from provisa.security.rights import (
    META_DOMAIN_ID,
    META_ROW_SCOPED_VIEWS,
    OPS_DOMAIN_ID,
    TAG_ROW_VIEWS,
)

PERSONAL = "Personal data"
RETENTION = "Retention"

APPROVED = Tag(id="approved", applies_to=["table"], requires_citation=True)
SENSITIVITY = Tag(id="sensitivity", applies_to=["column"], param_policy="required")
CHECKED = Tag(id="checked", applies_to=["column"], requires_citation=True)
REVIEWED = Tag(
    id="reviewed",
    applies_to=[
        "source",
        "relationship",
        "command",
        "product",
        "term",
        "rls_rule",
        "role",
        "metric",
        "document",
    ],
)


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
    """The model and the library: the handbook's and the policy's ids."""
    await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    await conn.execute_core(
        insert(sources).values(id="crm", type="postgresql", allowed_domains=["hr"])
    )
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
    for rid, target in (("r_sales", 1), ("r_cross", 2)):
        await conn.execute_core(
            insert(relationships).values(
                id=rid,
                source_table_id=1,
                target_table_id=target,
                source_column="email",
                target_column="email",
                cardinality="many-to-one",
            )
        )
    for name, domain in (("f_sales", "sales"), ("f_hr", "hr")):
        await conn.execute_core(insert(tracked_functions).values(name=name, domain_id=domain))
    for pid, domain in (("p_sales", "sales"), ("p_hr", "hr")):
        await conn.execute_core(insert(data_products).values(id=pid, domain_id=domain, name=pid))
    for term_id, domain in ((1, "sales"), (2, "hr")):
        await conn.execute_core(insert(glossary_terms).values(id=term_id, name=f"term{term_id}"))
        await conn.execute_core(
            insert(glossary_term_domains).values(term_id=term_id, domain_id=domain)
        )
    for role in ("teller", "clerk"):
        await conn.execute_core(insert(roles).values(id=role))
    for rule_id, table_id in ((1, 1), (2, 2)):
        await conn.execute_core(
            insert(rls_rules).values(
                id=rule_id, table_id=table_id, role_id="teller", filter_expr=b"1 = 1"
            )
        )
    for metric, readers in (("m_sales", ["teller"]), ("m_analyst", ["clerk"])):
        await conn.execute_core(
            insert(metrics).values(name=metric, expression="count(*)", visible_to=readers)
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
    handbook = await _document(conn, "handbook", ["hr"])
    return handbook, await _document(conn, "privacy", ["*"])


REVIEWED_ON = [
    TagAssignment(tag_id="reviewed", object_type="source", source_id="pg"),
    TagAssignment(tag_id="reviewed", object_type="source", source_id="crm"),
    TagAssignment(tag_id="reviewed", object_type="relationship", relationship_id="r_sales"),
    TagAssignment(tag_id="reviewed", object_type="relationship", relationship_id="r_cross"),
    TagAssignment(tag_id="reviewed", object_type="command", command_name="f_sales"),
    TagAssignment(tag_id="reviewed", object_type="command", command_name="f_hr"),
    TagAssignment(tag_id="reviewed", object_type="product", product_id="p_sales"),
    TagAssignment(tag_id="reviewed", object_type="product", product_id="p_hr"),
    TagAssignment(tag_id="reviewed", object_type="term", term_id=1),
    TagAssignment(tag_id="reviewed", object_type="term", term_id=2),
    TagAssignment(tag_id="reviewed", object_type="rls_rule", rls_rule_id=1),
    TagAssignment(tag_id="reviewed", object_type="rls_rule", rls_rule_id=2),
    TagAssignment(tag_id="reviewed", object_type="role", role_id="teller"),
    TagAssignment(tag_id="reviewed", object_type="role", role_id="clerk"),
    TagAssignment(tag_id="reviewed", object_type="metric", metric_name="m_sales"),
    TagAssignment(tag_id="reviewed", object_type="metric", metric_name="m_analyst"),
]


@pytest.fixture
async def report(control_plane):
    """The model, its tags and citations, the policy's Personal data section then revised, and
    the views made: the database, each report row as a dict keyed by (tag, object key), and the
    handbook's and the policy's object keys."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(text(ONTOLOGY_TABLE_VIEWS["_domains"]))
        for name in ("derived_tags", "effective_column_tags", "tag_instances"):
            await conn.execute_core(text(_META_TABLE_VIEWS[name]))
        await conn.execute_core(text(_OPS_REPORT_VIEWS["tags_report"]))
        handbook, policy = await _model(conn)
        for tag in (APPROVED, SENSITIVITY, CHECKED, REVIEWED):
            await tag_repo.upsert(conn, tag)
        await tag_repo.assign(conn, _on_table("approved", 1))
        await tag_repo.assign(conn, _on_table("approved", 2))
        await tag_repo.assign(conn, _on_column("sensitivity:high", "email"))
        await tag_repo.assign(
            conn, _on_column("deprecated", "total", reason="replaced", expires_on="2020-01-01")
        )
        await tag_repo.assign(conn, _on_column("checked", "email"))
        for assignment in REVIEWED_ON:
            await tag_repo.assign(conn, assignment)
        for document_id in (handbook, policy):
            await tag_repo.assign(
                conn,
                TagAssignment(tag_id="reviewed", object_type="document", document_id=document_id),
            )
        await tag_repo.cite_assignment(
            conn, _on_table("approved", 1), Place(policy, RETENTION), "steward"
        )
        await tag_repo.cite_assignment(
            conn, _on_column("checked", "email"), Place(policy, PERSONAL), "steward"
        )
        # A new version of the policy revises the Personal data section.
        await store.replace_content(
            conn,
            policy,
            media_type="text/markdown",
            content_sha256="b" * 64,
            embedding_model="m",
            passages=_passages("Personal data is anything that identifies a person."),
            source=None,
            refreshed=True,
        )
        rows = (await conn.execute_core(text("SELECT * FROM tags_report"))).mappings().all()
    yield (
        db,
        {(r["tag_id"], r["object_key"]): dict(r) for r in rows},
        (f"document:{handbook}", f"document:{policy}"),
    )
    await db.close()


def test_the_tags_report_is_a_meta_table_and_a_standard_report():
    assert "tag_instances" in _META_TABLES
    # A standard report is an ops-domain relation; the Reports list is the ops domain's tables.
    assert "tags_report" in _OPS_REPORT_VIEWS
    assert TAG_ROW_VIEWS == {(META_DOMAIN_ID, "tag_instances"), (OPS_DOMAIN_ID, "tags_report")}
    # Read as the tag admin reads assignments, not by the catalog's table scope.
    assert "tag_instances" not in META_ROW_SCOPED_VIEWS


@pytest.mark.asyncio
async def test_one_row_per_tag_on_an_object(report):
    rows = report[1]
    assert {key: r["via"] for key, r in rows.items() if key[0] != "reviewed"} == {
        ("approved", "table:1"): "assigned",
        ("approved", "table:2"): "assigned",
        ("checked", "column:1:email"): "assigned",
        ("deprecated", "column:1:total"): "assigned",
        ("sensitivity:high", "column:1:email"): "assigned",
    }
    assert len([k for k in rows if k[0] == "reviewed"]) == len(REVIEWED_ON) + 2


@pytest.mark.asyncio
async def test_each_row_names_its_object_and_tag(report):
    rows = report[1]
    approved = rows[("approved", "table:1")]
    assert (approved["object_kind"], approved["object_id"], approved["object_name"]) == (
        "table",
        "1",
        "pg.public.orders",
    )
    assert (approved["domain_id"], approved["table_id"], approved["tag_requires_citation"]) == (
        "sales",
        1,
        True,
    )
    sensitivity = rows[("sensitivity:high", "column:1:email")]
    assert (sensitivity["base_tag_id"], sensitivity["parameter"]) == ("sensitivity", "high")
    assert (sensitivity["domain_id"], sensitivity["table_id"]) == ("sales", 1)
    expired = rows[("deprecated", "column:1:total")]
    assert (expired["reason"], expired["expires_on"], expired["expired"]) == (
        "replaced",
        "2020-01-01",
        True,
    )
    # The names table names no role: its name is left empty.
    assert rows[("reviewed", "role:teller")]["object_name"] is None


@pytest.mark.asyncio
async def test_the_citation_measures_are_computed_when_read(report):
    rows = report[1]
    measures = {
        key: (r["citations"], r["citations_broken"], r["citations_drifted"], r["citation_missing"])
        for key, r in rows.items()
    }
    assert measures[("approved", "table:1")] == (1, 0, 0, False)
    assert measures[("approved", "table:2")] == (0, 0, 0, True)
    assert measures[("checked", "column:1:email")] == (1, 0, 1, False)


@pytest.mark.asyncio
async def test_each_row_rolls_up_to_a_status_with_its_reason(report):
    rows = report[1]
    statuses = {key: (r["status"], r["status_reason"]) for key, r in rows.items()}
    assert {k: v for k, v in statuses.items() if k[0] != "reviewed"} == {
        ("approved", "table:1"): ("clean", None),
        ("approved", "table:2"): ("needs attention", "required citation missing"),
        ("checked", "column:1:email"): (
            "needs attention",
            "a cited section changed since it was cited",
        ),
        ("deprecated", "column:1:total"): ("needs attention", "the assignment expired"),
        ("sensitivity:high", "column:1:email"): ("clean", None),
    }


async def _read(db, role: dict, readable_in: frozenset[str] | None) -> set[str]:
    """The object keys of the reviewed tag's rows ``role`` reads."""
    predicate = tag_row_predicate(role, readable_in).sql(dialect="postgres")
    async with db.acquire() as conn:
        read = (
            await conn.execute_core(
                text(
                    f"SELECT object_key FROM tags_report WHERE tag_id = 'reviewed' AND {predicate}"
                )
            )
        ).fetchall()
    return {r[0] for r in read}


def _role(role_id: str, *capabilities: str) -> dict:
    return {"id": role_id, "capabilities": list(capabilities)}


def _sales_scoped(policy: str) -> set[str]:
    return {
        "source:pg",  # no allowed domains: every domain reads it; crm is hr's
        "relationship:r_sales",  # both its tables in sales; r_cross reaches hr
        "command:f_sales",
        "product:p_sales",
        "term:1",
        policy,  # the whole org's; the handbook is hr's
        "role:clerk",  # the reader's own role
        "metric:m_analyst",  # granted to the reader
    }


@pytest.mark.asyncio
async def test_a_reader_in_one_domain_reads_the_tags_of_its_objects_only(report):
    db, _rows, (_handbook, policy) = report
    assert await _read(db, _role("clerk"), frozenset({"sales"})) == _sales_scoped(policy)


@pytest.mark.asyncio
async def test_a_row_rule_tag_is_read_with_access_config_within_the_rules_domains(report):
    db, _rows, (_handbook, policy) = report
    read = await _read(db, _role("clerk", "access_config"), frozenset({"sales"}))
    assert read - _sales_scoped(policy) == {"rls_rule:1"}
    whole = await _read(db, _role("clerk", "access_config"), None)
    assert {k for k in whole if k.startswith("rls_rule:")} == {"rls_rule:1", "rls_rule:2"}


@pytest.mark.asyncio
async def test_a_role_tag_is_read_by_the_role_or_with_user_management(report):
    db = report[0]
    own = await _read(db, _role("teller"), None)
    assert {k for k in own if k.startswith("role:")} == {"role:teller"}
    managed = await _read(db, _role("clerk", "user_management"), None)
    assert {k for k in managed if k.startswith("role:")} == {"role:teller", "role:clerk"}


@pytest.mark.asyncio
async def test_a_metric_tag_is_read_by_a_role_the_metric_is_granted_to(report):
    db = report[0]
    read = await _read(db, _role("teller"), None)
    assert {k for k in read if k.startswith("metric:")} == {"metric:m_sales"}


@pytest.mark.asyncio
async def test_a_whole_catalog_reader_without_the_rights_reads_no_rule_role_or_metric_tag(report):
    db, _rows, (handbook, policy) = report
    assert await _read(db, _role("auditor"), None) == {
        "source:pg",
        "source:crm",
        "relationship:r_sales",
        "relationship:r_cross",
        "command:f_sales",
        "command:f_hr",
        "product:p_sales",
        "product:p_hr",
        "term:1",
        "term:2",
        handbook,
        policy,
    }


@pytest.mark.asyncio
async def test_a_table_or_column_tag_is_read_in_the_tables_domain(report):
    db = report[0]
    predicate = tag_row_predicate(_role("clerk"), frozenset({"sales"})).sql(dialect="postgres")
    async with db.acquire() as conn:
        read = (
            await conn.execute_core(
                text(f"SELECT tag_id, object_key FROM tags_report WHERE {predicate}")
            )
        ).fetchall()
    assert {tuple(r) for r in read if r[0] != "reviewed"} == {
        ("approved", "table:1"),
        ("checked", "column:1:email"),
        ("deprecated", "column:1:total"),
        ("sensitivity:high", "column:1:email"),
    }


def test_the_compiler_applies_the_predicate_to_both_relations():
    ctx = CompilationContext()
    for tid, name, domain, field in (
        (100, "tag_instances", META_DOMAIN_ID, "tag_instances"),
        # An ops table's field carries its schema's prefix (naming.SYSTEM_SCHEMA_GQL_PREFIX).
        (101, "tags_report", OPS_DOMAIN_ID, "op_tags_report"),
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
    expected = tag_row_predicate(role, frozenset({"sales"}))
    assert gov.rls_rules[100] == expected
    assert gov.rls_rules[101] == expected
