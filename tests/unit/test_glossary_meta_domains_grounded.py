# Copyright (c) 2026 Kenneth Stott
# Canary: 0a58bc04-c850-474d-b18c-8cbf38dc576c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1591, REQ-1387: a term's domains and its groundedness in the meta domain.

glossary_term_domains_meta restates provisa.core.repositories.glossary.term_domains (a term's
refs' domains while it holds refs, its declared domains otherwise) and glossary_terms_meta.grounded
restates provisa.core.glossary.grounded_term_ids (connected over in-service edges to a term holding
a ref, defined or not). Both run on the real control plane and are checked against the Python rule
they restate.
"""

# Requirements: REQ-1591, REQ-1387

from __future__ import annotations

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS

# Terms: ROOTED holds a ref (and a stale declared domain); ABSTRACT declares the enterprise scope;
# BARE has neither; UNDEFINED has no definition but an edge to ROOTED; RETIRED is out of service.
ROOTED, ABSTRACT, BARE, UNDEFINED, RETIRED = 1, 2, 3, 4, 5


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import (
        domains,
        glossary_term_domains,
        glossary_term_edges,
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        sources,
    )

    for domain in ("sales", "hr"):
        await conn.execute_core(insert(domains).values(id=domain, description=domain))
    await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
    await conn.execute_core(
        insert(registered_tables).values(
            id=7, source_id="crm", domain_id="sales", schema_name="public", table_name="customers"
        )
    )
    terms = [
        dict(id=ROOTED, name="Customer", definition="A buyer"),
        dict(id=ABSTRACT, name="Party", definition="Anyone"),  # a Concept: grounds nothing
        dict(id=BARE, name="Lead", definition="A prospect"),
        dict(id=UNDEFINED, name="Account", definition=""),
        dict(id=RETIRED, name="Client", definition="Old word", retired=True),
    ]
    for t in terms:
        await conn.execute_core(insert(glossary_terms).values(**t))
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=ROOTED, table_id=7, column_name="email")
    )
    for term_id, domain in ((ROOTED, "hr"), (ABSTRACT, "*")):
        await conn.execute_core(
            insert(glossary_term_domains).values(term_id=term_id, domain_id=domain)
        )
    for a, b in ((UNDEFINED, ROOTED), (RETIRED, ROOTED)):
        await conn.execute_core(
            insert(glossary_term_edges).values(from_term_id=a, to_term_id=b, rel_type="RELATED_TO")
        )


@pytest.mark.asyncio
async def test_domains_and_groundedness_restate_the_python_rules(control_plane):
    from sqlalchemy import select, text

    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.glossary import grounded_term_ids
    from provisa.core.repositories.glossary import term_domains
    from provisa.core.schema_org import glossary_term_edges, glossary_terms

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            for name in ("glossary_term_domains", "glossary_terms"):
                await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[name]))
            view_domains: dict[int, set[str]] = {}
            for r in (
                await conn.execute_core(
                    text("SELECT term_id, domain_id FROM glossary_term_domains_meta")
                )
            ).fetchall():
                view_domains.setdefault(r.term_id, set()).add(r.domain_id)
            flags = {
                r.id: (r.grounded, r.live)
                for r in (
                    await conn.execute_core(
                        text("SELECT id, grounded, live FROM glossary_terms_meta")
                    )
                ).fetchall()
            }
            python_domains = await term_domains(conn)
            terms = [
                dict(r._mapping)
                for r in (await conn.execute_core(select(glossary_terms))).fetchall()
            ]
            edges = [
                (r.from_term_id, r.to_term_id)
                for r in (await conn.execute_core(select(glossary_term_edges))).fetchall()
            ]
            python_grounded = grounded_term_ids(terms, edges, {ROOTED})
    finally:
        await db.close()

    # The refs' domain wins over the stale declared one; the abstract term keeps its declared *.
    assert view_domains == {ROOTED: {"sales"}, ABSTRACT: {"*"}} == python_domains

    # The undefined term is grounded but not live; the retired one conducts nothing.
    assert {t for t, (grounded, _) in flags.items() if grounded} == python_grounded
    assert flags[UNDEFINED] == (True, False)
    assert flags[RETIRED] == (False, False)
    assert flags[BARE] == (False, False)


def test_a_term_reaches_its_domains_and_back():
    edges = {r[0]: (r[2], r[3], r[4], r[7]) for r in META_RELATIONSHIPS}
    assert edges["meta:glossary_terms:domains"] == (
        "glossary_terms",
        "id",
        "term_id",
        "glossary_term_domains",
    )
    assert edges["meta:glossary_term_domains:term"] == (
        "glossary_term_domains",
        "term_id",
        "id",
        "glossary_terms",
    )


# REQ-1593: the readable-term scope. SALES binds a sales column, FINANCE a finance one, ENTERPRISE
# is declared "*"; SALES relates to both.
SALES_T, FINANCE_T, ENTERPRISE_T = 11, 12, 13


async def _seed_scoped(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import (
        domains,
        glossary_term_domains,
        glossary_term_edges,
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        sources,
    )

    for domain in ("sales", "finance"):
        await conn.execute_core(insert(domains).values(id=domain, description=domain))
    await conn.execute_core(insert(sources).values(id="erp", type="postgres"))
    for table_id, domain in ((21, "sales"), (22, "finance")):
        await conn.execute_core(
            insert(registered_tables).values(
                id=table_id,
                source_id="erp",
                domain_id=domain,
                schema_name="public",
                table_name=f"{domain}_t",
            )
        )
    for term_id, name in ((SALES_T, "Deal"), (FINANCE_T, "Ledger"), (ENTERPRISE_T, "Customer")):
        await conn.execute_core(
            insert(glossary_terms).values(id=term_id, name=name, definition=name)
        )
    for term_id, table_id in ((SALES_T, 21), (FINANCE_T, 22)):
        await conn.execute_core(
            insert(glossary_term_refs).values(term_id=term_id, table_id=table_id, column_name="c")
        )
    await conn.execute_core(
        insert(glossary_term_domains).values(term_id=ENTERPRISE_T, domain_id="*")
    )
    for b in (FINANCE_T, ENTERPRISE_T):
        await conn.execute_core(
            insert(glossary_term_edges).values(
                from_term_id=SALES_T, to_term_id=b, rel_type="RELATED_TO"
            )
        )


@pytest.mark.asyncio
async def test_a_sales_caller_reads_only_the_terms_the_glossary_lets_it_read(control_plane):
    from sqlalchemy import text

    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.compiler.stage2 import _readable_term_predicate
    from provisa.security.rights import GLOSSARY_SCOPED_META_VIEWS

    def where(view: str) -> str:
        return " AND ".join(
            _readable_term_predicate(col, frozenset({"sales"}))
            for col in GLOSSARY_SCOPED_META_VIEWS[view]
        )

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed_scoped(conn)
            for name in (
                "glossary_term_domains",
                "glossary_terms",
                "glossary_term_refs",
                "glossary_term_edges",
            ):
                await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[name]))

            async def ids(sql: str) -> set:
                return {tuple(r) for r in (await conn.execute_core(text(sql))).fetchall()}

            terms = await ids(
                f"SELECT id FROM glossary_terms_meta WHERE {where('glossary_terms_meta')}"
            )
            refs = await ids(
                f"SELECT term_id FROM glossary_term_refs_meta WHERE {where('glossary_term_refs_meta')}"
            )
            edges = await ids(
                "SELECT from_term_id, to_term_id FROM glossary_term_edges_meta "
                f"WHERE {where('glossary_term_edges_meta')}"
            )
            scopes = await ids(
                "SELECT term_id, domain_id FROM glossary_term_domains_meta "
                f"WHERE {where('glossary_term_domains_meta')}"
            )
    finally:
        await db.close()

    # The finance term, its ref and its edge are not the sales caller's; the enterprise term is.
    assert terms == {(SALES_T,), (ENTERPRISE_T,)}
    assert refs == {(SALES_T,)}
    assert edges == {(SALES_T, ENTERPRISE_T)}
    assert scopes == {(SALES_T, "sales"), (ENTERPRISE_T, "*")}


def test_the_scope_is_injected_for_a_narrowed_role_and_not_for_an_unlimited_one():
    from types import SimpleNamespace

    from provisa.compiler.sql_types import CompilationContext, TableMeta
    from provisa.compiler.stage2 import build_governance_context
    from provisa.security.rights import GLOSSARY_SCOPED_META_VIEWS, META_DOMAIN_ID

    ctx = CompilationContext()
    for i, view in enumerate(GLOSSARY_SCOPED_META_VIEWS, start=400):
        ctx.tables[view] = TableMeta(
            table_id=i,
            field_name=view,
            type_name=view,
            source_id="provisa-admin",
            catalog_name="main",
            schema_name="public",
            table_name=view,
            domain_id=META_DOMAIN_ID,
        )

    def rules(domain_access: list[str]) -> dict:
        role = {"id": "r", "capabilities": [], "domain_access": domain_access}
        return build_governance_context(
            "r", SimpleNamespace(rules={}), {}, ctx, [], role, relationships=[]
        ).rls_rules

    ids = {view: i for i, view in enumerate(GLOSSARY_SCOPED_META_VIEWS, start=400)}
    narrowed = rules([META_DOMAIN_ID, "sales"])
    assert set(narrowed) == set(ids.values())
    assert "POSITION('|sales|' IN term_domains) > 0" in narrowed[400]
    assert "POSITION('|*|' IN term_domains) > 0" in narrowed[400]
    edges = narrowed[ids["glossary_term_edges_meta"]]
    assert "from_term_domains" in edges and "to_term_domains" in edges
    assert rules(["*"]) == {}
