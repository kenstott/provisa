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

_domains_ontology restates provisa.core.repositories.glossary.term_domains (a term's
refs' domains while it holds refs, its declared domains otherwise) and _terms_ontology.grounding
restates provisa.core.glossary.term_grounding (grounded, grounded_pending or not_grounded, looking
down a term's paths to data). Both run on the real control plane and are checked against the
Python rule they restate.
"""

# Requirements: REQ-1591, REQ-1387

from __future__ import annotations

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS

# Terms: ROOTED holds a ref (and a stale declared domain); ABSTRACT declares the enterprise scope;
# BARE has neither; UNDEFINED has no definition but an edge to ROOTED; RETIRED is out of service;
# TOP is approved and reaches data only through draft UNDEFINED. Down follows each edge type's
# direction (REQ-1387, DOWN IS FROM BROADER TO NARROWER): ROOTED is a kind of UNDEFINED, a kind of
# TOP; SIBLING is another kind of UNDEFINED, holding no data; SYNONYM is a synonym of TOP; RELATED is
# related to ROOTED; PART is a part of ROOTED.
ROOTED, ABSTRACT, BARE, UNDEFINED, RETIRED, TOP = 1, 2, 3, 4, 5, 6
SIBLING, SYNONYM, RELATED, PART = 8, 9, 10, 11


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
        dict(id=TOP, name="Holder", definition="Who holds an account", approved=True),
        dict(id=SIBLING, name="Supplier", definition="Who sells to us", approved=True),
        dict(id=SYNONYM, name="Owner", definition="Who holds an account", approved=True),
        dict(id=RELATED, name="Order", definition="What a buyer places", approved=True),
        dict(id=PART, name="Email", definition="A buyer's address", approved=True),
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
    for a, b, rel_type in (
        (ROOTED, UNDEFINED, "KIND_OF"),
        (ROOTED, RETIRED, "KIND_OF"),
        (UNDEFINED, TOP, "KIND_OF"),
        (SIBLING, UNDEFINED, "KIND_OF"),
        (SYNONYM, TOP, "SYNONYM_OF"),
        (RELATED, ROOTED, "RELATED_TO"),
        (ROOTED, RELATED, "RELATED_TO"),
        (PART, ROOTED, "PART_OF"),
    ):
        await conn.execute_core(
            insert(glossary_term_edges).values(from_term_id=a, to_term_id=b, rel_type=rel_type)
        )


@pytest.mark.asyncio
async def test_domains_and_groundedness_restate_the_python_rules(control_plane):
    from sqlalchemy import select, text

    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.glossary import term_grounding
    from provisa.core.repositories.glossary import term_domains
    from provisa.core.schema_org import glossary_term_edges, glossary_terms

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            for name in ("_domains", "_terms"):
                await conn.execute(_adapt_view_ddl(ONTOLOGY_TABLE_VIEWS[name]))
            view_domains: dict[int, set[str]] = {}
            for r in (
                await conn.execute_core(text("SELECT term_id, domain_id FROM _domains_ontology"))
            ).fetchall():
                view_domains.setdefault(r.term_id, set()).add(r.domain_id)
            flags = {
                r.id: r.grounding
                for r in (
                    await conn.execute_core(text("SELECT id, grounding FROM _terms_ontology"))
                ).fetchall()
            }
            python_domains = await term_domains(conn)
            terms = [
                dict(r._mapping)
                for r in (await conn.execute_core(select(glossary_terms))).fetchall()
            ]
            edges = [
                (r.from_term_id, r.to_term_id, r.rel_type)
                for r in (await conn.execute_core(select(glossary_term_edges))).fetchall()
            ]
            python_grounding = term_grounding(terms, edges, {ROOTED})
    finally:
        await db.close()

    # The refs' domain wins over the stale declared one; the abstract term keeps its declared *.
    assert view_domains == {ROOTED: {"sales"}, ABSTRACT: {"*"}} == python_domains

    # The draft next to data is grounded; the approved term above it is pending; the retired one
    # conducts nothing.
    assert flags == python_grounding
    assert flags[UNDEFINED] == "grounded"
    assert flags[TOP] == "grounded_pending"
    assert flags[RETIRED] == "not_grounded"
    assert flags[BARE] == "not_grounded"
    assert flags[SIBLING] == "not_grounded"  # never up to UNDEFINED and back down
    assert flags[SYNONYM] == "grounded_pending"  # through TOP, then draft UNDEFINED
    assert flags[RELATED] == "not_grounded"
    assert flags[PART] == "not_grounded"  # a part of data is not grounded by it


def test_a_term_reaches_its_domains_and_back():
    edges = {r[0]: (r[2], r[3], r[4], r[7]) for r in META_RELATIONSHIPS}
    assert edges["ontology:_terms:domains"] == ("_terms", "id", "term_id", "_domains")
    assert edges["ontology:_domains:term"] == ("_domains", "term_id", "id", "_terms")


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
    from provisa.security.rights import GLOSSARY_SCOPED_VIEWS

    def where(view: str) -> str:
        return " AND ".join(
            _readable_term_predicate(col, frozenset({"sales"}))
            for col in GLOSSARY_SCOPED_VIEWS[view]
        )

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed_scoped(conn)
            for name in ("_domains", "_terms", "_term_refs", "_term_edges"):
                await conn.execute(_adapt_view_ddl(ONTOLOGY_TABLE_VIEWS[name]))

            async def ids(sql: str) -> set:
                return {tuple(r) for r in (await conn.execute_core(text(sql))).fetchall()}

            terms = await ids(f"SELECT id FROM _terms_ontology WHERE {where('_terms_ontology')}")
            refs = await ids(
                f"SELECT term_id FROM _term_refs_ontology WHERE {where('_term_refs_ontology')}"
            )
            edges = await ids(
                "SELECT from_term_id, to_term_id FROM _term_edges_ontology "
                f"WHERE {where('_term_edges_ontology')}"
            )
            scopes = await ids(
                "SELECT term_id, domain_id FROM _domains_ontology "
                f"WHERE {where('_domains_ontology')}"
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
    from provisa.security.rights import GLOSSARY_SCOPED_VIEWS, ONTOLOGY_DOMAIN_ID

    ctx = CompilationContext()
    for i, view in enumerate(GLOSSARY_SCOPED_VIEWS, start=400):
        ctx.tables[view] = TableMeta(
            table_id=i,
            field_name=f"on_{view}",  # REQ-2163: the ontology schema's SDL prefix
            type_name=f"ON_{view}",
            source_id="provisa-admin",
            catalog_name="main",
            schema_name="public",
            table_name=view,
            domain_id=ONTOLOGY_DOMAIN_ID,
        )

    def rules(domain_access: list[str]) -> dict:
        role = {"id": "r", "capabilities": [], "domain_access": domain_access}
        return build_governance_context(
            "r", SimpleNamespace(rules={}, write_rules={}), {}, ctx, [], role, relationships=[]
        ).rls_rules

    ids = {view: i for i, view in enumerate(GLOSSARY_SCOPED_VIEWS, start=400)}
    narrowed = rules([ONTOLOGY_DOMAIN_ID, "sales"])
    assert set(narrowed) == set(ids.values())
    assert "POSITION('|sales|' IN term_domains) > 0" in narrowed[400]
    assert "POSITION('|*|' IN term_domains) > 0" in narrowed[400]
    edges = narrowed[ids["_term_edges_ontology"]]
    assert "from_term_domains" in edges and "to_term_domains" in edges
    assert rules(["*"]) == {}
