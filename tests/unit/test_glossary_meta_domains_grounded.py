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
        dict(id=ABSTRACT, name="Party", definition="Anyone", is_abstract=True),
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
            for name in ("glossary_terms", "glossary_term_domains"):
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
