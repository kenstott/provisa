# Copyright (c) 2026 Kenneth Stott
# Canary: 33b228c1-f6e5-420e-9db8-097b0ba0b886
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2174, REQ-348: the ontology graph's All hops query answers in seconds on real PG.

All hops opens as one Cypher variable-length query from the terms the view's root read
(provisa-ui graph3d/ontologyCypher.ts allHopsCypher): typed to the term links, its ends terms,
bounded by the depth the view walked, the walk starting only from the roots. Here a glossary of a
few hundred terms, a KIND_OF tree crossed by RELATED_TO links that close many cycles, is written to
the control plane's own tables; the query is compiled as the Cypher endpoints compile it (the
translator, its graph values, the semantic SQL transpiled for PostgreSQL as a read sent DIRECT to
it) and run on PostgreSQL. It answers the connected part -- each term once, and the links between them -- as a
breadth-first walk finds it, within the time the view allows.
"""

# Requirements: REQ-2174, REQ-348

import collections
import random
import time
from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.glossary import TERM_EDGE_TYPES
from provisa.core.repositories import glossary as glossary_repo
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, JunctionMapping, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import direct_physical, transpile

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()

_TERMS = 300
_CROSS_LINKS = 450
#: The view's own budget for answering: the report was a 60 s timeout.
_SECONDS = 5.0


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(tenant_db, _init_schema):
    async with tenant_db.acquire() as conn:
        await conn.execute(
            "TRUNCATE glossary_term_experts, glossary_term_edges, glossary_term_refs, "
            "glossary_terms CASCADE"
        )
    yield


def _label_map(catalog: str, schema: str) -> CypherLabelMap:
    terms = NodeMapping(
        label="Terms",
        type_name="Terms",
        domain_label=None,
        table_label="Terms",
        table_id=1,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name=catalog,
        schema_name=schema,
        table_name="glossary_terms",
        properties={"id": "id", "name": "name"},
    )
    rels = {
        t: RelationshipMapping(
            rel_type=t,
            source_label="Terms",
            target_label="Terms",
            join_source_column="id",
            join_target_column="id",
            field_name="edges",
            via=JunctionMapping(
                catalog_name=catalog,
                schema_name=schema,
                table_name="glossary_term_edges",
                type_name="TermEdges",
                source_columns=("from_term_id",),
                target_columns=("to_term_id",),
                type_column="rel_type",
                type_value=t,
                attributes={},
                label_source="column",
            ),
        )
        for t in TERM_EDGE_TYPES
    }
    return CypherLabelMap(
        nodes={"Terms": terms}, relationships=rels, aliases={k: [v] for k, v in rels.items()}
    )


def _all_hops(root: int, depth: int) -> str:
    """The All hops query as ontologyCypher.allHopsCypher writes it, every link type served."""
    typed = "|".join(sorted(TERM_EDGE_TYPES))
    return (
        f"MATCH (t:Terms {{id: {root}}})-[:{typed}*0..{depth}]-(a:Terms) "
        f"OPTIONAL MATCH (a)-[r:{typed}]->(b:Terms) RETURN DISTINCT a, r, b"
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_all_hops_answers_the_connected_part_in_seconds(tenant_db):
    rng = random.Random(2174)
    async with tenant_db.acquire() as conn:
        ids = [
            await glossary_repo.create_abstract_term(conn, f"term {i}", domains=set())
            for i in range(_TERMS)
        ]
        links: set[tuple[int, int, str]] = set()
        for i in range(1, _TERMS):
            links.add((ids[i], ids[rng.randrange(i)], "KIND_OF"))
        while len(links) < _TERMS - 1 + _CROSS_LINKS:
            a, b = rng.sample(ids, 2)
            links.add((a, b, "RELATED_TO"))
        for a, b, t in links:
            await glossary_repo.add_edge(conn, a, b, t)
        # A term apart, linked to one other: never reached from the root.
        apart = await glossary_repo.create_abstract_term(conn, "apart", domains=set())
        other = await glossary_repo.create_abstract_term(conn, "other", domains=set())
        await glossary_repo.add_edge(conn, apart, other, "RELATED_TO")
        where = (await conn.fetch("SELECT current_database() AS db, current_schema() AS sch"))[0]

        # The view's walk: breadth first from the root to the end of the connected part.
        adjacent: dict[int, set[int]] = collections.defaultdict(set)
        for a, b, _t in links:
            adjacent[a].add(b)
            adjacent[b].add(a)
        root, seen, frontier, depth = ids[0], {ids[0]}, {ids[0]}, 0
        while frontier := {n for f in frontier for n in adjacent[f]} - seen:
            seen |= frontier
            depth += 1

        label_map = _label_map(where["db"], where["sch"])
        ast, _, graph_vars = cypher_to_sql(parse_cypher(_all_hops(root, depth)), label_map, {})
        semantic = apply_graph_rewrites(ast, graph_vars, label_map).sql(dialect="postgres")
        sql = direct_physical(transpile(semantic, "postgres", generated=True), "postgres")
        started = time.monotonic()
        rows = await conn.fetch(sql)
        took = time.monotonic() - started

    assert took < _SECONDS, f"All hops took {took:.1f}s over {_TERMS} terms at depth {depth}"
    # Each row is a term reached (a), a link from it (r), and the link's far end (b), each once.
    assert len(rows) == len({(r["a"]["id"], r["r"] and r["r"]["identity"]) for r in rows})
    assert {r["a"]["properties"]["id"] for r in rows} == seen
    edges = {(r["r"]["start"], r["r"]["end"]) for r in rows if r["r"] is not None}
    assert edges == {(a, b) for a, b, _t in links}
