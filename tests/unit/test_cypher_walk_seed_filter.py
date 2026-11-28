# Copyright (c) 2026 Kenneth Stott
# Canary: 6c125af9-3f24-45c9-a8b1-abb4f4cd0874
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2174, REQ-348: a variable-length walk starts only from the rows its statement keeps.

The ontology graph's All hops opens as one Cypher variable-length query from the terms its root
read (provisa-ui graph3d/ontologyCypher.ts allHopsCypher). Its walk is typed to the term links,
its ends labelled as terms and bounded by the depth the view walked; and the statement's
conditions on the walk's start (``{id: 7}``, ``WHERE t.id IN [...]``) narrow the rows the
recursive walk starts from, so it reads the term links from the roots alone rather than walking
from every term and keeping the roots' rows afterwards.
"""

# Requirements: REQ-2174, REQ-348

import re

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, JunctionMapping, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql

_LINKS = ("KIND_OF", "PART_OF", "RELATED_TO")


def _terms() -> NodeMapping:
    return NodeMapping(
        label="Terms",
        type_name="Terms",
        domain_label=None,
        table_label="Terms",
        table_id=1,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="ontology",
        table_name="_terms",
        properties={"id": "id", "name": "name", "status": "status"},
    )


def _columns() -> NodeMapping:
    """Another domain's label the terms reach: a walk typed to the term links never reads it."""
    return NodeMapping(
        label="TableColumns",
        type_name="TableColumns",
        domain_label=None,
        table_label="TableColumns",
        table_id=2,
        source_id="provisa-admin",
        id_column="column_key",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="meta",
        table_name="table_columns",
        properties={"column_key": "column_key"},
    )


def _link(rel_type: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label="Terms",
        target_label="Terms",
        join_source_column="id",
        join_target_column="id",
        field_name="edges",
        via=JunctionMapping(
            catalog_name="provisa_admin",
            schema_name="ontology",
            table_name="_term_edges",
            type_name="TermEdges",
            source_columns=("from_term_id",),
            target_columns=("to_term_id",),
            type_column="rel_type",
            type_value=rel_type,
            attributes={},
            label_source="column",
        ),
    )


def _label_map() -> CypherLabelMap:
    rels = {t: _link(t) for t in _LINKS}
    refs = RelationshipMapping(
        rel_type="REFERENCED_BY",
        source_label="Terms",
        target_label="TableColumns",
        join_source_column="id",
        join_target_column="column_key",
        field_name="columns",
        via=JunctionMapping(
            catalog_name="provisa_admin",
            schema_name="ontology",
            table_name="_term_refs",
            type_name="TermRefs",
            source_columns=("term_id",),
            target_columns=("column_key",),
            type_column=None,
            type_value=None,
            attributes={},
            label_source="fixed",
        ),
    )
    rels["REFERENCED_BY"] = refs
    return CypherLabelMap(
        nodes={"Terms": _terms(), "TableColumns": _columns()},
        relationships=rels,
        aliases={k: [v] for k, v in rels.items()},
    )


def _all_hops(roots: list[int], depth: int) -> str:
    """The All hops query as ontologyCypher.allHopsCypher writes it."""
    typed = "|".join(_LINKS)
    start, where = (
        (f"(t:Terms {{id: {roots[0]}}})", "")
        if len(roots) == 1
        else ("(t:Terms)", f" WHERE t.id IN [{', '.join(map(str, roots))}]")
    )
    return (
        f"MATCH {start}-[:{typed}*0..{depth}]-(a:Terms){where} "
        f"OPTIONAL MATCH (a)-[r:{typed}]->(b:Terms) RETURN DISTINCT a, r, b"
    )


def _sql(query: str, dialect: str = "duckdb") -> str:
    ast, _, _ = cypher_to_sql(parse_cypher(query), _label_map(), {})
    return ast.sql(dialect=dialect)


def _walk(sql: str) -> str:
    """The recursive walk's own body: the CTE's text up to the statement it serves."""
    m = re.search(r"WITH RECURSIVE (_traverse_\w+) AS \((.*?)\) SELECT", sql, re.S)
    assert m is not None, sql
    return m.group(2)


@pytest.mark.parametrize(
    ("roots", "seed"),
    [([7], '_seed."id" = 7'), ([15, 16], '_seed."id" IN (15, 16)')],
)
def test_the_walk_starts_from_the_roots_alone(roots: list[int], seed: str) -> None:
    walk = _walk(_sql(_all_hops(roots, 4)))
    # Every first step starts from the roots: one per link type and way, and the zero-length seed.
    starts = walk.split(" UNION ALL ")
    seeded = [s for s in starts if "_seed" in s]
    assert seeded, walk
    assert all(seed in s for s in seeded), walk


def test_the_walk_is_bounded_to_the_term_links() -> None:
    sql = _sql(_all_hops([7], 4))
    walk = _walk(sql)
    # Only the terms and their links: no other label's table, no other relationship.
    tables = set(re.findall(r'"provisa_admin"\."(\w+)"\."(\w+)"', sql))
    assert tables == {("ontology", "_terms"), ("ontology", "_term_edges")}
    assert set(re.findall(r"\"rel_type\" = '(\w+)'", walk)) == set(_LINKS)
    # The walk goes no further than the depth the view walked.
    assert "hops < 4" in walk
    assert "table_columns" not in sql and "_term_refs" not in sql


def test_a_condition_on_another_variable_is_not_pushed_into_the_walk() -> None:
    walk = _walk(
        _sql(
            "MATCH (t:Terms)-[:KIND_OF*1..3]-(a:Terms) "
            "WHERE t.id IN [1, 2] AND a.name = 'x' AND t.name = a.name RETURN DISTINCT a"
        )
    )
    assert '_seed."id" IN (1, 2)' in walk
    assert "'x'" not in walk
    assert re.search(r"\ba\.", walk) is None
    assert "name" not in walk


def test_an_or_across_variables_is_not_pushed() -> None:
    walk = _walk(
        _sql("MATCH (t:Terms)-[:KIND_OF*1..3]-(a:Terms) WHERE t.id = 1 OR a.id = 2 RETURN a")
    )
    assert '_seed."id" = 1' not in walk


def _world(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.ontology")
    con.execute("CREATE TABLE provisa_admin.ontology._terms(id INT, name TEXT, status TEXT)")
    con.execute(
        "CREATE TABLE provisa_admin.ontology._term_edges"
        "(id INT, from_term_id INT, to_term_id INT, rel_type TEXT)"
    )
    # 1..6 connected (2 and 3 linked both ways round a cycle 1-2-3), 6 three steps from 1;
    # 7 and 8 apart, linked to each other.
    con.executemany(
        "INSERT INTO provisa_admin.ontology._terms VALUES (?, ?, 'approved')",
        [(i, f"t{i}") for i in range(1, 9)],
    )
    con.executemany(
        "INSERT INTO provisa_admin.ontology._term_edges VALUES (?, ?, ?, ?)",
        [
            (1, 2, 1, "KIND_OF"),
            (2, 3, 1, "KIND_OF"),
            (3, 2, 3, "RELATED_TO"),
            (4, 4, 3, "PART_OF"),
            (5, 5, 4, "KIND_OF"),
            (6, 6, 5, "PART_OF"),
            (7, 7, 8, "RELATED_TO"),
        ],
    )


def test_it_answers_the_connected_part_and_its_links() -> None:
    con = duckdb.connect()
    _world(con)

    def answer(depth: int) -> tuple[set[int], set[tuple[int, int]]]:
        rel = con.execute(_sql(_all_hops([1], depth)))
        cols = [d[0] for d in rel.description]
        rows = rel.fetchall()
        a, r = cols.index("id"), cols.index("r")
        links = {
            (link["start"], link["end"])
            for link in (__import__("json").loads(row[r]) for row in rows if row[r] is not None)
        }
        return {row[a] for row in rows}, links

    # 6 is four steps from 1 (1-3-4-5-6): the view walks four hops to the end, and at that depth
    # the query answers every term it drew and every link between them, the cycle's each once.
    terms, links = answer(4)
    assert terms == {1, 2, 3, 4, 5, 6}
    assert links == {(2, 1), (3, 1), (2, 3), (4, 3), (5, 4), (6, 5)}
    # 7 and 8, linked only to each other, are never reached; three steps stop short of 6.
    assert answer(3)[0] == {1, 2, 3, 4, 5}
