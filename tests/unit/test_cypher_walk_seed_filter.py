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
    """The view's query as ontologyCypher.allHopsCypher writes it, for every Hops value."""
    typed = "|".join(_LINKS)
    links = f"OPTIONAL MATCH (a)-[r:{typed}]->(b:Terms) RETURN DISTINCT a, r, b"
    among = f"[{', '.join(map(str, roots))}]"
    if depth == 0:
        start = (
            f"MATCH (a:Terms {{id: {roots[0]}}})"
            if len(roots) == 1
            else f"MATCH (a:Terms) WHERE a.id IN {among}"
        )
        return f"{start} {links}"
    start, where = (
        (f"(t:Terms {{id: {roots[0]}}})", "")
        if len(roots) == 1
        else ("(t:Terms)", f" WHERE t.id IN {among}")
    )
    return f"MATCH {start}-[:{typed}*0..{depth}]-(a:Terms){where} {links}"


def _drawn(terms: list[int]) -> str:
    """The terms a focus draws as ontologyCypher.drawnCypher writes them."""
    typed = "|".join(_LINKS)
    among = f"[{', '.join(map(str, terms))}]"
    return (
        f"MATCH (a:Terms) WHERE a.id IN {among} "
        f"OPTIONAL MATCH (a)-[r:{typed}]->(b:Terms) WHERE b.id IN {among} RETURN a, r, b"
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


# The query as the Cypher endpoints run it (bolt/session.py, api/flight/server.py): the
# translation, its graph values built (apply_graph_rewrites), the semantic SQL written for
# PostgreSQL, then transpiled for the engine or the source that runs it.


def _semantic(query: str) -> str:
    from provisa.gql.graph_rewriter import apply_graph_rewrites

    label_map = _label_map()
    ast, _, graph_vars = cypher_to_sql(parse_cypher(query), label_map, {})
    return apply_graph_rewrites(ast, graph_vars, label_map).sql(dialect="postgres")


def _physical(query: str, target: str) -> str:
    from provisa.transpiler.transpile import (
        direct_physical,
        rewrite_json_object_to_build_object,
        transpile,
    )

    semantic = _semantic(query)
    if target == "postgres":
        # The pg engine's form (PgBackend.transpile_physical) and a read sent DIRECT to a
        # PostgreSQL source (direct_physical) are one.
        engine = rewrite_json_object_to_build_object(
            transpile(semantic, "postgres", generated=True)
        )
        assert (
            direct_physical(transpile(semantic, "postgres", generated=True), "postgres") == engine
        )
        return engine
    return transpile(semantic, target, generated=True)


def _distinct_selects(sql: str, dialect: str) -> list:
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(sql, read=dialect)
    return [s for s in tree.find_all(exp.Select) if s.args.get("distinct")]


_JSON_BUILDERS = re.compile(
    r"\b(json_object|json_build_object|jsonb_build_object|tojson)\s*\(", re.I
)


@pytest.mark.parametrize("depth", [1, 2, 4])
@pytest.mark.parametrize("roots", [[7], [15, 16]])
def test_postgres_runs_it_as_postgresql_spells_it(roots: list[int], depth: int) -> None:
    # PostgreSQL 16 has no variadic json_object(k, v, ...): the objects are json_build_object,
    # and the DISTINCT compares the columns they are built from, never json, which PostgreSQL
    # cannot compare ("could not identify an equality operator for type json").
    sql = _physical(_all_hops(roots, depth), "postgres")
    assert re.search(r"\bjson_object\s*\(", sql, re.I) is None, sql
    assert "JSON_BUILD_OBJECT(" in sql.upper()
    distinct = _distinct_selects(sql, "postgres")
    assert len(distinct) == 1, sql
    assert _JSON_BUILDERS.search(distinct[0].sql(dialect="postgres")) is None


@pytest.mark.parametrize("target", ["duckdb", "trino", "clickhouse"])
def test_each_engine_compares_columns_not_json(target: str) -> None:
    sql = _physical(_all_hops([7], 3), target)
    distinct = _distinct_selects(sql, target)
    assert len(distinct) == 1, sql
    projected = " ".join(e.sql(dialect=target) for e in distinct[0].expressions)
    assert _JSON_BUILDERS.search(projected) is None, projected
    # The walk is still seeded by the root alone, and bounded.
    assert "hops < 3" in sql


def test_duckdb_answers_it_as_the_endpoints_run_it() -> None:
    import json

    con = duckdb.connect()
    _world(con)

    def answer(depth: int) -> tuple[list[tuple[int, str | None]], set[tuple[int, int]]]:
        rel = con.execute(_physical(_all_hops([1], depth), "duckdb"))
        cols = [d[0] for d in rel.description]
        rows = [dict(zip(cols, row)) for row in rel.fetchall()]
        keys = [
            (
                json.loads(row["a"])["properties"]["id"],
                row["r"] and json.loads(row["r"])["identity"],
            )
            for row in rows
        ]
        links = {
            (link["start"], link["end"])
            for link in (json.loads(row["r"]) for row in rows if row["r"] is not None)
        }
        return keys, links

    keys, links = answer(4)
    # Each term and each link from it once, though the cycle 1-2-3 reaches 2 and 3 twice.
    assert len(keys) == len(set(keys))
    assert {k[0] for k in keys} == {1, 2, 3, 4, 5, 6}
    assert links == {(2, 1), (3, 1), (2, 3), (4, 3), (5, 4), (6, 5)}
    assert {k[0] for k in answer(3)[0]} == {1, 2, 3, 4, 5}


def _answer(con: duckdb.DuckDBPyConnection, query: str) -> tuple[list, set[tuple[int, int]]]:
    import json

    rel = con.execute(_physical(query, "duckdb"))
    cols = [d[0] for d in rel.description]
    rows = [dict(zip(cols, row)) for row in rel.fetchall()]
    keys = [
        (json.loads(row["a"])["properties"]["id"], row["r"] and json.loads(row["r"])["identity"])
        for row in rows
    ]
    links = {
        (link["start"], link["end"])
        for link in (json.loads(row["r"]) for row in rows if row["r"] is not None)
    }
    return keys, links


@pytest.mark.parametrize(
    ("roots", "depth", "terms", "links"),
    [
        # No search and no focus: every term the filters keep, at no hop, and the links leaving it.
        (
            [1, 2, 3, 4, 5, 6, 7, 8],
            0,
            {1, 2, 3, 4, 5, 6, 7, 8},
            {(2, 1), (3, 1), (2, 3), (4, 3), (5, 4), (6, 5), (7, 8)},
        ),
        ([3], 0, {3}, {(3, 1)}),
        ([3], 1, {1, 2, 3, 4}, {(2, 1), (3, 1), (2, 3), (4, 3)}),
        ([3], 2, {1, 2, 3, 4, 5}, {(2, 1), (3, 1), (2, 3), (4, 3), (5, 4)}),
        ([6, 7], 1, {5, 6, 7, 8}, {(5, 4), (6, 5), (7, 8)}),
    ],
)
def test_every_hops_value_answers_as_it_opens(
    roots: list[int], depth: int, terms: set[int], links: set[tuple[int, int]]
) -> None:
    con = duckdb.connect()
    _world(con)
    keys, got = _answer(con, _all_hops(roots, depth))
    assert len(keys) == len(set(keys))
    assert {k[0] for k in keys} == terms
    assert got == links


def test_a_focus_opens_as_the_terms_it_draws() -> None:
    con = duckdb.connect()
    _world(con)
    keys, links = _answer(con, _drawn([1, 2, 4]))
    assert len(keys) == len(set(keys))
    assert {k[0] for k in keys} == {1, 2, 4}
    # Only the links between the terms drawn: 4's link to 3 is not.
    assert links == {(2, 1)}


@pytest.mark.parametrize("query", [_all_hops([3], 0), _drawn([1, 2, 4])])
def test_the_other_shapes_run_on_postgres_as_it_spells_them(query: str) -> None:
    sql = _physical(query, "postgres")
    assert re.search(r"\bjson_object\s*\(", sql, re.I) is None, sql
    for distinct in _distinct_selects(sql, "postgres"):
        assert _JSON_BUILDERS.search(distinct.sql(dialect="postgres")) is None
