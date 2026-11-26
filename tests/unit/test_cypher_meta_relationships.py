# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e51d7-6a2b-4f08-b1e4-8d7a2c5f9e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The meta and ontology relationships, as the server seeds them (meta_relationship_seed_rows),
read through Cypher: each statement compiled as a Cypher read is (cypher_to_sql, then the graph
rewrites) and run on DuckDB over the seeded tables, to the rows the relationships hold.

Junction-backed edges (MEMBER_TABLES, PRODUCT, the term edges) and FK edges alike; a pattern
read from either end, a subpattern (a pattern comprehension, EXISTS) over a self-referential type
read either way, and a free end of a type several tables carry."""

# Requirements: REQ-345, REQ-347, REQ-575, REQ-1586, REQ-1634

from __future__ import annotations

from collections import defaultdict

import duckdb
import pytest
import sqlglot

from provisa.api._meta_seed import meta_relationship_seed_rows
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql

_DOMAIN_ACCESS = ["*", "meta", "ontology", "ops"]


def _columns() -> dict[str, set[str]]:
    """Each seeded table's columns: its key and name, and every column a seeded relationship
    joins on. A junction has only the columns its relationships name."""
    rows = list(meta_relationship_seed_rows())
    junctions = {vt for _s, _t, vt, _c in rows if vt}
    cols: dict[str, set[str]] = defaultdict(set)
    for (_s, st), (_t, tt), vt, c in rows:
        for table, column in ((st, c["source_column"]), (tt, c["target_column"])):
            cols[table].add(column)
            if table not in junctions:
                cols[table] |= {"id", "name"}
        if vt:
            for key in ("via_source_column", "via_target_column", "via_type_column"):
                if c.get(key):
                    cols[vt].add(c[key])
    return cols


def _type(column: str) -> str:
    return "integer" if column == "id" or column.endswith("_id") else "varchar"


@pytest.fixture(scope="module")
def meta():
    cols = _columns()
    ids = {name: i for i, name in enumerate(sorted(cols), 1)}
    tables = [
        {
            "id": ids[name],
            "source_id": "provisa-admin",
            "domain_id": "ontology" if name.startswith("_") else "meta",
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": ["admin"]} for c in sorted(cols[name])],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
        for name in cols
    ]
    rels = [
        {
            **{k: v for k, v in c.items() if v is not None},
            "source_table_id": ids[st],
            "target_table_id": ids[tt],
            **({} if vt is None else {"via_table_id": ids[vt]}),
        }
        for (_s, st), (_t, tt), vt, c in meta_relationship_seed_rows()
    ]
    column_types = {
        ids[name]: [
            ColumnMetadata(column_name=c, data_type=_type(c), is_nullable=True)
            for c in sorted(cols[name])
        ]
        for name in cols
    }
    si = SchemaInput(
        tables=tables,
        relationships=rels,
        column_types=column_types,
        naming_rules=[],
        role={
            "id": "admin",
            "capabilities": ["query_development"],
            "domain_access": _DOMAIN_ACCESS,
        },
        domains=[
            {"id": "meta", "description": "Meta"},
            {"id": "ontology", "description": "Ontology", "graphql_alias": "ont"},
        ],
        domain_prefix=True,
        physical_table_map={("provisa-admin", name): f"{name}_meta" for name in cols},
    )
    ctx = build_context(si)
    # As the server builds it (cypher_exec._build_label_map): with every table and relationship.
    lm = CypherLabelMap.from_schema(
        ctx,
        domain_access=_DOMAIN_ACCESS,
        all_tables=tables,
        all_relationships=rels,
        all_column_types=column_types,
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    for name, columns in cols.items():
        defs = ", ".join(f'"{c}" {_type(c).upper()}' for c in sorted(columns))
        con.execute(f'CREATE TABLE provisa_admin.public."{name}_meta" ({defs})')
    _seed(con)
    return lm, con


def _insert(con, table: str, **values) -> None:
    names = ", ".join(f'"{k}"' for k in values)
    marks = ", ".join("?" for _ in values)
    con.execute(
        f'INSERT INTO provisa_admin.public."{table}_meta" ({names}) VALUES ({marks})',
        list(values.values()),
    )


def _seed(con) -> None:
    # Two sources; tables 10 and 11 of source 1, 12 of source 2; 11 checks 10.
    _insert(con, "sources", id=1, name="pg")
    _insert(con, "sources", id=2, name="duck")
    _insert(con, "registered_tables", id=10, name="orders", source_id=1)
    _insert(con, "registered_tables", id=11, name="orders_check", source_id=1, checked_table_id=10)
    _insert(con, "registered_tables", id=12, name="pets", source_id=2)
    # Product 100 holds tables 10 and 12; product 101 holds none.
    _insert(con, "data_products", id=100, name="sales")
    _insert(con, "data_products", id=101, name="empty")
    _insert(con, "data_product_tables", product_id=100, table_id=10)
    _insert(con, "data_product_tables", product_id=100, table_id=12)
    # Relationship 200 has owner 300; product 100 has owner 400.
    _insert(con, "relationships", id=200, name="orders_pets")
    _insert(con, "relationship_owners", id=300, name="ann", relationship_id=200)
    _insert(con, "data_product_owners", id=400, name="bo", product_id=100)
    # Terms 1 KIND_OF 2; term 3 has no edge.
    for term in (1, 2, 3):
        _insert(con, "_terms", id=term, name=f"t{term}")
    _insert(con, "_term_edges", from_term_id=1, to_term_id=2, rel_type="KIND_OF")


def _sql(lm: CypherLabelMap, cypher: str) -> str:
    ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher, lm), lm, {})
    return apply_graph_rewrites(ast, graph_vars, lm).sql(dialect="postgres")


def _rows(meta, cypher: str) -> list[tuple]:
    lm, con = meta
    sql = sqlglot.transpile(_sql(lm, cypher), read="postgres", write="duckdb")[0]
    return sorted(con.execute(sql).fetchall(), key=repr)


# --- the reported shape: a junction hop, then an FK hop, under OPTIONAL MATCH ------------------

_REPORTED = (
    "MATCH (n:Meta:DataProducts) "
    "OPTIONAL MATCH (n)-[rRegisteredTa:MEMBER_TABLES]-(mRegisteredTa:Meta:RegisteredTables)"
    "-[rSources:SOURCE]-(mSources:Meta:Sources) "
    "RETURN n, rRegisteredTa, mRegisteredTa, rSources, mSources LIMIT 25"
)


def test_the_reported_optional_junction_hop_joins_its_junction(meta):
    lm, _con = meta
    sql = _sql(lm, _REPORTED)
    assert '"data_product_tables_meta" AS rRegisteredTa__via' in sql
    assert 'n."id" = rRegisteredTa__via."product_id"' in sql
    assert 'rRegisteredTa__via."table_id" = mRegisteredTa."id"' in sql


def test_the_reported_optional_junction_hop_reads_each_member_and_its_source(meta):
    rows = _rows(
        meta,
        "MATCH (n:Meta:DataProducts) "
        "OPTIONAL MATCH (n)-[r:MEMBER_TABLES]-(m:Meta:RegisteredTables)-[s:SOURCE]-(src:Meta:Sources) "
        "RETURN n.id, m.id, src.name, r IS NULL, s IS NULL",
    )
    assert rows == sorted(
        [
            (100, 10, "pg", False, False),
            (100, 12, "duck", False, False),
            (101, None, None, True, True),
        ],
        key=repr,
    )


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (a:Meta:DataProducts)-[r:MEMBER_TABLES]->(b:Meta:RegisteredTables) RETURN a.id, b.id",
        "MATCH (a:Meta:DataProducts)-[r:MEMBER_TABLES]-(b:Meta:RegisteredTables) RETURN a.id, b.id",
        "MATCH (b:Meta:RegisteredTables)<-[r:MEMBER_TABLES]-(a:Meta:DataProducts) RETURN a.id, b.id",
        "MATCH (b:Meta:RegisteredTables)-[r:PRODUCT]->(a:Meta:DataProducts) RETURN a.id, b.id",
        "MATCH (a:Meta:DataProducts)-[:MEMBER_TABLES]-(b) RETURN a.id, b.id",
    ],
)
def test_a_junction_edge_reads_its_rows_from_either_end(meta, cypher):
    assert _rows(meta, cypher) == [(100, 10), (100, 12)]


# --- a subpattern over a self-referential type, read either way ----------------------------------


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        # FK: 11 CHECKS 10
        (
            "MATCH (a:Meta:RegisteredTables) RETURN a.id, [(a)-[:CHECKS]-(b) | b.id] AS bs",
            [(10, [11]), (11, [10]), (12, [])],
        ),
        # junction: 1 KIND_OF 2
        (
            "MATCH (a:Ontology:Terms) RETURN a.id, [(a)-[:KIND_OF]-(b) | b.id] AS bs",
            [(1, [2]), (2, [1]), (3, [])],
        ),
    ],
    ids=["fk", "junction"],
)
def test_an_undirected_self_referential_comprehension_reads_both_ways(meta, cypher, expected):
    assert _rows(meta, cypher) == expected


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        (
            "MATCH (a:Meta:RegisteredTables) "
            "WHERE EXISTS { (a)-[:CHECKS]-(:Meta:RegisteredTables) } RETURN a.id",
            [(10,), (11,)],
        ),
        (
            "MATCH (a:Ontology:Terms) WHERE EXISTS { (a)-[:KIND_OF]-(:Ontology:Terms) } "
            "RETURN a.id",
            [(1,), (2,)],
        ),
    ],
    ids=["fk", "junction"],
)
def test_an_undirected_self_referential_exists_reads_both_ways(meta, cypher, expected):
    assert _rows(meta, cypher) == expected


# --- a free end of a type several tables carry --------------------------------------------------


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        ("MATCH (a:Meta:Relationships)-[:OWNERS]->(b) RETURN a.id, b.id", [(200, 300)]),
        ("MATCH (a:Meta:Relationships)-[:OWNERS]-(b) RETURN a.id, b.id", [(200, 300)]),
        ("MATCH (a:Meta:DataProducts)-[:OWNERS]->(b) RETURN a.id, b.id", [(100, 400)]),
        (
            "MATCH (a:Meta:Relationships) OPTIONAL MATCH (a)-[:OWNERS]-(b) RETURN a.id, b.id",
            [(200, 300)],
        ),
    ],
)
def test_a_free_end_is_the_table_the_named_end_s_relationship_reaches(meta, cypher, expected):
    assert _rows(meta, cypher) == expected


# --- OPTIONAL MATCH over an undirected self-referential type: one outer join, either way --------
# REQ-348 (amended 2026-11-26): answered, not refused as reaching several relationships. FK: 11
# CHECKS 10 (checked_table_id). Junction: 1 KIND_OF 2 (_term_edges). Terms 1 and 2 are referenced
# by columns 'c1' and 'c2' (_term_refs), which a hop after the self-referential one reaches.


@pytest.fixture(scope="module")
def selfrel(meta):
    lm, con = meta
    for term in (1, 2):
        _insert(con, "table_columns", id=900 + term, name=f"c{term}", column_key=f"c{term}")
        _insert(con, "_term_refs", term_id=term, column_key=f"c{term}")
    return lm, con


_SELFREL_NODES = {
    "fk": ("Meta:RegisteredTables", "CHECKS"),
    "junction": ("Ontology:Terms", "KIND_OF"),
}


@pytest.mark.parametrize(
    ("shape", "expected"),
    [
        # a=10 reaches 11 against the arrow, a=11 reaches 10 along it, a=12 reaches none.
        ("fk", [(10, 11), (11, 10), (12, None)]),
        ("junction", [(1, 2), (2, 1), (3, None)]),
    ],
)
def test_an_optional_undirected_self_referential_hop_matches_either_direction(
    selfrel, shape, expected
):
    label, rel = _SELFREL_NODES[shape]
    rows = _rows(
        selfrel,
        f"MATCH (a:{label}) OPTIONAL MATCH (a)-[:{rel}]-(b:{label}) RETURN a.id, b.id",
    )
    assert rows == expected


@pytest.mark.parametrize("shape", ["fk", "junction"])
def test_an_optional_undirected_self_referential_hop_is_one_outer_join(selfrel, shape):
    lm, _con = selfrel
    label, rel = _SELFREL_NODES[shape]
    sql = _sql(lm, f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel}]-(b:{label}) RETURN a.id, b.id")
    assert "UNION" not in sql.upper()
    assert " OR " in sql


@pytest.mark.parametrize(
    ("shape", "expected"),
    [
        # Each edge object is the relationship as stored: its start the source, whichever end
        # the pattern named first.
        ("fk", [(10, 11, 10, False), (11, 11, 10, False), (12, None, None, True)]),
        ("junction", [(1, 1, 2, False), (2, 1, 2, False), (3, None, None, True)]),
    ],
)
def test_an_optional_undirected_self_referential_hop_returns_r_as_stored(selfrel, shape, expected):
    import json

    label, rel = _SELFREL_NODES[shape]
    rows = _rows(
        selfrel,
        f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel}]-(b:{label}) RETURN a.id, r, r IS NULL",
    )

    def key(node: dict) -> int:
        # A node's graph identity, "Label|key".
        return int(node["id"].rsplit("|", 1)[1])

    got = []
    for a_id, r, absent in rows:
        edge = None if r is None else (json.loads(r) if isinstance(r, str) else r)
        got.append(
            (
                a_id,
                None if edge is None else key(edge["startNode"]),
                None if edge is None else key(edge["endNode"]),
                absent,
            )
        )
    assert sorted(got, key=repr) == expected


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        (
            "MATCH (a:Meta:RegisteredTables) "
            "OPTIONAL MATCH (a)-[r:CHECKS]-(b:Meta:RegisteredTables)-[:SOURCE]-(s:Meta:Sources) "
            "RETURN a.id, b.id, s.name",
            [(10, 11, "pg"), (11, 10, "pg"), (12, None, None)],
        ),
        (
            "MATCH (a:Ontology:Terms) "
            "OPTIONAL MATCH (a)-[:KIND_OF]-(b:Ontology:Terms)-[:REFERENCED_BY]-(c) "
            "RETURN a.id, b.id, c.name",
            [(1, 2, "c2"), (2, 1, "c1"), (3, None, None)],
        ),
    ],
    ids=["fk", "junction"],
)
def test_an_optional_undirected_self_referential_hop_then_a_following_hop(
    selfrel, cypher, expected
):
    assert _rows(selfrel, cypher) == expected


@pytest.mark.parametrize(
    ("shape", "expected"),
    [
        ("fk", [(10, 11, 10), (11, 11, 10), (12, None, None)]),
        ("junction", [(1, 1, 2), (2, 1, 2), (3, None, None)]),
    ],
)
def test_an_optional_undirected_self_referential_hop_s_ends_are_as_stored(selfrel, shape, expected):
    import json

    label, rel = _SELFREL_NODES[shape]
    rows = _rows(
        selfrel,
        f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel}]-(b:{label}) "
        "RETURN a.id, startNode(r), endNode(r)",
    )
    got = [
        (a_id, *(None if n is None else json.loads(n)["properties"]["id"] for n in (s, e)))
        for a_id, s, e in rows
    ]
    assert sorted(got, key=repr) == expected
