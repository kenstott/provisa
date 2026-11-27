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

# Requirements: REQ-345, REQ-347, REQ-348, REQ-575, REQ-1586, REQ-1634

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
from provisa.gql.translator_types import CypherTranslateError

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
    con = _connect(cols)
    _seed(con)
    return lm, con


def _connect(cols: dict[str, set[str]]) -> duckdb.DuckDBPyConnection:
    """A DuckDB database with each seeded table, empty."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    for name, columns in cols.items():
        defs = ", ".join(f'"{c}" {_type(c).upper()}' for c in sorted(columns))
        con.execute(f'CREATE TABLE provisa_admin.public."{name}_meta" ({defs})')
    return con


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


# --- two relationships between the same two nodes, each way; a self-loop ------------------------
# REQ-348: an OPTIONAL undirected hop reaches each relationship once. FK: 20 CHECKS 21 and 21
# CHECKS 20 are two relationships; 22 CHECKS 22 is one. Junction: 5 KIND_OF 6 and 6 KIND_OF 5;
# 7 KIND_OF 7. 23 and 8 reach none.


@pytest.fixture(scope="module")
def mutual(meta):
    lm, _seeded = meta
    con = _connect(_columns())
    _insert(con, "registered_tables", id=20, name="a", checked_table_id=21)
    _insert(con, "registered_tables", id=21, name="b", checked_table_id=20)
    _insert(con, "registered_tables", id=22, name="loop", checked_table_id=22)
    _insert(con, "registered_tables", id=23, name="none")
    for term in (5, 6, 7, 8):
        _insert(con, "_terms", id=term, name=f"t{term}")
    for src, tgt in ((5, 6), (6, 5), (7, 7)):
        _insert(con, "_term_edges", from_term_id=src, to_term_id=tgt, rel_type="KIND_OF")
    return lm, con


_MUTUAL = {
    # (a, b, r's start, r's end): one row per relationship, NULLs where none is reached.
    "fk": [
        (20, 21, 20, 21),
        (20, 21, 21, 20),
        (21, 20, 20, 21),
        (21, 20, 21, 20),
        (22, 22, 22, 22),
        (23, None, None, None),
    ],
    "junction": [
        (5, 6, 5, 6),
        (5, 6, 6, 5),
        (6, 5, 5, 6),
        (6, 5, 6, 5),
        (7, 7, 7, 7),
        (8, None, None, None),
    ],
}


def _node_key(node: dict) -> int:
    # A node's graph identity, "Label|key".
    return int(node["id"].rsplit("|", 1)[1])


def _json(value):
    import json

    return None if value is None else (json.loads(value) if isinstance(value, str) else value)


@pytest.mark.parametrize("shape", ["fk", "junction"])
def test_an_optional_undirected_hop_reaches_each_relationship_once(mutual, shape):
    label, rel = _SELFREL_NODES[shape]
    rows = _rows(
        mutual,
        f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel}]-(b:{label}) RETURN a.id, b.id, r",
    )
    got = [
        (
            a_id,
            b_id,
            *((None, None) if e is None else (_node_key(e["startNode"]), _node_key(e["endNode"]))),
        )
        for a_id, b_id, e in ((a, b, _json(r)) for a, b, r in rows)
    ]
    assert sorted(got, key=repr) == _MUTUAL[shape]


def _path_rows(meta, cypher: str) -> list[tuple]:
    got = []
    for a_id, p in _rows(meta, cypher):
        path = _json(p)
        if path is None:
            got.append((a_id, None, None, None))
            continue
        (edge,) = path["edges"]
        far = path["nodes"][-1]["properties"]["id"]
        got.append((a_id, far, _node_key(edge["startNode"]), _node_key(edge["endNode"])))
    return sorted(got, key=repr)


@pytest.mark.parametrize("shape", ["fk", "junction"])
def test_an_optional_undirected_path_s_edge_is_each_relationship_as_stored(mutual, shape):
    label, rel = _SELFREL_NODES[shape]
    got = _path_rows(
        mutual, f"MATCH (a:{label}) OPTIONAL MATCH p = (a)-[:{rel}]-(b:{label}) RETURN a.id, p"
    )
    assert got == _MUTUAL[shape]


@pytest.mark.parametrize(
    ("shape", "expected"),
    [
        ("fk", [(10, 11, 11, 10), (11, 10, 11, 10), (12, None, None, None)]),
        ("junction", [(1, 2, 1, 2), (2, 1, 1, 2), (3, None, None, None)]),
    ],
)
def test_an_optional_undirected_path_is_null_where_nothing_matched(selfrel, shape, expected):
    label, rel = _SELFREL_NODES[shape]
    got = _path_rows(
        selfrel, f"MATCH (a:{label}) OPTIONAL MATCH p = (a)-[r:{rel}]-(b:{label}) RETURN a.id, p"
    )
    assert got == expected


# --- a named path's relationships, each as stored ------------------------------------------------
# Neo4j: a path's relationship starts at the node it leaves and ends at the node it enters,
# whichever way the pattern traversed it; its identity is the same either way. FK: tables 10 and
# 11 have SOURCE 1, table 12 SOURCE 2; 11 CHECKS 10. Junction: product 100 has MEMBER_TABLES 10
# and 12 (and each of them PRODUCT 100); term 1 KIND_OF 2.


def _stored_edges(meta, cypher: str) -> list:
    """Each row's path as its relationships: (start, end, startNode's key, endNode's key,
    identity) each; None for a null path."""
    got = []
    for (p,) in _rows(meta, cypher):
        path = _json(p)
        got.append(
            None
            if path is None
            else tuple(
                (
                    int(e["start"]),
                    int(e["end"]),
                    _node_key(e["startNode"]),
                    _node_key(e["endNode"]),
                    e["identity"],
                )
                for e in path["edges"]
            )
        )
    return sorted(got, key=repr)


def _stored(*edges: tuple) -> tuple:
    """A path's relationships as stored, from (type, start, end) each."""
    return tuple((s, e, s, e, f"{t}:{s}-{e}") for t, s, e in edges)


_SOURCE_PATHS = [_stored(("SOURCE", 10, 1)), _stored(("SOURCE", 11, 1)), _stored(("SOURCE", 12, 2))]
_MEMBER_PATHS = [_stored(("MEMBER_TABLES", 100, 10)), _stored(("MEMBER_TABLES", 100, 12))]

_STORED_PATHS = {
    # Directed, read against the arrow.
    "fk-reversed": (
        "MATCH p = (s:Meta:Sources)<-[:SOURCE]-(t:Meta:RegisteredTables) RETURN p",
        _SOURCE_PATHS,
    ),
    "junction-reversed": (
        "MATCH p = (b:Meta:RegisteredTables)<-[:MEMBER_TABLES]-(a:Meta:DataProducts) RETURN p",
        _MEMBER_PATHS,
    ),
    "fk-self-reversed": (
        "MATCH p = (a:Meta:RegisteredTables)<-[:CHECKS]-(b:Meta:RegisteredTables) RETURN p",
        [_stored(("CHECKS", 11, 10))],
    ),
    "junction-self-reversed": (
        "MATCH p = (a:Ontology:Terms)<-[:KIND_OF]-(b:Ontology:Terms) RETURN p",
        [_stored(("KIND_OF", 1, 2))],
    ),
    # Undirected, matched from the relationship's end.
    "fk-undirected": (
        "MATCH p = (s:Meta:Sources)-[:SOURCE]-(t:Meta:RegisteredTables) RETURN p",
        _SOURCE_PATHS,
    ),
    "junction-undirected": (
        "MATCH p = (b:Meta:RegisteredTables)-[:MEMBER_TABLES]-(a:Meta:DataProducts) RETURN p",
        _MEMBER_PATHS,
    ),
    # Undirected over a self-referential type: each relationship matched both ways.
    "fk-self-undirected": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS]-(b:Meta:RegisteredTables) RETURN p",
        [_stored(("CHECKS", 11, 10))] * 2,
    ),
    "junction-self-undirected": (
        "MATCH p = (a:Ontology:Terms)-[:KIND_OF]-(b:Ontology:Terms) RETURN p",
        [_stored(("KIND_OF", 1, 2))] * 2,
    ),
    # Variable-length, each way.
    "fk-varlen-reversed": (
        "MATCH p = (s:Meta:Sources)<-[:SOURCE*1..2]-(t:Meta:RegisteredTables) RETURN p",
        _SOURCE_PATHS,
    ),
    "fk-varlen-undirected": (
        "MATCH p = (s:Meta:Sources)-[:SOURCE*1..2]-(t:Meta:RegisteredTables) RETURN p",
        _SOURCE_PATHS,
    ),
    "junction-varlen-reversed": (
        "MATCH p = (b:Meta:RegisteredTables)<-[:MEMBER_TABLES*1..2]-(a:Meta:DataProducts) RETURN p",
        _MEMBER_PATHS,
    ),
    "junction-varlen-undirected": (
        "MATCH p = (b:Meta:RegisteredTables)-[:MEMBER_TABLES*1..2]-(a:Meta:DataProducts) RETURN p",
        _MEMBER_PATHS,
    ),
    # OPTIONAL: null where nothing matched.
    "fk-optional-reversed": (
        "MATCH (s:Meta:Sources) OPTIONAL MATCH p = (s)<-[:SOURCE]-(t:Meta:RegisteredTables) "
        "RETURN p",
        _SOURCE_PATHS,
    ),
    "fk-optional-undirected": (
        "MATCH (s:Meta:Sources) OPTIONAL MATCH p = (s)-[:SOURCE]-(t:Meta:RegisteredTables) "
        "RETURN p",
        _SOURCE_PATHS,
    ),
    "fk-self-optional-reversed": (
        "MATCH (a:Meta:RegisteredTables) "
        "OPTIONAL MATCH p = (a)<-[:CHECKS]-(b:Meta:RegisteredTables) RETURN p",
        [None, None, _stored(("CHECKS", 11, 10))],
    ),
    "junction-optional-reversed": (
        "MATCH (b:Meta:RegisteredTables) "
        "OPTIONAL MATCH p = (b)<-[:MEMBER_TABLES]-(a:Meta:DataProducts) RETURN p",
        [None, *_MEMBER_PATHS],
    ),
    "junction-optional-undirected": (
        "MATCH (b:Meta:RegisteredTables) "
        "OPTIONAL MATCH p = (b)-[:MEMBER_TABLES]-(a:Meta:DataProducts) RETURN p",
        [None, *_MEMBER_PATHS],
    ),
    # Two hops, one against the arrow and one along it.
    "fk-then-junction": (
        "MATCH p = (s:Meta:Sources)<-[:SOURCE]-(t:Meta:RegisteredTables)"
        "-[:PRODUCT]->(d:Meta:DataProducts) RETURN p",
        [
            _stored(("SOURCE", 10, 1), ("PRODUCT", 10, 100)),
            _stored(("SOURCE", 12, 2), ("PRODUCT", 12, 100)),
        ],
    ),
}


@pytest.mark.parametrize(("cypher", "expected"), _STORED_PATHS.values(), ids=_STORED_PATHS.keys())
def test_a_path_s_relationship_is_as_stored(meta, cypher, expected):
    assert _stored_edges(meta, cypher) == sorted(expected, key=repr)


# --- a variable-length path: its nodes in pattern order, its relationships as stored -------------
# Neo4j: a path's nodes run from the pattern's first node to its last, whichever way the arrow
# points, and each relationship is as stored. A walk between two types is flat joins; one over a
# self-referential type is a recursive walk, which returns the whole path too.


def _whole_paths(meta, cypher: str) -> list:
    """Each row's path as (its nodes' keys, its relationships as (start, end, startNode's key,
    endNode's key, identity)); None for a null path."""
    got = []
    # An unbounded walk also says whether it was cut at the depth limit (__cut).
    for p, *_cut in _rows(meta, cypher):
        path = _json(p)
        got.append(
            None
            if path is None
            else (
                tuple(_node_key(n) for n in path["nodes"]),
                tuple(
                    (
                        int(e["start"]),
                        int(e["end"]),
                        _node_key(e["startNode"]),
                        _node_key(e["endNode"]),
                        e["identity"],
                    )
                    for e in path["edges"]
                ),
            )
        )
    return sorted(got, key=repr)


def _whole(nodes: tuple, *edges: tuple) -> tuple:
    """A path of ``nodes``, its relationships as stored from (type, start, end) each."""
    return (nodes, _stored(*edges))


_VARLEN_PATHS = {
    # Between two types: flat joins.
    "fk-flat-reversed": (
        "MATCH p = (s:Meta:Sources)<-[:SOURCE*1..2]-(t:Meta:RegisteredTables) RETURN p",
        [
            _whole((1, 10), ("SOURCE", 10, 1)),
            _whole((1, 11), ("SOURCE", 11, 1)),
            _whole((2, 12), ("SOURCE", 12, 2)),
        ],
    ),
    "fk-flat-forward": (
        "MATCH p = (t:Meta:RegisteredTables)-[:SOURCE*1..2]->(s:Meta:Sources) RETURN p",
        [
            _whole((10, 1), ("SOURCE", 10, 1)),
            _whole((11, 1), ("SOURCE", 11, 1)),
            _whole((12, 2), ("SOURCE", 12, 2)),
        ],
    ),
    "junction-flat-reversed": (
        "MATCH p = (b:Meta:RegisteredTables)<-[:MEMBER_TABLES*1..2]-(a:Meta:DataProducts) RETURN p",
        [
            _whole((10, 100), ("MEMBER_TABLES", 100, 10)),
            _whole((12, 100), ("MEMBER_TABLES", 100, 12)),
        ],
    ),
    "junction-flat-forward": (
        "MATCH p = (a:Meta:DataProducts)-[:MEMBER_TABLES*1..2]->(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((100, 10), ("MEMBER_TABLES", 100, 10)),
            _whole((100, 12), ("MEMBER_TABLES", 100, 12)),
        ],
    ),
    # Over a self-referential type: a recursive walk. FK: 11 CHECKS 10; junction: 1 KIND_OF 2.
    "fk-recursive-reversed": (
        "MATCH p = (a:Meta:RegisteredTables)<-[:CHECKS*1..2]-(b:Meta:RegisteredTables) RETURN p",
        [_whole((10, 11), ("CHECKS", 11, 10))],
    ),
    "fk-recursive-forward": (
        "MATCH p = (b:Meta:RegisteredTables)-[:CHECKS*1..2]->(a:Meta:RegisteredTables) RETURN p",
        [_whole((11, 10), ("CHECKS", 11, 10))],
    ),
    "fk-recursive-undirected": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS*1..2]-(b:Meta:RegisteredTables) RETURN p",
        [_whole((10, 11), ("CHECKS", 11, 10)), _whole((11, 10), ("CHECKS", 11, 10))],
    ),
    "junction-recursive-reversed": (
        "MATCH p = (a:Ontology:Terms)<-[:KIND_OF*1..2]-(b:Ontology:Terms) RETURN p",
        [_whole((2, 1), ("KIND_OF", 1, 2))],
    ),
    "junction-recursive-forward": (
        "MATCH p = (b:Ontology:Terms)-[:KIND_OF*1..2]->(a:Ontology:Terms) RETURN p",
        [_whole((1, 2), ("KIND_OF", 1, 2))],
    ),
    "junction-recursive-undirected": (
        "MATCH p = (a:Ontology:Terms)-[:KIND_OF*1..2]-(b:Ontology:Terms) RETURN p",
        [_whole((1, 2), ("KIND_OF", 1, 2)), _whole((2, 1), ("KIND_OF", 1, 2))],
    ),
    "fk-recursive-optional-reversed": (
        "MATCH (a:Meta:RegisteredTables) "
        "OPTIONAL MATCH p = (a)<-[:CHECKS*1..2]-(b:Meta:RegisteredTables) RETURN p",
        [None, None, _whole((10, 11), ("CHECKS", 11, 10))],
    ),
}


@pytest.mark.parametrize(("cypher", "expected"), _VARLEN_PATHS.values(), ids=_VARLEN_PATHS.keys())
def test_a_variable_length_path_is_in_pattern_order_with_each_relationship_as_stored(
    meta, cypher, expected
):
    assert _whole_paths(meta, cypher) == sorted(expected, key=repr)


# Two hops over a self-referential type. FK: 30 CHECKS 31 CHECKS 32; junction: 4 KIND_OF 5
# KIND_OF 6.


@pytest.fixture(scope="module")
def chain(meta):
    lm, _seeded = meta
    con = _connect(_columns())
    _insert(con, "registered_tables", id=30, name="a", checked_table_id=31)
    _insert(con, "registered_tables", id=31, name="b", checked_table_id=32)
    _insert(con, "registered_tables", id=32, name="c")
    for term in (4, 5, 6):
        _insert(con, "_terms", id=term, name=f"t{term}")
    for src, tgt in ((4, 5), (5, 6)):
        _insert(con, "_term_edges", from_term_id=src, to_term_id=tgt, rel_type="KIND_OF")
    return lm, con


_CHAIN_PATHS = {
    "fk-reversed": (
        "MATCH p = (a:Meta:RegisteredTables)<-[:CHECKS*2]-(b:Meta:RegisteredTables) RETURN p",
        [_whole((32, 31, 30), ("CHECKS", 31, 32), ("CHECKS", 30, 31))],
    ),
    "fk-forward": (
        "MATCH p = (b:Meta:RegisteredTables)-[:CHECKS*2]->(a:Meta:RegisteredTables) RETURN p",
        [_whole((30, 31, 32), ("CHECKS", 30, 31), ("CHECKS", 31, 32))],
    ),
    "fk-undirected": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS*2]-(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((30, 31, 32), ("CHECKS", 30, 31), ("CHECKS", 31, 32)),
            _whole((32, 31, 30), ("CHECKS", 31, 32), ("CHECKS", 30, 31)),
        ],
    ),
    "junction-reversed": (
        "MATCH p = (a:Ontology:Terms)<-[:KIND_OF*2]-(b:Ontology:Terms) RETURN p",
        [_whole((6, 5, 4), ("KIND_OF", 5, 6), ("KIND_OF", 4, 5))],
    ),
    "junction-forward": (
        "MATCH p = (b:Ontology:Terms)-[:KIND_OF*2]->(a:Ontology:Terms) RETURN p",
        [_whole((4, 5, 6), ("KIND_OF", 4, 5), ("KIND_OF", 5, 6))],
    ),
    "junction-undirected": (
        "MATCH p = (a:Ontology:Terms)-[:KIND_OF*2]-(b:Ontology:Terms) RETURN p",
        [
            _whole((4, 5, 6), ("KIND_OF", 4, 5), ("KIND_OF", 5, 6)),
            _whole((6, 5, 4), ("KIND_OF", 5, 6), ("KIND_OF", 4, 5)),
        ],
    ),
}


@pytest.mark.parametrize(("cypher", "expected"), _CHAIN_PATHS.values(), ids=_CHAIN_PATHS.keys())
def test_a_two_hop_recursive_path_is_in_pattern_order_with_each_relationship_as_stored(
    chain, cypher, expected
):
    assert _whole_paths(chain, cypher) == sorted(expected, key=repr)


# --- a variable-length walk over several relationship types -----------------------------------
# Neo4j: [:A|B*] takes, at every hop, a relationship of any of the types. Junction (one table, told
# apart by its type column): 1 KIND_OF 2, 2 PART_OF 3; 3 RELATED_TO 4 is of no type walked. FK: 20
# CHECKS 21, 21 PROFILED_TABLE 22. Mixed: product 200 has MEMBER_TABLES 20 and 22 (junction); 22
# has SOURCE 3 (FK).


@pytest.fixture(scope="module")
def alternation(meta):
    lm, _seeded = meta
    con = _connect(_columns())
    for term in (1, 2, 3, 4):
        _insert(con, "_terms", id=term, name=f"t{term}")
    for src, tgt, rel_type in ((1, 2, "KIND_OF"), (2, 3, "PART_OF"), (3, 4, "RELATED_TO")):
        _insert(con, "_term_edges", from_term_id=src, to_term_id=tgt, rel_type=rel_type)
    _insert(con, "sources", id=3, name="s3")
    _insert(con, "registered_tables", id=20, name="a", checked_table_id=21)
    _insert(con, "registered_tables", id=21, name="b", profiled_table_id=22)
    _insert(con, "registered_tables", id=22, name="c", source_id=3)
    _insert(con, "data_products", id=200, name="p")
    _insert(con, "data_product_tables", product_id=200, table_id=20)
    _insert(con, "data_product_tables", product_id=200, table_id=22)
    return lm, con


_KIND_OF = ("KIND_OF", 1, 2)
_PART_OF = ("PART_OF", 2, 3)
_TERM_WALKS = [
    _whole((1, 2), _KIND_OF),
    _whole((1, 2, 3), _KIND_OF, _PART_OF),
    _whole((2, 3), _PART_OF),
]
_TERM_WALKS_BACK = [
    _whole((2, 1), _KIND_OF),
    _whole((3, 2), _PART_OF),
    _whole((3, 2, 1), _PART_OF, _KIND_OF),
]
_CHECKS = ("CHECKS", 20, 21)
_PROFILED = ("PROFILED_TABLE", 21, 22)

_ALTERNATION_PATHS = {
    # Self-referential, of one junction: a recursive walk.
    "junction-self-forward": (
        "MATCH p = (a:Ontology:Terms)-[:KIND_OF|PART_OF*1..2]->(b:Ontology:Terms) RETURN p",
        _TERM_WALKS,
    ),
    "junction-self-reversed": (
        "MATCH p = (a:Ontology:Terms)<-[:KIND_OF|PART_OF*1..2]-(b:Ontology:Terms) RETURN p",
        _TERM_WALKS_BACK,
    ),
    "junction-self-undirected": (
        "MATCH p = (a:Ontology:Terms)-[:KIND_OF|PART_OF*1..2]-(b:Ontology:Terms) RETURN p",
        _TERM_WALKS + _TERM_WALKS_BACK,
    ),
    # Self-referential, of two FKs.
    "fk-self-forward": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS|PROFILED_TABLE*1..2]->"
        "(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((20, 21), _CHECKS),
            _whole((20, 21, 22), _CHECKS, _PROFILED),
            _whole((21, 22), _PROFILED),
        ],
    ),
    "fk-self-reversed": (
        "MATCH p = (a:Meta:RegisteredTables)<-[:CHECKS|PROFILED_TABLE*1..2]-"
        "(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((21, 20), _CHECKS),
            _whole((22, 21), _PROFILED),
            _whole((22, 21, 20), _PROFILED, _CHECKS),
        ],
    ),
    "fk-self-undirected": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS|PROFILED_TABLE*1..2]-"
        "(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((20, 21), _CHECKS),
            _whole((20, 21, 22), _CHECKS, _PROFILED),
            _whole((21, 20), _CHECKS),
            _whole((21, 22), _PROFILED),
            _whole((22, 21), _PROFILED),
            _whole((22, 21, 20), _PROFILED, _CHECKS),
        ],
    ),
    # Between two types, a junction then an FK: flat joins.
    "mixed-flat-forward": (
        "MATCH p = (d:Meta:DataProducts)-[:MEMBER_TABLES|SOURCE*1..2]->(s:Meta:Sources) RETURN p",
        [_whole((200, 22, 3), ("MEMBER_TABLES", 200, 22), ("SOURCE", 22, 3))],
    ),
    "mixed-flat-reversed": (
        "MATCH p = (s:Meta:Sources)<-[:MEMBER_TABLES|SOURCE*1..2]-(d:Meta:DataProducts) RETURN p",
        [_whole((3, 22, 200), ("SOURCE", 22, 3), ("MEMBER_TABLES", 200, 22))],
    ),
    "mixed-flat-undirected": (
        "MATCH p = (s:Meta:Sources)-[:MEMBER_TABLES|SOURCE*1..2]-(d:Meta:DataProducts) RETURN p",
        [_whole((3, 22, 200), ("SOURCE", 22, 3), ("MEMBER_TABLES", 200, 22))],
    ),
    # An FK and a junction in one recursive walk.
    "mixed-recursive-forward": (
        "MATCH p = (d:Meta:DataProducts)-[:MEMBER_TABLES|CHECKS*1..2]->"
        "(t:Meta:RegisteredTables) RETURN p",
        [
            _whole((200, 20), ("MEMBER_TABLES", 200, 20)),
            _whole((200, 20, 21), ("MEMBER_TABLES", 200, 20), _CHECKS),
            _whole((200, 22), ("MEMBER_TABLES", 200, 22)),
        ],
    ),
    "mixed-recursive-undirected": (
        "MATCH p = (a:Meta:RegisteredTables)-[:CHECKS|MEMBER_TABLES*1..2]-"
        "(b:Meta:RegisteredTables) RETURN p",
        [
            _whole((20, 21), _CHECKS),
            _whole((21, 20), _CHECKS),
            _whole((20, 200, 22), ("MEMBER_TABLES", 200, 20), ("MEMBER_TABLES", 200, 22)),
            _whole((22, 200, 20), ("MEMBER_TABLES", 200, 22), ("MEMBER_TABLES", 200, 20)),
        ],
    ),
    # Ends the pattern names no label of, which every type walked places on one label.
    "free-ends-undirected": (
        "MATCH p = (t)-[:KIND_OF|PART_OF*]-(n) RETURN p",
        _TERM_WALKS + _TERM_WALKS_BACK,
    ),
    "free-ends-forward": (
        "MATCH p = (t)-[:KIND_OF|PART_OF*1..2]->(n) RETURN p",
        _TERM_WALKS,
    ),
    "free-ends-reversed": (
        "MATCH p = (t)<-[:KIND_OF|PART_OF*1..2]-(n) RETURN p",
        _TERM_WALKS_BACK,
    ),
    "free-ends-fk": (
        "MATCH p = (a)-[:CHECKS|PROFILED_TABLE*1..2]->(b) RETURN p",
        [
            _whole((20, 21), _CHECKS),
            _whole((20, 21, 22), _CHECKS, _PROFILED),
            _whole((21, 22), _PROFILED),
        ],
    ),
    # A start an earlier clause binds.
    "bound-start": (
        "MATCH (t:Ontology:Terms) WHERE t.id = 1 MATCH p = (t)-[:KIND_OF|PART_OF*]-(n) RETURN p",
        [_whole((1, 2), _KIND_OF), _whole((1, 2, 3), _KIND_OF, _PART_OF)],
    ),
    "bound-start-optional": (
        "MATCH (t:Ontology:Terms) WHERE t.id = 4 "
        "OPTIONAL MATCH p = (t)-[:KIND_OF|PART_OF*]-(n) RETURN p",
        [None],
    ),
}


@pytest.mark.parametrize(
    ("cypher", "expected"), _ALTERNATION_PATHS.values(), ids=_ALTERNATION_PATHS.keys()
)
def test_a_variable_length_walk_takes_any_of_its_relationship_types_at_every_hop(
    alternation, cypher, expected
):
    assert _whole_paths(alternation, cypher) == sorted(expected, key=repr)


def test_a_walk_between_free_ends_whose_types_reach_several_labels_is_refused_by_name(meta):
    lm, _con = meta
    with pytest.raises(CypherTranslateError, match="labeled source and target nodes"):
        _sql(lm, "MATCH p = (a)-[:MEMBER_TABLES|SOURCE*1..2]-(b) RETURN p")
