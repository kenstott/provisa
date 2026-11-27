# Copyright (c) 2026 Kenneth Stott
# Canary: 15ca7a1b-9b54-4ad6-9048-2aff4bf77290
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""startNode(r) and endNode(r) as values and as property sources: startNode(r).id, WITH
startNode(r) AS s ... s.id, and RETURN startNode(r), for an FK edge (SOURCE) and a junction
edge (MEMBER_TABLES), under MATCH and OPTIONAL MATCH. Each statement is compiled as a Cypher
read (cypher_to_sql, then the graph rewrites) and run on DuckDB over the meta relationships the
server seeds (meta_relationship_seed_rows)."""

# Requirements: REQ-345, REQ-1586

from __future__ import annotations

from collections import defaultdict

import duckdb
import json

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
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

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


# --- the cases: an FK edge (SOURCE: registered table -> source) and a junction edge
# (MEMBER_TABLES: product -> registered table, through data_product_tables) ------------------

_FK = "(a:Meta:RegisteredTables)-[r:SOURCE]->(b:Meta:Sources)"
_FK_BACK = "(b:Meta:Sources)<-[r:SOURCE]-(a:Meta:RegisteredTables)"
_JUNCTION = "(a:Meta:DataProducts)-[r:MEMBER_TABLES]->(b:Meta:RegisteredTables)"
_JUNCTION_BACK = "(b:Meta:RegisteredTables)<-[r:MEMBER_TABLES]-(a:Meta:DataProducts)"

# (pattern, its rows as (a.id, b.id))
_MATCHED = {
    "fk": (_FK, [(10, 1), (11, 1), (12, 2)]),
    "fk-back": (_FK_BACK, [(10, 1), (11, 1), (12, 2)]),
    "junction": (_JUNCTION, [(100, 10), (100, 12)]),
    "junction-back": (_JUNCTION_BACK, [(100, 10), (100, 12)]),
}

# (the label a's MATCH binds, the OPTIONAL pattern, its rows as (a.id, b.id)): a with no edge
# has null ends.
_OPTIONAL = {
    "fk": (
        "(a:Meta:RegisteredTables)",
        "(a)-[r:SOURCE]->(b:Meta:Sources)",
        [(10, 1), (11, 1), (12, 2)],
    ),
    "junction": (
        "(a:Meta:DataProducts)",
        "(a)-[r:MEMBER_TABLES]->(b:Meta:RegisteredTables)",
        [(100, 10), (100, 12), (101, None)],
    ),
}


def _sorted(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=repr)


def _ends(pairs: list[tuple]) -> list[tuple]:
    """(a.id, startNode(r).id, endNode(r).id) of each (a.id, b.id): a starts r, b ends it; with
    no r, both ends are null."""
    return _sorted([(a, a if b is not None else None, b) for a, b in pairs])


# --- startNode(r).prop, endNode(r).prop --------------------------------------------------------


@pytest.mark.parametrize("case", list(_MATCHED))
def test_endpoint_property_under_match(meta, case):
    pattern, pairs = _MATCHED[case]
    rows = _rows(meta, f"MATCH {pattern} RETURN a.id, startNode(r).id, endNode(r).id")
    assert rows == _ends(pairs)


@pytest.mark.parametrize("case", list(_OPTIONAL))
def test_endpoint_property_under_optional_match(meta, case):
    first, optional, pairs = _OPTIONAL[case]
    rows = _rows(
        meta,
        f"MATCH {first} OPTIONAL MATCH {optional} RETURN a.id, startNode(r).id, endNode(r).id",
    )
    assert rows == _ends(pairs)


# --- WITH startNode(r) AS s ... s.prop ---------------------------------------------------------


@pytest.mark.parametrize("case", list(_MATCHED))
def test_endpoint_carried_by_with_under_match(meta, case):
    pattern, pairs = _MATCHED[case]
    rows = _rows(
        meta,
        f"MATCH {pattern} WITH a, startNode(r) AS s, endNode(r) AS e RETURN a.id, s.id, e.id",
    )
    assert rows == _ends(pairs)


@pytest.mark.parametrize("case", list(_OPTIONAL))
def test_endpoint_carried_by_with_under_optional_match(meta, case):
    first, optional, pairs = _OPTIONAL[case]
    rows = _rows(
        meta,
        f"MATCH {first} OPTIONAL MATCH {optional} "
        "WITH a, startNode(r) AS s, endNode(r) AS e RETURN a.id, s.id, e.id",
    )
    assert rows == _ends(pairs)


# --- RETURN startNode(r), endNode(r) -----------------------------------------------------------


def _node_id(value) -> int | None:
    """The id property of a returned node value (its JSON object); None for null."""
    if value is None:
        return None
    node = json.loads(value) if isinstance(value, str) else value
    return node["properties"]["id"]


def _returned_ends(meta, cypher: str) -> list[tuple]:
    return _sorted([(a, _node_id(s), _node_id(e)) for a, s, e in _rows(meta, cypher)])


@pytest.mark.parametrize("case", list(_MATCHED))
def test_endpoint_returned_under_match(meta, case):
    pattern, pairs = _MATCHED[case]
    rows = _returned_ends(meta, f"MATCH {pattern} RETURN a.id, startNode(r), endNode(r)")
    assert rows == _ends(pairs)


@pytest.mark.parametrize("case", list(_OPTIONAL))
def test_endpoint_returned_under_optional_match(meta, case):
    first, optional, pairs = _OPTIONAL[case]
    rows = _returned_ends(
        meta, f"MATCH {first} OPTIONAL MATCH {optional} RETURN a.id, startNode(r), endNode(r)"
    )
    assert rows == _ends(pairs)


# --- WITH startNode(r) AS s alone, and carried on as a node ------------------------------------


@pytest.mark.parametrize("case", list(_MATCHED))
def test_endpoint_alone_carried_by_with(meta, case):
    pattern, pairs = _MATCHED[case]
    rows = _rows(meta, f"MATCH {pattern} WITH startNode(r) AS s, endNode(r) AS e RETURN s.id, e.id")
    assert rows == _sorted([(a, b) for a, b in pairs])


@pytest.mark.parametrize("case", list(_MATCHED))
def test_endpoint_carried_by_with_is_returned_as_its_node(meta, case):
    pattern, pairs = _MATCHED[case]
    rows = _rows(
        meta, f"MATCH {pattern} WITH a, startNode(r) AS s, endNode(r) AS e RETURN a.id, s, e"
    )
    assert _sorted([(a, _node_id(s), _node_id(e)) for a, s, e in rows]) == _ends(pairs)


# --- each engine dialect: the end's own column, never a bare or a WITH-qualified one ------------

_SHAPES = {
    "property": f"MATCH {_FK} RETURN a.id, startNode(r).id, endNode(r).id",
    "property-optional": (
        "MATCH (a:Meta:DataProducts) OPTIONAL MATCH (a)-[r:MEMBER_TABLES]->(b:Meta:RegisteredTables) "
        "RETURN a.id, startNode(r).id, endNode(r).id"
    ),
    "with": f"MATCH {_FK} WITH startNode(r) AS s, endNode(r) AS e RETURN s.id, e.id",
    "with-optional": (
        "MATCH (a:Meta:DataProducts) OPTIONAL MATCH (a)-[r:MEMBER_TABLES]->(b:Meta:RegisteredTables) "
        "WITH a, startNode(r) AS s, endNode(r) AS e RETURN a.id, s.id, e.id"
    ),
}


def _written(lm: CypherLabelMap, cypher: str, dialect: str) -> str:
    semantic = _sql(lm, cypher)
    if dialect == "duckdb":
        return transpile_to_duckdb(semantic, generated=False)
    return transpile(semantic, dialect, generated=False)


@pytest.mark.parametrize("dialect", ["postgres", "trino", "duckdb"])
@pytest.mark.parametrize("shape", list(_SHAPES))
def test_each_dialect_reads_the_end_not_a_bare_or_carried_column(meta, dialect, shape):
    lm, _con = meta
    written = _written(lm, _SHAPES[shape], dialect)
    columns = list(sqlglot.parse_one(written, read=dialect).find_all(sqlglot.exp.Column))
    assert not [c for c in columns if c.name == "id" and not c.table], written
    assert not [c for c in columns if c.table in ("s", "e")], written


@pytest.mark.parametrize("shape", list(_SHAPES))
def test_the_duckdb_transpile_runs_to_the_ends(meta, shape):
    lm, con = meta
    rows = sorted(con.execute(_written(lm, _SHAPES[shape], "duckdb")).fetchall(), key=repr)
    expected = {
        "property": [(10, 10, 1), (11, 11, 1), (12, 12, 2)],
        "property-optional": [(100, 100, 10), (100, 100, 12), (101, None, None)],
        "with": [(10, 1), (11, 1), (12, 2)],
        "with-optional": [(100, 100, 10), (100, 100, 12), (101, None, None)],
    }[shape]
    assert rows == _sorted(expected)
