# Copyright (c) 2026 Kenneth Stott
# Canary: 75e47dd0-466b-46ed-9c0e-9afb910eb4a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node's properties are its row's columns, each under its own name -- ``id`` included -- held
apart from the node's own keys, so its identity never shadows a column; and a node or relationship
an OPTIONAL MATCH matched nothing for is null, as in Neo4j, never a node of nulls."""

from __future__ import annotations

import json

import duckdb
import sqlglot

from provisa.gql.assembler import Edge, Node, assemble_rows, to_serializable
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.translator_types import GraphVarKind

DOMAINS = NodeMapping(
    label="Meta:Domains",
    type_name="Meta_Domains",
    domain_label="Meta",
    table_label="Domains",
    table_id=1,
    source_id="meta",
    id_column="id",
    pk_columns=["id"],
    catalog_name="meta",
    schema_name="meta",
    table_name="domains",
    properties={"id": "id", "label": "label", "description": "description"},
)
LABEL_MAP = CypherLabelMap(
    nodes={"Meta_Domains": DOMAINS},
    relationships={},
    domains={"Meta": ["Meta_Domains"]},
    nodes_by_table={"Domains": ["Meta_Domains"]},
)


def _nodes(sql: str) -> list[dict]:
    """``sql``'s node column, rewritten as the graph pipeline does, run on DuckDB and assembled."""
    con = duckdb.connect()
    con.execute("CREATE TABLE domains (id VARCHAR, label VARCHAR, description VARCHAR)")
    con.execute("INSERT INTO domains VALUES ('pet-store', 'Pet store', NULL)")
    con.execute("CREATE TABLE tables (domain_id VARCHAR, name VARCHAR)")
    rewritten = apply_graph_rewrites(
        sqlglot.parse_one(sql, dialect="postgres"), {"n": GraphVarKind.NODE}, LABEL_MAP
    )
    cur = con.execute(rewritten.sql(dialect="duckdb"))
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    return [to_serializable(row["n"]) for row in assemble_rows(rows, {"n": GraphVarKind.NODE})]


def test_a_column_named_id_is_a_property_under_its_own_name():
    [node] = _nodes("SELECT n AS n FROM domains AS n")
    assert node["properties"]["id"] == "pet-store"
    assert node["properties"]["label"] == "Pet store"
    # The node's own key is its identity, apart from the column; a null column is left out.
    assert node["id"] == "Meta:Domains|pet-store"
    assert "description" not in node["properties"]


def test_a_node_an_optional_match_matched_nothing_for_is_null():
    rows = assemble_rows(
        [{"n": json.dumps({"id": "Meta:Domains|pet-store", "label": "Meta:Domains"}), "m": None}],
        {"n": GraphVarKind.NODE, "m": GraphVarKind.NODE},
    )
    assert isinstance(rows[0]["n"], Node) and rows[0]["m"] is None
    # A node object whose key is null (the compound key of a null column) is null too.
    rows = assemble_rows(
        [{"m": json.dumps({"id": None, "label": "Meta:RegisteredTables", "properties": {}})}],
        {"m": GraphVarKind.NODE},
    )
    assert rows[0]["m"] is None


def test_a_relationship_with_a_null_end_is_null():
    domain = {"id": "Meta:Domains|pet-store", "label": "Meta:Domains", "properties": {}}
    ghost = {"id": None, "label": "Meta:RegisteredTables", "properties": {}}
    edge = {"identity": "HAS:1", "type": "HAS_REGISTERED_TABLES", "startNode": domain}
    rows = assemble_rows(
        [
            {"r": json.dumps({**edge, "endNode": ghost})},
            {"r": json.dumps({**edge, "start": "Meta:Domains|pet-store", "end": None})},
        ],
        {"r": GraphVarKind.EDGE},
    )
    assert [row["r"] for row in rows] == [None, None]
    whole = assemble_rows(
        [{"r": json.dumps({**edge, "endNode": domain})}], {"r": GraphVarKind.EDGE}
    )
    assert isinstance(whole[0]["r"], Edge)
