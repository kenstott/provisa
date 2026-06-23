# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a1c7e-8b4d-4e2a-6f5c-1d3b7a9f2c8e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Cypher path/relationship graph functions: relationships(), nodes(), etc."""

import json

from provisa.cypher.assembler import Node, assemble_rows
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import GraphVarKind, cypher_to_sql


def _make_lm() -> CypherLabelMap:
    person = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name"},
    )
    company = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    rels = {
        "WORKS_AT": RelationshipMapping(
            rel_type="WORKS_AT",
            source_label="Person",
            target_label="Company",
            join_source_column="company_id",
            join_target_column="id",
            field_name="works_at",
        ),
    }
    return CypherLabelMap(nodes={"Person": person, "Company": company}, relationships=rels)


def _sql(query: str, lm: CypherLabelMap) -> str:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), lm, {})
    return sql_ast.sql(dialect="trino")


def test_relationships_fn_on_named_path():
    lm = _make_lm()
    sql = _sql("MATCH p = (a:Person)-[:WORKS_AT]->(b:Company) RETURN relationships(p) AS rels", lm)
    assert "JSON_ARRAY" in sql


def test_relationship_singular_fn_on_named_path():
    lm = _make_lm()
    sql = _sql("MATCH p = (a:Person)-[:WORKS_AT]->(b:Company) RETURN relationship(p) AS rels", lm)
    assert "JSON_ARRAY" in sql


def test_nodes_fn_on_named_path():
    lm = _make_lm()
    sql = _sql("MATCH p = (a:Person)-[:WORKS_AT]->(b:Company) RETURN nodes(p) AS ns", lm)
    assert "JSON_ARRAY" in sql


def test_startnode_fn_on_rel_var():
    lm = _make_lm()
    sql = _sql("MATCH (a:Person)-[r:WORKS_AT]->(b:Company) RETURN startNode(r) AS sn", lm)
    assert "JSON_OBJECT" in sql


def test_endnode_fn_on_rel_var():
    lm = _make_lm()
    sql = _sql("MATCH (a:Person)-[r:WORKS_AT]->(b:Company) RETURN endNode(r) AS en", lm)
    assert "JSON_OBJECT" in sql


def test_properties_fn_on_node_var():
    lm = _make_lm()
    sql = _sql("MATCH (a:Person) RETURN properties(a) AS props", lm)
    assert "JSON_OBJECT" in sql


def test_node_list_column_deserialized_as_list():
    n1 = {"id": "1", "label": "Person", "tableLabel": "Person", "properties": {"name": "Alice"}}
    n2 = {"id": "2", "label": "Person", "tableLabel": "Person", "properties": {"name": "Bob"}}
    rows = [{"ns": json.dumps([n1, n2])}]
    result = assemble_rows(rows, {"ns": GraphVarKind.NODE})
    items = result[0]["ns"]
    assert isinstance(items, list)
    assert len(items) == 2
    assert all(isinstance(n, Node) for n in items)
    assert items[0].label == "Person"
