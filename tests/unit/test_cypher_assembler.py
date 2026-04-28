# Copyright (c) 2026 Kenneth Stott
# Canary: 5e3a9c7f-1b4d-4f2a-8c6e-2d4b6f8a3c7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/assembler.py."""

import json

import pytest

from provisa.cypher.assembler import (
    CypherAssemblyError,
    Edge,
    Node,
    Path,
    assemble_rows,
    to_serializable,
)
from provisa.cypher.translator import GraphVarKind


# ---------------------------------------------------------------------------
# Node deserialization
# ---------------------------------------------------------------------------

def test_node_json_column_deserialized():
    node_data = {"id": "1", "label": "Person", "name": "Alice", "age": 30}
    rows = [{"n": json.dumps(node_data), "age": 30}]
    result = assemble_rows(rows, {"n": GraphVarKind.NODE})
    assert len(result) == 1
    node = result[0]["n"]
    assert isinstance(node, Node)
    assert node.id == "1"
    assert node.label == "Person"
    assert node.properties.get("name") == "Alice"


def test_scalar_column_passed_through():
    node_data = {"id": "1", "label": "Person", "name": "Alice"}
    rows = [{"n": json.dumps(node_data), "count": 42}]
    result = assemble_rows(rows, {"n": GraphVarKind.NODE})
    assert result[0]["count"] == 42


def test_mixed_node_and_scalar():
    node_data = {"id": "2", "label": "Person", "name": "Bob"}
    rows = [{"n": json.dumps(node_data), "salary": 100000}]
    result = assemble_rows(rows, {"n": GraphVarKind.NODE})
    assert isinstance(result[0]["n"], Node)
    assert result[0]["salary"] == 100000


# ---------------------------------------------------------------------------
# Edge deserialization
# ---------------------------------------------------------------------------

def test_edge_json_column_deserialized_legacy():
    """Legacy format: id/startNode/endNode."""
    start = {"id": "1", "label": "Person"}
    end = {"id": "2", "label": "Company"}
    edge_data = {
        "id": "e1",
        "type": "WORKS_AT",
        "startNode": start,
        "endNode": end,
        "since": 2020,
    }
    rows = [{"rel": json.dumps(edge_data)}]
    result = assemble_rows(rows, {"rel": GraphVarKind.EDGE})
    edge = result[0]["rel"]
    assert isinstance(edge, Edge)
    assert edge.id == "e1"
    assert edge.type == "WORKS_AT"
    assert isinstance(edge.start_node, Node)
    assert edge.start_node.id == "1"
    assert isinstance(edge.end_node, Node)
    assert edge.end_node.id == "2"


def test_edge_json_column_deserialized_neo4j():
    """Neo4j wire format: identity/start/end + startNode/endNode."""
    start_node = {"id": "1", "label": "Person", "properties": {}}
    end_node = {"id": "2", "label": "Company", "properties": {}}
    edge_data = {
        "identity": "1-2",
        "start": "1",
        "end": "2",
        "type": "WORKS_AT",
        "properties": {"since": 2020},
        "startNode": start_node,
        "endNode": end_node,
    }
    rows = [{"rel": json.dumps(edge_data)}]
    result = assemble_rows(rows, {"rel": GraphVarKind.EDGE})
    edge = result[0]["rel"]
    assert isinstance(edge, Edge)
    assert edge.id == "1-2"
    assert edge.type == "WORKS_AT"
    assert edge.start_node.id == "1"
    assert edge.end_node.id == "2"
    assert edge.properties == {"since": 2020}


# ---------------------------------------------------------------------------
# Path assembly
# ---------------------------------------------------------------------------

def test_path_rows_collapsed_into_path():
    rows = [
        {"_path_id": "p1", "_depth": 1, "path": json.dumps({"nodes": [{"id": "1", "label": "Person"}], "edges": []})},
        {"_path_id": "p1", "_depth": 2, "path": json.dumps({"nodes": [{"id": "2", "label": "Company"}], "edges": [{"id": "e1", "type": "WORKS_AT", "startNode": {"id": "1", "label": "Person"}, "endNode": {"id": "2", "label": "Company"}}]})},
    ]
    result = assemble_rows(rows, {"path": GraphVarKind.PATH})
    assert len(result) == 1
    path_obj = result[0]["path"]
    assert isinstance(path_obj, Path)
    assert len(path_obj.nodes) >= 1


def test_multiple_paths_produce_separate_path_objects():
    rows = [
        {"_path_id": "p1", "_depth": 1, "path": json.dumps({"nodes": [{"id": "1", "label": "A"}], "edges": []})},
        {"_path_id": "p2", "_depth": 1, "path": json.dumps({"nodes": [{"id": "3", "label": "B"}], "edges": []})},
    ]
    result = assemble_rows(rows, {"path": GraphVarKind.PATH})
    assert len(result) == 2
    assert all(isinstance(r["path"], Path) for r in result)


def test_empty_path_no_hops():
    rows = [
        {"_path_id": "p1", "_depth": 0, "path": json.dumps({"nodes": [{"id": "1", "label": "Start"}], "edges": []})},
    ]
    result = assemble_rows(rows, {"path": GraphVarKind.PATH})
    assert len(result) == 1
    assert isinstance(result[0]["path"], Path)
    assert result[0]["path"].edges == []


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_malformed_json_raises():
    rows = [{"n": "not-valid-json"}]
    with pytest.raises(CypherAssemblyError, match="Malformed JSON"):
        assemble_rows(rows, {"n": GraphVarKind.NODE})


def test_empty_rows_returns_empty():
    result = assemble_rows([], {"n": GraphVarKind.NODE})
    assert result == []


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def test_to_serializable_node():
    node = Node(id="1", label="Person", properties={"name": "Alice"})
    s = to_serializable(node)
    assert s == {"id": "1", "label": "Person", "properties": {"name": "Alice"}}


def test_to_serializable_edge():
    start = Node(id="1", label="Person", properties={})
    end = Node(id="2", label="Company", properties={})
    edge = Edge(id="1-2", type="WORKS_AT", start_node=start, end_node=end, properties={"since": 2020})
    s = to_serializable(edge)
    assert s["identity"] == "1-2"
    assert s["start"] == "1"
    assert s["end"] == "2"
    assert s["type"] == "WORKS_AT"
    assert s["properties"] == {"since": 2020}
    assert s["startNode"]["id"] == "1"
    assert s["startNode"]["label"] == "Person"
    assert s["endNode"]["id"] == "2"
    assert s["endNode"]["label"] == "Company"


def test_to_serializable_path():
    node = Node(id="1", label="A", properties={})
    path = Path(nodes=[node], edges=[])
    s = to_serializable(path)
    assert "nodes" in s
    assert "edges" in s
