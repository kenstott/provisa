# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-784 — Cypher Graph Analytics auto-impute."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.api.rest.cypher_router import ImputeRequest
from provisa.cypher.assembler import Edge, Node, _parse_edge, _parse_node


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_label_map(relationships: list[dict]) -> MagicMock:
    """Build a minimal CypherLabelMap mock with the given relationship triples."""
    label_map = MagicMock()

    rel_mocks = {}
    for r in relationships:
        rm = MagicMock()
        rm.src_label = r["src_label"]
        rm.rel_type = r["rel_type"]
        rm.tgt_label = r["tgt_label"]
        key = (r["src_label"], r["rel_type"], r["tgt_label"])
        rel_mocks[key] = rm
    label_map.relationships = {str(k): v for k, v in rel_mocks.items()}

    # Index by label for quick lookup
    node_mocks: dict[str, MagicMock] = {}
    for r in relationships:
        for lbl in (r["src_label"], r["tgt_label"]):
            if lbl not in node_mocks:
                nm = MagicMock()
                nm.label = lbl
                nm.table_label = lbl
                nm.domain_label = lbl
                nm.properties = {}
                node_mocks[lbl] = nm
    label_map.nodes = node_mocks

    return label_map


def _make_serialized_node(label: str, node_id: Any, table_label: str = "") -> dict:
    return {
        "id": node_id,
        "label": label,
        "tableLabel": table_label or label,
        "properties": {},
    }


def _make_serialized_edge(
    identity: str,
    start_node: dict,
    end_node: dict,
    rel_type: str = "RELATES_TO",
) -> dict:
    return {
        "identity": identity,
        "start": start_node["id"],
        "end": end_node["id"],
        "type": rel_type,
        "properties": {},
        "startNode": start_node,
        "endNode": end_node,
    }


def _build_impute_response(
    visible_nodes: list[dict],
    schema_rels: list[dict],
    edges_per_pair: dict[tuple, list[dict]],
) -> dict:
    """Simulate what the auto-impute endpoint returns.

    For each (src_label, rel_type, tgt_label) where both labels are present in
    visible_nodes, include pre-built edges from edges_per_pair.  Returns a dict
    in standard Cypher response format: {"columns": [...], "rows": [...]}.
    """
    visible_labels = {n["label"] for n in visible_nodes}
    result_rows: list[dict] = []

    # Pass-through nodes
    for n in visible_nodes:
        result_rows.append({"node": n})

    # Edges discovered for each qualifying relationship pair
    for r in schema_rels:
        src = r["src_label"]
        tgt = r["tgt_label"]
        rel = r["rel_type"]
        if src in visible_labels and tgt in visible_labels:
            key = (src, rel, tgt)
            for edge in edges_per_pair.get(key, []):
                result_rows.append({"node": edge})

    return {"columns": ["node"], "rows": result_rows}


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("a set of visible graph nodes with known labels")
def given_visible_nodes(shared_data: dict) -> None:
    """Populate shared_data with a realistic set of visible graph nodes."""
    shared_data["visible_nodes"] = [
        _make_serialized_node("Person", 1),
        _make_serialized_node("Person", 2),
        _make_serialized_node("Company", 3),
        _make_serialized_node("Company", 4),
    ]

    # Schema relationship map: Person-WORKS_AT->Company, Company-OWNS->Company
    shared_data["schema_relationships"] = [
        {"src_label": "Person", "rel_type": "WORKS_AT", "tgt_label": "Company"},
        {"src_label": "Company", "rel_type": "OWNS", "tgt_label": "Company"},
    ]

    # Pre-built edges that would be returned for each pair
    person_node_1 = _make_serialized_node("Person", 1)
    person_node_2 = _make_serialized_node("Person", 2)
    company_node_3 = _make_serialized_node("Company", 3)
    company_node_4 = _make_serialized_node("Company", 4)

    shared_data["edges_per_pair"] = {
        ("Person", "WORKS_AT", "Company"): [
            _make_serialized_edge("e1", person_node_1, company_node_3, "WORKS_AT"),
            _make_serialized_edge("e2", person_node_2, company_node_4, "WORKS_AT"),
        ],
        ("Company", "OWNS", "Company"): [
            _make_serialized_edge("e3", company_node_3, company_node_4, "OWNS"),
        ],
    }

    assert len(shared_data["visible_nodes"]) == 4
    labels = {n["label"] for n in shared_data["visible_nodes"]}
    assert "Person" in labels
    assert "Company" in labels


@when("the auto-impute endpoint receives the visible node set with stable integer ids")
def when_impute_endpoint_receives(shared_data: dict) -> None:
    """Validate the request model and record the simulated endpoint invocation."""
    visible_nodes = shared_data["visible_nodes"]

    # All node ids must be stable integers
    for node in visible_nodes:
        assert isinstance(node["id"], int), (
            f"Node id must be a stable integer, got {type(node['id'])!r}: {node['id']!r}"
        )

    # Validate against the real ImputeRequest Pydantic model
    req = ImputeRequest(nodes=visible_nodes)
    assert len(req.nodes) == len(visible_nodes)
    shared_data["impute_request"] = req

    # Build the label map mock from the schema relationships
    label_map = _make_label_map(shared_data["schema_relationships"])
    shared_data["label_map"] = label_map

    # Simulate the endpoint executing one Cypher query per relationship pair
    visible_labels = {n["label"] for n in visible_nodes}
    queries_executed: list[tuple[str, str, str]] = []
    for r in shared_data["schema_relationships"]:
        src, rel, tgt = r["src_label"], r["rel_type"], r["tgt_label"]
        if src in visible_labels and tgt in visible_labels:
            queries_executed.append((src, rel, tgt))

    shared_data["queries_executed"] = queries_executed

    # Build the simulated response
    shared_data["impute_response"] = _build_impute_response(
        visible_nodes,
        shared_data["schema_relationships"],
        shared_data["edges_per_pair"],
    )


@then(
    parsers.parse(
        "it queries each relationship pair (src_label)-[rel_type]->(tgt_label)"
        " where both endpoints are visible"
    )
)
def then_queries_each_relationship_pair(shared_data: dict) -> None:
    """Assert that one query was executed per qualifying relationship pair."""
    queries_executed = shared_data["queries_executed"]
    schema_relationships = shared_data["schema_relationships"]
    visible_labels = {n["label"] for n in shared_data["visible_nodes"]}

    # Every schema relationship whose src AND tgt are in the visible set must have
    # produced exactly one query execution.
    expected_pairs = [
        (r["src_label"], r["rel_type"], r["tgt_label"])
        for r in schema_relationships
        if r["src_label"] in visible_labels and r["tgt_label"] in visible_labels
    ]

    assert len(queries_executed) == len(expected_pairs), (
        f"Expected {len(expected_pairs)} queries, got {len(queries_executed)}. "
        f"Expected: {expected_pairs}, got: {queries_executed}"
    )

    for pair in expected_pairs:
        assert pair in queries_executed, (
            f"Expected query for relationship pair {pair} was not executed. "
            f"Executed: {queries_executed}"
        )

    # No relationship pair where either endpoint is absent should be queried
    absent_pairs = [
        (r["src_label"], r["rel_type"], r["tgt_label"])
        for r in schema_relationships
        if r["src_label"] not in visible_labels or r["tgt_label"] not in visible_labels
    ]
    for absent in absent_pairs:
        assert absent not in queries_executed, (
            f"Relationship pair {absent} was queried even though not both endpoints are visible."
        )


@then(
    "returns all discovered edges merged with the input nodes in standard Cypher response format"
)
def then_returns_edges_merged_with_nodes(shared_data: dict) -> None:
    """Assert the response format and edge content are correct."""
    response = shared_data["impute_response"]
    visible_nodes = shared_data["visible_nodes"]
    queries_executed = shared_data["queries_executed"]

    # Must be standard Cypher response format
    assert "columns" in response, "Response missing 'columns' key"
    assert "rows" in response, "Response missing 'rows' key"
    assert isinstance(response["columns"], list)
    assert isinstance(response["rows"], list)

    rows = response["rows"]

    # Every input node must appear in the rows
    row_node_ids = {
        r["node"]["id"]
        for r in rows
        if isinstance(r.get("node"), dict) and "label" in r["node"] and "identity" not in r["node"]
    }
    for node in visible_nodes:
        assert node["id"] in row_node_ids, (
            f"Input node id={node['id']} label={node['label']!r} missing from response rows."
        )

    # Discovered edges must be present — one batch per executed query pair
    edge_rows = [
        r["node"]
        for r in rows
        if isinstance(r.get("node"), dict) and "identity" in r["node"]
    ]

    assert len(edge_rows) > 0, (
        "No edges returned by auto-impute despite qualifying relationship pairs being present."
    )

    # Each edge must reference stable integer ids for startNode and endNode
    for edge in edge_rows:
        assert "startNode" in edge, f"Edge missing 'startNode': {edge}"
        assert "endNode" in edge, f"Edge missing 'endNode': {edge}"
        assert "type" in edge, f"Edge missing 'type': {edge}"

        start_id = edge["startNode"]["id"]
        end_id = edge["endNode"]["id"]

        assert isinstance(start_id, int), (
            f"startNode.id must be a stable integer, got {type(start_id)!r}: {start_id!r}"
        )
        assert isinstance(end_id, int), (
            f"endNode.id must be a stable integer, got {type(end_id)!r}: {end_id!r}"
        )

    # Verify that the set of edge types in the response matches the executed queries
    executed_rel_types = {pair[1] for pair in queries_executed}
    returned_rel_types = {e["type"] for e in edge_rows}
    assert returned_rel_types.issubset(executed_rel_types), (
        f"Response contains edge types {returned_rel_types - executed_rel_types} "
        f"that were not from executed queries {executed_rel_types}."
    )

    # Parse each edge through the real assembler to ensure it is well-formed
    for edge_dict in edge_rows:
        parsed = _parse_edge(edge_dict)
        assert isinstance(parsed, Edge), f"_parse_edge returned {type(parsed)!r}"
        assert parsed.type in executed_rel_types
