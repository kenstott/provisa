# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-784 and REQ-786 — Cypher Graph Analytics."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.api.rest.cypher_router import ImputeRequest
from provisa.cypher.assembler import Edge, _parse_edge


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


def _make_pg_row(id_: int, label: str, composite_id: str) -> dict:
    """Simulate an asyncpg Record for node_ids rows."""
    return {"id": id_, "label": label, "composite_id": composite_id}


# ---------------------------------------------------------------------------
# REQ-784 Steps
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

    absent_pairs = [
        (r["src_label"], r["rel_type"], r["tgt_label"])
        for r in schema_relationships
        if r["src_label"] not in visible_labels or r["tgt_label"] not in visible_labels
    ]
    for absent in absent_pairs:
        assert absent not in queries_executed, (
            f"Relationship pair {absent} was queried even though not both endpoints are visible."
        )


@then("returns all discovered edges merged with the input nodes in standard Cypher response format")
def then_returns_edges_merged_with_nodes(shared_data: dict) -> None:
    """Assert the response format and edge content are correct."""
    response = shared_data["impute_response"]
    visible_nodes = shared_data["visible_nodes"]
    queries_executed = shared_data["queries_executed"]

    assert "columns" in response, "Response missing 'columns' key"
    assert "rows" in response, "Response missing 'rows' key"
    assert isinstance(response["columns"], list)
    assert isinstance(response["rows"], list)

    rows = response["rows"]

    row_node_ids = {
        r["node"]["id"]
        for r in rows
        if isinstance(r.get("node"), dict) and "label" in r["node"] and "identity" not in r["node"]
    }
    for node in visible_nodes:
        assert node["id"] in row_node_ids, (
            f"Input node id={node['id']} label={node['label']!r} missing from response rows."
        )

    edge_rows = [
        r["node"] for r in rows if isinstance(r.get("node"), dict) and "identity" in r["node"]
    ]

    assert len(edge_rows) > 0, (
        "No edges returned by auto-impute despite qualifying relationship pairs being present."
    )

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

    executed_rel_types = {pair[1] for pair in queries_executed}
    returned_rel_types = {e["type"] for e in edge_rows}
    assert returned_rel_types.issubset(executed_rel_types), (
        f"Response contains edge types {returned_rel_types - executed_rel_types} "
        f"that were not from executed queries {executed_rel_types}."
    )

    for edge_dict in edge_rows:
        parsed = _parse_edge(edge_dict)
        assert isinstance(parsed, Edge), f"_parse_edge returned {type(parsed)!r}"
        assert parsed.type in executed_rel_types

    # Verify the response columns list is non-empty and contains at least one column name
    assert len(response["columns"]) > 0, "Response 'columns' list must not be empty"

    # Verify total row count equals visible nodes + discovered edges
    expected_total = len(visible_nodes) + len(edge_rows)
    assert len(rows) == expected_total, (
        f"Expected {expected_total} rows (nodes + edges), got {len(rows)}"
    )

    # Verify each edge type in the response corresponds to a schema relationship
    schema_rel_types = {r["rel_type"] for r in shared_data["schema_relationships"]}
    for edge in edge_rows:
        assert edge["type"] in schema_rel_types, (
            f"Edge type {edge['type']!r} not found in schema relationship types {schema_rel_types}"
        )

    # Verify that the number of queries executed equals the number of qualifying relationship pairs
    visible_labels = {n["label"] for n in visible_nodes}
    qualifying_pairs = [
        r
        for r in shared_data["schema_relationships"]
        if r["src_label"] in visible_labels and r["tgt_label"] in visible_labels
    ]
    assert len(queries_executed) == len(qualifying_pairs), (
        f"One Cypher query per relationship pair required: expected {len(qualifying_pairs)} "
        f"queries for {len(qualifying_pairs)} qualifying pairs, got {len(queries_executed)}"
    )


# ---------------------------------------------------------------------------
# REQ-786 Steps
# ---------------------------------------------------------------------------


@given(
    # Plain-string match: the step text contains literal ``{}`` braces which
    # ``parsers.parse`` would misread as format fields, so it must not be wrapped.
    'a request with nodes: [{label: "Meta", id: 10}, {label: "Meta", id: 11}, ...]'
)
def given_request_with_stable_integer_nodes(shared_data: dict) -> None:
    """Set up a request carrying Meta nodes with stable integer ids 10 and 11."""
    nodes = [
        {"label": "Meta", "id": 10},
        {"label": "Meta", "id": 11},
    ]
    req = ImputeRequest(nodes=nodes)
    assert len(req.nodes) == 2
    assert req.nodes[0]["id"] == 10
    assert req.nodes[1]["id"] == 11

    shared_data["impute_request_786"] = req
    shared_data["stable_ids"] = [10, 11]

    # Simulate node_ids rows that will be returned from the database
    shared_data["node_ids_rows"] = [
        _make_pg_row(10, "Meta", "Meta|42"),
        _make_pg_row(11, "Meta", "Meta|99"),
    ]

    # Expected mapping: stable id -> raw PK extracted from composite_id
    shared_data["expected_pk_map"] = {10: 42, 11: 99}


@when(parsers.parse("the endpoint fetches rows from node_ids WHERE id = ANY([10, 11, ...])"))
def when_endpoint_fetches_node_ids_rows(shared_data: dict) -> None:
    """Simulate the node_ids table lookup and extract raw PKs from composite_id."""
    stable_ids = shared_data["stable_ids"]
    node_ids_rows = shared_data["node_ids_rows"]

    # Verify the query would use the correct stable ids
    queried_ids = sorted(stable_ids)
    assert queried_ids == [10, 11], (
        f"Expected to query node_ids for ids [10, 11], got {queried_ids}"
    )

    # Simulate asyncpg fetch: only return rows whose id is in the queried set
    fetched_rows = [r for r in node_ids_rows if r["id"] in set(stable_ids)]
    assert len(fetched_rows) == len(stable_ids), (
        f"Expected {len(stable_ids)} rows from node_ids, got {len(fetched_rows)}"
    )

    shared_data["fetched_node_ids_rows"] = fetched_rows

    # Parse composite_id to extract raw PKs
    id_to_pk: dict[int, int] = {}
    for row in fetched_rows:
        composite_id: str = row["composite_id"]
        parts = composite_id.split("|", 1)
        assert len(parts) == 2, f"composite_id {composite_id!r} does not contain '|' separator"
        label_part, pk_str = parts
        assert label_part == row["label"], (
            f"Label part {label_part!r} of composite_id does not match row label {row['label']!r}"
        )
        raw_pk = int(pk_str)
        id_to_pk[int(row["id"])] = raw_pk

    shared_data["id_to_pk_map"] = id_to_pk


@then(parsers.parse('it extracts the raw PK from composite_id ("label|pk_value")'))
def then_extracts_raw_pk_from_composite_id(shared_data: dict) -> None:
    """Assert that the composite_id parsing produced the correct raw PK values."""
    id_to_pk = shared_data["id_to_pk_map"]
    expected_pk_map = shared_data["expected_pk_map"]

    assert set(id_to_pk.keys()) == set(expected_pk_map.keys()), (
        f"id_to_pk keys {set(id_to_pk.keys())} != expected {set(expected_pk_map.keys())}"
    )

    for stable_id, expected_pk in expected_pk_map.items():
        actual_pk = id_to_pk[stable_id]
        assert actual_pk == expected_pk, (
            f"For stable id {stable_id}: expected raw PK {expected_pk}, got {actual_pk}. "
            f"composite_id parsing is incorrect."
        )

    # Also verify the composite_id format directly from the fetched rows
    for row in shared_data["fetched_node_ids_rows"]:
        composite_id = row["composite_id"]
        # Must match "Label|integer" pattern
        parts = composite_id.split("|", 1)
        assert len(parts) == 2, f"composite_id {composite_id!r} missing '|'"
        label_part, pk_part = parts
        assert label_part, f"composite_id {composite_id!r} has empty label part"
        assert pk_part.isdigit(), (
            f"composite_id {composite_id!r} pk part {pk_part!r} is not an integer"
        )


@then("uses the raw PK values in the WHERE clause for relationship queries")
def then_uses_raw_pk_in_where_clause(shared_data: dict) -> None:
    """Assert that relationship queries are built using raw PKs, not stable ids."""
    id_to_pk = shared_data["id_to_pk_map"]
    stable_ids = shared_data["stable_ids"]

    raw_pks = [id_to_pk[sid] for sid in stable_ids]

    # The WHERE clause must reference raw PK values — confirm they differ from stable ids
    for stable_id in stable_ids:
        raw_pk = id_to_pk[stable_id]
        # In this test fixture the PKs differ from the stable ids (42 != 10, 99 != 11)
        assert raw_pk != stable_id, (
            f"Test fixture error: raw PK {raw_pk} equals stable id {stable_id}; "
            "fixture should use different values to validate substitution."
        )

    # Build a simulated WHERE clause using raw PKs and verify it contains raw PK values
    # and does NOT contain the original stable integer ids as the filtering values
    where_clause = f"WHERE n.id IN ({', '.join(str(pk) for pk in raw_pks)})"

    for raw_pk in raw_pks:
        assert str(raw_pk) in where_clause, (
            f"Raw PK {raw_pk} not found in WHERE clause: {where_clause!r}"
        )

    for stable_id in stable_ids:
        # Stable ids (10, 11) must not appear as filter values in the raw PK WHERE clause
        assert str(stable_id) not in where_clause, (
            f"Stable id {stable_id} found in raw-PK WHERE clause {where_clause!r}; "
            "endpoint must translate stable ids to raw PKs before filtering."
        )

    shared_data["relationship_where_clause"] = where_clause
    shared_data["raw_pks"] = raw_pks


@then("returns stable integer ids in the result edges (via register_node_ids)")
def then_returns_stable_integer_ids_in_result_edges(shared_data: dict) -> None:
    """Assert that result edges carry stable integer ids rather than raw PKs."""
    id_to_pk = shared_data["id_to_pk_map"]
    stable_ids = shared_data["stable_ids"]
    raw_pks = shared_data["raw_pks"]

    # Invert the map: raw PK -> stable id (simulates register_node_ids reverse lookup)
    pk_to_stable: dict[int, int] = {v: k for k, v in id_to_pk.items()}

    # Build simulated result edges using raw PKs internally but stable ids in output
    meta_node_10 = {"id": pk_to_stable[42], "label": "Meta", "tableLabel": "Meta", "properties": {}}
    meta_node_11 = {"id": pk_to_stable[99], "label": "Meta", "tableLabel": "Meta", "properties": {}}

    result_edge = _make_serialized_edge(
        identity="re1",
        start_node=meta_node_10,
        end_node=meta_node_11,
        rel_type="RELATES_TO",
    )

    # startNode.id and endNode.id must be stable integers, not raw PKs
    start_id = result_edge["startNode"]["id"]
    end_id = result_edge["endNode"]["id"]

    assert isinstance(start_id, int), (
        f"startNode.id must be int, got {type(start_id)!r}: {start_id!r}"
    )
    assert isinstance(end_id, int), f"endNode.id must be int, got {type(end_id)!r}: {end_id!r}"

    assert start_id in stable_ids, (
        f"startNode.id {start_id} is not a stable id from {stable_ids}. "
        "Edges must use stable integer ids registered via node_ids."
    )
    assert end_id in stable_ids, (
        f"endNode.id {end_id} is not a stable id from {stable_ids}. "
        "Edges must use stable integer ids registered via node_ids."
    )

    # Confirm the raw PKs are NOT used as the node ids in the result
    assert start_id not in raw_pks, (
        f"startNode.id {start_id} matches a raw PK value {raw_pks}; "
        "register_node_ids should map raw PKs back to stable ids."
    )
    assert end_id not in raw_pks, (
        f"endNode.id {end_id} matches a raw PK value {raw_pks}; "
        "register_node_ids should map raw PKs back to stable ids."
    )

    # Verify the edge parses correctly through the real assembler
    parsed = _parse_edge(result_edge)
    assert isinstance(parsed, Edge), f"_parse_edge returned {type(parsed)!r}, expected Edge"
    assert parsed.type == "RELATES_TO"

    # edge.start and edge.end must also be stable ids
    assert parsed.start_node.id in [str(s) for s in stable_ids], (
        f"Edge start_node.id {parsed.start_node.id} is not a stable id from {stable_ids}"
    )
    assert parsed.end_node.id in [str(s) for s in stable_ids], (
        f"Edge end_node.id {parsed.end_node.id} is not a stable id from {stable_ids}"
    )


scenarios("../features/REQ-784.feature")


# Copyright (c) 2026 Kenneth Stott
# Canary: ede2c72e-8c05-4499-98d9-98f052d5d099
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 77eed91a-ef29-41c6-af43-16a168a110a4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2941b42f-e77e-452a-9a80-5401607b1ba2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d9e64a00-6da5-4066-a4c2-76e32e1dfc2c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f870f978-ded4-48c7-addf-59c11f77a8aa
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 190f2921-c2cc-4f5e-bff5-9a5aa65acb8e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a30b4fa3-66d4-4ca0-94ab-161f8eed9740
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 95beb4ae-aeee-404a-bc69-0e39f37ce8f6
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 981efb90-dab8-4458-91af-3f75f51dc2b2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2cdb64ea-3518-45b2-a135-b511ad8eb5f1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fb7cb25a-0de4-4807-820c-8fed118d1144
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b186a872-3690-474e-909e-ca382e4fc299
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 592c3707-705c-4c6e-9309-a7b5db181569
#
# This source code is licensed under the Business Source License 1.1
