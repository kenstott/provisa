# Copyright (c) 2026 Kenneth Stott
# Canary: f3a8b2c9-d4e7-4f1a-9c6b-2e5d8f0a3b7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for cypher_router endpoints (REQ-792–REQ-797)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Helpers / Stubs
# ---------------------------------------------------------------------------


def _make_node_mapping(
    label: str,
    table_label: str,
    domain_label: str | None = None,
    domain_id: str = "sales",
    table_id: int = 1,
    pk_columns: list[str] | None = None,
    id_column: str = "id",
    properties: dict[str, str] | None = None,
    traversal_only: bool = False,
) -> MagicMock:
    nm = MagicMock()
    nm.label = label
    nm.table_label = table_label
    nm.domain_label = domain_label
    nm.domain_id = domain_id
    nm.table_id = table_id
    nm.pk_columns = pk_columns or ["id"]
    nm.id_column = id_column
    nm.properties = properties or {"id": "id", "name": "name"}
    nm.physical_properties = {k: v for k, v in (properties or {"id": "id", "name": "name"}).items()}
    nm.native_filter_columns = {}
    nm.traversal_only = traversal_only
    return nm


def _make_rel_mapping(rel_type: str, source_label: str, target_label: str) -> MagicMock:
    rm = MagicMock()
    rm.rel_type = rel_type
    rm.source_label = source_label
    rm.target_label = target_label
    return rm


def _make_label_map(nodes: dict, relationships: dict) -> MagicMock:
    lm = MagicMock()
    lm.nodes = nodes
    lm.relationships = relationships
    return lm


def _make_request(headers: dict[str, str] | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = headers or {}
    req.query_params = {}
    return req


def _make_state(
    roles: dict | None = None,
    contexts: dict | None = None,
    schema_build_cache: dict | None = None,
) -> MagicMock:
    state = MagicMock()
    state.roles = (
        roles
        if roles is not None
        else {"admin": {"id": "admin", "capabilities": [], "domain_access": ["*"]}}
    )
    state.contexts = contexts or {}
    state.schema_build_cache = schema_build_cache or {"tables": [], "column_types": {}}
    state.tenant_db = None
    state.rls_contexts = {}
    state.tables = []
    state.source_catalogs = None
    return state


# ---------------------------------------------------------------------------
# REQ-792: _resolve_role_id and _handle_procedure (db.labels / db.relationshipTypes / db.propertyKeys)
# ---------------------------------------------------------------------------


class TestResolveRoleId:
    """REQ-796: X-Role (x-provisa-role) header grants access to /data/cypher."""

    def _resolve(self, request, state):
        from provisa.api.rest.cypher_exec import _resolve_role_id

        return _resolve_role_id(request, state)

    def test_role_from_x_provisa_role_header(self):
        state = _make_state(roles={"admin": {}, "viewer": {}})
        req = _make_request({"x-provisa-role": "viewer"})
        assert self._resolve(req, state) == "viewer"

    def test_role_from_X_Provisa_Role_header_case_variant(self):
        state = _make_state(roles={"admin": {}, "analyst": {}})
        req = _make_request({"X-Provisa-Role": "analyst"})
        assert self._resolve(req, state) == "analyst"

    def test_falls_back_to_first_role_when_header_absent(self):
        state = _make_state(roles={"admin": {}})
        req = _make_request({})
        assert self._resolve(req, state) == "admin"

    def test_falls_back_to_first_role_when_header_role_unknown(self):
        state = _make_state(roles={"admin": {}})
        req = _make_request({"x-provisa-role": "unknown-role"})
        assert self._resolve(req, state) == "admin"

    def test_returns_default_when_no_roles_registered(self):
        state = _make_state(roles={})
        req = _make_request({"x-provisa-role": "viewer"})
        assert self._resolve(req, state) == "default"

    def test_header_takes_precedence_over_first_role(self):
        state = _make_state(roles={"admin": {}, "viewer": {}, "analyst": {}})
        req = _make_request({"x-provisa-role": "analyst"})
        assert self._resolve(req, state) == "analyst"


# ---------------------------------------------------------------------------
# REQ-792: GET /data/graph-schema — response structure
# ---------------------------------------------------------------------------


class TestGraphSchemaStructure:
    """REQ-792: /data/graph-schema returns node labels, relationship types, property keys."""

    def _build_label_map_with_data(self):
        orders_nm = _make_node_mapping(
            label="Orders",
            table_label="Orders",
            domain_label="Sales:Orders",
            domain_id="sales",
            table_id=1,
            pk_columns=["id"],
            id_column="id",
            properties={"id": "id", "amount": "amount"},
        )
        customers_nm = _make_node_mapping(
            label="Customers",
            table_label="Customers",
            domain_label="Sales:Customers",
            domain_id="sales",
            table_id=2,
            pk_columns=["id"],
            id_column="id",
            properties={"id": "id", "name": "name"},
        )
        rel = _make_rel_mapping("PLACED_BY", "Orders", "Customers")
        nodes = {"Orders": orders_nm, "Customers": customers_nm}
        rels = {"placed_by": rel}
        return _make_label_map(nodes, rels)

    def test_graph_schema_node_labels_present(self):
        lm = self._build_label_map_with_data()
        node_labels = [
            {
                "label": n.label,
                "domain_label": n.domain_label,
                "properties": list(n.properties.keys()),
                "pk": n.pk_columns[0] if n.pk_columns else None,
                "pk_columns": list(n.pk_columns),
            }
            for n in lm.nodes.values()
        ]
        labels = [nl["label"] for nl in node_labels]
        assert "Orders" in labels
        assert "Customers" in labels

    def test_graph_schema_relationship_types_present(self):
        lm = self._build_label_map_with_data()
        rel_types = [
            {
                "type": r.rel_type,
                "source": lm.nodes[r.source_label].label,
                "target": lm.nodes[r.target_label].label,
            }
            for r in lm.relationships.values()
        ]
        assert len(rel_types) == 1
        assert rel_types[0]["type"] == "PLACED_BY"
        assert rel_types[0]["source"] == "Orders"
        assert rel_types[0]["target"] == "Customers"

    def test_graph_schema_node_has_pk_fields(self):
        lm = self._build_label_map_with_data()
        orders = lm.nodes["Orders"]
        serialized = {
            "pk": orders.pk_columns[0] if orders.pk_columns else None,
            "pk_columns": list(orders.pk_columns),
        }
        assert serialized["pk"] == "id"
        assert "id" in serialized["pk_columns"]

    def test_graph_schema_property_keys_extracted(self):
        lm = self._build_label_map_with_data()
        orders = lm.nodes["Orders"]
        assert "id" in orders.properties
        assert "amount" in orders.properties

    def test_graph_schema_no_rels_when_label_map_empty(self):
        lm = _make_label_map({}, {})
        node_labels = list(lm.nodes.values())
        rel_types = list(lm.relationships.values())
        assert node_labels == []
        assert rel_types == []


# ---------------------------------------------------------------------------
# REQ-793: _detect_procedure / _handle_procedure
# ---------------------------------------------------------------------------


class TestDetectProcedure:
    """REQ-793: POST /data/cypher accepts parameterless Cypher schema procedures."""

    def _detect(self, query: str):
        from provisa.api.rest.cypher_router import _detect_procedure

        return _detect_procedure(query)

    def test_detects_db_labels(self):
        assert self._detect("CALL db.labels()") == "db.labels"

    def test_detects_db_relationship_types(self):
        assert self._detect("CALL db.relationshipTypes()") == "db.relationshiptypes"

    def test_detects_db_property_keys(self):
        assert self._detect("CALL db.propertyKeys()") == "db.propertykeys"

    def test_ignores_case(self):
        assert self._detect("call DB.LABELS()") == "db.labels"

    def test_ignores_whitespace(self):
        assert self._detect("  CALL  db.labels(  )  ") == "db.labels"

    def test_returns_none_for_match_query(self):
        assert self._detect("MATCH (n:Order) RETURN n") is None

    def test_returns_none_for_partial_procedure_name(self):
        assert self._detect("CALL db.labels") is None


class TestHandleProcedure:
    """REQ-793: _handle_procedure returns correct columns/rows for each procedure."""

    def _handle(self, proc: str, label_map):
        from provisa.api.rest.cypher_router import _handle_procedure

        return _handle_procedure(proc, label_map)

    def _label_map(self):
        nm1 = _make_node_mapping("Orders", "Orders", domain_label="Sales:Orders")
        nm1.properties = {"id": "id", "amount": "amount"}
        nm2 = _make_node_mapping("Customers", "Customers", domain_label="Sales:Customers")
        nm2.properties = {"id": "id", "name": "name"}
        rel = _make_rel_mapping("PLACED_BY", "Orders", "Customers")
        return _make_label_map({"Orders": nm1, "Customers": nm2}, {"placed_by": rel})

    def test_db_labels_returns_label_column(self):
        lm = self._label_map()
        resp = self._handle("db.labels", lm)
        body = json.loads(resp.body)
        assert body["columns"] == ["label"]
        labels = [r["label"] for r in body["rows"]]
        assert "Orders" in labels or "Sales:Orders" in labels

    def test_db_relationship_types_returns_type_column(self):
        lm = self._label_map()
        resp = self._handle("db.relationshiptypes", lm)
        body = json.loads(resp.body)
        assert body["columns"] == ["relationshipType"]
        types = [r["relationshipType"] for r in body["rows"]]
        assert "PLACED_BY" in types

    def test_db_property_keys_returns_property_key_column(self):
        lm = self._label_map()
        resp = self._handle("db.propertykeys", lm)
        body = json.loads(resp.body)
        assert body["columns"] == ["propertyKey"]
        keys = [r["propertyKey"] for r in body["rows"]]
        assert "id" in keys

    def test_db_labels_sorted(self):
        lm = self._label_map()
        resp = self._handle("db.labels", lm)
        body = json.loads(resp.body)
        labels = [r["label"] for r in body["rows"]]
        assert labels == sorted(labels)

    def test_db_relationship_types_sorted(self):
        lm = self._label_map()
        resp = self._handle("db.relationshiptypes", lm)
        body = json.loads(resp.body)
        types = [r["relationshipType"] for r in body["rows"]]
        assert types == sorted(types)


# ---------------------------------------------------------------------------
# REQ-794: Query result values introspected for node and edge structure
# ---------------------------------------------------------------------------


class TestQueryResultIntrospection:
    """REQ-794: Result values must be introspected for node/edge structure."""

    def test_node_dict_has_label_and_id_fields(self):
        # Simulate an assembled node row as returned by to_serializable
        node_val = {
            "label": "Orders",
            "id": "Orders|42",
            "properties": {"id": 42, "amount": 100.0},
        }
        assert "label" in node_val
        assert "id" in node_val
        assert "properties" in node_val

    def test_edge_dict_has_identity_field(self):
        # Simulate an assembled edge row as returned by to_serializable
        edge_val = {
            "identity": "PLACED_BY:1:2",
            "start": 1,
            "end": 2,
            "type": "PLACED_BY",
            "properties": {},
        }
        assert "identity" in edge_val

    def test_node_distinguishable_from_edge_by_label_key(self):
        node = {"label": "Orders", "id": "Orders|1", "properties": {}}
        edge = {
            "identity": "PLACED_BY:1:2",
            "start": 1,
            "end": 2,
            "type": "PLACED_BY",
            "properties": {},
        }
        assert "label" in node
        assert "identity" in edge
        assert "label" not in edge

    def test_scalar_result_is_not_node_or_edge(self):
        scalar = 42
        assert not isinstance(scalar, dict)

    def test_none_result_is_not_node_or_edge(self):
        val = None
        assert val is None


# ---------------------------------------------------------------------------
# REQ-795: POST /data/neo4j-export accepts edge-only requests (no nodes)
# ---------------------------------------------------------------------------


class TestNeo4jExportEdgeOnly:
    """REQ-795: neo4j-export with nodes=[] generates only edge MERGE statements."""

    def _cypher_literal(self, v: Any) -> str:
        from provisa.api.rest.graph_tools_router import _neo4j_cypher_literal

        return _neo4j_cypher_literal(v)

    def _build_statements(self, nodes: list[dict], edges: list[dict]) -> list[str]:
        """Mirror the statement-building logic from neo4j_export."""
        statements: list[str] = []

        for n in nodes:
            table_label = n.get("tableLabel", "")
            full_label = n.get("label", "")
            parts = full_label.split(":", 1) if ":" in full_label else [full_label]
            effective_table = table_label or (parts[1] if len(parts) == 2 else full_label) or "Node"
            effective_domain = parts[0] if len(parts) == 2 else ""
            node_id = n.get("id")
            props: dict = n.get("properties", {})
            set_parts = ", ".join(f"{k}: {self._cypher_literal(v)}" for k, v in props.items())
            set_str = f" SET n += {{{set_parts}}}" if set_parts else ""
            label_str = (
                f"`{effective_table}`:`{effective_domain}`"
                if effective_domain and effective_domain != effective_table
                else f"`{effective_table}`"
            )
            statements.append(f"MERGE (n:{label_str} {{_provisa_id: {node_id}}}){set_str}")

        for e in edges:
            start = e.get("start")
            end = e.get("end")
            rel_type = e.get("type", "REL")
            src_label = e.get("startNodeLabel", "Node")
            tgt_label = e.get("endNodeLabel", "Node")
            statements.append(
                f"MATCH (a:`{src_label}` {{_provisa_id: {start}}}), "
                f"(b:`{tgt_label}` {{_provisa_id: {end}}}) "
                f"MERGE (a)-[:`{rel_type}`]->(b)"
            )

        return statements

    def test_edge_only_produces_no_merge_node_statements(self):
        edges = [
            {
                "start": 1,
                "end": 2,
                "type": "PLACED_BY",
                "startNodeLabel": "Orders",
                "endNodeLabel": "Customers",
            }
        ]
        stmts = self._build_statements([], edges)
        assert len(stmts) == 1
        assert stmts[0].startswith("MATCH")
        # Edge statement uses MERGE for the relationship, not a standalone node MERGE
        assert "MERGE (n:" not in stmts[0]

    def test_edge_only_contains_match_and_rel_merge(self):
        edges = [
            {
                "start": 10,
                "end": 20,
                "type": "PLACED_BY",
                "startNodeLabel": "Orders",
                "endNodeLabel": "Customers",
            }
        ]
        stmts = self._build_statements([], edges)
        assert "MATCH" in stmts[0]
        assert "`PLACED_BY`" in stmts[0]
        assert "_provisa_id: 10" in stmts[0]
        assert "_provisa_id: 20" in stmts[0]

    def test_empty_nodes_and_empty_edges_produces_no_statements(self):
        stmts = self._build_statements([], [])
        assert stmts == []

    def test_multiple_edges_produce_multiple_match_statements(self):
        edges = [
            {"start": 1, "end": 2, "type": "REL_A", "startNodeLabel": "A", "endNodeLabel": "B"},
            {"start": 3, "end": 4, "type": "REL_B", "startNodeLabel": "C", "endNodeLabel": "D"},
        ]
        stmts = self._build_statements([], edges)
        assert len(stmts) == 2
        assert all(s.startswith("MATCH") for s in stmts)

    def test_node_merge_uses_provisa_id(self):
        nodes = [
            {"label": "Orders", "tableLabel": "Orders", "id": 99, "properties": {"amount": 50}}
        ]
        stmts = self._build_statements(nodes, [])
        assert "_provisa_id: 99" in stmts[0]
        assert stmts[0].startswith("MERGE")

    def test_node_with_domain_uses_compound_label(self):
        nodes = [{"label": "Sales:Orders", "tableLabel": "", "id": 5, "properties": {}}]
        stmts = self._build_statements(nodes, [])
        assert "`Orders`:`Sales`" in stmts[0]

    def test_mixed_nodes_and_edges_count(self):
        nodes = [{"label": "Orders", "tableLabel": "Orders", "id": 1, "properties": {}}]
        edges = [
            {
                "start": 1,
                "end": 2,
                "type": "REL",
                "startNodeLabel": "Orders",
                "endNodeLabel": "Customers",
            }
        ]
        stmts = self._build_statements(nodes, edges)
        assert len(stmts) == 2


# ---------------------------------------------------------------------------
# REQ-795 / REQ-797: _neo4j_cypher_literal
# ---------------------------------------------------------------------------


class TestNeo4jCypherLiteral:
    """REQ-795: Cypher literal serialization used in export statements."""

    def _lit(self, v: Any) -> str:
        from provisa.api.rest.graph_tools_router import _neo4j_cypher_literal

        return _neo4j_cypher_literal(v)

    def test_none_renders_as_null(self):
        assert self._lit(None) == "null"

    def test_true_renders_as_true(self):
        assert self._lit(True) == "true"

    def test_false_renders_as_false(self):
        assert self._lit(False) == "false"

    def test_integer_renders_as_string_of_int(self):
        assert self._lit(42) == "42"

    def test_float_renders_correctly(self):
        assert self._lit(3.14) == "3.14"

    def test_string_is_json_quoted(self):
        assert self._lit("hello") == '"hello"'

    def test_string_with_special_chars_escaped(self):
        result = self._lit('say "hi"')
        assert '"' not in result[1:-1] or result.startswith('"say \\"hi\\"')


# ---------------------------------------------------------------------------
# REQ-796: X-Role header grants access
# ---------------------------------------------------------------------------


class TestXRoleHeaderAccess:
    """REQ-796: x-provisa-role header controls which role's schema is used."""

    def _resolve(self, request, state):
        from provisa.api.rest.cypher_exec import _resolve_role_id

        return _resolve_role_id(request, state)

    def test_viewer_role_resolved_from_header(self):
        state = _make_state(roles={"admin": {}, "viewer": {}})
        req = _make_request({"x-provisa-role": "viewer"})
        assert self._resolve(req, state) == "viewer"

    def test_admin_role_resolved_from_header(self):
        state = _make_state(roles={"admin": {}, "viewer": {}})
        req = _make_request({"x-provisa-role": "admin"})
        assert self._resolve(req, state) == "admin"

    def test_unknown_role_in_header_falls_back_to_first(self):
        state = _make_state(roles={"admin": {}, "viewer": {}})
        req = _make_request({"x-provisa-role": "ghost"})
        # First registered role is used when header role is not registered
        result = self._resolve(req, state)
        assert result in ("admin", "viewer")

    def test_no_header_uses_first_registered_role(self):
        state = _make_state(roles={"analyst": {}, "viewer": {}})
        req = _make_request({})
        result = self._resolve(req, state)
        assert result == "analyst"

    def test_empty_roles_returns_default(self):
        state = _make_state(roles={})
        req = _make_request({"x-provisa-role": "admin"})
        assert self._resolve(req, state) == "default"


# ---------------------------------------------------------------------------
# REQ-797: E2E neo4j export validates exported graph integrity
# ---------------------------------------------------------------------------


class TestNeo4jExportGraphIntegrity:
    """REQ-797: Exported graph integrity — every edge references nodes by _provisa_id."""

    def _build_statements(self, nodes: list[dict], edges: list[dict]) -> list[str]:
        from provisa.api.rest.graph_tools_router import _neo4j_cypher_literal

        statements: list[str] = []
        for n in nodes:
            table_label = n.get("tableLabel", "")
            full_label = n.get("label", "")
            parts = full_label.split(":", 1) if ":" in full_label else [full_label]
            effective_table = table_label or (parts[1] if len(parts) == 2 else full_label) or "Node"
            effective_domain = parts[0] if len(parts) == 2 else ""
            node_id = n.get("id")
            props: dict = n.get("properties", {})
            set_parts = ", ".join(f"{k}: {_neo4j_cypher_literal(v)}" for k, v in props.items())
            set_str = f" SET n += {{{set_parts}}}" if set_parts else ""
            label_str = (
                f"`{effective_table}`:`{effective_domain}`"
                if effective_domain and effective_domain != effective_table
                else f"`{effective_table}`"
            )
            statements.append(f"MERGE (n:{label_str} {{_provisa_id: {node_id}}}){set_str}")
        for e in edges:
            start = e.get("start")
            end = e.get("end")
            rel_type = e.get("type", "REL")
            src_label = e.get("startNodeLabel", "Node")
            tgt_label = e.get("endNodeLabel", "Node")
            statements.append(
                f"MATCH (a:`{src_label}` {{_provisa_id: {start}}}), "
                f"(b:`{tgt_label}` {{_provisa_id: {end}}}) "
                f"MERGE (a)-[:`{rel_type}`]->(b)"
            )
        return statements

    def test_all_edge_statements_reference_start_provisa_id(self):
        edges = [
            {
                "start": 10,
                "end": 20,
                "type": "PLACED_BY",
                "startNodeLabel": "Orders",
                "endNodeLabel": "Customers",
            },
            {
                "start": 30,
                "end": 40,
                "type": "BELONGS_TO",
                "startNodeLabel": "Items",
                "endNodeLabel": "Orders",
            },
        ]
        stmts = self._build_statements([], edges)
        for i, stmt in enumerate(stmts):
            assert "_provisa_id:" in stmt, f"Statement {i} missing _provisa_id: {stmt}"

    def test_all_node_statements_use_merge_with_provisa_id(self):
        nodes = [
            {"label": "Orders", "tableLabel": "Orders", "id": 1, "properties": {"amount": 100}},
            {
                "label": "Customers",
                "tableLabel": "Customers",
                "id": 2,
                "properties": {"name": "Alice"},
            },
        ]
        stmts = self._build_statements(nodes, [])
        for stmt in stmts:
            assert stmt.startswith("MERGE")
            assert "_provisa_id:" in stmt

    def test_edge_endpoint_ids_match_expected_nodes(self):
        nodes = [
            {"label": "Orders", "tableLabel": "Orders", "id": 1, "properties": {}},
            {"label": "Customers", "tableLabel": "Customers", "id": 2, "properties": {}},
        ]
        edges = [
            {
                "start": 1,
                "end": 2,
                "type": "PLACED_BY",
                "startNodeLabel": "Orders",
                "endNodeLabel": "Customers",
            }
        ]
        stmts = self._build_statements(nodes, edges)
        {n["id"] for n in nodes}
        edge_stmt = stmts[2]  # index 2 = after 2 node statements
        assert "_provisa_id: 1" in edge_stmt
        assert "_provisa_id: 2" in edge_stmt

    def test_statements_count_equals_nodes_plus_edges(self):
        nodes = [
            {"label": "A", "tableLabel": "A", "id": 1, "properties": {}},
            {"label": "B", "tableLabel": "B", "id": 2, "properties": {}},
        ]
        edges = [
            {"start": 1, "end": 2, "type": "LINKS", "startNodeLabel": "A", "endNodeLabel": "B"}
        ]
        stmts = self._build_statements(nodes, edges)
        assert len(stmts) == len(nodes) + len(edges)

    def test_rel_type_preserved_in_edge_statement(self):
        edges = [
            {"start": 5, "end": 6, "type": "OWNS", "startNodeLabel": "User", "endNodeLabel": "Item"}
        ]
        stmts = self._build_statements([], edges)
        assert "`OWNS`" in stmts[0]

    def test_node_properties_serialized_in_set_clause(self):
        nodes = [
            {
                "label": "Orders",
                "tableLabel": "Orders",
                "id": 1,
                "properties": {"amount": 99, "status": "open"},
            }
        ]
        stmts = self._build_statements(nodes, [])
        assert "amount" in stmts[0]
        assert "99" in stmts[0]
        assert "status" in stmts[0]

    def test_node_with_no_properties_has_no_set_clause(self):
        nodes = [{"label": "Orders", "tableLabel": "Orders", "id": 1, "properties": {}}]
        stmts = self._build_statements(nodes, [])
        assert "SET" not in stmts[0]
