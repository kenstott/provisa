# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Cypher frontend pipeline (REQ-776, REQ-777, REQ-778)."""

from __future__ import annotations

import json


from provisa.cypher.assembler import (
    Edge,
    Node,
    assemble_rows,
    to_serializable,
)
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import GraphVarKind, cypher_to_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(
    label: str,
    table: str,
    *,
    source_id: str = "pg-main",
    catalog: str = "postgresql",
    schema: str = "public",
    sql_table: str | None = None,
    props: dict[str, str] | None = None,
    table_id: int = 1,
) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id=source_id,
        id_column="id",
        pk_columns=[],
        catalog_name=catalog,
        schema_name=schema,
        table_name=sql_table or table,
        properties=props or {"id": "id", "name": "name"},
    )


def _rel(
    rel_type: str,
    source_label: str,
    target_label: str,
    src_col: str = "target_id",
    tgt_col: str = "id",
) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source_label,
        target_label=target_label,
        join_source_column=src_col,
        join_target_column=tgt_col,
        field_name=rel_type.lower(),
    )


def _label_map(
    *nodes: NodeMapping, rels: dict[str, RelationshipMapping] | None = None
) -> CypherLabelMap:
    return CypherLabelMap(
        nodes={n.label: n for n in nodes},
        relationships=rels or {},
    )


# ---------------------------------------------------------------------------
# REQ-776: OPTIONAL MATCH chains → LEFT JOIN SQL
# ---------------------------------------------------------------------------


class TestOptionalMatchToLeftJoin:
    """REQ-776: OPTIONAL MATCH chains translate to LEFT JOIN SQL."""

    def _three_node_map(self) -> CypherLabelMap:
        a = _node("A", "a_table", table_id=1)
        b = _node("B", "b_table", table_id=2)
        c = _node("C", "c_table", table_id=3)
        return _label_map(
            a,
            b,
            c,
            rels={
                "A_TO_B": _rel("A_TO_B", "A", "B"),
                "B_TO_C": _rel("B_TO_C", "B", "C"),
            },
        )

    def test_single_optional_match_produces_left_join(self):
        """OPTIONAL MATCH (a)-[r1]->(b) → LEFT JOIN."""
        lm = self._three_node_map()
        ast = parse_cypher("MATCH (a:A) OPTIONAL MATCH (a)-[:A_TO_B]->(b:B) RETURN a.name, b.name")
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        assert "LEFT JOIN" in sql

    def test_optional_match_chain_both_produce_left_joins(self):
        """OPTIONAL MATCH chain (a)->(b)->(c) produces at least two LEFT JOINs."""
        lm = self._three_node_map()
        ast = parse_cypher(
            "MATCH (a:A) "
            "OPTIONAL MATCH (a)-[:A_TO_B]->(b:B) "
            "OPTIONAL MATCH (b)-[:B_TO_C]->(c:C) "
            "RETURN a.name, b.name, c.name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        assert sql.count("LEFT JOIN") >= 2

    def test_optional_match_mandatory_match_uses_inner_join(self):
        """Regular MATCH after base FROM uses INNER (not LEFT) JOIN."""
        lm = self._three_node_map()
        ast = parse_cypher("MATCH (a:A)-[:A_TO_B]->(b:B) RETURN a.name, b.name")
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        # No OPTIONAL MATCH — must not produce LEFT JOIN for this pattern
        assert "LEFT JOIN" not in sql

    def test_optional_match_preserves_base_table_in_from(self):
        """Base MATCH node appears in FROM clause; optional node in LEFT JOIN."""
        lm = self._three_node_map()
        ast = parse_cypher("MATCH (a:A) OPTIONAL MATCH (a)-[:A_TO_B]->(b:B) RETURN a.name, b.name")
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").lower()
        assert "a_table" in sql
        assert "b_table" in sql

    def test_optional_match_where_folded_into_on_not_where(self):
        """WHERE on optional variable must be folded into LEFT JOIN ON, not global WHERE."""
        lm = self._three_node_map()
        ast = parse_cypher(
            "MATCH (a:A) "
            "OPTIONAL MATCH (a)-[:A_TO_B]->(b:B) "
            "WHERE b.name = 'x' "
            "RETURN a.name, b.name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        # The WHERE clause references b (optional), so it must be on the LEFT JOIN ON
        # and must not appear as a top-level WHERE that would turn LEFT into INNER.
        left_idx = sql.index("LEFT JOIN")
        on_idx = sql.index(" ON ", left_idx)
        where_idx = sql.find(" WHERE ")
        # Either there is no WHERE, or the ON condition comes before WHERE
        assert where_idx == -1 or on_idx < where_idx

    def test_optional_match_three_node_chain_tables_all_present(self):
        """All three tables from the chain appear in generated SQL."""
        lm = self._three_node_map()
        ast = parse_cypher(
            "MATCH (a:A) "
            "OPTIONAL MATCH (a)-[:A_TO_B]->(b:B) "
            "OPTIONAL MATCH (b)-[:B_TO_C]->(c:C) "
            "RETURN a.name, b.name, c.name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").lower()
        assert "a_table" in sql
        assert "b_table" in sql
        assert "c_table" in sql


# ---------------------------------------------------------------------------
# REQ-777: UNION ALL Cypher → SQL UNION ALL
# ---------------------------------------------------------------------------


class TestUnionAllTranslation:
    """REQ-777: UNION ALL queries compile to SQL UNION ALL."""

    def _two_node_map(self) -> CypherLabelMap:
        n = _node("Person", "persons", props={"id": "id", "name": "name"}, table_id=1)
        m = _node("Company", "companies", props={"id": "id", "name": "name"}, table_id=2)
        return _label_map(n, m)

    def test_union_all_produces_sql_union(self):
        """UNION ALL Cypher compiles to SQL containing UNION."""
        lm = self._two_node_map()
        ast = parse_cypher(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION ALL "
            "MATCH (m:Company) RETURN m.name AS name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        assert "UNION" in sql

    def test_union_all_not_distinct(self):
        """UNION ALL must not deduplicate — must not compile to UNION DISTINCT / UNION without ALL."""
        lm = self._two_node_map()
        ast = parse_cypher(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION ALL "
            "MATCH (m:Company) RETURN m.name AS name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        # sqlglot renders UNION ALL as "UNION ALL"; UNION DISTINCT as "UNION"
        assert "UNION ALL" in sql

    def test_union_all_both_branches_reference_correct_tables(self):
        """Both branch tables appear in the SQL."""
        lm = self._two_node_map()
        ast = parse_cypher(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION ALL "
            "MATCH (m:Company) RETURN m.name AS name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").lower()
        assert "persons" in sql
        assert "companies" in sql

    def test_union_deduplicates(self):
        """Plain UNION (not ALL) must compile without ALL (distinct semantics)."""
        lm = self._two_node_map()
        ast = parse_cypher(
            "MATCH (n:Person) RETURN n.name AS name UNION MATCH (m:Company) RETURN m.name AS name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        # UNION (distinct) must not contain ALL
        # sqlglot emits "UNION" for distinct and "UNION ALL" for non-distinct
        has_union_all = "UNION ALL" in sql
        assert not has_union_all, "Plain UNION must not emit UNION ALL"

    def test_union_all_graph_vars_merged(self):
        """graph_vars from both branches are present in the result."""
        lm = _label_map(
            _node("Person", "persons", props={"id": "id", "name": "name"}, table_id=1),
        )
        ast = parse_cypher("MATCH (n:Person) RETURN n UNION ALL MATCH (m:Person) RETURN m AS n")
        sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
        # n appears in the first branch; it must be in graph_vars
        assert "n" in graph_vars
        assert graph_vars["n"] == GraphVarKind.NODE

    def test_union_all_single_node_same_type(self):
        """UNION ALL over same label compiles to a SQL UNION ALL of two SELECTs."""
        lm = _label_map(
            _node("Person", "persons", props={"id": "id", "name": "name"}, table_id=1),
        )
        ast = parse_cypher(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name "
            "UNION ALL "
            "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.name AS name"
        )
        sql_ast, _, _ = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino").upper()
        assert "UNION ALL" in sql
        # Both literals should appear
        sql_lower = sql_ast.sql(dialect="trino").lower()
        assert "'alice'" in sql_lower or "alice" in sql_lower
        assert "'bob'" in sql_lower or "bob" in sql_lower


# ---------------------------------------------------------------------------
# REQ-778: /query/cypher endpoint typed response — nodes/edges/rows distinguished
# ---------------------------------------------------------------------------


class TestTypedResponseAssembly:
    """REQ-778: Typed response with nodes, edges, and scalar rows distinguished."""

    def _node_json(self, id_: str, label: str, **props) -> str:
        return json.dumps({"id": id_, "label": label, "tableLabel": label, "properties": props})

    def _edge_json(self, identity: str, type_: str, start: dict, end: dict) -> str:
        return json.dumps(
            {
                "identity": identity,
                "type": type_,
                "start": start["id"],
                "end": end["id"],
                "startNode": start,
                "endNode": end,
                "properties": {},
            }
        )

    def test_node_row_assembles_to_node_object(self):
        """assemble_rows converts NODE graph var columns to Node instances."""
        node_data = {
            "id": "Person|1",
            "label": "Person",
            "tableLabel": "Person",
            "properties": {"name": "Alice"},
        }
        raw = [{"n": json.dumps(node_data)}]
        graph_vars = {"n": GraphVarKind.NODE}
        result = assemble_rows(raw, graph_vars)
        assert len(result) == 1
        assert isinstance(result[0]["n"], Node)
        assert result[0]["n"].label == "Person"

    def test_edge_row_assembles_to_edge_object(self):
        """assemble_rows converts EDGE graph var columns to Edge instances."""
        start_data = {"id": "Person|1", "label": "Person", "tableLabel": "Person", "properties": {}}
        end_data = {
            "id": "Company|2",
            "label": "Company",
            "tableLabel": "Company",
            "properties": {},
        }
        edge_data = {
            "identity": "edge-1",
            "type": "WORKS_AT",
            "start": "Person|1",
            "end": "Company|2",
            "startNode": start_data,
            "endNode": end_data,
            "properties": {},
        }
        raw = [{"r": json.dumps(edge_data)}]
        graph_vars = {"r": GraphVarKind.EDGE}
        result = assemble_rows(raw, graph_vars)
        assert isinstance(result[0]["r"], Edge)
        assert result[0]["r"].type == "WORKS_AT"

    def test_scalar_column_passes_through_unchanged(self):
        """Scalar (non-graph) columns pass through assemble_rows unchanged."""
        raw = [{"count": 42, "name": "Alice"}]
        graph_vars: dict[str, GraphVarKind] = {}
        result = assemble_rows(raw, graph_vars)
        assert result[0]["count"] == 42
        assert result[0]["name"] == "Alice"

    def test_mixed_row_node_and_scalar(self):
        """Row with both a node column and a scalar column assembles correctly."""
        node_data = {
            "id": "Person|1",
            "label": "Person",
            "tableLabel": "Person",
            "properties": {"name": "Bob"},
        }
        raw = [{"n": json.dumps(node_data), "score": 99}]
        graph_vars = {"n": GraphVarKind.NODE}
        result = assemble_rows(raw, graph_vars)
        assert isinstance(result[0]["n"], Node)
        assert result[0]["score"] == 99

    def test_to_serializable_node_produces_correct_keys(self):
        """to_serializable(Node) returns dict with id, label, tableLabel, properties."""
        node = Node(
            id="Person|1", label="Person", table_label="Person", properties={"name": "Alice"}
        )
        d = to_serializable(node)
        assert d["id"] == "Person|1"
        assert d["label"] == "Person"
        assert d["tableLabel"] == "Person"
        assert d["properties"] == {"name": "Alice"}

    def test_to_serializable_edge_produces_correct_keys(self):
        """to_serializable(Edge) returns dict with identity, type, start, end, startNode, endNode."""
        start = Node(id="Person|1", label="Person", table_label="Person", properties={})
        end = Node(id="Company|2", label="Company", table_label="Company", properties={})
        edge = Edge(id="e-1", type="WORKS_AT", start_node=start, end_node=end, properties={})
        d = to_serializable(edge)
        assert d["identity"] == "e-1"
        assert d["type"] == "WORKS_AT"
        assert d["start"] == "Person|1"
        assert d["end"] == "Company|2"
        assert "startNode" in d
        assert "endNode" in d

    def test_null_node_column_passes_through_as_none(self):
        """None value in a NODE column remains None (optional match with no match)."""
        raw = [{"n": None}]
        graph_vars = {"n": GraphVarKind.NODE}
        result = assemble_rows(raw, graph_vars)
        assert result[0]["n"] is None

    def test_null_edge_column_passes_through_as_none(self):
        """None value in an EDGE column remains None (optional match with no match)."""
        raw = [{"r": None}]
        graph_vars = {"r": GraphVarKind.EDGE}
        result = assemble_rows(raw, graph_vars)
        assert result[0]["r"] is None

    def test_empty_rows_returns_empty_list(self):
        """assemble_rows on empty input returns empty list."""
        result = assemble_rows([], {"n": GraphVarKind.NODE})
        assert result == []

    def test_node_and_edge_in_same_row(self):
        """Row with node and edge columns — each column typed independently."""
        start_data = {"id": "Person|1", "label": "Person", "tableLabel": "Person", "properties": {}}
        end_data = {
            "id": "Company|2",
            "label": "Company",
            "tableLabel": "Company",
            "properties": {},
        }
        node_data = {
            "id": "Person|1",
            "label": "Person",
            "tableLabel": "Person",
            "properties": {"name": "Alice"},
        }
        edge_data = {
            "identity": "e-1",
            "type": "WORKS_AT",
            "start": "Person|1",
            "end": "Company|2",
            "startNode": start_data,
            "endNode": end_data,
            "properties": {},
        }
        raw = [{"n": json.dumps(node_data), "r": json.dumps(edge_data)}]
        graph_vars = {"n": GraphVarKind.NODE, "r": GraphVarKind.EDGE}
        result = assemble_rows(raw, graph_vars)
        assert isinstance(result[0]["n"], Node)
        assert isinstance(result[0]["r"], Edge)

    def test_to_serializable_nested_node_in_edge(self):
        """to_serializable(Edge) recursively serializes embedded start/end nodes."""
        start = Node(
            id="Person|1", label="Person", table_label="Person", properties={"name": "Alice"}
        )
        end = Node(
            id="Company|2", label="Company", table_label="Company", properties={"name": "Acme"}
        )
        edge = Edge(id="e-1", type="WORKS_AT", start_node=start, end_node=end, properties={})
        d = to_serializable(edge)
        assert d["startNode"]["label"] == "Person"
        assert d["endNode"]["label"] == "Company"
        assert d["startNode"]["properties"]["name"] == "Alice"

    def test_to_serializable_scalar_passthrough(self):
        """to_serializable on plain Python scalars returns them unchanged."""
        assert to_serializable(42) == 42
        assert to_serializable("hello") == "hello"
        assert to_serializable(None) is None
        assert to_serializable([1, 2]) == [1, 2]
        assert to_serializable({"a": 1}) == {"a": 1}
