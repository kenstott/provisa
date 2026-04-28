# Copyright (c) 2026 Kenneth Stott
# Canary: bae0b12d-255c-47a8-916d-a46248b5d271
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/label_map.py — focus on _resolve_id_column and cross-domain traversal."""

import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping, _resolve_id_column
from provisa.compiler.introspect import ColumnMetadata


# ---------------------------------------------------------------------------
# _resolve_id_column
# ---------------------------------------------------------------------------

def test_join_target_wins_over_all():
    # target_pk explicitly says "user_id" — must win even if "id" is present
    assert _resolve_id_column("User", ["id", "user_id", "name"], {"User": "user_id"}) == "user_id"


def test_exact_id_column():
    assert _resolve_id_column("Person", ["id", "name", "age"], {}) == "id"


def test_exact_underscore_id():
    assert _resolve_id_column("Event", ["_id", "title"], {}) == "_id"


def test_exact_pk():
    assert _resolve_id_column("Record", ["pk", "value"], {}) == "pk"


def test_exact_oid():
    assert _resolve_id_column("Doc", ["oid", "body"], {}) == "oid"


def test_exact_id_case_insensitive():
    assert _resolve_id_column("Table", ["ID", "name"], {}) == "ID"


def test_single_suffix_id():
    # Only one column ending in _id — unambiguous
    assert _resolve_id_column("Order", ["order_id", "amount", "created_at"], {}) == "order_id"


def test_ambiguous_suffix_falls_through_to_first_col():
    # Two _id columns — suffix heuristic is ambiguous; falls to first column
    result = _resolve_id_column("Link", ["source_id", "target_id", "weight"], {})
    assert result == "source_id"


def test_single_prefix_id():
    assert _resolve_id_column("Record", ["id_hash", "value"], {}) == "id_hash"


def test_first_column_fallback():
    assert _resolve_id_column("Thing", ["ref", "name", "code"], {}) == "ref"


def test_empty_columns_returns_hard_fallback():
    assert _resolve_id_column("Ghost", [], {}) == "id"


def test_type_not_in_target_pk_uses_column_heuristic():
    # target_pk has a different type — should not affect this one
    assert _resolve_id_column("Person", ["id", "name"], {"Company": "cid"}) == "id"


def test_join_target_overrides_exact_id():
    # Even if "id" is in columns, the join-declared PK wins
    assert _resolve_id_column("Company", ["id", "cid"], {"Company": "cid"}) == "cid"


# ---------------------------------------------------------------------------
# NodeMapping.traversal_only default
# ---------------------------------------------------------------------------

def test_node_mapping_traversal_only_defaults_false():
    nm = NodeMapping(
        label="Orders", type_name="Orders", domain_label=None, table_label="Orders",
        table_id=1, source_id="pg", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="orders",
        properties={},
    )
    assert nm.traversal_only is False


def test_node_mapping_traversal_only_can_be_set():
    nm = NodeMapping(
        label="Logistics:Shipments", type_name="Logistics_Shipments", domain_label="Logistics",
        table_label="Shipments", table_id=99, source_id="pg2", id_column="shipment_id",
        pk_columns=[], catalog_name="pg2", schema_name="logistics", table_name="shipments",
        properties={"shipmentId": "shipment_id"},
        traversal_only=True,
    )
    assert nm.traversal_only is True


# ---------------------------------------------------------------------------
# CypherLabelMap.from_schema — cross-domain traversal_only nodes (REQ-440–444)
# ---------------------------------------------------------------------------

def _make_ctx(owned_table_id: int = 1):
    """Minimal fake CompilationContext with one table in 'sales' domain."""
    from types import SimpleNamespace
    table = SimpleNamespace(
        type_name="Sales_Orders",
        table_id=owned_table_id,
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="sa_orders",
        domain_id="sales",
    )
    ctx = SimpleNamespace(
        tables={"sales__orders": table},
        joins={},
        aggregate_columns={owned_table_id: [("id", "integer"), ("amount", "float")]},
        pk_columns={},
        native_filter_columns={},
    )
    return ctx


def test_from_schema_no_cross_domain_params_no_traversal_only():
    """Without cross-domain params, no traversal_only nodes added."""
    ctx = _make_ctx()
    lm = CypherLabelMap.from_schema(ctx)
    assert all(not nm.traversal_only for nm in lm.nodes.values())


def test_from_schema_cross_domain_adds_traversal_only_node():
    """Cross-domain target reachable via relationship gets traversal_only=True.

    Table name "l_shipments" has domain initials "l" (logistics → "l"), so
    _strip_domain_prefix strips "l_" prefix → table_label = "Shipments",
    type_name = "Logistics_Shipments".
    """
    ctx = _make_ctx(owned_table_id=1)
    all_tables = [
        {"id": 1, "table_name": "sa_orders", "schema_name": "public", "source_id": "pg", "domain_id": "sales"},
        # "l_" prefix matches domain initials for "logistics" (first letter = "l")
        {"id": 2, "table_name": "l_shipments", "schema_name": "logistics_schema", "source_id": "pg2", "domain_id": "logistics"},
    ]
    all_relationships = [
        {
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "shipment_id",
            "target_column": "id",
            "alias": "SHIPPED_VIA",
            "computed_cypher_alias": None,
            "graphql_alias": "l_shipments",
            "disable_cypher": False,
        }
    ]
    all_column_types = {
        2: [ColumnMetadata("id", "integer", False), ColumnMetadata("status", "varchar", True)],
    }
    lm = CypherLabelMap.from_schema(
        ctx,
        domain_access=["sales"],
        all_tables=all_tables,
        all_relationships=all_relationships,
        all_column_types=all_column_types,
    )
    # Cross-domain node should exist and be traversal_only
    assert "Logistics_Shipments" in lm.nodes
    xnode = lm.nodes["Logistics_Shipments"]
    assert xnode.traversal_only is True
    assert xnode.domain_label == "Logistics"
    assert xnode.table_label == "Shipments"
    # Owned node must not be traversal_only
    assert not lm.nodes["Sales_Orders"].traversal_only
    # Cross-domain relationship should be registered
    assert any(r.rel_type == "SHIPPED_VIA" for r in lm.relationships.values())


def test_from_schema_star_domain_access_skips_cross_domain():
    """domain_access=['*'] means all tables are owned; no traversal_only nodes added."""
    ctx = _make_ctx(owned_table_id=1)
    all_tables = [
        {"id": 1, "table_name": "sa_orders", "schema_name": "public", "source_id": "pg", "domain_id": "sales"},
        {"id": 2, "table_name": "l_shipments", "schema_name": "logistics_schema", "source_id": "pg2", "domain_id": "logistics"},
    ]
    all_relationships = [
        {
            "source_table_id": 1, "target_table_id": 2,
            "source_column": "shipment_id", "target_column": "id",
            "alias": "SHIPPED_VIA", "computed_cypher_alias": None, "graphql_alias": "l_shipments",
            "disable_cypher": False,
        }
    ]
    all_column_types = {2: [ColumnMetadata("id", "integer", False)]}
    lm = CypherLabelMap.from_schema(
        ctx,
        domain_access=["*"],
        all_tables=all_tables,
        all_relationships=all_relationships,
        all_column_types=all_column_types,
    )
    # No traversal_only nodes because * access means everything is owned
    assert "Logistics_Shipments" not in lm.nodes


def test_from_schema_no_cross_domain_rel_when_target_already_owned():
    """No traversal_only node added when target table is already in the role's context."""
    ctx = _make_ctx(owned_table_id=1)
    # Add a second owned table manually to ctx
    from types import SimpleNamespace
    table2 = SimpleNamespace(
        type_name="Sales_Products",
        table_id=2,
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="sa_products",
        domain_id="sales",
    )
    ctx.tables["sales__products"] = table2
    ctx.aggregate_columns[2] = [("id", "integer"), ("name", "varchar")]

    all_tables = [
        {"id": 1, "table_name": "sa_orders", "schema_name": "public", "source_id": "pg", "domain_id": "sales"},
        {"id": 2, "table_name": "sa_products", "schema_name": "public", "source_id": "pg", "domain_id": "sales"},
    ]
    all_relationships = [
        {
            "source_table_id": 1, "target_table_id": 2,
            "source_column": "product_id", "target_column": "id",
            "alias": "HAS_PRODUCT", "computed_cypher_alias": None, "graphql_alias": "sa_products",
            "disable_cypher": False,
        }
    ]
    all_column_types = {2: [ColumnMetadata("id", "integer", False)]}
    lm = CypherLabelMap.from_schema(
        ctx,
        domain_access=["sales"],
        all_tables=all_tables,
        all_relationships=all_relationships,
        all_column_types=all_column_types,
    )
    # Both nodes owned — no traversal_only
    assert all(not nm.traversal_only for nm in lm.nodes.values())
