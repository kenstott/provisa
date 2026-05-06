# Copyright (c) 2026 Kenneth Stott
# Canary: eea232ed-8ab8-4723-9703-0485141e62ac
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/sql_to_cypher.py — semantic SQL → Cypher."""

from dataclasses import dataclass, field

import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher


# ---------------------------------------------------------------------------
# Minimal stubs for CompilationContext / TableMeta
# ---------------------------------------------------------------------------

@dataclass
class _TableMeta:
    table_id: int
    field_name: str
    type_name: str
    source_id: str
    catalog_name: str
    schema_name: str
    table_name: str
    domain_id: str = ""
    column_presets: list = field(default_factory=list)
    source_type: str = ""


@dataclass
class _Ctx:
    tables: dict = field(default_factory=dict)
    joins: dict = field(default_factory=dict)
    aggregate_columns: dict = field(default_factory=dict)
    pk_columns: dict = field(default_factory=dict)


def _make_simple_ctx_and_label_map():
    """Single table, no domain prefix in field_name."""
    meta = _TableMeta(
        table_id=1, field_name="persons", type_name="Person",
        source_id="pg-main", catalog_name="postgresql",
        schema_name="public", table_name="persons",
        domain_id="public",
    )
    ctx = _Ctx(
        tables={"persons": meta},
        aggregate_columns={1: [("id", "integer"), ("name", "varchar")]},
    )
    node = NodeMapping(
        label="Person", type_name="Person", domain_label=None,
        table_label="Person", table_id=1, source_id="pg-main",
        id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"id": "id", "name": "name"},
    )
    lm = CypherLabelMap(nodes={"Person": node}, relationships={})
    return ctx, lm


def _make_prefixed_ctx_and_label_map():
    """Single table where field_name has domain prefix (sa__orders style).

    _semantic_table_ref strips the prefix: "sales_analytics"."orders"
    domain_to_label must look up ("sales_analytics", "orders") not
    ("sales_analytics", "sa__orders").
    """
    meta = _TableMeta(
        table_id=2, field_name="sa__orders", type_name="Sa_Orders",
        source_id="pg-main", catalog_name="postgresql",
        schema_name="sales_analytics", table_name="sa_orders",
        domain_id="sales_analytics",
    )
    ctx = _Ctx(
        tables={"sa__orders": meta},
        aggregate_columns={2: [("id", "integer"), ("amount", "float")]},
    )
    node = NodeMapping(
        label="SalesAnalytics:Orders", type_name="Sa_Orders",
        domain_label="SalesAnalytics", table_label="Orders",
        table_id=2, source_id="pg-main", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="sales_analytics",
        table_name="orders",
        properties={"id": "id", "amount": "amount"},
    )
    lm = CypherLabelMap(nodes={"Sa_Orders": node}, relationships={})
    return ctx, lm


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSimpleTable:
    def test_simple_select_produces_match(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "persons"."name" FROM "public"."persons"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "MATCH" in result
        assert "Person" in result
        assert "RETURN" in result

    def test_returns_none_for_non_select(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        result = semantic_sql_to_cypher("UPDATE persons SET name = 'x'", lm, ctx)
        assert result is None

    def test_where_clause_translated(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "persons"."name" FROM "public"."persons" WHERE "persons"."id" = 1'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "WHERE" in result

    def test_limit_produces_cypher_limit(self):
        """Regression: LIMIT in semantic SQL must emit LIMIT in Cypher (not crash)."""
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "id", "name" FROM "public"."persons" LIMIT 10000'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "LIMIT 10000" in result

    def test_limit_and_offset(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "id" FROM "public"."persons" LIMIT 50 OFFSET 10'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "LIMIT 50" in result
        assert "SKIP 10" in result


class TestDomainPrefixedFieldName:
    """Regression: field_name with __ prefix must still resolve in domain_to_label."""

    def test_prefixed_field_name_resolves(self):
        """semantic_sql_to_cypher must not return None when field_name has domain prefix.

        The semantic SQL for sa__orders uses "sales_analytics"."orders" as the table
        reference (domain prefix stripped by _semantic_table_ref). The domain_to_label
        dict must be keyed on "orders", not "sa__orders".
        """
        ctx, lm = _make_prefixed_ctx_and_label_map()
        # Semantic SQL uses the stripped name ("orders"), not "sa__orders"
        sql = 'SELECT "sa_orders"."amount" FROM "sales_analytics"."orders"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, (
            "semantic_sql_to_cypher returned None for domain-prefixed field_name — "
            "domain_to_label key mismatch between '__'-prefixed field_name and "
            "stripped semantic SQL table name"
        )
        assert "SalesAnalytics" in result or "Orders" in result

    def test_prefixed_field_name_cypher_has_match(self):
        ctx, lm = _make_prefixed_ctx_and_label_map()
        sql = 'SELECT "sa_orders"."amount" FROM "sales_analytics"."orders"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "MATCH" in result
        assert "RETURN" in result


class TestTraversalOnlyNode:
    """Regression: traversal-only nodes (in label_map.nodes but NOT in ctx.tables) must resolve."""

    def _make_ctx_and_label_map(self):
        # ctx only contains the 'pets' table — user has access to it
        pets_meta = _TableMeta(
            table_id=1, field_name="pets", type_name="Pets",
            source_id="pg-main", catalog_name="postgresql",
            schema_name="public", table_name="pets",
            domain_id="public",
        )
        ctx = _Ctx(
            tables={"pets": pets_meta},
            aggregate_columns={1: [("id", "integer"), ("name", "varchar")]},
        )
        pets_node = NodeMapping(
            label="Pets", type_name="Pets", domain_label=None,
            table_label="Pets", table_id=1, source_id="pg-main",
            id_column="id", pk_columns=[],
            catalog_name="postgresql", schema_name="public", table_name="pets",
            properties={"id": "id", "name": "name"},
        )
        # Traversal-only node for ops.spans — not in ctx.tables
        spans_node = NodeMapping(
            label="Ops:Traces", type_name="Ops_Traces", domain_label="Ops",
            table_label="Traces", table_id=99, source_id="ops",
            id_column="span_id", pk_columns=[],
            catalog_name="ops", schema_name="ops", table_name="spans",
            properties={"serviceName": "service_name", "spanId": "span_id"},
            traversal_only=True,
            domain_id="ops",
        )
        rel = RelationshipMapping(
            rel_type="HAS_TRACES",
            source_label="Pets",
            target_label="Ops_Traces",
            join_source_column="id",
            join_target_column="pet_id",
            field_name="_traces",
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "Ops_Traces": spans_node},
            relationships={"HAS_TRACES::Pets→Ops_Traces": rel},
            nodes_by_table={"Pets": ["Pets"], "Traces": ["Ops_Traces"]},
        )
        return ctx, lm

    def test_lateral_join_to_traversal_node_does_not_return_none(self):
        """LATERAL join to a traversal-only node must not produce None (no crash)."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name", "t3"."span_id" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 10) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, (
            "semantic_sql_to_cypher returned None for LATERAL join to traversal-only node — "
            "traversal-only nodes must be added to domain_to_label even if not in ctx.tables"
        )
        assert "MATCH" in result
        assert "OPTIONAL MATCH" in result
        assert "RETURN" in result

    def test_lateral_join_return_uses_camel_case(self):
        """Properties from traversal-only nodes must use camelCase in RETURN."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 10) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "serviceName" in result
        assert "service_name" not in result
