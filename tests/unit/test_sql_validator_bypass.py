# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for sql_validator bypass_uncovered_relationships.

Validates:
  - Uncovered table pairs where one side is graphql_remote or grpc_remote are
    exempted from V002 when bypass_uncovered=True.
  - Registered table pairs with wrong columns still produce V002 (bypass does
    not apply when the pair appears in covered_pairs).
  - Non-remote uncovered pairs still produce V002 even with bypass_uncovered=True.
  - Correct columns on a registered pair always pass.

Also covers the sql_to_cypher inverse-relationship fix: two inverse rels sharing
the same column names must produce distinct rel_types depending on which table is
the JOIN target.
"""

from __future__ import annotations


from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _meta(
    table_id: int,
    table_name: str,
    schema_name: str = "public",
    source_id: str = "pg",
    source_type: str = "postgresql",
    domain_id: str = "sales",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name=schema_name,
        table_name=table_name,
        domain_id=domain_id,
        source_type=source_type,
    )


def _gov(
    *pairs: tuple[str, int],
    visible: dict[int, list[str]] | None = None,
) -> GovernanceContext:
    """Build a GovernanceContext with the given (qualified_name → table_id) pairs."""
    gov = GovernanceContext()
    for name, tid in pairs:
        gov.table_map[name] = tid
    if visible:
        gov.visible_columns = {
            tid: frozenset(cols) if cols else None for tid, cols in visible.items()
        }
    return gov


_ADMIN_ROLE = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}


# ---------------------------------------------------------------------------
# bypass_uncovered_relationships: remote uncovered pairs
# ---------------------------------------------------------------------------


class TestRemoteUncoveredPairs:
    """V002 is bypassed only when BOTH tables share the same remote source_id.

    The remote source's SDL/spec owns its relationship model. To restrict a
    remote-to-remote join to specific columns, register a covered_pair.
    Cross-source joins (local↔remote, or remote↔different-remote) always
    require a covered_pair and are never bypassed.
    """

    def _same_source_ctx_and_gov(self) -> tuple[CompilationContext, GovernanceContext]:
        """Two graphql_remote tables from the same source_id — bypass applies."""
        meta_a = _meta(
            1, "products", source_type="graphql_remote", source_id="gql-shop", domain_id="shop"
        )
        meta_b = _meta(
            2,
            "categories",
            source_type="graphql_remote",
            source_id="gql-shop",
            domain_id="shop",
            schema_name="gql_shop",
        )
        ctx = CompilationContext()
        ctx.tables = {"products": meta_a, "categories": meta_b}
        gov = _gov(("gql_shop.products", 1), ("gql_shop.categories", 2))
        return ctx, gov

    def test_same_source_remote_join_passes_with_bypass(self):
        ctx, gov = self._same_source_ctx_and_gov()
        sql = (
            'SELECT "p"."id", "c"."name" '
            'FROM "gql_shop"."products" AS "p" '
            'LEFT JOIN "gql_shop"."categories" AS "c" ON "c"."id" = "p"."category_id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002 == [], f"Expected no V002 for same-source remote join with bypass, got: {v002}"

    def test_same_source_remote_join_fails_without_bypass(self):
        ctx, gov = self._same_source_ctx_and_gov()
        sql = (
            'SELECT "p"."id", "c"."name" '
            'FROM "gql_shop"."products" AS "p" '
            'LEFT JOIN "gql_shop"."categories" AS "c" ON "c"."id" = "p"."category_id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=False,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, "Expected V002 for same-source remote join without bypass"

    def test_cross_source_local_to_remote_fails_even_with_bypass(self):
        """local (postgresql) + remote (graphql_remote) — cross-source, bypass does NOT apply.
        A covered_pair relationship must be registered to allow this join."""
        local_meta = _meta(1, "orders", source_type="postgresql")
        remote_meta = _meta(
            2, "products", source_type="graphql_remote", source_id="gql-shop", domain_id="shop"
        )
        ctx = CompilationContext()
        ctx.tables = {"orders": local_meta, "products": remote_meta}
        gov = _gov(("public.orders", 1), ("gql_shop.products", 2))
        sql = (
            'SELECT "orders"."id", "p"."sku" '
            'FROM "public"."orders" '
            'LEFT JOIN "gql_shop"."products" AS "p" ON "p"."order_id" = "orders"."id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, (
            "Expected V002 for local↔remote cross-source join — bypass only applies to "
            "same-source remote pairs; cross-source joins require a covered_pair"
        )

    def test_cross_source_different_remote_fails_even_with_bypass(self):
        """Two remote tables from DIFFERENT source_ids — bypass does NOT apply."""
        meta_gql = _meta(
            1, "products", source_type="graphql_remote", source_id="gql-shop", domain_id="shop"
        )
        meta_grpc = _meta(
            2, "inventory", source_type="grpc_remote", source_id="grpc-inv", domain_id="inventory"
        )
        ctx = CompilationContext()
        ctx.tables = {"products": meta_gql, "inventory": meta_grpc}
        gov = _gov(("gql_shop.products", 1), ("grpc_inv.inventory", 2))
        sql = (
            'SELECT "p"."id" '
            'FROM "gql_shop"."products" AS "p" '
            'LEFT JOIN "grpc_inv"."inventory" AS "inv" ON "inv"."product_ref" = "p"."id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, (
            "Expected V002 for remote↔different-remote cross-source join — "
            "bypass only applies to same-source remote pairs"
        )

    def test_grpc_same_source_uncovered_join_passes_with_bypass(self):
        """Two grpc_remote tables from the same source_id — bypass applies."""
        meta_a = _meta(
            1, "orders", source_type="grpc_remote", source_id="grpc-inv", domain_id="inventory"
        )
        meta_b = _meta(
            2,
            "inventory",
            source_type="grpc_remote",
            source_id="grpc-inv",
            domain_id="inventory",
            schema_name="grpc_inv",
        )
        ctx = CompilationContext()
        ctx.tables = {"orders": meta_a, "inventory": meta_b}
        gov = _gov(("public.orders", 1), ("grpc_inv.inventory", 2))
        sql = (
            'SELECT "orders"."id" '
            'FROM "public"."orders" AS "orders" '
            'LEFT JOIN "grpc_inv"."inventory" AS "inv" ON "inv"."order_ref" = "orders"."id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002 == [], (
            f"Expected no V002 for same-source grpc_remote join with bypass, got: {v002}"
        )


# ---------------------------------------------------------------------------
# bypass_uncovered_relationships: covered pairs (bypass does NOT apply)
# ---------------------------------------------------------------------------


class TestCoveredPairsNotBypassed:
    """When a pair IS registered (covered_pairs), bypass_uncovered has no effect.

    The join must use the exact registered columns to pass V002.
    Joins with wrong columns on a registered pair always fail.
    """

    def _ctx_and_gov(self) -> tuple[CompilationContext, GovernanceContext]:
        orders_meta = _meta(1, "orders", source_type="postgresql")
        customers_meta = _meta(2, "customers", source_type="postgresql")

        join_meta = JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=customers_meta,
            cardinality="many-to-one",
        )
        ctx = CompilationContext()
        ctx.tables = {"orders": orders_meta, "customers": customers_meta}
        ctx.joins = {("Orders", "customer"): join_meta}
        gov = _gov(("public.orders", 1), ("public.customers", 2))
        return ctx, gov

    def test_registered_pair_correct_columns_passes(self):
        ctx, gov = self._ctx_and_gov()
        sql = (
            'SELECT "o"."id", "c"."name" '
            'FROM "public"."orders" AS "o" '
            'LEFT JOIN "public"."customers" AS "c" ON "c"."id" = "o"."customer_id"'
        )
        violations = validate_sql(sql, ctx, gov, _ADMIN_ROLE, [])
        v002 = [v for v in violations if v.code == "V002"]
        assert v002 == [], f"Expected no V002 for correct columns, got: {v002}"

    def test_registered_pair_wrong_columns_fails_with_bypass(self):
        """bypass_uncovered does not exempt wrong-column joins on registered pairs."""
        ctx, gov = self._ctx_and_gov()
        sql = (
            'SELECT "o"."id" '
            'FROM "public"."orders" AS "o" '
            'LEFT JOIN "public"."customers" AS "c" ON "c"."id" = "o"."wrong_col"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, (
            "Expected V002 for wrong-column join on registered pair even with bypass — "
            "bypass only applies to completely unregistered table pairs"
        )

    def test_registered_pair_wrong_columns_fails_without_bypass(self):
        ctx, gov = self._ctx_and_gov()
        sql = (
            'SELECT "o"."id" '
            'FROM "public"."orders" AS "o" '
            'LEFT JOIN "public"."customers" AS "c" ON "c"."id" = "o"."wrong_col"'
        )
        violations = validate_sql(sql, ctx, gov, _ADMIN_ROLE, [])
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, "Expected V002 for wrong-column join on registered pair"


# ---------------------------------------------------------------------------
# bypass_uncovered_relationships: non-remote uncovered pairs (no bypass)
# ---------------------------------------------------------------------------


class TestNonRemoteUncoveredPairs:
    """bypass_uncovered does NOT exempt two local (non-remote) tables."""

    def test_local_uncovered_pair_fails_with_bypass(self):
        meta_a = _meta(1, "orders", source_type="postgresql")
        meta_b = _meta(2, "shipments", source_type="postgresql")
        ctx = CompilationContext()
        ctx.tables = {"orders": meta_a, "shipments": meta_b}
        gov = _gov(("public.orders", 1), ("public.shipments", 2))
        sql = (
            'SELECT "o"."id" '
            'FROM "public"."orders" AS "o" '
            'LEFT JOIN "public"."shipments" AS "s" ON "s"."order_ref" = "o"."id"'
        )
        violations = validate_sql(
            sql,
            ctx,
            gov,
            _ADMIN_ROLE,
            [],
            bypass_uncovered_relationships=True,
        )
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, (
            "Expected V002 for unregistered join between two local tables even with bypass — "
            "bypass only applies when at least one side is graphql_remote or grpc_remote"
        )


# ---------------------------------------------------------------------------
# sql_to_cypher: inverse relationship collision fix
# ---------------------------------------------------------------------------


class TestInverseRelationshipCollision:
    """Regression: two inverse rels sharing the same column pair must emit distinct rel_types.

    pets→assignments uses (breed_name → breedName); assignments→pets uses
    (breedName → breed_name).  Before the fix, join_to_rel keyed only by
    (col1, col2) so the second rel overwrote the first.  After the fix, the
    key includes the target label, so each direction resolves correctly.
    """

    def _make_ctx_and_label_map(self) -> tuple[object, CypherLabelMap]:
        from dataclasses import dataclass, field as dc_field

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
            column_presets: list = dc_field(default_factory=list)
            source_type: str = ""

        @dataclass
        class _Ctx:
            tables: dict = dc_field(default_factory=dict)
            joins: dict = dc_field(default_factory=dict)
            aggregate_columns: dict = dc_field(default_factory=dict)
            pk_columns: dict = dc_field(default_factory=dict)

        pets_meta = _TableMeta(
            table_id=1,
            field_name="pets",
            type_name="Pets",
            source_id="pg",
            catalog_name="postgresql",
            schema_name="public",
            table_name="pets",
        )
        assignments_meta = _TableMeta(
            table_id=2,
            field_name="assignments",
            type_name="Assignments",
            source_id="shelter-pg",
            catalog_name="shelter_pg",
            schema_name="shelter",
            table_name="assignments",
        )
        ctx = _Ctx(
            tables={"pets": pets_meta, "assignments": assignments_meta},
            aggregate_columns={
                1: [("id", "integer"), ("breed_name", "varchar")],
                2: [("breedName", "varchar"), ("employee_id", "integer")],
            },
        )
        pets_node = NodeMapping(
            label="Pets",
            type_name="Pets",
            domain_label=None,
            table_label="Pets",
            table_id=1,
            source_id="pg",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name="pets",
            properties={"id": "id", "breedName": "breed_name"},
        )
        assignments_node = NodeMapping(
            label="Assignments",
            type_name="Assignments",
            domain_label=None,
            table_label="Assignments",
            table_id=2,
            source_id="shelter-pg",
            id_column="breedName",
            pk_columns=[],
            catalog_name="shelter_pg",
            schema_name="shelter",
            table_name="assignments",
            properties={"breedName": "breedName"},
        )
        # Forward: pets → assignments (breed_name → breedName), cardinality many-to-one
        pets_to_assignments = RelationshipMapping(
            rel_type="IS_ASSIGNMENT",
            source_label="Pets",
            target_label="Assignments",
            join_source_column="breed_name",
            join_target_column="breedName",
            field_name="assignment",
            many=False,
        )
        # Inverse: assignments → pets (breedName → breed_name), cardinality one-to-many
        assignments_to_pets = RelationshipMapping(
            rel_type="HAS_PETS",
            source_label="Assignments",
            target_label="Pets",
            join_source_column="breedName",
            join_target_column="breed_name",
            field_name="pets",
            many=True,
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "Assignments": assignments_node},
            relationships={
                "IS_ASSIGNMENT::Pets→Assignments": pets_to_assignments,
                "HAS_PETS::Assignments→Pets": assignments_to_pets,
            },
            nodes_by_table={"Pets": ["Pets"], "Assignments": ["Assignments"]},
        )
        return ctx, lm

    def test_forward_join_pets_to_assignments_correct_rel_type(self):
        """Joining FROM pets TO assignments must emit IS_ASSIGNMENT, not HAS_PETS."""
        ctx, lm = self._make_ctx_and_label_map()
        # FROM pets LEFT JOIN assignments ON assignments.breedName = pets.breed_name
        sql = (
            'SELECT "pets"."id", "a"."breedName" '
            'FROM "public"."pets" '
            'LEFT JOIN "shelter"."assignments" AS "a" ON "a"."breedName" = "pets"."breed_name"'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, "semantic_sql_to_cypher returned None"
        assert "IS_ASSIGNMENT" in result, (
            f"Expected IS_ASSIGNMENT for pets→assignments join but got: {result}"
        )
        assert "HAS_PETS" not in result, (
            f"Forward pets→assignments join must not emit HAS_PETS: {result}"
        )

    def test_inverse_join_assignments_to_pets_correct_rel_type(self):
        """Joining FROM assignments TO pets must emit HAS_PETS, not IS_ASSIGNMENT."""
        ctx, lm = self._make_ctx_and_label_map()
        # FROM assignments LEFT JOIN pets ON pets.breed_name = assignments.breedName
        sql = (
            'SELECT "a"."breedName", "p"."id" '
            'FROM "shelter"."assignments" AS "a" '
            'LEFT JOIN "public"."pets" AS "p" ON "p"."breed_name" = "a"."breedName"'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, "semantic_sql_to_cypher returned None"
        assert "HAS_PETS" in result, (
            f"Expected HAS_PETS for assignments→pets join but got: {result}"
        )
        assert "IS_ASSIGNMENT" not in result, (
            f"Inverse assignments→pets join must not emit IS_ASSIGNMENT: {result}"
        )
