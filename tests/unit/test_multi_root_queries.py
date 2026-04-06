# Copyright (c) 2026 Kenneth Stott
# Canary: e4f5a6b7-c8d9-4e0f-1a2b-3c4d5e6f7a8b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for multi-root GraphQL query handling.

Architecture note: Provisa uses Trino federation (single-engine, single query
per root field) and NOT client-side parallel execution. When a GraphQL document
contains multiple root fields, compile_query returns one CompiledQuery per root
field. The executor handles them independently (each gets its own SQL).

The run_pipeline function in compiler/pipeline.py iterates over the list
returned by compile_graphql and applies governance (RLS, masking, sampling)
to each independently.

These tests verify:
1. Two root fields in one document → two CompiledQuery objects.
2. Each CompiledQuery has the correct root_field name.
3. Each root field compiles to its own independent SQL.
4. RLS is applied per-root-field (different tables → different filters).
5. Sampling is applied per-root-field independently.
6. An unknown root field causes a ValueError (not silent skipping).
"""

from __future__ import annotations

import pytest

from graphql import build_schema, parse

from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
    compile_query,
)

# ---------------------------------------------------------------------------
# Shared schema / context helpers
# ---------------------------------------------------------------------------

_SDL = """
type Query {
    orders: [Order]
    customers: [Customer]
}

type Order {
    id: Int
    amount: Float
    region: String
}

type Customer {
    id: Int
    name: String
    region: String
}
"""

_SCHEMA = build_schema(_SDL)

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2


def _orders_meta() -> TableMeta:
    return TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Order",
        source_id="pg-main",
        catalog_name="pg_main",
        schema_name="public",
        table_name="orders",
    )


def _customers_meta() -> TableMeta:
    return TableMeta(
        table_id=CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customer",
        source_id="pg-main",
        catalog_name="pg_main",
        schema_name="public",
        table_name="customers",
    )


def _two_table_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _orders_meta()
    ctx.tables["customers"] = _customers_meta()
    return ctx


def _single_table_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _orders_meta()
    return ctx


# ---------------------------------------------------------------------------
# Field parsing / result structure
# ---------------------------------------------------------------------------


class TestTwoRootFieldsParsedCorrectly:
    """compile_query returns one CompiledQuery per root selection field."""

    def test_two_root_fields_return_two_compiled_queries(self):
        """{ orders { id } customers { name } } → two CompiledQuery objects."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)

        assert len(results) == 2

    def test_root_field_names_are_correct(self):
        """Each CompiledQuery.root_field matches the GraphQL field name."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)
        names = {r.root_field for r in results}

        assert "orders" in names
        assert "customers" in names

    def test_single_root_field_returns_one_compiled_query(self):
        """A single root field → exactly one CompiledQuery."""
        doc = parse("{ orders { id amount } }")
        ctx = _single_table_ctx()

        results = compile_query(doc, ctx)

        assert len(results) == 1
        assert results[0].root_field == "orders"

    def test_field_alias_used_as_root_field_name(self):
        """An aliased root field uses the alias as root_field."""
        doc = parse("{ myOrders: orders { id } }")
        ctx = _single_table_ctx()

        results = compile_query(doc, ctx)

        assert len(results) == 1
        assert results[0].root_field == "myOrders"


# ---------------------------------------------------------------------------
# SQL independence
# ---------------------------------------------------------------------------


class TestIndependentFieldCompilation:
    """Each root field compiles to its own SQL statement."""

    def test_orders_sql_references_orders_table(self):
        """orders root field SQL must reference the 'orders' physical table."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)
        orders_compiled = next(r for r in results if r.root_field == "orders")

        assert '"orders"' in orders_compiled.sql
        assert '"customers"' not in orders_compiled.sql

    def test_customers_sql_references_customers_table(self):
        """customers root field SQL must reference the 'customers' physical table."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)
        customers_compiled = next(r for r in results if r.root_field == "customers")

        assert '"customers"' in customers_compiled.sql
        assert "orders" not in customers_compiled.sql.lower().replace("customers", "")

    def test_two_root_fields_produce_different_sql(self):
        """The two root fields must compile to distinct SQL strings."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)

        assert results[0].sql != results[1].sql

    def test_selected_columns_per_field_are_independent(self):
        """Columns selected in each root field are independent of each other."""
        doc = parse("{ orders { id amount } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)
        orders_q = next(r for r in results if r.root_field == "orders")
        customers_q = next(r for r in results if r.root_field == "customers")

        orders_cols = {c.field_name for c in orders_q.columns}
        customers_cols = {c.field_name for c in customers_q.columns}

        assert "id" in orders_cols
        assert "amount" in orders_cols
        assert "name" in customers_cols
        # orders does not leak into customers columns and vice versa
        assert "amount" not in customers_cols
        assert "name" not in orders_cols

    def test_sources_set_per_compiled_query(self):
        """Each CompiledQuery carries its own sources set."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()

        results = compile_query(doc, ctx)

        for r in results:
            assert isinstance(r.sources, set)
            assert len(r.sources) >= 1


# ---------------------------------------------------------------------------
# RLS applied per root field
# ---------------------------------------------------------------------------


class TestRLSAppliedPerRootField:
    """inject_rls applies rules per-table; multi-root queries each get correct RLS."""

    def test_orders_rls_injected_into_orders_query_only(self):
        """RLS rule for orders table is injected only into the orders SQL."""
        doc = parse("{ orders { id region } customers { name } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        orders_q = next(r for r in results if r.root_field == "orders")
        customers_q = next(r for r in results if r.root_field == "customers")

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})

        orders_with_rls = inject_rls(orders_q, ctx, rls)
        customers_with_rls = inject_rls(customers_q, ctx, rls)

        # Orders gets the filter
        assert "region = 'us-east'" in orders_with_rls.sql
        # Customers does NOT (different table_id, no rule for it)
        assert "region = 'us-east'" not in customers_with_rls.sql

    def test_customers_rls_injected_into_customers_query_only(self):
        """RLS rule for customers table is injected only into the customers SQL."""
        doc = parse("{ orders { id } customers { name region } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        orders_q = next(r for r in results if r.root_field == "orders")
        customers_q = next(r for r in results if r.root_field == "customers")

        rls = RLSContext(rules={CUSTOMERS_TABLE_ID: "region = 'eu-west'"})

        orders_with_rls = inject_rls(orders_q, ctx, rls)
        customers_with_rls = inject_rls(customers_q, ctx, rls)

        assert "region = 'eu-west'" not in orders_with_rls.sql
        assert "region = 'eu-west'" in customers_with_rls.sql

    def test_both_tables_have_rls_each_receives_its_own_filter(self):
        """When both tables have RLS rules, each query receives only its own filter."""
        doc = parse("{ orders { id region } customers { name region } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        orders_q = next(r for r in results if r.root_field == "orders")
        customers_q = next(r for r in results if r.root_field == "customers")

        rls = RLSContext(rules={
            ORDERS_TABLE_ID: "region = 'us-east'",
            CUSTOMERS_TABLE_ID: "region = 'eu-west'",
        })

        orders_final = inject_rls(orders_q, ctx, rls)
        customers_final = inject_rls(customers_q, ctx, rls)

        assert "us-east" in orders_final.sql
        assert "eu-west" not in orders_final.sql

        assert "eu-west" in customers_final.sql
        assert "us-east" not in customers_final.sql

    def test_no_rls_rules_sql_unchanged(self):
        """Empty RLS context leaves SQL unchanged for all root fields."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        rls = RLSContext.empty()

        for r in results:
            after = inject_rls(r, ctx, rls)
            assert after.sql == r.sql


# ---------------------------------------------------------------------------
# Sampling applied per root field
# ---------------------------------------------------------------------------


class TestSamplingAppliedPerRootField:
    """apply_sampling applies independently to each CompiledQuery."""

    def test_sampling_applied_to_each_root_field(self):
        """Each CompiledQuery in the list gets its own LIMIT."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        sampled = [apply_sampling(r, 25) for r in results]

        for s in sampled:
            assert "LIMIT 25" in s.sql

    def test_sampling_does_not_cross_contaminate_root_fields(self):
        """Sampling one root field's CompiledQuery does not affect the other."""
        doc = parse("{ orders { id } customers { name } }")
        ctx = _two_table_ctx()
        results = compile_query(doc, ctx)

        orders_q = next(r for r in results if r.root_field == "orders")
        customers_q = next(r for r in results if r.root_field == "customers")

        # Apply different sample sizes to each
        orders_sampled = apply_sampling(orders_q, 10)
        customers_sampled = apply_sampling(customers_q, 50)

        assert "LIMIT 10" in orders_sampled.sql
        assert "LIMIT 50" in customers_sampled.sql
        # The original objects are unchanged (CompiledQuery is effectively immutable here)
        assert "LIMIT" not in orders_q.sql
        assert "LIMIT" not in customers_q.sql


# ---------------------------------------------------------------------------
# Error behaviour
# ---------------------------------------------------------------------------


class TestUnknownRootFieldError:
    """compile_query raises ValueError for unrecognised root fields."""

    def test_unknown_root_field_raises_value_error(self):
        """A root field not present in ctx.tables must raise ValueError."""
        doc = parse("{ orders { id } }")
        ctx = CompilationContext()  # empty — orders not registered

        with pytest.raises(ValueError, match="Unknown root query field"):
            compile_query(doc, ctx)

    def test_partial_unknown_raises_on_first_unknown(self):
        """If one of two root fields is unknown, ValueError is raised."""
        # Only 'orders' is registered; 'customers' is missing.
        doc = parse("{ orders { id } customers { name } }")
        ctx = _single_table_ctx()

        with pytest.raises(ValueError, match="Unknown root query field"):
            compile_query(doc, ctx)
