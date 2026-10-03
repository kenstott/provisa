# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8f2c6d-b3e1-5907-8d2a-f0c9e7b5d341
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for batch mutation execution (Phase AA7, REQ-217).

Verifies that multiple mutations submitted in a single GraphQL document are
each compiled independently and correctly, with RLS and rights applied per
operation.
"""

from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import compile_mutation
from tests.write_governance import admitted, write_governance
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build(extra_tables=None):
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]
    if extra_tables:
        tables.extend(extra_tables)

    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("status", "varchar(20)"),
        ],
        2: [_col("id", "integer"), _col("name", "varchar(100)"), _col("email", "varchar(255)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestTwoInsertMutations:
    """Two INSERT mutations in one document are each compiled."""

    def test_two_inserts_produce_two_results(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { amount: 42.0, region: "us-east" }) { affected_rows }
                insertCustomers(input: { name: "Alice", email: "alice@example.com" }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 2

    def test_first_insert_targets_orders(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { amount: 10.0, region: "eu" }) { affected_rows }
                insertCustomers(input: { name: "Bob", email: "bob@example.com" }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        orders_result = next(r for r in results if r.table_name == "orders")
        assert orders_result.mutation_type == "insert"
        assert "INSERT INTO" in orders_result.sql
        assert '"amount"' in orders_result.sql
        assert '"region"' in orders_result.sql

    def test_second_insert_targets_customers(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { amount: 10.0, region: "eu" }) { affected_rows }
                insertCustomers(input: { name: "Bob", email: "bob@example.com" }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        cust_result = next(r for r in results if r.table_name == "customers")
        assert cust_result.mutation_type == "insert"
        assert "INSERT INTO" in cust_result.sql
        assert '"name"' in cust_result.sql

    def test_params_are_independent(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { amount: 99.9, region: "us-west" }) { affected_rows }
                insertCustomers(input: { name: "Carol", email: "carol@example.com" }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        orders_r = next(r for r in results if r.table_name == "orders")
        customers_r = next(r for r in results if r.table_name == "customers")
        # Each mutation has its own parameter list starting at $1
        assert orders_r.params == [99.9, "us-west"]
        assert "Carol" in customers_r.params


class TestInsertAndUpdate:
    """INSERT and UPDATE in the same document compile to different SQL."""

    def test_insert_and_update_different_sql_types(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { amount: 5.0, region: "ca" }) { affected_rows }
                updateOrders(set: { status: "shipped" }, where: { id: { eq: 1 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 2
        types = {r.mutation_type for r in results}
        assert types == {"insert", "update"}

    def test_insert_uses_insert_sql(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { region: "au" }) { affected_rows }
                updateOrders(set: { status: "done" }, where: { id: { eq: 2 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        insert_r = next(r for r in results if r.mutation_type == "insert")
        assert "INSERT INTO" in insert_r.sql
        assert "UPDATE" not in insert_r.sql

    def test_update_uses_update_sql(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertOrders(input: { region: "au" }) { affected_rows }
                updateOrders(set: { status: "done" }, where: { id: { eq: 2 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        update_r = next(r for r in results if r.mutation_type == "update")
        assert "UPDATE" in update_r.sql
        assert "SET" in update_r.sql
        assert "INSERT" not in update_r.sql


class TestInsertAndDelete:
    """INSERT and DELETE in the same document compile independently."""

    def test_two_results_produced(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertCustomers(input: { name: "Dave", email: "dave@example.com" }) { affected_rows }
                deleteOrders(where: { id: { eq: 99 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 2

    def test_delete_has_where_clause(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                insertCustomers(input: { name: "Eve", email: "eve@example.com" }) { affected_rows }
                deleteOrders(where: { id: { eq: 7 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        delete_r = next(r for r in results if r.mutation_type == "delete")
        assert "DELETE FROM" in delete_r.sql
        assert "WHERE" in delete_r.sql


_BOTH = {
    "public.orders": (1, ["id", "status", "region"]),
    "public.customers": (2, ["id", "name", "active"]),
}


class TestRLSAppliedPerMutation:
    """Each mutation in a batch is admitted on its own, by the one write admission, with the
    role's filter for the table that mutation writes."""

    def test_rls_applied_to_each_update(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                updateOrders(set: { status: "done" }, where: { id: { eq: 1 } }) { affected_rows }
                updateCustomers(set: { name: "Frank" }, where: { id: { eq: 2 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        # One role, a filter on each table.
        gov = write_governance(_BOTH, rls={1: "region = 'us'", 2: "active = true"})

        orders_r = next(r for r in results if r.table_name == "orders")
        customers_r = next(r for r in results if r.table_name == "customers")
        orders_sql = admitted(orders_r.sql, gov, orders_r.params)
        customers_sql = admitted(customers_r.sql, gov, customers_r.params)

        assert "\"region\" = 'us'" in orders_sql
        assert '"active" = TRUE' in customers_sql.replace("true", "TRUE")
        # A table's filter does not reach another table's statement
        assert "region" not in customers_sql
        assert "active" not in orders_sql

    def test_rls_not_applied_when_no_rules(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                updateOrders(set: { status: "x" }, where: { id: { eq: 1 } }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        r = results[0]
        assert admitted(r.sql, write_governance(_BOTH), r.params) == r.sql


class TestSameTableBatch:
    """Two operations on the same table are compiled as separate statements.

    GraphQL requires aliases when the same field name is used twice with
    different arguments in the same selection set.
    """

    def test_two_inserts_same_table(self):
        schema, ctx = _build()
        # Aliases required when using the same mutation field twice
        doc = parse("""
            mutation {
                first: insertOrders(input: { amount: 1.0, region: "x" }) { affected_rows }
                second: insertOrders(input: { amount: 2.0, region: "y" }) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 2
        assert all(r.mutation_type == "insert" for r in results)
        assert all(r.table_name == "orders" for r in results)
        # Parameters must differ
        params = [r.params for r in results]
        assert params[0] != params[1]
