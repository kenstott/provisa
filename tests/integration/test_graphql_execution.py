# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3a1c2e-9b4d-4e8f-b6a0-3d5c2e1f8a9b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: end-to-end GraphQL query execution.

Build schema → compile GraphQL → execute SQL → verify results.
All tests use the real PostgreSQL instance available via conftest fixtures.
"""

import os

import pytest
import pytest_asyncio
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(*, relay_pagination: bool = False):
    """Build a minimal SchemaInput + CompilationContext matching the test DB schema."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [
            _col("id", "integer", nullable=False),
            _col("customer_id", "integer", nullable=True),
            _col("amount", "decimal(10,2)", nullable=True),
            _col("region", "varchar(50)", nullable=True),
            _col("status", "varchar(20)", nullable=True),
            _col("created_at", "timestamp", nullable=True),
        ],
        2: [
            _col("id", "integer", nullable=False),
            _col("name", "varchar(100)", nullable=True),
            _col("email", "varchar(200)", nullable=True),
            _col("region", "varchar(50)", nullable=True),
        ],
    }
    role = {"id": "admin", "capabilities": ["admin", "query_development"], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
        relay_pagination=relay_pagination,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


# ---------------------------------------------------------------------------
# Session-scoped source pool
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def exec_pool():
    sp = SourcePool()
    await sp.add(
        "sales-pg",
        source_type="postgresql",
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    yield sp
    await sp.close_all()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGraphQLExecution:

    async def test_simple_list_query(self, exec_pool):
        """`{ orders { id amount region } }` returns rows with correct columns."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse("{ orders { id amount region } }")
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        assert len(results) == 1
        compiled = results[0]

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert "id" in result.column_names
        assert "amount" in result.column_names
        assert "region" in result.column_names
        assert len(result.rows) > 0

    async def test_filtered_query(self, exec_pool):
        """`{ orders(where: {region: {eq: "us-east"}}) { id } }` returns only matching rows."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse('{ orders(where: { region: { eq: "us-east" } }) { id region } }')
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        assert len(results) == 1
        compiled = results[0]

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert "region" in result.column_names
        region_idx = result.column_names.index("region")
        for row in result.rows:
            assert row[region_idx] == "us-east"

    async def test_limit_offset(self, exec_pool):
        """`{ orders(limit: 3, offset: 0) { id } }` returns exactly 3 rows."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse("{ orders(limit: 3, offset: 0) { id } }")
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        compiled = results[0]

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert len(result.rows) == 3
        assert "id" in result.column_names

    async def test_order_by(self, exec_pool):
        """`{ orders(order_by: [{amount: desc}]) { id amount } }` returns rows in descending order."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse("{ orders(order_by: [{ amount: desc }]) { id amount } }")
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        compiled = results[0]

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert "amount" in result.column_names
        amount_idx = result.column_names.index("amount")
        amounts = [row[amount_idx] for row in result.rows if row[amount_idx] is not None]
        assert amounts == sorted(amounts, reverse=True)

    async def test_relationship_join(self, exec_pool):
        """`{ orders { id customers { name } } }` returns nested customer data."""
        schema, ctx = _build_schema_and_ctx()
        # Relationship field name is target table's field name: "customers"
        doc = parse("{ orders { id customer_id customers { name } } }")
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        compiled = results[0]

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert "id" in result.column_names
        assert "name" in result.column_names
        assert len(result.rows) > 0
        # Verify some customer names are strings (not null for joined rows)
        name_idx = result.column_names.index("name")
        non_null_names = [r[name_idx] for r in result.rows if r[name_idx] is not None]
        assert len(non_null_names) > 0
        for name in non_null_names:
            assert isinstance(name, str)

    async def test_cursor_pagination(self, exec_pool):
        """`{ orders_connection(first: 5) { edges { node { id } } pageInfo { hasNextPage endCursor } } }`"""
        schema, ctx = _build_schema_and_ctx(relay_pagination=True)

        doc = parse("""
            {
                orders_connection(first: 5) {
                    edges { node { id } }
                    pageInfo { hasNextPage endCursor }
                }
            }
        """)
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        # Connection queries compile to at least one query
        assert len(results) >= 1
        compiled = results[0]
        assert compiled.is_connection

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        # Should return at most first+1 rows (for hasNextPage detection)
        assert len(result.rows) <= 6  # 5 + 1 lookahead

    async def test_distinct_on(self, exec_pool):
        """`{ orders(distinct_on: [region]) { region } }` returns distinct regions."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse("{ orders(distinct_on: [region]) { region } }")
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        compiled = results[0]
        assert "DISTINCT ON" in compiled.sql

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert "region" in result.column_names
        region_idx = result.column_names.index("region")
        regions = [row[region_idx] for row in result.rows]
        # Distinct: no duplicates
        assert len(regions) == len(set(regions))

    async def test_aggregate_query(self, exec_pool):
        """`{ orders_aggregate { aggregate { count sum { amount } } } }` returns aggregate values."""
        schema, ctx = _build_schema_and_ctx()
        doc = parse("""
            {
                orders_aggregate {
                    aggregate {
                        count
                        sum { amount }
                    }
                }
            }
        """)
        errors = validate(schema, doc)
        assert not errors, errors

        results = compile_query(doc, ctx)
        assert len(results) >= 1
        compiled = results[0]
        assert "COUNT(*)" in compiled.sql or "count" in compiled.sql.lower()

        result = await execute_direct(exec_pool, "sales-pg", compiled.sql, compiled.params)
        assert len(result.rows) == 1
        # Aggregate result row should have a numeric count value
        row = result.rows[0]
        assert any(isinstance(v, (int, float)) for v in row if v is not None)
