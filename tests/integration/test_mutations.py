# Copyright (c) 2026 Kenneth Stott
# Canary: 4c9e2b1a-7f3d-4a8e-9c6b-1d5f0e2a8b3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for GraphQL mutation compilation and execution.

Tests INSERT, UPDATE, DELETE, and UPSERT against the real PostgreSQL instance.
All test data is cleaned up after each test using the pg_pool fixture.
"""

import os

import pytest
import pytest_asyncio
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import (
    apply_column_presets,
    compile_mutation,
)
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# Sentinel region tag used to identify and clean up all test-inserted rows
_TEST_REGION = "integration-test-mutations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx():
    """Build a minimal SchemaInput + CompilationContext for mutation tests."""
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
                {"column_name": "product_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer", nullable=False),
            _col("customer_id", "integer", nullable=True),
            _col("product_id", "integer", nullable=True),
            _col("amount", "decimal(10,2)", nullable=True),
            _col("region", "varchar(50)", nullable=True),
            _col("status", "varchar(20)", nullable=True),
        ],
    }
    role = {"id": "admin", "capabilities": ["admin", "query_development"], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


# ---------------------------------------------------------------------------
# Session-scoped source pool
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def mut_pool():
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


async def _cleanup(pg_pool):
    """Delete all rows inserted by mutation tests using the sentinel region."""
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM orders WHERE region = $1", _TEST_REGION
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInsertMutation:

    async def test_insert_mutation(self, mut_pool, pg_pool):
        """Compile + execute insert mutation; verify the row is persisted."""
        schema, ctx = _build_schema_and_ctx()

        # Get a valid customer_id from the DB
        async with pg_pool.acquire() as conn:
            cid = await conn.fetchval("SELECT id FROM customers LIMIT 1")

        # Get a valid product_id from the DB
        async with pg_pool.acquire() as conn:
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")

        doc = parse(f"""
            mutation {{
                insert_orders(input: {{
                    customer_id: {cid},
                    product_id: {pid},
                    amount: 99.99,
                    region: "{_TEST_REGION}"
                }}) {{ affected_rows }}
            }}
        """)
        assert not validate(schema, doc)
        mutations = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(mutations) == 1
        m = mutations[0]
        assert m.mutation_type == "insert"

        try:
            result = await execute_direct(mut_pool, "sales-pg", m.sql, m.params)
            # INSERT ... RETURNING returns the inserted row
            assert len(result.rows) == 1

            # Verify row exists in the DB
            async with pg_pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM orders WHERE region = $1", _TEST_REGION
                )
            assert count >= 1
        finally:
            await _cleanup(pg_pool)

    async def test_update_mutation(self, mut_pool, pg_pool):
        """Compile + execute update mutation; verify affected rows are updated."""
        schema, ctx = _build_schema_and_ctx()

        # Insert a row to update
        async with pg_pool.acquire() as conn:
            cid = await conn.fetchval("SELECT id FROM customers LIMIT 1")
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
            await conn.execute(
                "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4)",
                cid, pid, 50.00, _TEST_REGION,
            )

        try:
            doc = parse(f"""
                mutation {{
                    update_orders(
                        set: {{ amount: 111.11 }},
                        where: {{ region: {{ eq: "{_TEST_REGION}" }} }}
                    ) {{ affected_rows }}
                }}
            """)
            assert not validate(schema, doc)
            mutations = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
            assert len(mutations) == 1
            m = mutations[0]
            assert m.mutation_type == "update"

            result = await execute_direct(mut_pool, "sales-pg", m.sql, m.params)
            # UPDATE ... RETURNING returns updated rows
            assert len(result.rows) >= 1
            amount_idx = result.column_names.index("amount")
            for row in result.rows:
                # Amount should be updated to 111.11
                assert float(row[amount_idx]) == pytest.approx(111.11, abs=0.01)
        finally:
            await _cleanup(pg_pool)

    async def test_delete_mutation(self, mut_pool, pg_pool):
        """Compile + execute delete mutation; verify rows are removed."""
        schema, ctx = _build_schema_and_ctx()

        # Insert a row to delete
        async with pg_pool.acquire() as conn:
            cid = await conn.fetchval("SELECT id FROM customers LIMIT 1")
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
            await conn.execute(
                "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4)",
                cid, pid, 77.77, _TEST_REGION,
            )

        doc = parse(f"""
            mutation {{
                delete_orders(where: {{ region: {{ eq: "{_TEST_REGION}" }} }}) {{
                    affected_rows
                }}
            }}
        """)
        assert not validate(schema, doc)
        mutations = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(mutations) == 1
        m = mutations[0]
        assert m.mutation_type == "delete"

        result = await execute_direct(mut_pool, "sales-pg", m.sql, m.params)
        # DELETE ... RETURNING * returns deleted rows
        assert len(result.rows) >= 1

        # Verify rows are gone
        async with pg_pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM orders WHERE region = $1", _TEST_REGION
            )
        assert count == 0

    async def test_upsert_mutation(self, mut_pool, pg_pool):
        """Test the upsert path (ON CONFLICT ... DO UPDATE)."""
        schema, ctx = _build_schema_and_ctx()

        # Insert an initial row
        async with pg_pool.acquire() as conn:
            cid = await conn.fetchval("SELECT id FROM customers LIMIT 1")
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
            inserted_id = await conn.fetchval(
                "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4) RETURNING id",
                cid, pid, 10.00, _TEST_REGION,
            )

        try:
            # Upsert on the id column — update amount
            doc = parse(f"""
                mutation {{
                    upsert_orders(
                        input: {{ id: {inserted_id}, customer_id: {cid}, product_id: {pid}, amount: 200.00, region: "{_TEST_REGION}" }},
                        on_conflict: [id]
                    ) {{ affected_rows }}
                }}
            """)
            assert not validate(schema, doc)
            mutations = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
            assert len(mutations) == 1
            m = mutations[0]
            assert m.mutation_type == "upsert"
            assert "ON CONFLICT" in m.sql
            assert "DO UPDATE" in m.sql

            result = await execute_direct(mut_pool, "sales-pg", m.sql, m.params)
            # RETURNING clause returns the upserted row
            assert len(result.rows) >= 1
        finally:
            await _cleanup(pg_pool)

    async def test_mutation_respects_column_preset(self, mut_pool, pg_pool):
        """Column preset is applied automatically (test via checking inserted row)."""
        schema, ctx = _build_schema_and_ctx()

        async with pg_pool.acquire() as conn:
            cid = await conn.fetchval("SELECT id FROM customers LIMIT 1")
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")

        # Build input and apply a literal preset for the region column
        input_data = {"customer_id": cid, "product_id": pid, "amount": 55.55}
        presets = [{"column": "region", "source": "literal", "value": _TEST_REGION}]
        enriched = apply_column_presets(input_data, presets)

        # Verify the preset was applied before the mutation is even compiled
        assert enriched["region"] == _TEST_REGION
        assert enriched["customer_id"] == cid
        assert float(enriched["amount"]) == pytest.approx(55.55)

        try:
            # Now manually build and execute the INSERT SQL so we can verify the preset row
            from provisa.compiler.sql_gen import _q
            from provisa.compiler.params import ParamCollector

            collector = ParamCollector()
            cols = list(enriched.keys())
            placeholders = [collector.add(enriched[c]) for c in cols]
            cols_sql = ", ".join(_q(c) for c in cols)
            vals_sql = ", ".join(placeholders)
            sql = (
                f'INSERT INTO "public"."orders" ({cols_sql}) VALUES ({vals_sql})'
                f' RETURNING {cols_sql}'
            )
            result = await execute_direct(mut_pool, "sales-pg", sql, collector.params)
            assert len(result.rows) == 1
            region_idx = result.column_names.index("region")
            assert result.rows[0][region_idx] == _TEST_REGION
        finally:
            await _cleanup(pg_pool)

    async def test_mutation_blocked_for_read_only_column(self, mut_pool, pg_pool):
        """Mutation on a NoSQL (non-writable) source type raises an appropriate error."""
        schema, ctx = _build_schema_and_ctx()

        doc = parse(f"""
            mutation {{
                insert_orders(input: {{ customer_id: 1, amount: 9.99, region: "{_TEST_REGION}" }}) {{
                    affected_rows
                }}
            }}
        """)
        assert not validate(schema, doc)

        # Pass mongodb as the source type — mutations on NoSQL sources are blocked
        with pytest.raises(ValueError, match="NoSQL"):
            compile_mutation(doc, ctx, {"sales-pg": "mongodb"})
