# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-596 — Audit Logging."""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid

import asyncpg
import pytest
import pytest_asyncio
from pytest_bdd import given, scenarios, then, when

from provisa.audit.query_log import init_audit_schema, log_query

scenarios("../features/REQ-596.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


@pytest_asyncio.fixture
async def audit_pool():
    """Real asyncpg pool against the configured PostgreSQL instance."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    dsn = os.getenv(
        "PROVISA_DATABASE_URL",
        "postgresql://provisa:provisa@localhost:5432/provisa",
    )
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        await init_audit_schema(pool, org_id="default")
        async with pool.acquire() as conn:
            await conn.execute("SET search_path TO org_default")
        yield pool
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("any query executed against the system", target_fixture="shared_data")
@pytest.mark.integration
def given_any_query(shared_data: dict):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    query_text = (
        f"SELECT id, name FROM customers WHERE region = 'EU' -- {uuid.uuid4()}"
    )
    shared_data["query_text"] = query_text
    shared_data["expected_hash"] = hashlib.sha256(query_text.encode()).hexdigest()
    shared_data["tenant_id"] = str(uuid.uuid4())
    shared_data["user_id"] = f"user-{uuid.uuid4()}"
    shared_data["role_id"] = "analyst"
    shared_data["table_ids"] = ["customers"]
    shared_data["source"] = "graphql"
    shared_data["status_code"] = 200
    shared_data["duration_ms"] = 42
    return shared_data


@when("the query completes")
@pytest.mark.integration
def when_query_completes(shared_data: dict, audit_pool):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _do_log():
        await log_query(
            audit_pool,
            tenant_id=shared_data["tenant_id"],
            user_id=shared_data["user_id"],
            role_id=shared_data["role_id"],
            query_text=shared_data["query_text"],
            table_ids=shared_data["table_ids"],
            source=shared_data["source"],
            status_code=shared_data["status_code"],
            duration_ms=shared_data["duration_ms"],
        )

    asyncio.run(_do_log())


@then(
    "it is recorded in query_audit_log with required fields and only the SHA-256 hash of the query text")
@pytest.mark.integration
def then_recorded_with_hash_only(shared_data: dict, audit_pool):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _fetch_and_assert():
        async with audit_pool.acquire() as conn:
            await conn.execute("SET search_path TO org_default")
            row = await conn.fetchrow(
                "SELECT tenant_id, user_id, role_id, query_hash, table_ids,"
                " source, status_code, duration_ms, logged_at"
                " FROM query_audit_log WHERE user_id = $1 ORDER BY id DESC LIMIT 1",
                shared_data["user_id"],
            )

            assert row is not None, "query was not recorded in query_audit_log"

            # All required fields present and correct
            assert str(row["tenant_id"]) == shared_data["tenant_id"]
            assert row["user_id"] == shared_data["user_id"]
            assert row["role_id"] == shared_data["role_id"]
            assert list(row["table_ids"]) == shared_data["table_ids"]
            assert row["source"] == shared_data["source"]
            assert row["status_code"] == shared_data["status_code"]
            assert row["duration_ms"] == shared_data["duration_ms"]
            assert row["logged_at"] is not None

            # Only the SHA-256 hash is stored — never the verbatim query text
            assert row["query_hash"] == shared_data["expected_hash"]
            assert len(row["query_hash"]) == 64
            assert shared_data["query_text"] not in row["query_hash"]

            # Verify the raw query text appears nowhere in the row
            for value in row.values():
                if isinstance(value, str):
                    assert shared_data["query_text"] not in value

    asyncio.run(_fetch_and_assert())


@then("the table is append-only (DELETE and UPDATE are blocked)")
@pytest.mark.integration
def then_append_only(shared_data: dict, audit_pool):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _assert_immutable():
        async with audit_pool.acquire() as conn:
            await conn.execute("SET search_path TO org_default")

            before = await conn.fetchval(
                "SELECT count(*) FROM query_audit_log WHERE user_id = $1",
                shared_data["user_id"],
            )
            assert before >= 1

            # DELETE is silently rewritten to NOTHING by the PG rule
            await conn.execute(
                "DELETE FROM query_audit_log WHERE user_id = $1",
                shared_data["user_id"],
            )
            after_delete = await conn.fetchval(
                "SELECT count(*) FROM query_audit_log WHERE user_id = $1",
                shared_data["user_id"],
            )
            assert after_delete == before, "DELETE was not blocked — log not append-only"

            # UPDATE is silently rewritten to NOTHING by the PG rule
            await conn.execute(
                "UPDATE query_audit_log SET status_code = 500 WHERE user_id = $1",
                shared_data["user_id"],
            )
            row = await conn.fetchrow(
                "SELECT status_code FROM query_audit_log"
                " WHERE user_id = $1 ORDER BY id DESC LIMIT 1",
                shared_data["user_id"],
            )
            assert row["status_code"] == shared_data["status_code"], (
                "UPDATE was not blocked — log not append-only"
            )

    asyncio.run(_assert_immutable())


@then("two indexes support tenant-scoped and per-user time-range queries")
@pytest.mark.integration
def then_indexes_present(shared_data: dict, audit_pool):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _assert_indexes():
        async with audit_pool.acquire() as conn:
            await conn.execute("SET search_path TO org_default")

            index_names = {
                r["indexname"]
                for r in await conn.fetch(
                    "SELECT indexname FROM pg_indexes"
                    " WHERE schemaname = 'org_default'"
                    " AND tablename = 'query_audit_log'"
                )
            }

            assert "idx_audit_tenant_time" in index_names, (
                "tenant-scoped time-range index missing"
            )
            assert "idx_audit_user_time" in index_names, (
                "per-user time-range index missing"
            )

            # Confirm the planner can use the tenant index for a time-range scan
            tenant_plan = await conn.fetchval(
                "EXPLAIN (FORMAT TEXT)"
                " SELECT * FROM query_audit_log"
                " WHERE tenant_id = $1 ORDER BY logged_at DESC LIMIT 10",
                shared_data["tenant_id"],
            )
            assert tenant_plan is not None

    asyncio.run(_assert_indexes())
