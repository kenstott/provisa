# Copyright (c) 2025 Kenneth Stott
# Canary: 765dec45-1089-4344-8706-4ad1b4e16fc6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for persisted query registry — store/retrieve/approve/deprecate in PG."""

import pytest
import pytest_asyncio

from provisa.registry import store
from provisa.registry.approval import flag_queries_for_table

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture
async def conn(pg_pool):
    async with pg_pool.acquire() as c:
        # Ensure schema exists
        from pathlib import Path
        schema_sql = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
        await c.execute(schema_sql)
        # Clean up from prior runs
        await c.execute("DELETE FROM approval_log")
        await c.execute("DELETE FROM persisted_queries")
        yield c


class TestSubmitAndRetrieve:
    async def test_submit_returns_id(self, conn):
        qid = await store.submit(
            conn, "{ orders { id } }", 'SELECT "id" FROM orders',
            [1], "dev@example.com",
        )
        assert qid > 0

    async def test_get_by_id(self, conn):
        qid = await store.submit(
            conn, "{ orders { id } }", 'SELECT "id" FROM orders',
            [1], "dev@example.com",
        )
        q = await store.get_by_id(conn, qid)
        assert q is not None
        assert q["query_text"] == "{ orders { id } }"
        assert q["status"] == "pending"
        assert q["stable_id"] is None

    async def test_list_pending(self, conn):
        await store.submit(conn, "q1", "sql1", [1], "dev1")
        await store.submit(conn, "q2", "sql2", [1], "dev2")
        pending = await store.list_pending(conn)
        assert len(pending) >= 2


class TestApproval:
    async def test_approve_assigns_stable_id(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        stable = await store.approve(conn, qid, "steward")
        assert stable  # UUID string
        q = await store.get_by_id(conn, qid)
        assert q["status"] == "approved"
        assert q["stable_id"] == stable
        assert q["approved_by"] == "steward"

    async def test_get_by_stable_id(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        stable = await store.approve(conn, qid, "steward")
        q = await store.get_by_stable_id(conn, stable)
        assert q is not None
        assert q["id"] == qid

    async def test_list_approved(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        await store.approve(conn, qid, "steward")
        approved = await store.list_approved(conn)
        assert any(a["id"] == qid for a in approved)


class TestDeprecation:
    async def test_deprecate(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        await store.approve(conn, qid, "steward")
        await store.deprecate(conn, qid, "steward", "new-stable-id")
        q = await store.get_by_id(conn, qid)
        assert q["status"] == "deprecated"
        assert q["deprecated_by"] == "new-stable-id"


class TestFlagging:
    async def test_flag_for_review(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        await store.approve(conn, qid, "steward")
        await store.flag_for_review(conn, qid, "table changed")
        q = await store.get_by_id(conn, qid)
        assert q["status"] == "flagged"

    async def test_flag_queries_for_table(self, conn):
        qid1 = await store.submit(conn, "q1", "sql1", [1], "dev")
        await store.approve(conn, qid1, "steward")
        qid2 = await store.submit(conn, "q2", "sql2", [2], "dev")
        await store.approve(conn, qid2, "steward")
        # Flag queries referencing table 1
        count = await flag_queries_for_table(conn, 1)
        assert count >= 1
        q1 = await store.get_by_id(conn, qid1)
        q2 = await store.get_by_id(conn, qid2)
        assert q1["status"] == "flagged"
        assert q2["status"] == "approved"  # table 2 not affected


class TestAuditLog:
    async def test_log_entries_created(self, conn):
        qid = await store.submit(conn, "q1", "sql1", [1], "dev")
        await store.approve(conn, qid, "steward")
        log = await store.get_log(conn, qid)
        actions = [entry["action"] for entry in log]
        assert "submitted" in actions
        assert "approved" in actions
