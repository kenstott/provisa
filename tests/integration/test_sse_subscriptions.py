# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1b2c3d-4e5f-6789-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for SSE live query subscriptions.

Pure-logic tests (TestSSEFanout, TestLiveEngineWatermark) have been moved to
tests/unit/test_live_sse.py — they require no infrastructure.

Only live PG LISTEN/NOTIFY tests remain here.
"""

from __future__ import annotations

import asyncio
import json
import os

import pytest
import pytest_asyncio

from provisa.subscriptions.base import ChangeEvent

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _pg_env() -> dict:
    return dict(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )


async def _try_pg_pool():
    """Create an asyncpg pool; raises if PG is unavailable."""
    import asyncpg  # noqa: PLC0415
    env = _pg_env()
    pool = await asyncio.wait_for(
        asyncpg.create_pool(
            host=env["host"],
            port=env["port"],
            database=env["database"],
            user=env["user"],
            password=env["password"],
            min_size=1,
            max_size=3,
            command_timeout=10,
        ),
        timeout=5.0,
    )
    return pool


# ---------------------------------------------------------------------------
# PgNotificationProvider integration test (skipped if PG unavailable)
# ---------------------------------------------------------------------------

class TestPgNotificationProvider:
    async def test_pg_provider_yields_change_event(self):
        """Provider yields ChangeEvent when a NOTIFY arrives on the channel."""
        pool = await _try_pg_pool()

        from provisa.subscriptions.pg_provider import PgNotificationProvider, CHANNEL_PREFIX

        provider = PgNotificationProvider(pool)
        table = "orders"
        channel = f"{CHANNEL_PREFIX}{table}"
        received: list[ChangeEvent] = []

        async def _consume():
            async for event in provider.watch(table):
                received.append(event)
                break  # stop after first event

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.2)  # let listener register

        # Send a NOTIFY from a separate connection
        async with pool.acquire() as notify_conn:
            payload = json.dumps({"op": "insert", "row": {"id": 42, "amount": 9.99}})
            await notify_conn.execute(f"SELECT pg_notify($1, $2)", channel, payload)

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            pytest.fail("Provider did not yield an event within timeout")

        assert len(received) == 1
        evt = received[0]
        assert evt.operation == "insert"
        assert evt.table == table
        assert evt.row["id"] == 42
        await pool.close()
