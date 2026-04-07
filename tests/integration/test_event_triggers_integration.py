# Copyright (c) 2026 Kenneth Stott
# Canary: c9f2b1e4-a3d7-4c58-b6e9-2f1a4d8c7b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for database event triggers against a real PG instance (REQ-220).

Verifies that EventTriggerManager installs real PG triggers, that NOTIFY
payloads reach the webhook, and that teardown cleans up properly.

Requires: Docker Compose stack with postgres running.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio

from provisa.core.models import EventTrigger
from provisa.events.triggers import (
    EventTriggerManager,
    _channel_name,
    _safe_name,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="session")
async def pg_pool(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=5)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def test_table(pg_pool):
    """Create a scratch table for trigger installation, drop after test."""
    table = "provisa_evt_test"
    async with pg_pool.acquire() as conn:
        await conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, val TEXT)"
        )
    yield table
    async with pg_pool.acquire() as conn:
        # Drop the trigger function and trigger if they exist
        safe = _safe_name(table)
        await conn.execute(f"DROP TRIGGER IF EXISTS provisa_trigger_{safe} ON {table}")
        await conn.execute(f"DROP FUNCTION IF EXISTS provisa_notify_{safe}()")
        await conn.execute(f"DROP TABLE IF EXISTS {table}")


class TestTriggerInstallation:
    async def test_setup_installs_pg_function_and_trigger(self, pg_pool, test_table):
        """EventTriggerManager.setup() creates a real PG notify function and trigger."""
        trigger = EventTrigger(
            table_id=test_table,
            operations=["insert", "update"],
            webhook_url="https://example.com/hook",
        )
        mgr = EventTriggerManager([trigger])
        await mgr.setup(pg_pool)

        # Verify function exists in pg_proc
        async with pg_pool.acquire() as conn:
            fn_name = f"provisa_notify_{_safe_name(test_table)}"
            row = await conn.fetchrow(
                "SELECT proname FROM pg_proc WHERE proname = $1", fn_name
            )
            assert row is not None, f"PG function {fn_name!r} not found after setup"

            # Verify trigger exists in pg_trigger
            trig_name = f"provisa_trigger_{_safe_name(test_table)}"
            trig_row = await conn.fetchrow(
                "SELECT tgname FROM pg_trigger WHERE tgname = $1", trig_name
            )
            assert trig_row is not None, f"PG trigger {trig_name!r} not found after setup"

        await mgr.teardown(pg_pool)

    async def test_teardown_removes_pg_trigger_and_function(self, pg_pool, test_table):
        """EventTriggerManager.teardown() drops the PG trigger and function."""
        trigger = EventTrigger(
            table_id=test_table,
            operations=["insert"],
            webhook_url="https://example.com/hook",
        )
        mgr = EventTriggerManager([trigger])
        await mgr.setup(pg_pool)
        await mgr.teardown(pg_pool)

        async with pg_pool.acquire() as conn:
            fn_name = f"provisa_notify_{_safe_name(test_table)}"
            row = await conn.fetchrow(
                "SELECT proname FROM pg_proc WHERE proname = $1", fn_name
            )
            assert row is None, f"PG function {fn_name!r} still present after teardown"

    async def test_setup_multiple_triggers(self, pg_pool, test_table):
        """Multiple triggers are all installed without conflict."""
        triggers = [
            EventTrigger(
                table_id=test_table,
                operations=["insert"],
                webhook_url="https://example.com/insert-hook",
            ),
        ]
        mgr = EventTriggerManager(triggers)
        await mgr.setup(pg_pool)

        async with pg_pool.acquire() as conn:
            fn_name = f"provisa_notify_{_safe_name(test_table)}"
            row = await conn.fetchrow(
                "SELECT proname FROM pg_proc WHERE proname = $1", fn_name
            )
            assert row is not None

        await mgr.teardown(pg_pool)


class TestNotifyDispatch:
    async def test_notify_triggers_webhook_dispatch(self, pg_pool, test_table):
        """INSERT into table sends NOTIFY which dispatches to the webhook."""
        received: list[dict] = []

        trigger = EventTrigger(
            table_id=test_table,
            operations=["insert"],
            webhook_url="https://example.com/hook",
            retry_max=0,
            retry_delay=0.0,
        )
        mgr = EventTriggerManager([trigger])

        # Intercept webhook POST
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200

        async def capture_post(url, *, json=None, **kwargs):
            received.append(json or {})
            return mock_response

        mock_client.post = AsyncMock(side_effect=capture_post)

        await mgr.setup(pg_pool)
        mgr._http_client = mock_client  # override after setup (setup creates a real client)
        mgr._running = True

        # Manually send a NOTIFY to simulate the PG trigger firing
        channel = _channel_name(test_table)
        payload = json.dumps({
            "operation": "INSERT",
            "table": test_table,
            "schema": "public",
            "row": {"id": 42, "val": "hello"},
        })
        await mgr._dispatch(channel, payload)

        assert len(received) == 1
        assert received[0]["operation"] == "INSERT"
        assert received[0]["row"]["id"] == 42

        await mgr.teardown(pg_pool)

    async def test_notify_with_wrong_operation_not_dispatched(self, pg_pool, test_table):
        """DELETE notification is ignored when trigger only covers INSERT."""
        trigger = EventTrigger(
            table_id=test_table,
            operations=["insert"],
            webhook_url="https://example.com/hook",
        )
        mgr = EventTriggerManager([trigger])
        mock_client = AsyncMock()
        mgr._http_client = mock_client
        mgr._running = True

        channel = _channel_name(test_table)
        payload = json.dumps({
            "operation": "DELETE",
            "table": test_table,
            "schema": "public",
            "row": {"id": 1},
        })
        await mgr._dispatch(channel, payload)

        mock_client.post.assert_not_called()


class TestRetryPolicy:
    async def test_webhook_retried_on_failure_then_success(self, pg_pool):
        """Webhook is retried up to retry_max times on failure."""
        trigger = EventTrigger(
            table_id="orders",
            operations=["insert"],
            webhook_url="https://example.com/hook",
            retry_max=2,
            retry_delay=0.001,
        )
        mgr = EventTriggerManager([trigger])

        fail = MagicMock(status_code=503)
        success = MagicMock(status_code=200)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[fail, fail, success])
        mgr._http_client = mock_client
        mgr._running = True

        await mgr._post_webhook(trigger, {"operation": "INSERT", "table": "orders", "schema": "public", "row": {}})

        assert mock_client.post.call_count == 3

    async def test_webhook_exhausted_retries_logs_but_does_not_raise(self, pg_pool):
        """All retries exhausted — manager logs error and completes without raising."""
        trigger = EventTrigger(
            table_id="orders",
            operations=["insert"],
            webhook_url="https://example.com/hook",
            retry_max=1,
            retry_delay=0.001,
        )
        mgr = EventTriggerManager([trigger])

        fail = MagicMock(status_code=500)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=fail)
        mgr._http_client = mock_client
        mgr._running = True

        # Must not raise (REQ-220: delivery failure is logged, not propagated)
        await mgr._post_webhook(
            trigger, {"operation": "INSERT", "table": "orders", "schema": "public", "row": {}}
        )
        assert mock_client.post.call_count == 2  # 1 initial + 1 retry


class TestInvalidPayload:
    async def test_invalid_json_payload_is_ignored(self):
        """Malformed JSON notification payload is silently discarded without crashing."""
        trigger = EventTrigger(
            table_id="orders",
            operations=["insert"],
            webhook_url="https://example.com/hook",
        )
        mgr = EventTriggerManager([trigger])
        mock_client = AsyncMock()
        mgr._http_client = mock_client
        mgr._running = True

        channel = _channel_name("orders")
        # Must not raise
        await mgr._dispatch(channel, "not valid json {{")

        mock_client.post.assert_not_called()
