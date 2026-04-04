# Copyright (c) 2025 Kenneth Stott
# Canary: d355a790-c249-4268-92d3-1c60675e0a8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for database event triggers (Phase AB3)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import EventTrigger
from provisa.events.triggers import (
    EventTriggerManager,
    _channel_name,
    _operations_clause,
    _safe_name,
)


def _make_trigger(**kwargs) -> EventTrigger:
    defaults = {
        "table_id": "public.orders",
        "operations": ["insert", "update"],
        "webhook_url": "https://example.com/hook",
        "retry_max": 2,
        "retry_delay": 0.01,
        "enabled": True,
    }
    defaults.update(kwargs)
    return EventTrigger(**defaults)


# --- Helper unit tests ---


def test_safe_name():
    assert _safe_name("public.orders") == "public_orders"
    assert _safe_name("my-schema.my-table") == "my_schema_my_table"


def test_channel_name():
    assert _channel_name("orders") == "provisa_evt_orders"
    assert _channel_name("public.orders") == "provisa_evt_public_orders"


def test_operations_clause():
    assert _operations_clause(["insert"]) == "INSERT"
    assert _operations_clause(["insert", "update", "delete"]) == "INSERT OR UPDATE OR DELETE"


# --- EventTrigger model tests ---


def test_event_trigger_defaults():
    t = EventTrigger(table_id="orders", webhook_url="https://x.com/hook")
    assert t.operations == ["insert", "update", "delete"]
    assert t.retry_max == 3
    assert t.retry_delay == 1.0
    assert t.enabled is True


def test_event_trigger_custom():
    t = _make_trigger(operations=["delete"], retry_max=5, retry_delay=2.0)
    assert t.operations == ["delete"]
    assert t.retry_max == 5
    assert t.retry_delay == 2.0


# --- Setup installs PG triggers ---


class _MockAcquireContext:
    """Mimics asyncpg PoolAcquireContext: works as both await and async-with."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False

    def __await__(self):
        async def _resolve():
            return self._conn
        return _resolve().__await__()


def _mock_pool_with_conn(mock_conn):
    """Build a mock asyncpg.Pool whose acquire() works as await and async-with."""
    mock_pool = MagicMock()
    mock_pool.acquire.return_value = _MockAcquireContext(mock_conn)
    mock_pool.release = AsyncMock()
    return mock_pool


@pytest.mark.asyncio
async def test_setup_installs_triggers():
    trigger = _make_trigger()
    mgr = EventTriggerManager([trigger])

    mock_conn = AsyncMock()
    mock_pool = _mock_pool_with_conn(mock_conn)

    await mgr.setup(mock_pool)

    # Should have called execute for function + trigger creation
    assert mock_conn.execute.call_count == 2
    func_sql = mock_conn.execute.call_args_list[0][0][0]
    assert "provisa_notify_public_orders" in func_sql
    assert "pg_notify" in func_sql

    trig_sql = mock_conn.execute.call_args_list[1][0][0]
    assert "provisa_trigger_public_orders" in trig_sql
    assert "INSERT OR UPDATE" in trig_sql

    await mgr.teardown(mock_pool)


# --- Webhook dispatch ---


@pytest.mark.asyncio
async def test_dispatch_fires_webhook():
    """Notification payload triggers HTTP POST to webhook URL."""
    trigger = _make_trigger()
    mgr = EventTriggerManager([trigger])

    mock_response = MagicMock()
    mock_response.status_code = 200

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mgr._http_client = mock_client
    mgr._running = True

    payload = json.dumps({
        "operation": "INSERT",
        "table": "orders",
        "schema": "public",
        "row": {"id": 1, "amount": 99.99},
    })

    channel = _channel_name("public.orders")
    await mgr._dispatch(channel, payload)

    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    assert call_args[0][0] == "https://example.com/hook"
    posted_data = call_args[1]["json"]
    assert posted_data["operation"] == "INSERT"
    assert posted_data["row"]["id"] == 1


@pytest.mark.asyncio
async def test_dispatch_filters_operations():
    """Operations not in the trigger config are ignored."""
    trigger = _make_trigger(operations=["insert"])
    mgr = EventTriggerManager([trigger])

    mock_client = AsyncMock()
    mgr._http_client = mock_client
    mgr._running = True

    payload = json.dumps({
        "operation": "DELETE",
        "table": "orders",
        "schema": "public",
        "row": {"id": 1},
    })

    channel = _channel_name("public.orders")
    await mgr._dispatch(channel, payload)

    mock_client.post.assert_not_called()


# --- Retry with exponential backoff ---


@pytest.mark.asyncio
async def test_retry_on_webhook_failure():
    """Failed webhooks are retried with exponential backoff."""
    trigger = _make_trigger(retry_max=2, retry_delay=0.01)
    mgr = EventTriggerManager([trigger])

    fail_response = MagicMock()
    fail_response.status_code = 500

    success_response = MagicMock()
    success_response.status_code = 200

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=[fail_response, fail_response, success_response])
    mgr._http_client = mock_client
    mgr._running = True

    data = {"operation": "insert", "table": "orders", "schema": "public", "row": {"id": 1}}
    await mgr._post_webhook(trigger, data)

    assert mock_client.post.call_count == 3


@pytest.mark.asyncio
async def test_retry_exhaustion():
    """All retries exhausted still completes without raising."""
    trigger = _make_trigger(retry_max=1, retry_delay=0.01)
    mgr = EventTriggerManager([trigger])

    fail_response = MagicMock()
    fail_response.status_code = 502

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=fail_response)
    mgr._http_client = mock_client
    mgr._running = True

    data = {"operation": "insert", "table": "orders", "schema": "public", "row": {"id": 1}}
    await mgr._post_webhook(trigger, data)

    # retry_max=1 means 1 initial + 1 retry = 2 calls
    assert mock_client.post.call_count == 2


@pytest.mark.asyncio
async def test_retry_on_http_exception():
    """httpx.HTTPError triggers retry."""
    import httpx as _httpx

    trigger = _make_trigger(retry_max=1, retry_delay=0.01)
    mgr = EventTriggerManager([trigger])

    success_response = MagicMock()
    success_response.status_code = 200

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=[_httpx.ConnectError("connection refused"), success_response],
    )
    mgr._http_client = mock_client
    mgr._running = True

    data = {"operation": "insert", "table": "orders", "schema": "public", "row": {"id": 1}}
    await mgr._post_webhook(trigger, data)

    assert mock_client.post.call_count == 2


# --- on_notify callback ---


def test_on_notify_schedules_dispatch():
    """_on_notify schedules _dispatch via asyncio.ensure_future."""
    trigger = _make_trigger()
    mgr = EventTriggerManager([trigger])
    mgr._running = True

    mock_conn = MagicMock()
    payload = '{"operation": "INSERT", "table": "orders"}'

    with patch("provisa.events.triggers.asyncio.ensure_future") as mock_ef:
        mgr._on_notify(mock_conn, 123, _channel_name("public.orders"), payload)
        mock_ef.assert_called_once()


def test_on_notify_ignored_when_stopped():
    """_on_notify is a no-op when manager is not running."""
    trigger = _make_trigger()
    mgr = EventTriggerManager([trigger])
    mgr._running = False

    with patch("provisa.events.triggers.asyncio.ensure_future") as mock_ef:
        mgr._on_notify(MagicMock(), 123, "ch", '{}')
        mock_ef.assert_not_called()


# --- Disabled triggers ---


@pytest.mark.asyncio
async def test_disabled_trigger_skipped():
    """Disabled triggers are not installed."""
    trigger = _make_trigger(enabled=False)
    mgr = EventTriggerManager([trigger])

    mock_conn = AsyncMock()
    mock_pool = _mock_pool_with_conn(mock_conn)

    await mgr.setup(mock_pool)

    # No SQL executed for disabled trigger
    mock_conn.execute.assert_not_called()

    await mgr.teardown(mock_pool)


# --- Empty triggers ---


@pytest.mark.asyncio
async def test_empty_triggers_noop():
    """Manager with no triggers does nothing on setup."""
    mgr = EventTriggerManager([])

    mock_conn = AsyncMock()
    mock_pool = _mock_pool_with_conn(mock_conn)
    await mgr.setup(mock_pool)

    # No pool interaction for empty triggers (setup returns early)
    mock_pool.acquire.assert_not_called()

    await mgr.teardown(mock_pool)
