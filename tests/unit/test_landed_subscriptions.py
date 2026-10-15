# Copyright (c) 2026 Kenneth Stott
# Canary: 7b9d7aa6-2026-4172-ace0-7c3d0226b56d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subscription on a table a push listener lands (Kafka, WebSocket) re-runs when a batch has
landed in it -- told by the listener, which runs on a thread of its own -- and never for another
org's table of the same name (REQ-258, REQ-1733).

Before: on DuckDB it took the engine-polling path, which a native engine does not offer, or built
a Kafka consumer with no brokers; and read from the topic it would have raced the landing."""

# Requirements: REQ-258, REQ-1733

from __future__ import annotations

import asyncio
import json
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.subscriptions import landed as hub

pytestmark = pytest.mark.unbound


def _from_thread(org: str, *key: str) -> None:
    """A landing, as the listener reports it: on its own thread, its org bound."""

    def _run() -> None:
        token = set_current_org(org)
        try:
            hub.landed(*key)
        finally:
            reset_current_org(token)

    thread = threading.Thread(target=_run)
    thread.start()
    thread.join()


async def test_a_landing_reaches_the_subscriptions_of_its_org_s_table_only():
    token = set_current_org("acme")
    try:
        provider = hub.LandedNotificationProvider("orders", "default")
        changes = provider.watch("orders")
        first = asyncio.ensure_future(changes.__anext__())
        await asyncio.sleep(0)  # the subscription is waiting
        _from_thread("globex", "orders", "default", "orders")  # another org's table
        _from_thread("acme", "orders", "default", "refunds")  # another table
        await asyncio.sleep(0.05)
        assert not first.done()
        _from_thread("acme", "orders", "default", "orders")
        event = await asyncio.wait_for(first, timeout=2)
        assert event.operation == "landed" and event.table == "orders"
        await provider.close()
        await changes.aclose()
    finally:
        reset_current_org(token)
    assert hub._waiting == {}  # a closed subscription waits for nothing


async def test_a_kafka_table_s_subscription_re_runs_when_a_batch_lands():
    from fastapi.responses import StreamingResponse

    from provisa.api.data.subscription_sse import handle_subscription_sse
    from tests.unit.test_subscription_sink_header import (
        _make_minimal_document,
        _make_request,
        _make_state,
    )

    state, ctx, table_meta = _make_state(source_id="orders")
    state.source_types = {"orders": "kafka"}
    table_meta.schema_name = "default"
    request = _make_request({})
    request.is_disconnected = AsyncMock(return_value=False)
    directives = MagicMock(sink_topic=None, sink_broker=None, watermark_column=None)
    runs = iter([{"data": {"orders": [1]}}, {"data": {"orders": [1, 2]}}])

    token = set_current_org("acme")
    try:
        with (
            patch("provisa.compiler.parser.parse_query", MagicMock()),
            patch(
                "provisa.api.data.endpoint._handle_query",
                AsyncMock(side_effect=lambda *a, **k: next(runs)),
            ),
            patch(
                "provisa.subscriptions.registry.get_provider", MagicMock(side_effect=AssertionError)
            ),
        ):
            response = await handle_subscription_sse(
                document=_make_minimal_document("orders"),
                ctx=ctx,
                rls=MagicMock(),
                state=state,
                variables=None,
                role="user",
                role_id="role1",
                raw_request=request,
                directives=directives,
            )
            assert isinstance(response, StreamingResponse)
            frames = response.body_iterator
            first = await frames.__anext__()
            second = asyncio.ensure_future(frames.__anext__())
            for _ in range(50):  # until the subscription waits on its table
                if hub._waiting:
                    break
                await asyncio.sleep(0.01)
            _from_thread("acme", "orders", "default", "orders")
            second_frame = await asyncio.wait_for(second, timeout=2)
            await frames.aclose()
    finally:
        reset_current_org(token)
    assert json.loads(first.removeprefix("data: ")) == {"data": {"orders": [1]}}
    assert json.loads(second_frame.removeprefix("data: ")) == {"data": {"orders": [1, 2]}}
