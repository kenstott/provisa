# Copyright (c) 2026 Kenneth Stott
# Canary: c15130a2-7378-4bd9-ba77-5095179572be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-932: a change event received from a provider is published downstream as an SSE frame.

Broker-free: a fake provider stands in for Debezium/Kafka so the publish path (provider.watch →
mask/RLS → ``data:`` frame) is exercised without infrastructure. The provider *selection* from
change_signal is covered in test_source_cdc_config::TestProviderRouting; this covers the delivery.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from provisa.api.data.subscribe import _stream_provider_events
from provisa.subscriptions.base import ChangeEvent


class _FakeProvider:
    """Yields a fixed set of change events, then completes. Records close()."""

    def __init__(self, events):
        self._events = events
        self.closed = False

    async def watch(self, table, filter_expr=None):
        for ev in self._events:
            yield ev

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_change_events_published_as_sse_frames():
    events = [
        ChangeEvent(operation="insert", table="orders", row={"id": 1, "status": "new"}),
        ChangeEvent(operation="delete", table="orders", row={"id": 1}),
    ]
    provider = _FakeProvider(events)

    frames = [
        frame
        async for frame in _stream_provider_events(
            provider,
            table="orders",
            table_id=None,
            role_id=None,
            rls_contexts={},
            masking_rules=None,
            disconnect=asyncio.Event(),
        )
    ]

    assert frames[0] == ": connected\n\n"
    payloads = [json.loads(f.removeprefix("data: ").strip()) for f in frames[1:]]
    assert payloads == [
        {"op": "INSERT", "row": {"id": 1, "status": "new"}},
        {"op": "DELETE", "row": {"id": 1}},
    ]
    assert provider.closed  # generator drains → provider.close() in finally


@pytest.mark.asyncio
async def test_disconnect_stops_the_stream():
    disconnect = asyncio.Event()
    disconnect.set()  # already disconnected before the first event
    provider = _FakeProvider([ChangeEvent(operation="insert", table="orders", row={"id": 1})])

    frames = [
        frame
        async for frame in _stream_provider_events(
            provider,
            table="orders",
            table_id=None,
            role_id=None,
            rls_contexts={},
            masking_rules=None,
            disconnect=disconnect,
        )
    ]

    # Only the connected preamble; the event is skipped because disconnect is set.
    assert frames == [": connected\n\n"]
    assert provider.closed
