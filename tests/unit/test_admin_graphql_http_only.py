# Copyright (c) 2026 Kenneth Stott
# Canary: 6ebc46ac-ddd0-45d3-becd-4912a6976caf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin GraphQL API answers HTTP alone (REQ-1958).

The auth middleware resolves an identity for HTTP requests only; a WebSocket passes through it
unresolved. The admin schema has no subscriptions, so its router offers no WebSocket protocol: a
connection is closed before any query runs, and no admin resolver is ever reached without the
identity its catalog and rights are read from."""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect


def test_a_websocket_to_the_admin_graphql_api_runs_no_query(monkeypatch):
    monkeypatch.setenv("PROVISA_CONFIG", "config/provisa.yaml")
    from provisa.api.admin import schema_query
    from provisa.api.app import create_app

    reached: list[str] = []

    async def _tables(self, info):
        reached.append("tables")
        return []

    monkeypatch.setattr(schema_query.Query, "tables", _tables)
    client = TestClient(create_app())
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(
            "/admin/graphql", subprotocols=["graphql-transport-ws"]
        ) as ws:
            ws.send_json({"type": "connection_init"})
            ws.send_json(
                {"type": "subscribe", "id": "1", "payload": {"query": "{ tables { id } }"}}
            )
            ws.receive_json()
    assert reached == []
