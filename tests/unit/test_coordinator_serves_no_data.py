# Copyright (c) 2026 Kenneth Stott
# Canary: ad26f73e-8955-49e2-8905-29c6128702ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A coordinator serves no data requests (REQ-1916): on every transport a request is refused by
name, at the transport's own boundary, before anything is read."""

# Requirements: REQ-1916

from __future__ import annotations

import asyncio

import pytest

from provisa.core import process_mode


@pytest.fixture
def coordinator():
    was = process_mode.mode()
    process_mode.set_mode(process_mode.COORDINATOR)
    yield
    process_mode.set_mode(was)


_SAID = "this node runs in coordinator mode and serves no data requests"


@pytest.mark.parametrize("transport", ["pgwire", "bolt", "grpc", "mcp", "graphql", "sql_http"])
def test_a_request_on_any_transport_is_refused_naming_it(coordinator, transport):
    from provisa.core import request_deadline

    with pytest.raises(process_mode.CoordinatorServesNoData) as refused:
        request_deadline.open_request(transport)
    assert _SAID in str(refused.value) and transport in str(refused.value)


def test_a_flight_request_is_refused(coordinator):
    from provisa.api.flight.deadline import request_budget

    with pytest.raises(process_mode.CoordinatorServesNoData, match="flight"):
        with request_budget(30.0):
            pass


@pytest.mark.parametrize("mode", [process_mode.EVERY, process_mode.QUERY])
def test_query_and_every_nodes_serve_requests(mode):
    from provisa.core import request_deadline

    was = process_mode.mode()
    process_mode.set_mode(mode)
    try:
        request_deadline.open_request("pgwire").stop()
    finally:
        process_mode.set_mode(was)


def _http(path: str) -> tuple[int, bytes]:
    from provisa.api.coordinator_gate import CoordinatorDataGate

    sent: list[dict] = []
    reached: list[str] = []

    async def inner(scope, receive, send):
        reached.append(scope["path"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        sent.append(message)

    gate = CoordinatorDataGate(inner)
    asyncio.run(gate({"type": "http", "path": path, "method": "POST"}, receive, send))
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return sent[0]["status"], body


def test_an_http_data_request_is_refused_and_the_admin_is_served(coordinator):
    status, body = _http("/data/graphql")
    assert status == 503
    assert b"node.coordinator_serves_no_data" in body and _SAID.encode() in body
    assert _http("/data/anything/else")[0] == 503
    assert _http("/admin/graphql") == (200, b"ok")
    assert _http("/health") == (200, b"ok")
