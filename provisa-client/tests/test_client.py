# Copyright (c) 2026 Kenneth Stott
# Canary: d6ad4309-bafd-4cef-bcca-69d54a523da7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import json
import pytest
import respx
import httpx

from provisa_client import ProvisaClient

BASE = "http://localhost:8001"


@pytest.fixture
def client():
    return ProvisaClient(BASE, token="tok", role="analyst")


# ── query() ──────────────────────────────────────────────────────────────────

@respx.mock
def test_query_returns_raw_response(client):
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"orders": [{"id": 1}]}})
    )
    result = client.query("{ orders { id } }")
    assert result["data"]["orders"] == [{"id": 1}]


@respx.mock
def test_query_sends_auth_and_role_headers(client):
    route = respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"x": []}})
    )
    client.query("{ x { id } }")
    req = route.calls[0].request
    assert req.headers["authorization"] == "Bearer tok"
    assert req.headers["x-role"] == "analyst"


@respx.mock
def test_query_sends_variables(client):
    route = respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"orders": []}})
    )
    client.query("query Q($id: ID!) { orders(id: $id) { id } }", {"id": "42"})
    body = json.loads(route.calls[0].request.content)
    assert body["variables"] == {"id": "42"}


@respx.mock
def test_query_no_token(capfd):
    c = ProvisaClient(BASE, role="guest")
    route = respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"x": []}})
    )
    c.query("{ x { id } }")
    req = route.calls[0].request
    assert "authorization" not in req.headers


# ── query_df() ───────────────────────────────────────────────────────────────

@respx.mock
def test_query_df_returns_dataframe(client):
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200, json={"data": {"orders": [{"id": 1, "amount": 9.99}, {"id": 2, "amount": 4.50}]}}
        )
    )
    df = client.query_df("{ orders { id amount } }")
    assert list(df.columns) == ["id", "amount"]
    assert len(df) == 2
    assert df["id"].tolist() == [1, 2]


@respx.mock
def test_query_df_raises_on_graphql_errors(client):
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200, json={"errors": [{"message": "field 'bad' not found"}]}
        )
    )
    with pytest.raises(RuntimeError, match="field 'bad' not found"):
        client.query_df("{ bad { field } }")


# ── aquery() ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_aquery_returns_response(client):
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"orders": [{"id": 99}]}})
    )
    result = await client.aquery("{ orders { id } }")
    assert result["data"]["orders"] == [{"id": 99}]


@pytest.mark.asyncio
@respx.mock
async def test_aquery_sends_role_header(client):
    route = respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"x": []}})
    )
    await client.aquery("{ x { id } }")
    assert route.calls[0].request.headers["x-role"] == "analyst"


# ── _flight_ticket() ─────────────────────────────────────────────────────────

def test_flight_ticket_encodes_query_and_role(client):
    ticket = client._flight_ticket("{ orders { id } }", None)
    data = json.loads(ticket.ticket.decode())
    assert data["query"] == "{ orders { id } }"
    assert data["role"] == "analyst"


def test_flight_ticket_includes_variables(client):
    ticket = client._flight_ticket("{ orders { id } }", {"limit": 10})
    data = json.loads(ticket.ticket.decode())
    assert data["variables"] == {"limit": 10}


def test_flight_ticket_omits_variables_when_none(client):
    ticket = client._flight_ticket("{ orders { id } }", None)
    data = json.loads(ticket.ticket.decode())
    assert "variables" not in data
