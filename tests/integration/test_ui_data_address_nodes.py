# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5d8e31-7b26-4f94-a1e3-9d2f6b8c4a07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A coordinator-served UI's data call reaches a query node (REQ-1916, amended 2026-11-02).

Two real servers on one control plane, sign-in enforced: a coordinator (which serves the UI and
refuses data) and a query node launched with the address clients reach it at. The coordinator
offers the UI that node, the UI's sign-in token is minted by the coordinator, and the query node
answers the UI's cross-origin preflight and its data call with that token."""

# Requirements: REQ-1916

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request

import bcrypt
import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema, free_port

pytestmark = [pytest.mark.integration]

_ORG = "uidataaddr"
_UI = "https://app.example.com"
_PASSWORD = "correct horse"


def _auth() -> dict:
    hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "ui-data-address-test-signing-key-32bytes",
        "simple": {"users": [{"username": "ada", "password_hash": hashed, "roles": ["org_admin"]}]},
    }


def _call(url: str, *, method: str = "GET", body: dict | None = None, headers: dict | None = None):
    req = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read()


@pytest.fixture(scope="module")
def nodes():
    config = "tests/fixtures/sample_config.yaml"
    coordinator = IsolatedServer(
        _ORG, engine="duckdb", config=config, auth=_auth(), env={"PROVISA_MODE": "coordinator"}
    )
    port = free_port()
    query = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=config,
        auth=_auth(),
        env={"PROVISA_MODE": "query", "PROVISA_NODE_URL": f"http://127.0.0.1:{port}"},
    )
    query.http_port = port  # the address it registers is the one it serves at
    coordinator.start()
    try:
        query.start()
        try:
            yield coordinator, query
        finally:
            query.stop_process()
    finally:
        coordinator.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def test_the_uis_data_call_reaches_the_query_node_the_coordinator_offers(nodes):
    coordinator, query = nodes
    status, _, body = _call(
        f"{coordinator.base_url}/auth/login",
        method="POST",
        body={"username": "ada", "password": _PASSWORD},
    )
    assert status == 200, body
    token = json.loads(body)["access_token"]
    signed_in = {"Authorization": f"Bearer {token}", "Origin": _UI}

    # The coordinator offers its region's registered query node; it serves no data itself.
    status, _, body = _call(f"{coordinator.base_url}/admin/data-addresses", headers=signed_in)
    assert status == 200, body
    assert json.loads(body) == {"choose": True, "addresses": [query.base_url]}
    status, _, body = _call(
        f"{coordinator.base_url}/data/graphql",
        method="POST",
        body={"query": "{ __typename }"},
        headers=signed_in,
    )
    assert status == 503 and json.loads(body)["code"] == "node.coordinator_serves_no_data"

    # The browser's preflight carries no token, and is answered.
    status, headers, _ = _call(
        f"{query.base_url}/data/graphql",
        method="OPTIONS",
        headers={
            "Origin": _UI,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization,content-type,x-org-provisa",
        },
    )
    lower = {k.lower(): v for k, v in headers.items()}
    assert status == 200
    assert lower["access-control-allow-origin"] == _UI
    assert "authorization" in lower["access-control-allow-headers"]

    # The data call, with the token the coordinator minted.
    status, headers, body = _call(
        f"{query.base_url}/data/graphql",
        method="POST",
        body={"query": "{ __typename }"},
        headers=signed_in,
    )
    lower = {k.lower(): v for k, v in headers.items()}
    assert status == 200, body
    assert json.loads(body)["data"]["__typename"]
    assert lower["access-control-allow-origin"] == _UI

    # Without the token the query node refuses, and the UI can read why.
    status, headers, _ = _call(
        f"{query.base_url}/data/graphql",
        method="POST",
        body={"query": "{ __typename }"},
        headers={"Origin": _UI},
    )
    assert status == 401
    assert {k.lower(): v for k, v in headers.items()}["access-control-allow-origin"] == _UI
