# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7f43-2c1d-4a8e-9f6b-7d3a1c9e2b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A query node serves no UI (REQ-1916, amended 2026-11-02): the UI and the UI's backend reaching a
query node are refused by name, with the coordinator's UI address; data, health and the token
exchange data clients sign in with are served. The mirror of a coordinator refusing data."""

# Requirements: REQ-1916

from __future__ import annotations

import asyncio
import json

import pytest

from provisa.core import process_mode

_UI = "https://app.example.com"
_SAID = "this node runs in query mode and serves no UI"


@pytest.fixture
def in_mode():
    was = process_mode.mode()

    def _set(mode: str) -> None:
        process_mode.set_mode(mode)

    yield _set
    process_mode.set_mode(was)


def _http(path: str, *, everything: bool = False, ui_address: str | None = _UI):
    from provisa.api.query_gate import QueryUiGate, is_ui_path

    sent: list[dict] = []

    async def inner(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        sent.append(message)

    gate = QueryUiGate(
        inner,
        refuses=(lambda _path: True) if everything else is_ui_path,
        ui_address=lambda: ui_address,
    )
    asyncio.run(gate({"type": "http", "path": path, "method": "GET"}, receive, send))
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return sent[0]["status"], body


# The UI's backend: the admin API, the account and interactive sign-in routes the UI calls, its
# setup wizard, demos, branding, the control plane's pages, billing and the product panels.
_UI_PATHS = [
    "/admin/graphql",
    "/admin/orgs/acme/users",
    "/auth/me",
    "/auth/tokens",
    "/auth/provider-type",
    "/auth/superuser-login",
    "/auth/saml/login",
    "/auth/saml/acs",
    "/auth/redeem-invite",
    "/setup/status",
    "/setup/",
    "/orgs/branding",
    "/orgs/branding/logo",
    "/public/invite-info/tok",
    "/public/demos/retail",
    "/sandboxes/limits",
    "/control-plane/orgs",
    "/internal/ui-server-settings",
    "/billing/signup",
    "/data-products/p1/lineage",
    "/data-products/p1/contracts.json",
    "/data-products/p1/contracts/m1.json",
    "/data-products/p1/contracts/m1/versions.json",
]

# What a data client needs: every /data route, the other query surfaces (neo4j's HTTP API, NL
# queries), the catalog's pull of a product's open-standard documents, health and the probes, the
# API's own docs, and the password-for-token exchange the JDBC driver and scripts sign in with.
_DATA_PATHS = [
    "/data/graphql",
    "/data/sql",
    "/data/rest/orders",
    "/data/odata/Orders",
    "/data/openapi/openapi.json",
    "/",
    "/db/neo4j/query/v2",
    "/query/nl",
    "/data-products/p1/odps.yaml",
    "/data-products/p1/contracts/m1.yaml",
    "/health",
    "/live",
    "/ready",
    "/auth/login",
    "/docs",
    "/openapi.json",
    "/administrator",
    "/authors",
]


@pytest.mark.parametrize("path", _UI_PATHS)
def test_a_ui_route_on_a_query_node_is_refused_with_the_coordinator_address(in_mode, path):
    in_mode(process_mode.QUERY)
    status, body = _http(path)
    assert status == 503
    answer = json.loads(body)
    assert answer["code"] == "node.query_serves_no_ui"
    assert _SAID in answer["detail"] and _UI in answer["detail"]
    assert answer["params"] == {"ui_address": _UI}


@pytest.mark.parametrize("path", _DATA_PATHS)
def test_data_health_and_token_routes_are_served_on_a_query_node(in_mode, path):
    in_mode(process_mode.QUERY)
    assert _http(path) == (200, b"ok")


@pytest.mark.parametrize("mode", [process_mode.EVERY, process_mode.COORDINATOR])
@pytest.mark.parametrize("path", ["/admin/graphql", "/auth/me", "/setup/status", "/health"])
def test_every_and_coordinator_nodes_serve_the_ui(in_mode, mode, path):
    in_mode(mode)
    assert _http(path) == (200, b"ok")


def test_the_ui_server_on_a_query_node_refuses_every_path(in_mode):
    in_mode(process_mode.QUERY)
    for path in ("/", "/assets/index.js", "/admin/overview", "/data/graphql"):
        status, body = _http(path, everything=True)
        assert status == 503 and json.loads(body)["code"] == "node.query_serves_no_ui"


def test_the_ui_server_on_an_every_node_serves(in_mode):
    in_mode(process_mode.EVERY)
    assert _http("/", everything=True) == (200, b"ok")


def test_a_refusal_before_the_config_is_loaded_names_the_setting(in_mode):
    in_mode(process_mode.QUERY)
    status, body = _http("/admin/graphql", ui_address=None)
    answer = json.loads(body)
    assert status == 503 and answer["code"] == "node.query_serves_no_ui"
    assert "mail.base_url" in answer["detail"]
    assert answer["params"] == {"ui_address": None}


def test_refuse_ui_request_is_silent_off_query_nodes(in_mode):
    for mode in (process_mode.EVERY, process_mode.COORDINATOR):
        in_mode(mode)
        process_mode.refuse_ui_request(_UI)
    in_mode(process_mode.QUERY)
    with pytest.raises(process_mode.QueryServesNoUi) as refused:
        process_mode.refuse_ui_request(_UI)
    assert refused.value.code == "node.query_serves_no_ui"
    assert refused.value.ui_address == _UI


def _mounted_paths(app) -> list[str]:
    from starlette.routing import Mount

    found: list[str] = []

    def walk(routes, prefix: str) -> None:
        for route in routes:
            path = prefix + getattr(route, "path", "")
            found.append(path)
            if isinstance(route, Mount):
                walk(route.routes or [], path)

    walk(app.router.routes, "")
    return found


def test_the_built_api_refuses_its_ui_backend_on_a_query_node(monkeypatch, in_mode):
    """The gate is wired into the app, outside auth: the refusal comes before any credential is
    asked for, and the probes still answer."""
    monkeypatch.setenv("PROVISA_CONFIG", "config/provisa.yaml")
    from starlette.testclient import TestClient

    from provisa.api.app import create_app

    # No lifespan runs, so a route reached in every mode has no store and errors: what matters
    # there is that the gate let it through.
    client = TestClient(create_app(), raise_server_exceptions=False)
    in_mode(process_mode.QUERY)
    refused = client.post("/admin/graphql", json={"query": "{ __typename }"})
    assert refused.status_code == 503
    assert refused.json()["code"] == "node.query_serves_no_ui"
    assert client.get("/auth/me").json()["code"] == "node.query_serves_no_ui"
    assert client.get("/live").status_code == 200
    in_mode(process_mode.EVERY)
    passed = client.get("/auth/me")
    assert b"node.query_serves_no_ui" not in passed.content


def test_every_mounted_route_is_classified(monkeypatch):
    """Over the routes the app really mounts: the admin API and every /auth route but the token
    exchange are the UI's; no /data route, probe or neo4j route is."""
    monkeypatch.setenv("PROVISA_CONFIG", "config/provisa.yaml")
    from provisa.api.app import create_app
    from provisa.api.query_gate import is_ui_path

    paths = _mounted_paths(create_app())
    assert any(p.startswith("/admin/") for p in paths) and "/auth/login" in paths
    for path in paths:
        if path.startswith(("/admin/", "/setup", "/control-plane/")) or (
            path.startswith("/auth/") and path != "/auth/login"
        ):
            assert is_ui_path(path), path
        if path.startswith("/data/") or path in {"/", "/health", "/live", "/ready", "/auth/login"}:
            assert not is_ui_path(path), path
        if path.startswith(("/db/", "/query/nl")):
            assert not is_ui_path(path), path
