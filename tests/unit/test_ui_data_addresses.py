# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b2c61-9a7d-4f30-b5e1-6c0d3a9f7e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a coordinator-served UI sends its data calls (REQ-1916, amended 2026-11-02): each region
declares its data addresses; with none declared, the region's live query and every nodes at the
addresses they registered; an every node's UI is offered nothing. Query nodes take the UI's
cross-origin data calls with its sign-in token."""

# Requirements: REQ-1916

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.core import data_addresses, process_mode, process_region
from provisa.core.regions import PlatformConfig

_UI = "https://app.example.com"


@pytest.fixture(autouse=True)
def _launch():
    mode, region = process_mode.mode(), process_region._region
    yield
    process_mode.set_mode(mode)
    process_region._region = region


def _platform(**eu) -> PlatformConfig:
    return PlatformConfig.model_validate(
        {
            "regions": [
                {"id": "eu", "address": "https://eu.example.com", **eu},
                {"id": "us", "address": "https://us.example.com"},
            ]
        }
    )


# --- the config shape -----------------------------------------------------------------------------


def test_a_region_declares_its_data_addresses():
    platform = _platform(data_addresses=["https://data.eu.example.com", "http://10.0.0.7:8001"])
    assert platform.regions[0].data_addresses == [
        "https://data.eu.example.com",
        "http://10.0.0.7:8001",
    ]
    assert platform.regions[1].data_addresses == []


@pytest.mark.parametrize(
    "address",
    ["data.eu.example.com", "ftp://data.example.com", "https://", "https://d.example.com/?a=1"],
)
def test_a_data_address_that_is_not_an_http_url_is_refused(address):
    with pytest.raises(ValidationError, match="is not an http\\(s\\) URL"):
        _platform(data_addresses=[address])


def test_a_data_address_listed_twice_is_refused():
    with pytest.raises(ValidationError, match="lists a data address twice"):
        _platform(data_addresses=["https://d.example.com", "https://d.example.com"])


# --- the list -------------------------------------------------------------------------------------


def _node(mode: str, region: str | None, address: str | None) -> dict:
    node = {"node_id": f"{mode}-{address}", "mode": mode, "address": address}
    if region is not None:
        node["region"] = region
    return node


def test_registered_nodes_are_the_regions_live_data_nodes_with_an_address():
    live = [
        _node("query", "eu", "https://q1.eu.example.com"),
        _node("coordinator", "eu", "https://c.eu.example.com"),
        _node("every", "eu", "https://e.eu.example.com"),
        _node("query", "us", "https://q.us.example.com"),
        _node("query", "eu", None),
        _node("query", "eu", "https://q1.eu.example.com"),
    ]
    assert data_addresses.registered(live, "eu") == [
        "https://q1.eu.example.com",
        "https://e.eu.example.com",
    ]


def test_registered_nodes_in_the_implicit_region_carry_no_region():
    live = [_node("query", None, "http://q:8001"), _node("coordinator", None, "http://c:8001")]
    assert data_addresses.registered(live, "default") == ["http://q:8001"]


def test_the_declared_list_is_the_regions_own():
    platform = _platform(data_addresses=["https://lb.eu.example.com"])
    assert data_addresses.declared(platform, "eu") == ["https://lb.eu.example.com"]
    assert data_addresses.declared(platform, "us") == []
    assert data_addresses.declared(PlatformConfig(), "default") == []


@pytest.fixture
async def platform_db(control_plane):
    from provisa.core.database import Database

    return Database(control_plane.engine(), "platform-state", holds="platform_state")


async def _register_query_node(db, address: str | None, monkeypatch) -> None:
    from provisa.core.platform_state import nodes

    if address is None:
        monkeypatch.delenv("PROVISA_NODE_URL", raising=False)
    else:
        monkeypatch.setenv("PROVISA_NODE_URL", address)
    process_mode.set_mode(process_mode.QUERY)
    await nodes.register(db)


async def test_a_node_registers_the_address_it_was_launched_with(platform_db, monkeypatch):
    from provisa.core.platform_state import nodes

    process_region.bind_launch({}, requested=None)
    await _register_query_node(platform_db, "https://q7.example.com", monkeypatch)
    (node,) = await nodes.live(platform_db)
    assert node["address"] == "https://q7.example.com"
    await _register_query_node(platform_db, None, monkeypatch)
    (node,) = await nodes.live(platform_db)
    assert node["address"] is None


async def test_a_coordinator_offers_the_declared_list_over_the_registered_nodes(
    platform_db, monkeypatch
):
    platform = _platform(data_addresses=["https://lb.eu.example.com"])
    process_region.bind_launch(platform.model_dump(), requested="eu")
    await _register_query_node(platform_db, "https://q1.eu.example.com", monkeypatch)
    process_mode.set_mode(process_mode.COORDINATOR)
    assert await data_addresses.for_ui(platform, platform_db) == ["https://lb.eu.example.com"]


async def test_a_coordinator_without_declared_addresses_offers_the_registered_nodes(
    platform_db, monkeypatch
):
    platform = _platform()
    process_region.bind_launch(platform.model_dump(), requested="eu")
    await _register_query_node(platform_db, "https://q1.eu.example.com", monkeypatch)
    process_mode.set_mode(process_mode.COORDINATOR)
    assert await data_addresses.for_ui(platform, platform_db) == ["https://q1.eu.example.com"]


async def test_an_every_node_offers_nothing(platform_db):
    platform = _platform(data_addresses=["https://lb.eu.example.com"])
    process_region.bind_launch(platform.model_dump(), requested="eu")
    process_mode.set_mode(process_mode.EVERY)
    assert data_addresses.ui_chooses() is False
    assert await data_addresses.for_ui(platform, platform_db) == []


# --- the route and the query node's CORS ----------------------------------------------------------


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("PROVISA_CONFIG", "config/provisa.yaml")
    from starlette.testclient import TestClient

    from provisa.api.app import create_app

    # No lifespan runs: what is asserted is what the middleware answers.
    return TestClient(create_app(), raise_server_exceptions=False)


def test_the_route_is_the_uis_backend_so_a_query_node_refuses_it(client):
    from provisa.api.query_gate import is_ui_path

    assert is_ui_path("/admin/data-addresses")
    process_mode.set_mode(process_mode.QUERY)
    assert client.get("/admin/data-addresses").json()["code"] == "node.query_serves_no_ui"


async def test_the_route_answers_choose_and_the_list(platform_db, monkeypatch):
    from provisa.api.admin.data_addresses_router import get_data_addresses
    from provisa.api.app import state
    from provisa.core.models import ProvisaConfig

    platform = _platform(data_addresses=["https://lb.eu.example.com"])
    process_region.bind_launch(platform.model_dump(), requested="eu")
    config = ProvisaConfig.model_construct(platform=platform)
    monkeypatch.setattr(state, "config", config)
    monkeypatch.setattr(state, "platform_state_db", platform_db)
    process_mode.set_mode(process_mode.COORDINATOR)
    assert await get_data_addresses() == {
        "choose": True,
        "addresses": ["https://lb.eu.example.com"],
    }
    process_mode.set_mode(process_mode.EVERY)
    assert await get_data_addresses() == {"choose": False, "addresses": []}


class _ViewerProvider(AuthProvider):
    """Signs in one user holding only the viewer role: the least a signed-in UI user holds."""

    provider_name = "test"

    async def validate_token(self, token: str) -> AuthIdentity:
        if token == "viewer-token":
            return AuthIdentity(
                user_id="v1", email="v1@example.com", display_name="Viewer", roles=["viewer"]
            )
        raise ValueError("Invalid token")


@pytest.fixture
def signed_in_app(platform_db, monkeypatch, serve_deployment_org):
    """The route behind the bearer gate of a deployment with an auth provider configured."""
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    from provisa.api.admin.data_addresses_router import router
    from provisa.api.app import state
    from provisa.auth.middleware import AuthMiddleware
    from provisa.core.models import ProvisaConfig

    serve_deployment_org("root")
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])
    platform = _platform(data_addresses=["https://lb.eu.example.com"])
    process_region.bind_launch(platform.model_dump(), requested="eu")
    monkeypatch.setattr(state, "config", ProvisaConfig.model_construct(platform=platform))
    monkeypatch.setattr(state, "platform_state_db", platform_db)
    process_mode.set_mode(process_mode.COORDINATOR)
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=_ViewerProvider())
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def test_a_caller_with_no_sign_in_is_refused_the_list(signed_in_app):
    refused = signed_in_app.get("/admin/data-addresses")
    assert refused.status_code == 401
    assert "lb.eu.example.com" not in refused.text
    forged = signed_in_app.get(
        "/admin/data-addresses", headers={"Authorization": "Bearer not-a-token"}
    )
    assert forged.status_code == 401


def test_any_signed_in_ui_user_gets_the_list(signed_in_app):
    """The setting is every UI user's, so the least role a user can hold reads it."""
    answer = signed_in_app.get(
        "/admin/data-addresses", headers={"Authorization": "Bearer viewer-token"}
    )
    assert answer.status_code == 200
    assert answer.json() == {"choose": True, "addresses": ["https://lb.eu.example.com"]}


_PREFLIGHT = {
    "Origin": _UI,
    "Access-Control-Request-Method": "POST",
    "Access-Control-Request-Headers": "authorization,content-type,x-org-provisa,x-provisa-role",
}


def test_a_query_node_answers_the_uis_preflight_before_sign_in_is_asked_for(client):
    """A browser's preflight carries no token: it is answered before auth, naming the UI's origin
    and the headers the UI's data calls carry (its bearer among them)."""
    process_mode.set_mode(process_mode.QUERY)
    answer = client.options("/data/graphql", headers=_PREFLIGHT)
    assert answer.status_code == 200
    assert answer.headers["access-control-allow-origin"] == _UI
    assert answer.headers["access-control-allow-credentials"] == "true"
    assert "authorization" in answer.headers["access-control-allow-headers"]


def test_every_answer_to_the_ui_origin_is_readable_refusals_included(client):
    """The UI reads a refusal's code; a refusal some outer layer answers with carries the CORS
    headers too, or the browser hides it as a network failure."""
    process_mode.set_mode(process_mode.COORDINATOR)
    refused = client.post(
        "/data/graphql", json={"query": "{ __typename }"}, headers={"Origin": _UI}
    )
    assert refused.status_code == 503
    assert refused.json()["code"] == "node.coordinator_serves_no_data"
    assert refused.headers["access-control-allow-origin"] == _UI


def test_cors_is_the_outermost_layer(client):
    from starlette.middleware.cors import CORSMiddleware

    assert client.app.user_middleware[0].cls is CORSMiddleware
