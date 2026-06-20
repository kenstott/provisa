# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Tests for REQ-073 control plane store and router."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.control_plane.models import DataPlane, Tenant
from provisa.control_plane.store import ControlPlaneStore


# ---------------------------------------------------------------------------
# ControlPlaneStore unit tests
# ---------------------------------------------------------------------------


def test_register_and_get_tenant():
    store = ControlPlaneStore()
    tenant = Tenant(
        id="t1", name="Acme", data_plane_id="dp1", created_at="2026-01-01T00:00:00+00:00"
    )
    store.register_tenant(tenant)
    result = store.get_tenant("t1")
    assert result.id == "t1"
    assert result.name == "Acme"


def test_register_and_route_query():
    store = ControlPlaneStore()
    dp = DataPlane(
        id="dp1",
        tenant_id="t1",
        endpoint="https://dp1.example.com",
        region="us-east-1",
        active=True,
    )
    store.register_data_plane(dp)
    tenant = Tenant(
        id="t1", name="Acme", data_plane_id="dp1", created_at="2026-01-01T00:00:00+00:00"
    )
    store.register_tenant(tenant)
    result = store.route_query("t1")
    assert result.endpoint == "https://dp1.example.com"


def test_route_query_inactive_data_plane_raises():
    store = ControlPlaneStore()
    dp = DataPlane(
        id="dp1",
        tenant_id="t1",
        endpoint="https://dp1.example.com",
        region="us-east-1",
        active=False,
    )
    store.register_data_plane(dp)
    tenant = Tenant(
        id="t1", name="Acme", data_plane_id="dp1", created_at="2026-01-01T00:00:00+00:00"
    )
    store.register_tenant(tenant)
    with pytest.raises(ValueError):
        store.route_query("t1")


def test_get_tenant_missing_raises():
    store = ControlPlaneStore()
    with pytest.raises(KeyError):
        store.get_tenant("nonexistent")


def test_list_tenants_and_data_planes():
    store = ControlPlaneStore()
    dp = DataPlane(
        id="dp1",
        tenant_id="t1",
        endpoint="https://dp1.example.com",
        region="us-east-1",
        active=True,
    )
    store.register_data_plane(dp)
    tenant = Tenant(
        id="t1", name="Acme", data_plane_id="dp1", created_at="2026-01-01T00:00:00+00:00"
    )
    store.register_tenant(tenant)
    assert len(store.list_tenants()) == 1
    assert len(store.list_data_planes()) == 1


# ---------------------------------------------------------------------------
# Router endpoint tests (FastAPI TestClient with mocked state.multitenancy)
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch):
    """Create a TestClient with multitenancy=True and a fresh router store."""
    import provisa.control_plane.router as _router_module
    import provisa.api.app as _app_module

    # Reset the module-level store so tests are isolated
    monkeypatch.setattr(_router_module, "_store", ControlPlaneStore())

    # Patch state.multitenancy = True
    monkeypatch.setattr(_app_module.state, "multitenancy", True)

    from provisa.control_plane.router import router as control_plane_router

    app = FastAPI()
    app.include_router(control_plane_router)
    return TestClient(app)


def test_router_register_data_plane(client):
    resp = client.post(
        "/control-plane/data-planes",
        json={
            "id": "dp1",
            "tenant_id": "t1",
            "endpoint": "https://dp1.example.com",
            "region": "us-east-1",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "dp1"
    assert data["active"] is True


def test_router_register_and_list_tenants(client):
    client.post(
        "/control-plane/data-planes",
        json={
            "id": "dp1",
            "tenant_id": "t1",
            "endpoint": "https://dp1.example.com",
            "region": "us-east-1",
        },
    )
    resp = client.post(
        "/control-plane/tenants",
        json={"id": "t1", "name": "Acme", "data_plane_id": "dp1"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "t1"
    assert "created_at" in data

    list_resp = client.get("/control-plane/tenants")
    assert list_resp.status_code == 200
    tenants = list_resp.json()
    assert len(tenants) == 1
    assert tenants[0]["name"] == "Acme"


def test_router_route_returns_endpoint(client):
    client.post(
        "/control-plane/data-planes",
        json={
            "id": "dp1",
            "tenant_id": "t1",
            "endpoint": "https://dp1.example.com",
            "region": "us-east-1",
        },
    )
    client.post(
        "/control-plane/tenants",
        json={"id": "t1", "name": "Acme", "data_plane_id": "dp1"},
    )
    resp = client.get("/control-plane/tenants/t1/route")
    assert resp.status_code == 200
    assert resp.json() == {"endpoint": "https://dp1.example.com"}


def test_router_route_missing_tenant_returns_404(client):
    resp = client.get("/control-plane/tenants/nonexistent/route")
    assert resp.status_code == 404


def test_router_list_data_planes(client):
    client.post(
        "/control-plane/data-planes",
        json={
            "id": "dp1",
            "tenant_id": "t1",
            "endpoint": "https://dp1.example.com",
            "region": "us-east-1",
        },
    )
    resp = client.get("/control-plane/data-planes")
    assert resp.status_code == 200
    dps = resp.json()
    assert len(dps) == 1
    assert dps[0]["region"] == "us-east-1"


def test_router_multitenancy_disabled_returns_403(monkeypatch):
    import provisa.control_plane.router as _router_module
    import provisa.api.app as _app_module

    monkeypatch.setattr(_router_module, "_store", ControlPlaneStore())
    monkeypatch.setattr(_app_module.state, "multitenancy", False)

    from provisa.control_plane.router import router as control_plane_router

    app = FastAPI()
    app.include_router(control_plane_router)
    with TestClient(app) as tc:
        resp = tc.get("/control-plane/tenants")
    assert resp.status_code == 403
