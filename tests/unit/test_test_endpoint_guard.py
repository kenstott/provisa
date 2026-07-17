# Copyright (c) 2026 Kenneth Stott
# Canary: 47411607-eb94-4ed8-ac8e-a7b1e9f208a8

"""REQ-004: the developer test endpoint must not be exposed in production."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.admin.actions_router import _test_endpoints_enabled, router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestTestEndpointGuard:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("PROVISA_ENABLE_TEST_ENDPOINTS", raising=False)
        assert _test_endpoints_enabled() is False

    def test_enabled_when_flag_truthy(self, monkeypatch):
        for val in ("1", "true", "TRUE", "yes", "on"):
            monkeypatch.setenv("PROVISA_ENABLE_TEST_ENDPOINTS", val)
            assert _test_endpoints_enabled() is True

    def test_disabled_for_other_values(self, monkeypatch):
        for val in ("", "0", "false", "no"):
            monkeypatch.setenv("PROVISA_ENABLE_TEST_ENDPOINTS", val)
            assert _test_endpoints_enabled() is False

    def test_endpoint_returns_404_when_disabled(self, monkeypatch):
        monkeypatch.delenv("PROVISA_ENABLE_TEST_ENDPOINTS", raising=False)
        resp = _client().post(
            "/admin/actions/test", json={"actionType": "function", "name": "x"}
        )
        assert resp.status_code == 404

    def test_endpoint_passes_guard_when_enabled(self, monkeypatch):
        # Enabled → guard passes; with no DB connected the handler returns 503, not 404.
        monkeypatch.setenv("PROVISA_ENABLE_TEST_ENDPOINTS", "true")
        resp = _client().post(
            "/admin/actions/test", json={"actionType": "function", "name": "x"}
        )
        assert resp.status_code != 404
