# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b1f6a-5c2e-4a97-9e0b-4f6c2a8d1e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Fabric engine resumes its own capacity before connecting (REQ-1775)."""

# Requirements: REQ-1775

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from provisa.federation import fabric_capacity as fc


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("FABRIC_CAPACITY_NAME", "cap1")
    monkeypatch.setenv("FABRIC_RESOURCE_GROUP", "rg1")
    monkeypatch.setattr(fc, "_POLL_S", 0.0)


def _arm(states: list[str], calls: list[str]):
    """A fake ARM: two subscriptions, the capacity lives in the second; GETs walk ``states``."""
    seq = iter(states)
    current = {"state": None}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append(f"{request.method} {path}")
        if path == "/subscriptions":
            return httpx.Response(
                200, json={"value": [{"subscriptionId": "s0"}, {"subscriptionId": "s1"}]}
            )
        if "/subscriptions/s0/" in path:
            return httpx.Response(404)
        if request.method == "POST":
            return httpx.Response(202)
        current["state"] = next(seq, current["state"])
        return httpx.Response(200, json={"properties": {"state": current["state"]}})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_unset_means_the_capacity_is_managed_externally(monkeypatch):
    monkeypatch.delenv("FABRIC_CAPACITY_NAME", raising=False)
    monkeypatch.delenv("FABRIC_RESOURCE_GROUP", raising=False)
    assert fc.capacity_configured() is False


def test_half_configured_is_a_misconfiguration(monkeypatch):
    monkeypatch.setenv("FABRIC_CAPACITY_NAME", "cap1")
    monkeypatch.delenv("FABRIC_RESOURCE_GROUP", raising=False)
    with pytest.raises(RuntimeError, match="set together"):
        fc.capacity_configured()


def test_an_active_capacity_is_left_alone(configured, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Active"], calls))
    fc.ensure_capacity_resumed()
    assert not any(c.startswith("POST") for c in calls)


def test_a_paused_capacity_is_resumed_and_waited_for(configured, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        fc, "_client", lambda: _arm(["Paused", "Resuming", "Resuming", "Active"], calls)
    )
    fc.ensure_capacity_resumed()
    posts = [c for c in calls if c.startswith("POST")]
    assert posts == [
        "POST /subscriptions/s1/resourceGroups/rg1/providers/Microsoft.Fabric/capacities/cap1/resume"
    ]


def test_a_terminal_state_raises(configured, monkeypatch):
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Failed"], []))
    with pytest.raises(RuntimeError, match="terminal state"):
        fc.ensure_capacity_resumed()


def test_the_fabric_runtime_resumes_before_it_connects_and_synapse_does_not(monkeypatch):
    from provisa.federation import mssql_warehouse_runtime as mwr

    order: list[str] = []
    monkeypatch.setattr(fc, "capacity_configured", lambda: True)
    monkeypatch.setattr(fc, "ensure_capacity_resumed", lambda: order.append("resume"))
    monkeypatch.setitem(
        __import__("sys").modules,
        "pyodbc",
        SimpleNamespace(connect=lambda *a, **k: order.append("connect") or object()),
    )

    class _Cred:
        def get_token(self, _scope):
            return SimpleNamespace(token="t")

    monkeypatch.setattr("azure.identity.DefaultAzureCredential", _Cred)
    mwr.MssqlWarehouseRuntime(server="s", database="d", engine_name="fabric")
    assert order == ["resume", "connect"]
    order.clear()
    mwr.MssqlWarehouseRuntime(server="s", database="d", engine_name="synapse")
    assert order == ["connect"]
