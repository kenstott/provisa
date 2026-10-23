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
    # Fabric's own listing reports the capacity Active (each test that asks otherwise says so).
    monkeypatch.setattr(fc, "_fabric_client", lambda: _fabric(["Active"], []))


def _fabric(states: list[str], calls: list[str], name: str = "cap1"):
    """A fake Fabric REST API: ``GET /v1/capacities`` lists ``name``, its state walking
    ``states``."""
    seq = iter(states)
    current = {"state": None}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        assert request.url.path == "/v1/capacities"
        current["state"] = next(seq, current["state"])
        listed = [{"id": "c-9", "displayName": name, "state": current["state"]}]
        return httpx.Response(200, json={"value": listed})

    return httpx.Client(transport=httpx.MockTransport(handler))


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


def test_a_capacity_found_while_pausing_is_resumed_once_it_has_paused(configured, monkeypatch):
    """Found mid-pause (another client just suspended it), the capacity settles at Paused. Nothing
    resumes it but this call, so the resume is sent when it gets there — once."""
    calls: list[str] = []
    monkeypatch.setattr(fc, "_RESUME_TIMEOUT_S", 2.0)
    monkeypatch.setattr(
        fc,
        "_client",
        lambda: _arm(["Pausing", "Pausing", "Paused", "Paused", "Resuming", "Active"], calls),
    )
    fc.ensure_capacity_resumed()
    assert len([c for c in calls if c.startswith("POST")]) == 1


def test_a_terminal_state_raises(configured, monkeypatch):
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Failed"], []))
    with pytest.raises(RuntimeError, match="terminal state"):
        fc.ensure_capacity_resumed()


def test_a_capacity_active_in_azure_is_waited_for_until_fabric_serves_it(configured, monkeypatch):
    """ARM reports a resumed capacity Active before Fabric's data plane serves it, and a statement
    in between is refused CapacityNotActive (run 40024234397): the wait ends only when Fabric's
    own listing says Active too."""
    fabric_calls: list[str] = []
    monkeypatch.setattr(
        fc, "_fabric_client", lambda: _fabric(["Inactive", "Inactive", "Active"], fabric_calls)
    )
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Paused", "Active"], []))
    fc.ensure_capacity_resumed()
    assert fabric_calls == ["GET /v1/capacities"] * 3


def test_a_capacity_fabric_never_serves_raises_naming_it(configured, monkeypatch):
    monkeypatch.setattr(fc, "_RESUME_TIMEOUT_S", 0.0)
    monkeypatch.setattr(fc, "_fabric_client", lambda: _fabric(["Inactive"], []))
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Active"], []))
    with pytest.raises(RuntimeError, match="'cap1' is Active in Azure but Fabric still reports"):
        fc.ensure_capacity_resumed()


def test_a_capacity_fabric_does_not_list_raises_saying_what_access_is_missing(
    configured, monkeypatch
):
    monkeypatch.setattr(fc, "_fabric_client", lambda: _fabric(["Active"], [], name="other"))
    monkeypatch.setattr(fc, "_client", lambda: _arm(["Active"], []))
    with pytest.raises(RuntimeError, match="not in Fabric's capacity listing"):
        fc.ensure_capacity_resumed()


def test_the_fabric_runtime_resumes_before_it_connects_and_synapse_does_not(monkeypatch):
    from provisa.federation import mssql_warehouse_runtime as mwr

    order: list[str] = []
    monkeypatch.setattr(fc, "capacity_configured", lambda: True)
    monkeypatch.setattr(fc, "ensure_capacity_resumed", lambda: order.append("resume"))
    monkeypatch.setitem(
        __import__("sys").modules,
        "pyodbc",
        # A connection takes the datetimeoffset converter (REQ-2050), as pyodbc's does.
        SimpleNamespace(
            connect=lambda *a, **k: (
                order.append("connect") or SimpleNamespace(add_output_converter=lambda *_: None)
            )
        ),
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
