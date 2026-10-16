# Copyright (c) 2026 Kenneth Stott
# Canary: f5ce79d4-fdc6-4e4e-bf50-2f291e33e695
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The UI server starts the sandbox tier's VM when a request for the tier finds it down
(REQ-2006): the VM powers itself off when no demo sandbox has been used for a while."""

# Requirements: REQ-2006

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from provisa import sandbox_wake

INSTANCE = "projects/p/zones/z/instances/provisa-saas-sandbox"


@pytest.fixture
def compute(monkeypatch):
    """A Compute Engine API and metadata server in one MockTransport; ``state`` is the
    instance's status, ``calls`` every request made."""
    state = {"status": "TERMINATED"}
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        if request.url.host == "metadata.google.internal":
            assert request.headers["Metadata-Flavor"] == "Google"
            return httpx.Response(200, json={"access_token": "tok"})
        assert request.headers["Authorization"] == "Bearer tok"
        if request.method == "POST":
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"status": state["status"]})

    monkeypatch.setattr(
        sandbox_wake, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    monkeypatch.setattr(sandbox_wake, "_last_call", float("-inf"))
    return state, calls


def test_a_stopped_vm_is_started_once_and_a_running_one_left_alone(compute):
    state, calls = compute
    assert asyncio.run(sandbox_wake.wake(INSTANCE)) == "TERMINATED"
    assert f"POST /compute/v1/{INSTANCE}/start" in calls
    calls.clear()
    asyncio.run(sandbox_wake.wake(INSTANCE))
    assert not [c for c in calls if c.startswith("POST")], "started again within the debounce"
    state["status"] = "RUNNING"
    sandbox_wake._last_call = float("-inf")
    calls.clear()
    assert asyncio.run(sandbox_wake.wake(INSTANCE)) == "RUNNING"
    assert not [c for c in calls if c.startswith("POST")]


def test_a_suspended_vm_is_resumed(compute):
    state, calls = compute
    state["status"] = "SUSPENDED"
    asyncio.run(sandbox_wake.wake(INSTANCE))
    assert f"POST /compute/v1/{INSTANCE}/resume" in calls


def _ui(monkeypatch, instance, tmp_path):
    import provisa.ui_server as ui_server

    (tmp_path / "index.html").write_text("<html></html>")  # a built UI: requests reach the proxy
    monkeypatch.setattr(ui_server, "STATIC_DIR", tmp_path)

    async def _timeout():
        return 5.0

    monkeypatch.setattr(ui_server, "SANDBOX_API_URL", "http://127.0.0.1:9")  # nothing listens
    monkeypatch.setattr(ui_server, "proxy_timeout_s", _timeout)
    monkeypatch.setattr(sandbox_wake, "SANDBOX_INSTANCE", instance)
    return TestClient(ui_server.app)


def test_a_demo_opened_while_the_tiers_vm_is_down_starts_it(monkeypatch, compute, tmp_path):
    response = _ui(monkeypatch, INSTANCE, tmp_path).post("/public/demos/weblogs")
    assert response.status_code == 503
    assert response.json()["code"] == "sandbox.starting"
    assert response.headers["Retry-After"] == "60"
    assert f"POST /compute/v1/{INSTANCE}/start" in compute[1]


def test_a_vm_that_cannot_be_started_is_a_502(monkeypatch, tmp_path):
    async def _refused(instance):
        raise RuntimeError("start: 403 forbidden")

    client = _ui(monkeypatch, INSTANCE, tmp_path)
    monkeypatch.setattr(sandbox_wake, "wake", _refused)
    assert client.post("/public/demos/weblogs").status_code == 502


def test_with_the_tier_on_this_node_nothing_is_started(monkeypatch, tmp_path):
    def _never(instance):
        raise AssertionError("no VM to start")

    client = _ui(monkeypatch, None, tmp_path)
    monkeypatch.setattr(sandbox_wake, "wake", _never)
    assert client.post("/public/demos/weblogs").status_code == 502


def _idle_case(monkeypatch, latest):
    from datetime import UTC, datetime, timedelta

    from provisa.api import demo_sandbox
    from provisa.scheduler import jobs

    stopped: list[str] = []

    async def _stop(instance):
        stopped.append(instance)

    async def _last_active():
        return None if latest is None else datetime.now(UTC) - timedelta(seconds=latest)

    monkeypatch.setattr(sandbox_wake, "SANDBOX_INSTANCE", INSTANCE)
    monkeypatch.setattr(sandbox_wake, "stop", _stop)
    monkeypatch.setattr(demo_sandbox, "last_active", _last_active)
    monkeypatch.setenv("PROVISA_SANDBOX_IDLE_STOP_SECONDS", "1800")
    return (
        stopped,
        jobs.stop_idle_sandbox_vm,
        lambda ago: datetime.now(UTC) - timedelta(seconds=ago),
    )


def test_the_tier_stops_its_vm_once_no_sandbox_has_been_used_for_the_idle_window(monkeypatch):
    stopped, job, ago = _idle_case(monkeypatch, latest=1900)
    asyncio.run(job(since=ago(7200)))
    assert stopped == [INSTANCE]


def test_a_sandbox_used_within_the_window_keeps_the_vm_up(monkeypatch):
    stopped, job, ago = _idle_case(monkeypatch, latest=600)
    asyncio.run(job(since=ago(7200)))
    assert stopped == []


def test_a_vm_just_started_is_given_the_whole_window(monkeypatch):
    """Counted from when the tier started when no open sandbox was reached since: the request
    that woke the VM has not opened its sandbox yet."""
    stopped, job, ago = _idle_case(monkeypatch, latest=None)
    asyncio.run(job(since=ago(300)))
    assert stopped == []
    stopped, job, ago = _idle_case(monkeypatch, latest=9000)
    asyncio.run(job(since=ago(300)))
    assert stopped == []
    asyncio.run(job(since=ago(1900)))
    assert stopped == [INSTANCE]


def test_a_stop_is_a_call_to_the_compute_api(compute):
    _state, calls = compute
    asyncio.run(sandbox_wake.stop(INSTANCE))
    assert calls[-1] == f"POST /compute/v1/{INSTANCE}/stop"


def test_an_empty_instance_names_no_vm(monkeypatch):
    """The compose passes the variable on every node, empty where the tier runs on the node."""
    import importlib

    monkeypatch.setenv("PROVISA_SANDBOX_INSTANCE", "")
    try:
        assert importlib.reload(sandbox_wake).SANDBOX_INSTANCE is None
    finally:
        monkeypatch.delenv("PROVISA_SANDBOX_INSTANCE")
        importlib.reload(sandbox_wake)
