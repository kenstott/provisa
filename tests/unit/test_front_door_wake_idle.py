# Copyright (c) 2026 Kenneth Stott
# Canary: 4d90b6e2-7c15-4a38-91f6-05e83b2c7a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1333 / REQ-1334: the front door's wake and idle-stop decisions.

Both directions cost real money when they are wrong, and neither is cheap to observe in place:
a missed stop bills a coordinator all night, a spurious stop kills a cluster someone is querying,
and a wake storm hammers the Compute API with start calls for a box that is already booting.

The proxy runs on a bare e2-micro with no framework, reading its config at import — so the module
is loaded here against a config written for the test, and the decisions are called directly. The
socket plumbing around them is what the deployed lane exercises.
"""

# Requirements: REQ-1333, REQ-1334

from __future__ import annotations

import importlib.util
import json
import sys

from pathlib import Path

import pytest

PROXY_PATH = Path(__file__).resolve().parents[2] / "terraform/gcp-saas/front-door/proxy.py"

CONFIG = {
    "project": "provisa-cloud",
    "zone": "us-east1-b",
    "instance": "coordinator",
    "backend_host": "10.0.0.2",
    "ports": {"5432": {"wake_style": "hold"}, "8080": {"wake_style": "http"}},
    "status_port": 9443,
    "status_token": "s3cr3t-token",
    "idle_stop_minutes": 60,
    "boot_grace_seconds": 300,
    "tls_cert": "/etc/ssl/front-door.crt",
    "tls_key": "/etc/ssl/front-door.key",
}


@pytest.fixture
def proxy(tmp_path, monkeypatch):
    """The proxy module, loaded against a test config, with the Compute API recorded."""
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(CONFIG))

    source = PROXY_PATH.read_text().replace(
        'CONFIG_PATH = "/etc/provisa-front-door/config.json"',
        f'CONFIG_PATH = "{config_file}"',
    )
    module_path = tmp_path / "front_door_proxy.py"
    module_path.write_text(source)

    spec = importlib.util.spec_from_file_location("front_door_proxy_under_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "front_door_proxy_under_test", module)
    spec.loader.exec_module(module)

    calls: list[tuple[str, str]] = []
    status = {"value": "RUNNING"}

    def _compute_api(method, path):
        calls.append((method, path))
        return {"status": status["value"]}

    monkeypatch.setattr(module, "_compute_api", _compute_api)
    module.calls = calls  # pyright: ignore[reportAttributeAccessIssue]
    module.set_status = lambda v: status.update(value=v)  # pyright: ignore[reportAttributeAccessIssue]
    module._status_cache = ("", 0.0)
    return module


def _clock(monkeypatch, module, now):
    monkeypatch.setattr(module.time, "monotonic", lambda: now)


# --- wake (REQ-1334) --------------------------------------------------------------------------


def test_a_wake_starts_a_stopped_coordinator(proxy):
    proxy.set_status("TERMINATED")

    proxy.trigger_wake()

    assert ("POST", "instances/coordinator/start") in proxy.calls


@pytest.mark.parametrize("status", ["TERMINATED", "STOPPED", "SUSPENDED"])
def test_every_stopped_state_is_wakeable(proxy, status):
    proxy.set_status(status)

    proxy.trigger_wake()

    assert ("POST", "instances/coordinator/start") in proxy.calls


@pytest.mark.parametrize("status", ["RUNNING", "STAGING", "PROVISIONING"])
def test_a_coordinator_that_is_up_or_coming_up_is_not_started_again(proxy, status):
    """A start against a booting instance is an API error, and against a running one it is a
    request to restart the cluster someone is using."""
    proxy.set_status(status)

    proxy.trigger_wake()

    assert ("POST", "instances/coordinator/start") not in proxy.calls


def test_a_burst_of_traffic_produces_one_start_call(proxy, monkeypatch):
    """Every connection on every port hits wake while the box boots. Without the debounce that
    is one Compute API start per client, for ninety seconds."""
    proxy.set_status("TERMINATED")
    _clock(monkeypatch, proxy, 1000.0)

    proxy.trigger_wake()
    _clock(monkeypatch, proxy, 1000.0 + proxy.START_DEBOUNCE_SECONDS - 1)
    proxy.trigger_wake()
    proxy.trigger_wake()

    starts = [c for c in proxy.calls if c == ("POST", "instances/coordinator/start")]
    assert len(starts) == 1


def test_a_wake_after_the_debounce_window_starts_again(proxy, monkeypatch):
    """The debounce must not become a latch — a box that stopped again has to be wakeable."""
    proxy.set_status("TERMINATED")
    _clock(monkeypatch, proxy, 1000.0)
    proxy.trigger_wake()

    _clock(monkeypatch, proxy, 1000.0 + proxy.START_DEBOUNCE_SECONDS + 1)
    proxy._status_cache = ("", 0.0)
    proxy.trigger_wake()

    starts = [c for c in proxy.calls if c == ("POST", "instances/coordinator/start")]
    assert len(starts) == 2


def test_a_failing_compute_api_does_not_take_the_front_door_down(proxy):
    """The proxy is the only thing listening; an exception out of wake would drop the port."""
    import urllib.error

    proxy.set_status("TERMINATED")

    def _boom(method, path):
        raise urllib.error.URLError("compute API unreachable")

    proxy._compute_api = _boom
    proxy.trigger_wake()  # must not raise


# --- status (REQ-1334) ------------------------------------------------------------------------


def test_status_reports_the_coordinator_ports_and_idle_time(proxy, monkeypatch):
    monkeypatch.setattr(proxy, "_backend_connect", lambda port, timeout=None: None)
    _clock(monkeypatch, proxy, 5000.0)
    proxy._last_activity = 4400.0

    payload = proxy._status_payload()

    assert payload["coordinator"] == "RUNNING"
    assert set(payload["ports"]) == {"5432", "8080"}
    assert payload["all_up"] is False
    assert payload["idle_seconds"] == 600


def test_status_does_not_probe_the_ports_of_a_stopped_coordinator(proxy, monkeypatch):
    """A SYN to a stopped VM hangs for the full timeout; seven of them make /status time out."""
    probed: list[int] = []

    def _connect(port, timeout=None):
        probed.append(port)
        return None

    monkeypatch.setattr(proxy, "_backend_connect", _connect)
    proxy.set_status("TERMINATED")

    payload = proxy._status_payload()

    assert probed == []
    assert payload["ports"] == {"5432": False, "8080": False}
    assert payload["all_up"] is False


def test_status_reads_the_coordinator_state_fresh_rather_than_from_cache(proxy):
    """/status is what an operator polls while waiting for a wake; a ten-second cache would
    report TERMINATED after the box came up."""
    proxy.set_status("TERMINATED")
    proxy._status_payload()
    proxy.set_status("RUNNING")

    assert proxy._status_payload()["coordinator"] == "RUNNING"


# --- idle stop (REQ-1333) ---------------------------------------------------------------------


def test_a_coordinator_idle_past_the_window_is_stopped(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 100_000.0 - proxy.IDLE_STOP_SECONDS - 1
    proxy._last_start_call = 0.0
    proxy._active_conns = 0

    assert proxy.idle_stop_due() is not None


def test_a_coordinator_inside_the_idle_window_is_left_running(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 100_000.0 - proxy.IDLE_STOP_SECONDS + 60
    proxy._last_start_call = 0.0
    proxy._active_conns = 0

    assert proxy.idle_stop_due() is None


def test_an_open_connection_vetoes_the_stop(proxy, monkeypatch):
    """A long-running query holds a spliced connection with no new bytes for minutes. Stopping
    the coordinator under it kills the query."""
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 0.0
    proxy._last_start_call = 0.0
    proxy._active_conns = 1

    assert proxy.idle_stop_due() is None


def test_a_box_still_inside_its_boot_grace_is_not_stopped(proxy, monkeypatch):
    """We woke it seconds ago; its own boot has not yet produced the traffic that would reset
    the idle clock, so without the grace the reaper stops what it just started."""
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 0.0
    proxy._active_conns = 0
    proxy._last_start_call = 100_000.0 - proxy.BOOT_GRACE_SECONDS + 10

    assert proxy.idle_stop_due() is None


def test_a_coordinator_that_is_already_stopped_is_not_stopped_again(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 0.0
    proxy._active_conns = 0
    proxy._last_start_call = 0.0
    proxy.set_status("TERMINATED")

    assert proxy.idle_stop_due() is None


def test_the_idle_window_comes_from_the_configured_minutes(proxy):
    assert proxy.IDLE_STOP_SECONDS == CONFIG["idle_stop_minutes"] * 60
