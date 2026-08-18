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

import datetime
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
    "ports": {"5432": {"wake_style": "raw"}, "443": {"wake_style": "html"}},
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
    # Uptime is read off the instance's own lastStartTimestamp, in wall-clock time, so the
    # fixture holds how long the box has been up and renders the timestamp from it.
    uptime = {"seconds": 10 * 24 * 3600.0}

    def _compute_api(method, path):
        calls.append((method, path))
        started = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=uptime["seconds"]
        )
        return {"status": status["value"], "lastStartTimestamp": started.isoformat()}

    monkeypatch.setattr(module, "_compute_api", _compute_api)
    module.calls = calls  # pyright: ignore[reportAttributeAccessIssue]
    module.set_status = lambda v: status.update(value=v)  # pyright: ignore[reportAttributeAccessIssue]
    module.set_uptime = lambda s: uptime.update(seconds=s)  # pyright: ignore[reportAttributeAccessIssue]
    module._status_cache = ("", 0.0)  # pyright: ignore[reportAttributeAccessIssue]
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
    assert set(payload["ports"]) == {"443", "5432"}
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
    assert payload["ports"] == {"443": False, "5432": False}
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


def test_a_box_started_outside_the_front_door_is_not_stopped_at_boot(proxy, monkeypatch):
    """An operator deploying with `gcloud compute instances start` leaves _last_start_call and
    _last_activity untouched, so the box was already past the idle window the moment it came up
    and the reaper stopped it mid startup script. Uptime bounds how long it can have been idle."""
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 0.0
    proxy._active_conns = 0
    proxy._last_start_call = 0.0
    proxy.set_uptime(30.0)

    assert proxy.idle_stop_due() is None


def test_a_box_up_longer_than_the_idle_window_is_still_stopped(proxy, monkeypatch):
    """The uptime bound must not become a latch that keeps a genuinely idle box billing."""
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 100_000.0 - proxy.IDLE_STOP_SECONDS - 1
    proxy._active_conns = 0
    proxy._last_start_call = 0.0
    proxy.set_uptime(proxy.IDLE_STOP_SECONDS + 60)

    assert proxy.idle_stop_due() is not None


def test_a_coordinator_that_is_already_stopped_is_not_stopped_again(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 0.0
    proxy._active_conns = 0
    proxy._last_start_call = 0.0
    proxy.set_status("TERMINATED")

    assert proxy.idle_stop_due() is None


def test_the_idle_window_comes_from_the_configured_minutes(proxy):
    assert proxy.IDLE_STOP_SECONDS == CONFIG["idle_stop_minutes"] * 60


# --- readiness (REQ-1333) ---------------------------------------------------------------------
#
# The UI container binds its port the moment it restarts, while the API's lifespan is still
# running. Routing on the accept handed the browser an SPA whose every call was refused, so the
# site reported it could not reach Provisa — between the waking page and a working site.


def _tls_backend(tmp_path, responder):
    """A TLS listener on localhost answering each request with ``responder(request)``.

    Returns ``(port, requests)``. The certificate is self-signed and issued for a name that is not
    127.0.0.1, which is the deployed shape too: the coordinator's cert is for the public hostname
    and the front door dials its internal address.
    """
    import socket as _socket
    import ssl as _ssl
    import threading as _threading

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "cloud.provisa.test")])
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    cert_file = tmp_path / "backend.crt"
    key_file = tmp_path / "backend.key"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    srv = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert_file), str(key_file))
    requests: list[str] = []

    def serve():
        while True:
            try:
                raw, _ = srv.accept()
            except OSError:
                return
            try:
                with ctx.wrap_socket(raw, server_side=True) as tls:
                    tls.settimeout(5)
                    requests.append(tls.recv(65536).decode("latin-1"))
                    tls.sendall(responder(requests[-1]))
            except OSError:
                continue

    _threading.Thread(target=serve, daemon=True).start()
    return port, requests


def _http(status: str, body: bytes = b"") -> bytes:
    return (
        f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        + body
    )


@pytest.fixture
def local_backend(proxy, monkeypatch):
    """Point the proxy's backend at localhost so a real TLS probe can be answered."""
    monkeypatch.setattr(proxy, "BACKEND_HOST", "127.0.0.1")
    proxy._health_cache.clear()
    return proxy


def test_a_port_that_answers_health_is_ready(local_backend, tmp_path):
    port, requests = _tls_backend(tmp_path, lambda _r: _http("200 OK", b'{"status":"ok"}'))
    local_backend.PORTS[port] = {"wake_style": "html"}

    assert local_backend._backend_ready(port) is True
    assert requests[0].startswith("GET /health ")


def test_a_port_that_accepts_but_is_not_serving_yet_is_not_ready(local_backend, tmp_path):
    """This is the state the middle attempt landed in: the socket is open, the app is not up."""
    port, _ = _tls_backend(tmp_path, lambda _r: _http("503 Service Unavailable"))
    local_backend.PORTS[port] = {"wake_style": "html"}

    assert local_backend._backend_ready(port) is False


def test_a_port_nothing_listens_on_is_not_ready(local_backend):
    port = 9  # discard; nothing accepts
    local_backend.PORTS[port] = {"wake_style": "html"}
    assert local_backend._backend_ready(port) is False


def test_readiness_is_cached_across_the_connections_of_one_page_load(local_backend, tmp_path):
    """A browser opens several connections per document; a probe on each would multiply the
    latency of every request the front door forwards."""
    port, requests = _tls_backend(tmp_path, lambda _r: _http("200 OK"))
    local_backend.PORTS[port] = {"wake_style": "html"}

    for _ in range(5):
        assert local_backend._backend_ready(port) is True
    assert len(requests) == 1


def test_readiness_is_re_probed_once_the_cache_expires(local_backend, tmp_path, monkeypatch):
    answers = ["503 Service Unavailable", "200 OK"]
    port, _ = _tls_backend(tmp_path, lambda _r: _http(answers.pop(0) if answers else "200 OK"))
    local_backend.PORTS[port] = {"wake_style": "html"}
    now = [1000.0]
    monkeypatch.setattr(local_backend.time, "monotonic", lambda: now[0])

    assert local_backend._backend_ready(port) is False
    now[0] += local_backend.HEALTH_CACHE_TTL + 1
    assert local_backend._backend_ready(port) is True


def test_a_raw_protocol_port_is_still_judged_by_the_accept(proxy, monkeypatch):
    """Bolt, pgwire and Flight speak no HTTP, so there is nothing to ask them for."""
    probed: list[int] = []

    def _connect(port, timeout=None):
        probed.append(port)
        return None

    monkeypatch.setattr(proxy, "_backend_connect", _connect)
    proxy._status_payload()
    assert 5432 in probed


def _routing(proxy, monkeypatch, ready: bool):
    """Drive ``_handle`` on the browser port with the readiness answer fixed, recording the route."""
    took: list[str] = []
    monkeypatch.setattr(proxy, "_backend_ready", lambda _port: ready)
    monkeypatch.setattr(proxy, "_backend_connect", lambda port, timeout=None: object())
    monkeypatch.setattr(proxy, "_splice", lambda *_a: took.append("spliced"))
    monkeypatch.setattr(proxy, "_serve_wake_response", lambda *_a: took.append("wake_page"))
    monkeypatch.setattr(proxy, "trigger_wake", lambda: took.append("wake_call"))
    proxy._handle(object(), 443)
    return took


def test_an_unready_backend_gets_the_waking_page_not_the_broken_site(proxy, monkeypatch):
    assert _routing(proxy, monkeypatch, ready=False) == ["wake_call", "wake_page"]


def test_a_ready_backend_is_spliced_straight_through(proxy, monkeypatch):
    assert _routing(proxy, monkeypatch, ready=True) == ["spliced"]
