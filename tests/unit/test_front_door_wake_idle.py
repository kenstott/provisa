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
import time

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
    "activity_cidrs": ["10.9.0.0/16"],
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


# --- whose traffic counts (REQ-1333) ----------------------------------------------------------
#
# The shared IP takes unsolicited connections from the internet all day. Each one reached the
# splice, reset the idle clock, and — while the box was stopped — started it: a coordinator meant
# to scale to zero billed as a 24/7 VM. Only the CIDRs the firewall admits vote on its lifetime.


@pytest.mark.parametrize("peer", ["10.9.0.4", "10.9.255.255"])
def test_an_admitted_client_counts_as_activity(proxy, peer):
    assert proxy.counts_as_activity(peer) is True


@pytest.mark.parametrize("peer", ["10.8.0.4", "45.33.32.156"])
def test_a_client_outside_the_admitted_range_does_not(proxy, peer):
    assert proxy.counts_as_activity(peer) is False


def _dead_pair():
    """A socket whose peer is already closed, so a splice returns on the first read."""
    import socket as _socket

    near, far = _socket.socketpair()
    far.close()
    return near


def test_a_splice_from_an_unadmitted_client_leaves_the_idle_clock_alone(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 1.0

    proxy._splice(_dead_pair(), _dead_pair(), False)

    assert proxy._last_activity == 1.0


def test_a_splice_from_an_admitted_client_resets_the_idle_clock(proxy, monkeypatch):
    _clock(monkeypatch, proxy, 100_000.0)
    proxy._last_activity = 1.0

    proxy._splice(_dead_pair(), _dead_pair(), True)

    assert proxy._last_activity == 100_000.0


class _FakeClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


@pytest.mark.parametrize("port,ready", [(443, False), (5432, None)])
def test_an_unadmitted_client_cannot_start_a_stopped_coordinator(proxy, monkeypatch, port, ready):
    """Waking is the expensive half: a port scan should not buy an hour of VM time."""
    woke: list[bool] = []
    monkeypatch.setattr(proxy, "trigger_wake", lambda: woke.append(True))
    monkeypatch.setattr(proxy, "_https_ready", lambda p: False)
    monkeypatch.setattr(proxy, "_backend_connect", lambda p, timeout=None: ready)
    client = _FakeClient()

    proxy._handle(client, port, "45.33.32.156")

    assert woke == []
    assert client.closed is True


@pytest.mark.parametrize("port", [443, 5432])
def test_an_admitted_client_still_wakes_a_stopped_coordinator(proxy, monkeypatch, port):
    """The scoping must not become a latch that leaves the operator unable to wake the box."""
    woke: list[bool] = []
    monkeypatch.setattr(proxy, "trigger_wake", lambda: woke.append(True))
    monkeypatch.setattr(proxy, "_https_ready", lambda p: False)
    monkeypatch.setattr(proxy, "_backend_connect", lambda p, timeout=None: None)
    monkeypatch.setattr(proxy, "_serve_wake_response", lambda c, p: None)
    monkeypatch.setattr(proxy, "WAKE_HOLD_SECONDS", 0)

    proxy._handle(_FakeClient(), port, "10.9.0.4")

    assert woke == [True]


# --- readiness (REQ-1333) ---------------------------------------------------------------------
#
# The UI container binds its port the moment it restarts, while the API's lifespan is still
# running. Routing on the accept handed the browser an SPA whose every call was refused, so the
# site reported it could not reach Provisa — between the waking page and a working site.


def _self_signed(tmp_path, stem: str):
    """A self-signed cert/key pair on disk, returned as ``(cert_file, key_file)``.

    The name is not 127.0.0.1, which is the deployed shape too: the coordinator's cert is for the
    public hostname and the front door dials its internal address.
    """
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
    cert_file = tmp_path / f"{stem}.crt"
    key_file = tmp_path / f"{stem}.key"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_file, key_file


def _tls_backend(tmp_path, responder):
    """A TLS listener on localhost answering each request with ``responder(request)``.

    Returns ``(port, requests)``.
    """
    import socket as _socket
    import ssl as _ssl
    import threading as _threading

    cert_file, key_file = _self_signed(tmp_path, "backend")

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


# --- one bad sample is not an outage (REQ-1333) ------------------------------------------------
# _backend_ready is a single connect/request/recv. A GC pause in the app or a momentary accept
# backlog answers NO for a site that is serving, and the NO is then cached — so every connection
# in that window gets the waking page and the SPA's calls fail against a live instance. That is
# what put "Error: Service Unavailable" on an admin page while the coordinator was RUNNING.


def test_a_running_box_whose_probe_missed_once_is_asked_again(local_backend, tmp_path):
    answers = ["503 Service Unavailable", "200 OK"]
    port, requests = _tls_backend(
        tmp_path, lambda _r: _http(answers.pop(0) if answers else "200 OK")
    )
    local_backend.PORTS[port] = {"wake_style": "html"}
    local_backend.set_status("RUNNING")

    assert local_backend._https_ready(port) is True
    assert len(requests) == 2


def test_a_running_box_that_is_really_down_still_gets_the_waking_page(local_backend, tmp_path):
    port, requests = _tls_backend(tmp_path, lambda _r: _http("503 Service Unavailable"))
    local_backend.PORTS[port] = {"wake_style": "html"}
    local_backend.set_status("RUNNING")

    assert local_backend._https_ready(port) is False
    assert len(requests) == 2


def test_a_stopped_box_is_not_probed_twice(local_backend, tmp_path):
    """A second 2.5s connect timeout per connection buys nothing on a box that is not there."""
    port, requests = _tls_backend(tmp_path, lambda _r: _http("503 Service Unavailable"))
    local_backend.PORTS[port] = {"wake_style": "html"}
    local_backend.set_status("TERMINATED")

    assert local_backend._https_ready(port) is False
    assert len(requests) == 1


def test_a_healthy_port_is_answered_from_one_probe(local_backend, tmp_path):
    port, requests = _tls_backend(tmp_path, lambda _r: _http("200 OK"))
    local_backend.PORTS[port] = {"wake_style": "html"}

    assert local_backend._https_ready(port) is True
    assert len(requests) == 1


def test_the_re_probe_replaces_the_cached_no(local_backend, tmp_path):
    """Otherwise the stale NO keeps answering for the rest of the cache window."""
    answers = ["503 Service Unavailable", "200 OK"]
    port, requests = _tls_backend(
        tmp_path, lambda _r: _http(answers.pop(0) if answers else "200 OK")
    )
    local_backend.PORTS[port] = {"wake_style": "html"}
    local_backend.set_status("RUNNING")

    assert local_backend._https_ready(port) is True
    assert local_backend._backend_ready(port) is True
    assert len(requests) == 2


# --- the waking answer speaks the caller's language (REQ-1350) ---------------------------------
# Port 443 carries both document navigations and the SPA's fetch() calls, and wake_style is a
# property of the port, so an XHR was handed the HTML waking page. res.json() fails on it and the
# UI falls back to the bare reason phrase — "Error: Service Unavailable" — instead of saying the
# instance is starting.


@pytest.mark.parametrize(
    "request_bytes",
    [
        b"GET /admin/my-secrets HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: document\r\n\r\n",
        b"GET /admin/my-secrets HTTP/1.1\r\nHost: h\r\nAccept: text/html,*/*\r\n\r\n",
    ],
)
def test_a_browser_navigating_is_recognised(proxy, request_bytes):
    assert proxy._wants_html(request_bytes) is True


@pytest.mark.parametrize(
    "request_bytes",
    [
        b"GET /admin/orgs/default/my-secrets HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: empty\r\n"
        b"Accept: application/json\r\n\r\n",
        b"POST /data/sql HTTP/1.1\r\nHost: h\r\nAccept: text/html\r\n\r\n",
        b"GET /assets/index.js HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: script\r\n\r\n",
        b"",
    ],
)
def test_an_api_call_is_not_mistaken_for_a_navigation(proxy, request_bytes):
    assert proxy._wants_html(request_bytes) is False


def _wake_answer(
    proxy, tmp_path, monkeypatch, request_bytes: bytes, trailer: bytes = b""
) -> tuple[str, bytes]:
    """Serve one real wake response over TLS and return ``(headers, body)`` as the client sees it."""
    import socket as _socket
    import ssl as _ssl
    import threading as _threading

    cert_file, key_file = _self_signed(tmp_path, "front-door")
    monkeypatch.setattr(proxy, "TLS_CERT", str(cert_file))
    monkeypatch.setattr(proxy, "TLS_KEY", str(key_file))

    srv = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def serve():
        raw, _ = srv.accept()
        proxy._serve_wake_response(raw, 443)

    _threading.Thread(target=serve, daemon=True).start()

    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    with ctx.wrap_socket(_socket.create_connection(("127.0.0.1", port), timeout=5)) as tls:
        tls.settimeout(5)
        tls.sendall(request_bytes)
        if trailer:
            # A separate write, so the server's first recv sees the headers alone — the shape a
            # real POST arrives in.
            tls.sendall(trailer)
        received = b""
        while True:
            chunk = tls.recv(65536)
            if not chunk:
                break
            received += chunk
    srv.close()
    head, _, body = received.partition(b"\r\n\r\n")
    return head.decode("latin-1"), body


def test_a_navigation_to_a_waking_instance_gets_the_waking_page(proxy, tmp_path, monkeypatch):
    head, body = _wake_answer(
        proxy,
        tmp_path,
        monkeypatch,
        b"GET / HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: document\r\n\r\n",
    )

    assert head.startswith("HTTP/1.1 503 Service Unavailable")
    assert "text/html" in head
    assert b"Waking your instance" in body


def test_an_xhr_to_a_waking_instance_gets_a_translatable_json_error(proxy, tmp_path, monkeypatch):
    head, body = _wake_answer(
        proxy,
        tmp_path,
        monkeypatch,
        b"GET /admin/orgs/default/my-secrets HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: empty\r\n\r\n",
    )

    assert head.startswith("HTTP/1.1 503 Service Unavailable")
    assert "application/json" in head
    assert "Retry-After: 120" in head
    payload = json.loads(body)
    # REQ-1350: the UI translates `code` and falls back to the English `detail`. Neither may be
    # missing, or the SPA is back to rendering res.statusText.
    assert payload["code"] == "front_door.coordinator_waking"
    assert payload["detail"]
    assert payload["retry_after_seconds"] == 120


def test_a_graphql_post_to_a_waking_instance_gets_the_json_error(proxy, tmp_path, monkeypatch):
    """GraphQL is a POST whose body lands in a segment after the headers."""
    payload = b'{"query":"{ tables { id } }"}'
    head, body = _wake_answer(
        proxy,
        tmp_path,
        monkeypatch,
        b"POST /graphql HTTP/1.1\r\nHost: h\r\nSec-Fetch-Dest: empty\r\n"
        b"Content-Type: application/json\r\n" + f"Content-Length: {len(payload)}\r\n\r\n".encode(),
        trailer=payload,
    )

    assert head.startswith("HTTP/1.1 503 Service Unavailable")
    assert json.loads(body)["code"] == "front_door.coordinator_waking"


def _tls_pair(tmp_path, monkeypatch, proxy):
    """A connected TLS client/server socket pair on localhost, returned as ``(client, server)``."""
    import socket as _socket
    import ssl as _ssl
    import threading as _threading

    cert_file, key_file = _self_signed(tmp_path, "pair")
    srv = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    server_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(str(cert_file), str(key_file))
    accepted: list = []

    def accept():
        raw, _ = srv.accept()
        accepted.append(server_ctx.wrap_socket(raw, server_side=True))

    thread = _threading.Thread(target=accept, daemon=True)
    thread.start()
    client_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.check_hostname = False
    client_ctx.verify_mode = _ssl.CERT_NONE
    client = client_ctx.wrap_socket(_socket.create_connection(("127.0.0.1", port), timeout=5))
    thread.join(5)
    srv.close()
    return client, accepted[0]


def test_the_rest_of_a_request_is_read_before_the_close(proxy, tmp_path, monkeypatch):
    """Unread bytes in the receive buffer make the close an RST rather than a FIN, and the browser
    reports a transport failure instead of the 503 just written -- which is what put "Could not
    load registered tables: Failed to fetch" on the tables page while the coordinator was up."""
    client, server = _tls_pair(tmp_path, monkeypatch, proxy)
    client.sendall(b'{"query":"{ tables { id } }"}')

    proxy._drain(server)

    server.settimeout(0.2)
    with pytest.raises((TimeoutError, OSError)):
        server.recv(65536)  # nothing left: the body was consumed
    client.close()
    server.close()


def test_draining_a_request_with_nothing_left_does_not_hang_the_answer(
    proxy, tmp_path, monkeypatch
):
    """The common case is a GET whose bytes were all taken by the first recv."""
    client, server = _tls_pair(tmp_path, monkeypatch, proxy)
    started = time.monotonic()

    proxy._drain(server)

    assert time.monotonic() - started < proxy.DRAIN_TIMEOUT * 4
    client.close()
    server.close()


def _routing(proxy, monkeypatch, ready: bool):
    """Drive ``_handle`` on the browser port with the readiness answer fixed, recording the route."""
    took: list[str] = []
    monkeypatch.setattr(proxy, "_https_ready", lambda _port: ready)
    monkeypatch.setattr(proxy, "_backend_connect", lambda port, timeout=None: object())
    monkeypatch.setattr(proxy, "_splice", lambda *_a: took.append("spliced"))
    monkeypatch.setattr(proxy, "_serve_wake_response", lambda *_a: took.append("wake_page"))
    monkeypatch.setattr(proxy, "trigger_wake", lambda: took.append("wake_call"))
    proxy._handle(object(), 443, "10.9.0.4")
    return took


def test_an_unready_backend_gets_the_waking_page_not_the_broken_site(proxy, monkeypatch):
    assert _routing(proxy, monkeypatch, ready=False) == ["wake_call", "wake_page"]


def test_a_ready_backend_is_spliced_straight_through(proxy, monkeypatch):
    assert _routing(proxy, monkeypatch, ready=True) == ["spliced"]
