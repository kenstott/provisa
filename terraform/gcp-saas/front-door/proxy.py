# Copyright (c) 2026 Kenneth Stott
# Canary: ba111767-8713-4077-ab57-c3c0bc989fa8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Front-door proxy for the SaaS coordinator (REQ: wake-on-hit + idle-stop).

Runs on the always-free e2-micro that owns the shared static IP. Splices every
protocol port straight through to the coordinator when it is up. When it is
stopped, a hit on any port triggers instances.start; HTTPS ports get a TLS
"waking up" page (browser) / 503 JSON (API), raw TCP ports are held open until
the coordinator accepts, then spliced. After idle_stop_minutes with zero
traffic the coordinator is stopped again.

stdlib only (python3.10 / ubuntu 22.04); thread-per-connection is deliberate —
this box fronts dev-scale traffic, not the data plane.
"""

import json
import logging
import select
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("front-door")

CONFIG_PATH = "/etc/provisa-front-door/config.json"
with open(CONFIG_PATH, encoding="utf-8") as fh:
    CFG = json.load(fh)

PROJECT = CFG["project"]
ZONE = CFG["zone"]
INSTANCE = CFG["instance"]
BACKEND_HOST = CFG["backend_host"]
PORTS = {int(p): meta for p, meta in CFG["ports"].items()}
STATUS_PORT = CFG["status_port"]
STATUS_TOKEN = CFG["status_token"]
IDLE_STOP_SECONDS = CFG["idle_stop_minutes"] * 60
BOOT_GRACE_SECONDS = CFG["boot_grace_seconds"]
TLS_CERT = CFG["tls_cert"]  # path; "" when the deploy runs without TLS material
TLS_KEY = CFG["tls_key"]

BACKEND_CONNECT_TIMEOUT = 2.0
WAKE_HOLD_SECONDS = 110  # raw-TCP clients: hold until boot completes or they give up
STATUS_CACHE_TTL = 10.0
START_DEBOUNCE_SECONDS = 30.0
SPLICE_BUF = 65536

_state_lock = threading.Lock()
_last_activity = time.monotonic()
_active_conns = 0
_last_start_call = 0.0
_status_cache = ("", 0.0)

_metadata_token = ("", 0.0)


def _gcp_token() -> str:
    global _metadata_token
    tok, exp = _metadata_token
    if time.time() < exp - 60:
        return tok
    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        payload = json.loads(resp.read())
    _metadata_token = (payload["access_token"], time.time() + payload["expires_in"])
    return _metadata_token[0]


def _compute_api(method: str, path: str) -> dict:
    url = f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{ZONE}/{path}"
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {_gcp_token()}")
    if method == "POST":
        req.add_header("Content-Length", "0")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def coordinator_status(fresh: bool = False) -> str:
    global _status_cache
    status, at = _status_cache
    if not fresh and status and time.monotonic() - at < STATUS_CACHE_TTL:
        return status
    status = _compute_api("GET", f"instances/{INSTANCE}")["status"]
    _status_cache = (status, time.monotonic())
    return status


def trigger_wake() -> None:
    global _last_start_call
    with _state_lock:
        if time.monotonic() - _last_start_call < START_DEBOUNCE_SECONDS:
            return
        _last_start_call = time.monotonic()
    try:
        status = coordinator_status(fresh=True)
        if status in ("TERMINATED", "STOPPED", "SUSPENDED"):
            log.info("waking %s (status=%s)", INSTANCE, status)
            _compute_api("POST", f"instances/{INSTANCE}/start")
        else:
            log.info("wake requested but %s is %s; not starting", INSTANCE, status)
    except urllib.error.URLError as exc:
        log.error("wake failed: %s", exc)


def _touch() -> None:
    global _last_activity
    _last_activity = time.monotonic()


def _backend_connect(port: int, timeout: float = BACKEND_CONNECT_TIMEOUT):
    try:
        return socket.create_connection((BACKEND_HOST, port), timeout=timeout)
    except OSError:
        return None


def _splice(client: socket.socket, backend: socket.socket) -> None:
    global _active_conns
    with _state_lock:
        _active_conns += 1
    _touch()
    try:
        # Both sockets stay BLOCKING. select() only decides what is readable; the
        # write is a blocking sendall, which is the only way it can deliver a body
        # larger than the peer's send buffer. On a non-blocking socket sendall
        # raises BlockingIOError the moment that buffer fills, which read as a dead
        # connection here and truncated every response at 2 x SPLICE_BUF (128 KiB) —
        # small assets fit and survived, the large vendor bundles did not.
        # Thread-per-connection is what makes the blocking write safe.
        client.setblocking(True)
        backend.setblocking(True)
        pairs = {client: backend, backend: client}
        while True:
            readable, _, errored = select.select(list(pairs), [], list(pairs), 300)
            if errored or not readable:
                return
            for sock in readable:
                try:
                    data = sock.recv(SPLICE_BUF)
                except OSError:
                    return
                if not data:
                    return
                _touch()
                try:
                    pairs[sock].sendall(data)
                except OSError:
                    return
    finally:
        with _state_lock:
            _active_conns -= 1
        _touch()
        for sock in (client, backend):
            try:
                sock.close()
            except OSError:
                pass


WAKE_HTML = """\
<!doctype html><html><head><title>Provisa &mdash; waking up</title>
<meta http-equiv="refresh" content="8">
<style>body{font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0b1220;color:#e6edf3}
.card{text-align:center;max-width:26rem}.spin{width:2.5rem;height:2.5rem;border:3px solid #2d3b55;border-top-color:#4c9aff;border-radius:50%;margin:0 auto 1.2rem;animation:s 1s linear infinite}@keyframes s{to{transform:rotate(360deg)}}</style>
</head><body><div class="card"><div class="spin"></div>
<h2>Waking your instance</h2>
<p>The environment scaled to zero while idle. It is starting now and will be ready in about two minutes. This page refreshes automatically.</p>
</div></body></html>"""


def _serve_wake_response(client: socket.socket, port: int) -> None:
    if not (TLS_CERT and TLS_KEY):
        # No TLS material in this deploy: a plaintext answer on an HTTPS port is
        # useless to the browser, so just close (the wake was already triggered).
        client.close()
        return
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(TLS_CERT, TLS_KEY)
    try:
        tls = ctx.wrap_socket(client, server_side=True)
        tls.settimeout(10)
        tls.recv(SPLICE_BUF)  # consume the request; content is irrelevant
        if PORTS[port]["wake_style"] == "html":
            body = WAKE_HTML.encode()
            ctype = "text/html; charset=utf-8"
        else:
            body = json.dumps({"error": "coordinator_waking", "retry_after_seconds": 120}).encode()
            ctype = "application/json"
        tls.sendall(
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Retry-After: 120\r\n"
            b"Cache-Control: no-store\r\n"
            + f"Content-Type: {ctype}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        tls.close()
    except (OSError, ssl.SSLError) as exc:
        log.debug("wake page handshake failed on :%s: %s", port, exc)
        try:
            client.close()
        except OSError:
            pass


def _status_payload() -> dict:
    from concurrent.futures import ThreadPoolExecutor

    with _state_lock:
        idle = time.monotonic() - _last_activity
    status = coordinator_status(fresh=True)
    if status == "RUNNING":
        # Probe concurrently: SYNs to a stopped/booting VM hang the full timeout,
        # and 7 sequential probes would push /status past typical client limits.
        def probe(port: int) -> bool:
            backend = _backend_connect(port, timeout=1.5)
            if backend:
                backend.close()
            return backend is not None

        with ThreadPoolExecutor(max_workers=len(PORTS)) as pool:
            ports = dict(zip((str(p) for p in sorted(PORTS)), pool.map(probe, sorted(PORTS))))
    else:
        ports = {str(p): False for p in sorted(PORTS)}
    return {
        "coordinator": status,
        "ports": ports,
        "all_up": all(ports.values()),
        "idle_seconds": int(idle),
    }


def _serve_status(client: socket.socket) -> None:
    """Authenticated wake/verify endpoint: GET /status, POST /wake (Bearer token)."""
    import hmac

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(TLS_CERT, TLS_KEY)
    try:
        tls = ctx.wrap_socket(client, server_side=True)
        tls.settimeout(10)
        raw = tls.recv(SPLICE_BUF).decode("latin-1")
        request_line = raw.split("\r\n", 1)[0]
        method, path = request_line.split(" ")[0:2]
        auth = ""
        for line in raw.split("\r\n"):
            if line.lower().startswith("authorization:"):
                auth = line.split(":", 1)[1].strip()
        supplied = auth.removeprefix("Bearer ").strip()
        if not hmac.compare_digest(supplied, STATUS_TOKEN):
            code, body = "401 Unauthorized", {"error": "invalid_token"}
        elif method == "GET" and path == "/status":
            code, body = "200 OK", _status_payload()
        elif method == "POST" and path == "/wake":
            trigger_wake()
            code, body = "202 Accepted", {"status": coordinator_status(fresh=True)}
        else:
            code, body = "404 Not Found", {"error": "unknown_route"}
        payload = json.dumps(body).encode()
        tls.sendall(
            f"HTTP/1.1 {code}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n".encode()
            + payload
        )
        tls.close()
    except (OSError, ssl.SSLError, ValueError) as exc:
        log.debug("status endpoint request failed: %s", exc)
        try:
            client.close()
        except OSError:
            pass


def _listen_status() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", STATUS_PORT))
    srv.listen(16)
    log.info("status/wake endpoint on :%s", STATUS_PORT)
    while True:
        client, _ = srv.accept()
        threading.Thread(target=_serve_status, args=(client,), daemon=True).start()


def _handle(client: socket.socket, port: int) -> None:
    backend = _backend_connect(port)
    if backend:
        _splice(client, backend)
        return
    trigger_wake()
    if PORTS[port]["wake_style"] in ("html", "json"):
        _serve_wake_response(client, port)
        return
    # Raw TCP protocol: hold the client while the coordinator boots, then splice.
    deadline = time.monotonic() + WAKE_HOLD_SECONDS
    while time.monotonic() < deadline:
        time.sleep(3)
        backend = _backend_connect(port)
        if backend:
            _splice(client, backend)
            return
    client.close()


def _listen(port: int) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(128)
    log.info("listening on :%s (%s)", port, PORTS[port]["wake_style"])
    while True:
        client, _ = srv.accept()
        threading.Thread(target=_handle, args=(client, port), daemon=True).start()


def idle_stop_due() -> float | None:
    """Seconds idle when the coordinator should be stopped now, else None.

    Split out of the reaper loop so the decision — the part that costs a live cluster when it
    is wrong — is observable without waiting 60 seconds per tick. Four things veto a stop: a
    connection in flight, too little idle time, a coordinator that is not running, and the boot
    grace after a wake we triggered.
    """
    with _state_lock:
        busy = _active_conns > 0
        idle_for = time.monotonic() - _last_activity
    if busy or idle_for < IDLE_STOP_SECONDS:
        return None
    if coordinator_status(fresh=True) != "RUNNING":
        return None
    # Grace: never stop a box that just booted (its own start counts from
    # the wake we triggered, which reset _last_activity via the splice).
    with _state_lock:
        if time.monotonic() - _last_start_call < BOOT_GRACE_SECONDS:
            return None
    return idle_for


def _idle_reaper() -> None:
    while True:
        time.sleep(60)
        try:
            idle_for = idle_stop_due()
            if idle_for is None:
                continue
            log.info("idle %.0fs >= %ss; stopping %s", idle_for, IDLE_STOP_SECONDS, INSTANCE)
            _compute_api("POST", f"instances/{INSTANCE}/stop")
        except urllib.error.URLError as exc:
            log.error("idle stop failed: %s", exc)


def main() -> None:
    for port in PORTS:
        threading.Thread(target=_listen, args=(port,), daemon=True).start()
    if STATUS_TOKEN and TLS_CERT and TLS_KEY:
        threading.Thread(target=_listen_status, daemon=True).start()
    threading.Thread(target=_idle_reaper, daemon=True).start()
    log.info(
        "front-door up: backend=%s ports=%s idle_stop=%ss",
        BACKEND_HOST, sorted(PORTS), IDLE_STOP_SECONDS,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    sys.exit(main())
