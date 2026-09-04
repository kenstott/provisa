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

import datetime
import ipaddress
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
# Whose traffic counts as use of the coordinator. Every spliced connection reset the idle clock,
# and the shared IP takes unsolicited connections from the internet all day, so the reaper fired
# twice in a fortnight and a machine meant to scale to zero billed as a 24/7 VM. These are the
# same CIDRs the firewall admits: a client that is not allowed to reach the coordinator is not
# allowed to keep it awake either.
ACTIVITY_NETWORKS = [ipaddress.ip_network(c) for c in CFG["activity_cidrs"]]
TLS_CERT = CFG["tls_cert"]  # path; "" when the deploy runs without TLS material
TLS_KEY = CFG["tls_key"]

BACKEND_CONNECT_TIMEOUT = 2.0
HEALTH_PATH = "/health"
HEALTH_TIMEOUT = 2.5
HEALTH_CACHE_TTL = 3.0
WAKE_HOLD_SECONDS = 110  # raw-TCP clients: hold until boot completes or they give up
STATUS_CACHE_TTL = 10.0
START_DEBOUNCE_SECONDS = 30.0
SPLICE_BUF = 65536
DRAIN_TIMEOUT = 0.5  # bound on reading the rest of a request before closing; see _drain
DRAIN_LIMIT = 1 << 20

_state_lock = threading.Lock()
_last_activity = time.monotonic()
_active_conns = 0
_last_start_call = 0.0
_status_cache = ("", 0.0)
_health_cache: dict[int, tuple[bool, float]] = {}

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


def coordinator_uptime_seconds() -> float:
    """Seconds since the coordinator last started, from the instance itself.

    The boot grace has to cover every start, not only the ones this proxy issued: an operator
    running `gcloud compute instances start` or `reset` for a deploy leaves _last_start_call
    untouched and _last_activity at its pre-stop value, so the reaper stopped the box on its
    next tick — mid startup script. lastStartTimestamp is the instance's own record of when it
    came up, so a start from any source counts.
    """
    started = _compute_api("GET", f"instances/{INSTANCE}")["lastStartTimestamp"]
    delta = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(started)
    return delta.total_seconds()


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


def counts_as_activity(peer: str) -> bool:
    """Whether a connection from this address is evidence the coordinator is in use."""
    addr = ipaddress.ip_address(peer)
    return any(addr in net for net in ACTIVITY_NETWORKS)


def _backend_connect(port: int, timeout: float = BACKEND_CONNECT_TIMEOUT):
    try:
        return socket.create_connection((BACKEND_HOST, port), timeout=timeout)
    except OSError:
        return None


def _backend_ready(port: int, fresh: bool = False) -> bool:
    """Whether the app behind an HTTPS port is SERVING, not merely accepting.

    A TCP accept is the UI container, which binds its port the moment it restarts. The API's
    lifespan runs long after that — it re-seeds the bootstrap org and rebuilds org_registry — and
    /health is the endpoint that only answers once it has finished. Splicing on the accept is what
    produced the middle state users saw: the waking page stopped appearing, the SPA loaded, and
    every call it made was refused, so the site said it could not reach Provisa. Held to /health,
    the client keeps getting the waking page until the whole path is up.

    The result is cached briefly because a browser opens several connections per page load and each
    one would otherwise cost a probe. ``fresh`` skips that read (never the write): a cached NO is
    an answer about a moment that has passed, and _https_ready re-asks it before calling a live
    site down.
    """
    ready, at = _health_cache.get(port, (False, 0.0))
    if not fresh and time.monotonic() - at < HEALTH_CACHE_TTL:
        return ready
    ready = False
    sock = _backend_connect(port, timeout=HEALTH_TIMEOUT)
    if sock is not None:
        # The backend's certificate is issued for the public hostname and this dials the VM's
        # internal address, so a verifying context would fail on the name for a perfectly healthy
        # app. The hop is inside the VPC and the question here is liveness, not identity.
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            with ctx.wrap_socket(sock) as tls:
                tls.settimeout(HEALTH_TIMEOUT)
                tls.sendall(
                    f"GET {HEALTH_PATH} HTTP/1.1\r\nHost: {BACKEND_HOST}\r\n"
                    "Connection: close\r\n\r\n".encode()
                )
                status_line = tls.recv(SPLICE_BUF).split(b"\r\n", 1)[0].split()
                ready = len(status_line) > 1 and status_line[1] == b"200"
        except (OSError, ssl.SSLError):
            ready = False
    _health_cache[port] = (ready, time.monotonic())
    return ready


def _https_ready(port: int) -> bool:
    """The routing decision for an HTTPS port: is the site up, or does this connection wake it?

    _backend_ready is one connect, one request, one recv against a 2.5s budget, and a single
    unlucky sample — a GC pause in the app, a momentary accept backlog — answers NO for a site
    that is serving. That NO is then cached for HEALTH_CACHE_TTL, so every connection in the
    window gets the waking page and the SPA's XHRs fail against a live instance. This asks the
    instance state first (already cached, no network cost when it is warm) and re-probes past the
    cache only when the box is RUNNING: a genuinely stopped coordinator never pays a second
    connect timeout, and a live one is not declared down on one sample.
    """
    if _backend_ready(port):
        return True
    if coordinator_status() != "RUNNING":
        return False
    return _backend_ready(port, fresh=True)


def _splice(client: socket.socket, backend: socket.socket, activity: bool) -> None:
    """Pump bytes between the two sockets.

    ``activity`` says whether this connection may reset the idle clock -- see counts_as_activity.
    An unrecognised client is still served; it simply does not vote on the coordinator's lifetime.
    """
    global _active_conns
    with _state_lock:
        _active_conns += 1
    if activity:
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
                if activity:
                    _touch()
                try:
                    pairs[sock].sendall(data)
                except OSError:
                    return
    finally:
        with _state_lock:
            _active_conns -= 1
        if activity:
            _touch()
        for sock in (client, backend):
            try:
                sock.close()
            except OSError:
                pass


WAKE_HTML = """\
<!doctype html><html><head><title>Provisa &mdash; waking up</title>
<meta http-equiv="refresh" content="8">
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:radial-gradient(120% 120% at 50% 45%,#16213f 0%,#0f1117 55%,#0b0d13 100%);color:#e6edf3;overflow:hidden}
#wake-canvas{position:fixed;inset:0;width:100%;height:100%}
.card{position:relative;text-align:center;max-width:26rem}
.mark{display:flex;flex-direction:column;align-items:center;gap:14px;animation:rise 700ms ease both}
.mark svg{filter:drop-shadow(0 0 22px rgba(16,185,129,.35))}
.word{font:600 15px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;letter-spacing:.42em;text-indent:.42em;color:#e1e4ed;opacity:.9}
h2{margin:18px 0 8px;font-size:1.25rem}
p{margin:0;color:#9aa3b5;font-size:.9rem;line-height:1.5}
@keyframes rise{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@media (prefers-reduced-motion: reduce){.mark{animation:none}#wake-canvas{display:none}}
</style>
</head><body>
<canvas id="wake-canvas"></canvas>
<div class="card">
<div class="mark">
<svg viewBox="0 0 100 100" width="76" height="76" role="img" aria-label="Provisa">
<g fill="#e1e4ed"><rect x="28" y="18" width="15" height="64" rx="7"/><circle cx="50" cy="35" r="22"/></g>
<circle cx="50" cy="35" r="10.5" fill="#0f1117"/><circle cx="50" cy="35" r="4.5" fill="#10B981"/>
</svg>
<div class="word">PROVISA</div>
</div>
<h2>Waking your instance</h2>
<p>The environment scaled to zero while idle. It is starting now and will be ready in about two minutes. This page refreshes automatically.</p>
</div>
<script>
(function(){
var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion: reduce)").matches;
var canvas=document.getElementById("wake-canvas");
if(reduce||!canvas||!canvas.getContext)return;
var ctx=canvas.getContext("2d");
var dpr=Math.min(window.devicePixelRatio||1,2);
var W=0,H=0,cx=0,cy=0,spawnR=0,coreR=0;
var mark=document.querySelector(".mark svg");
function markCenter(){
if(mark){var r=mark.getBoundingClientRect();if(r.width)return{x:r.left+r.width/2,y:r.top+r.height/2};}
return{x:W/2,y:H*0.4};
}
function resize(){
W=canvas.clientWidth;H=canvas.clientHeight;
canvas.width=W*dpr;canvas.height=H*dpr;
ctx.setTransform(dpr,0,0,dpr,0,0);
var c=markCenter();cx=c.x;cy=c.y;
spawnR=Math.max(W,H)*0.72;
coreR=Math.max(56,Math.min(W,H)*0.13);
ctx.fillStyle="#0f1117";ctx.fillRect(0,0,W,H);
}
window.addEventListener("resize",resize);
resize();
requestAnimationFrame(function(){var c=markCenter();cx=c.x;cy=c.y;});
setTimeout(function(){var c=markCenter();cx=c.x;cy=c.y;},760);
var N=Math.max(140,Math.min(320,Math.round((W*H)/5200)));
var P=[];
function spawn(p){
var a=Math.random()*Math.PI*2;
var r=spawnR*(0.7+Math.random()*0.5);
p.x=cx+Math.cos(a)*r;p.y=cy+Math.sin(a)*r*0.82;
p.px=p.x;p.py=p.y;p.spd=0.6+Math.random()*1.1;p.life=0;
}
for(var i=0;i<N;i++){var seed={};spawn(seed);P.push(seed);}
function frame(){
ctx.fillStyle="rgba(15,17,23,0.085)";ctx.fillRect(0,0,W,H);
for(var i=0;i<P.length;i++){
var p=P[i];
var dx=cx-p.x,dy=cy-p.y;
var d=Math.sqrt(dx*dx+dy*dy)||1;
var curl=Math.sin(p.x*0.012+p.y*0.01)*0.9;
var ang=Math.atan2(dy,dx)+curl*(d/spawnR);
var v=p.spd*(0.6+(1-d/spawnR)*2.2);
p.px=p.x;p.py=p.y;
p.x+=Math.cos(ang)*v;p.y+=Math.sin(ang)*v;
p.life++;
var t=Math.max(0,Math.min(1,1-d/(spawnR*0.55)));
var rr=Math.round(99+(16-99)*t);
var gg=Math.round(102+(185-102)*t);
var bb=Math.round(241+(129-241)*t);
var alpha=0.22+t*0.6;
ctx.strokeStyle="rgba("+rr+","+gg+","+bb+","+alpha+")";
ctx.lineWidth=0.9+t*1.6;
ctx.beginPath();ctx.moveTo(p.px,p.py);ctx.lineTo(p.x,p.y);ctx.stroke();
if(d<coreR||p.life>900)spawn(p);
}
var pulse=0.55+0.45*Math.sin(Date.now()*0.002);
var g=ctx.createRadialGradient(cx,cy,0,cx,cy,coreR*1.3);
g.addColorStop(0,"rgba(16,185,129,"+0.16*pulse+")");
g.addColorStop(0.5,"rgba(52,120,180,"+0.06*pulse+")");
g.addColorStop(1,"rgba(16,185,129,0)");
ctx.fillStyle=g;
ctx.beginPath();ctx.arc(cx,cy,coreR*1.3,0,Math.PI*2);ctx.fill();
requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
})();
</script>
</body></html>"""


def _wants_html(request: bytes) -> bool:
    """Whether these request bytes are a browser navigating, as opposed to the SPA calling an API.

    Port 443 carries both, so wake_style alone cannot decide: an HTML waking page handed to a
    fetch() is JSON.parse garbage, and the SPA falls back to the bare reason phrase ("Service
    Unavailable") instead of saying the instance is starting. This is the same discrimination
    ui_server.is_spa_navigation makes -- Sec-Fetch-Dest: document, or, for a UA that omits it, the
    GET + Accept: text/html pair.
    """
    head = request.split(b"\r\n\r\n", 1)[0].decode("latin-1")
    lines = head.split("\r\n")
    method = lines[0].split(" ")[0].upper() if lines else ""
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    dest = headers.get("sec-fetch-dest")
    if dest is not None:
        return dest == "document"
    return method == "GET" and "text/html" in headers.get("accept", "")


def _drain(tls: ssl.SSLSocket) -> None:
    """Read whatever is left of the request before closing.

    A GraphQL call is a POST, and the first recv takes the headers -- the body often arrives in a
    later segment. Closing with unread bytes in the receive buffer makes the kernel send RST
    instead of FIN, and the browser reports a transport failure ("Failed to fetch") rather than
    the 503 that was just written, so the SPA never sees the waking message at all. Bounded by a
    short timeout and a byte cap: the point is a clean close, not reading an upload.
    """
    tls.settimeout(DRAIN_TIMEOUT)
    left = DRAIN_LIMIT
    try:
        while left > 0 and tls.recv(min(SPLICE_BUF, left)):
            left -= SPLICE_BUF
    except (OSError, ssl.SSLError):
        pass


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
        request = tls.recv(SPLICE_BUF)
        if PORTS[port]["wake_style"] == "html" and _wants_html(request):
            body = WAKE_HTML.encode()
            ctype = "text/html; charset=utf-8"
        else:
            # REQ-1350 error shape: `code` is what the UI's catalog translates, `detail` is the
            # English sentence it falls back to when the key is absent.
            body = json.dumps(
                {
                    "detail": "The environment is starting. It will be ready in about two minutes.",
                    "code": "front_door.coordinator_waking",
                    "params": {"retry_after_seconds": 120},
                    "retry_after_seconds": 120,
                }
            ).encode()
            ctype = "application/json"
        tls.sendall(
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Retry-After: 120\r\n"
            b"Cache-Control: no-store\r\n"
            + f"Content-Type: {ctype}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        _drain(tls)
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
            # Same definition of "up" the proxy routes on, so all_up means a client sent here now
            # gets the site rather than the app's own cannot-connect page.
            if PORTS[port]["wake_style"] in ("html", "json"):
                return _backend_ready(port)
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


def _handle(client: socket.socket, port: int, peer: str) -> None:
    # An unsolicited connection from the internet is served whatever is already up and nothing
    # more: it neither starts the coordinator nor resets its idle clock. Both halves matter --
    # waking costs an hour of VM time, and the reset is what kept a scale-to-zero box running
    # all month. counts_as_activity names the CIDRs the firewall admits.
    activity = counts_as_activity(peer)
    if PORTS[port]["wake_style"] in ("html", "json"):
        # Readiness on an HTTPS port is /health, not an accept: see _backend_ready. A port that
        # accepts but is not serving still gets the waking page, and still triggers the wake —
        # the box may be RUNNING with the app mid-boot, where trigger_wake is a no-op.
        if _https_ready(port):
            backend = _backend_connect(port)
            if backend:
                _splice(client, backend, activity)
                return
        if not activity:
            client.close()
            return
        trigger_wake()
        _serve_wake_response(client, port)
        return
    backend = _backend_connect(port)
    if backend:
        _splice(client, backend, activity)
        return
    if not activity:
        client.close()
        return
    trigger_wake()
    # Raw TCP protocol: hold the client while the coordinator boots, then splice.
    deadline = time.monotonic() + WAKE_HOLD_SECONDS
    while time.monotonic() < deadline:
        time.sleep(3)
        backend = _backend_connect(port)
        if backend:
            _splice(client, backend, activity)
            return
    client.close()


def _listen(port: int) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(128)
    log.info("listening on :%s (%s)", port, PORTS[port]["wake_style"])
    while True:
        client, addr = srv.accept()
        threading.Thread(target=_handle, args=(client, port, addr[0]), daemon=True).start()


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
    # A box cannot have been idle for longer than it has been up. Without this the counter
    # kept running across the stop, so a start from anywhere but this proxy — an operator
    # deploying with `gcloud compute instances start` or `reset` — was already past the idle
    # threshold at boot and got stopped on the next tick, mid startup script.
    if coordinator_uptime_seconds() < IDLE_STOP_SECONDS:
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
        BACKEND_HOST,
        sorted(PORTS),
        IDLE_STOP_SECONDS,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    sys.exit(main())
