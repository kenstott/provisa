# Copyright (c) 2026 Kenneth Stott
# Canary: ad80413b-5711-4c1a-b5cf-ac878ec99185
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What a benchmark device reports about itself (REQ-1911, demo/named/perf/METHODOLOGY.md).

Started on device 1 (Provisa) and device 3 (the backends) of the three-device benchmark, read
by the load generator on device 2. Read-only; it listens only where ``--listen`` says (there is no
default address):

    GET /host           the machine read off itself: machine type and zone (GCE metadata),
                        physical cores (lscpu), each network interface's driver, and the
                        ``--fact`` values the operator's script states (telemetry, engine)
    GET /cpu            cumulative CPU seconds of ``--pid`` and its descendants (device 1)
    GET /docker-stats   one ``docker stats`` snapshot (device 3's containers)
    GET /paging         the host's major faults, swap in and out and available memory, and the
                        major faults of ``--pid`` and its descendants as class ``provisa``
                        (paging.py); the reader takes one at each end of a step
    GET /rtt/<name>     the TCP round trip to the ``--rtt-target`` of that name

A fact that cannot be read is reported under ``errors`` with the reason, never guessed.

    python host_agent.py --listen 10.90.0.2:9555 --pid 12345 --fact telemetry=off \\
        --rtt-target backends=10.90.0.4:25632
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import time
import urllib.request
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import measurements
import paging
from server_cpu_agent import cpu_seconds, parse_listen

_METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/"
_METADATA_TIMEOUT_S = 2
RTT_CONNECTS = 100


def parse_lscpu(text: str) -> dict[str, int]:
    """Physical cores, sockets and threads per core from ``lscpu`` output."""
    fields = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        fields[key.strip()] = value.strip()
    cores_per_socket = int(fields["Core(s) per socket"])
    sockets = int(fields["Socket(s)"])
    return {
        "physical_cores": cores_per_socket * sockets,
        "sockets": sockets,
        "threads_per_core": int(fields["Thread(s) per core"]),
    }


def _lscpu() -> str:
    return subprocess.run(["lscpu"], capture_output=True, text=True, check=True, timeout=10).stdout


def _metadata(path: str) -> str:
    request = urllib.request.Request(_METADATA + path, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=_METADATA_TIMEOUT_S) as resp:  # noqa: S310  # nosec B310
        return resp.read().decode()


def nic_drivers(net: Path = Path("/sys/class/net")) -> dict[str, str]:
    """Each network interface's driver (``gve`` is GCE's gVNIC), loopback and virtual ones
    (no device) left out."""
    return {
        iface.name: (iface / "device" / "driver").resolve().name
        for iface in sorted(net.iterdir())
        if (iface / "device" / "driver").exists()
    }


def host_facts(
    facts: Mapping[str, str],
    *,
    lscpu: Callable[[], str] = _lscpu,
    metadata: Callable[[str], str] = _metadata,
    nics: Callable[[], dict[str, str]] = nic_drivers,
) -> dict[str, Any]:
    out: dict[str, Any] = {"hostname": socket.gethostname(), "facts": dict(facts), "errors": {}}
    readers: dict[str, Callable[[], Any]] = {
        # GCE answers "projects/<n>/machineTypes/<type>" and "projects/<n>/zones/<zone>"
        "machine_type": lambda: metadata("machine-type").rsplit("/", 1)[-1],
        "zone": lambda: metadata("zone").rsplit("/", 1)[-1],
        "cpu": lambda: parse_lscpu(lscpu()),
        "nic_drivers": nics,
    }
    for name, read in readers.items():
        try:
            out[name] = read()
        except Exception as exc:  # noqa: BLE001 - reported under errors, never guessed
            out[name] = None
            out["errors"][name] = f"{type(exc).__name__}: {exc}"
    return out


def tcp_rtt(host: str, port: int, connects: int = RTT_CONNECTS) -> dict[str, Any]:
    """The round trip to ``host:port`` as the time a TCP connect takes: median and p99 of
    ``connects`` connects, in milliseconds."""
    times = []
    for _ in range(connects):
        start = time.perf_counter()
        with socket.create_connection((host, port), timeout=5):
            times.append((time.perf_counter() - start) * 1000)
    times.sort()
    return {
        "host": host,
        "port": port,
        "connects": connects,
        "median_ms": round(statistics.median(times), 3),
        "p99_ms": round(times[min(len(times) - 1, int(0.99 * len(times)))], 3),
    }


def handler(
    *,
    pid: int | None,
    facts: Mapping[str, str],
    rtt_targets: Mapping[str, tuple[str, int]],
    docker_stats: Callable[[], str] = measurements.docker_stats_text,
) -> type[BaseHTTPRequestHandler]:
    def answer(path: str) -> tuple[int, Any]:
        if path == "/host":
            return 200, {**host_facts(facts), "rtt_targets": sorted(rtt_targets)}
        if path == "/cpu":
            if pid is None:
                return 404, {"error": "this agent was started without --pid"}
            return 200, {"pid": pid, "cpu_seconds": cpu_seconds(pid)}
        if path == "/docker-stats":
            return 200, docker_stats()
        if path == "/paging":
            return 200, paging.snapshot({} if pid is None else {"provisa": pid})
        if path.startswith("/rtt/"):
            name = path.removeprefix("/rtt/")
            if name not in rtt_targets:
                return 404, {"error": f"no --rtt-target named {name!r}"}
            return 200, tcp_rtt(*rtt_targets[name])
        return 404, {"error": f"no such path {path!r}"}

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - the http.server hook's name
            status, value = answer(self.path)
            text = value if isinstance(value, str) else json.dumps(value)
            body = text.encode()
            self.send_response(status)
            kind = "text/plain" if isinstance(value, str) else "application/json"
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

    return _Handler


def _pair(value: str, what: str) -> tuple[str, str]:
    key, sep, rest = value.partition("=")
    if not sep or not key or not rest:
        raise argparse.ArgumentTypeError(f"{what} must be NAME=VALUE, got {value!r}")
    return key, rest


def parse_fact(value: str) -> tuple[str, str]:
    return _pair(value, "--fact")


def parse_rtt_target(value: str) -> tuple[str, tuple[str, int]]:
    name, address = _pair(value, "--rtt-target")
    return name, parse_listen(address)


def serve(
    listen: tuple[str, int],
    *,
    pid: int | None,
    facts: Mapping[str, str],
    rtt_targets: Mapping[str, tuple[str, int]],
) -> ThreadingHTTPServer:
    if pid is not None:
        cpu_seconds(pid)  # refuse a pid that is not there before listening
    return ThreadingHTTPServer(listen, handler(pid=pid, facts=facts, rtt_targets=rtt_targets))


def main() -> int:
    parser = argparse.ArgumentParser(description="A benchmark device's facts, read-only")
    parser.add_argument("--listen", type=parse_listen, required=True, help="HOST:PORT, private")
    parser.add_argument("--pid", type=int, help="the Provisa server's process id (device 1)")
    parser.add_argument("--fact", type=parse_fact, action="append", default=[])
    parser.add_argument("--rtt-target", type=parse_rtt_target, action="append", default=[])
    args = parser.parse_args()
    server = serve(
        args.listen, pid=args.pid, facts=dict(args.fact), rtt_targets=dict(args.rtt_target)
    )
    host, port = server.server_address[:2]
    print(f"host agent at http://{host}:{port}/ (pid {os.getpid()})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
