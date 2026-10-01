# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3a9e52-1c84-4f06-b5d9-8e2f0a4c6b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How do HTTP connections spread over `uvicorn --workers N` — shared socket vs a socket per
worker? (REQ-1900)

BEFORE: uvicorn's one accept socket, shared by every worker. AFTER: the same uvicorn launch on a
unix socket, with each worker starting provisa's ``WorkerHttpListener`` (its own SO_REUSEPORT
socket on the public port) in its lifespan — what the server does when the launcher sets
``PROVISA_HTTP_LISTEN``. Clients open C keep-alive connections at once; each answer names the
worker process, so the output is connections per worker.

The spread is the kernel's, so this runs where the product is deployed — a Linux container
(needs only python + uvicorn, and this repository mounted)::

    docker run --rm -v "$PWD":/src:ro -w /src python:3.12-slim \\
        sh -c 'pip -q install uvicorn && python tests/integration/http_listener_spread_check.py'
"""

# Requirements: REQ-1900

from __future__ import annotations

import collections
import contextlib
import http.client
import importlib.util
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

WORKERS = 16
_LISTENER_PATH = Path(__file__).parents[2] / "provisa" / "api" / "http_listener.py"


def _load_listener():
    # By path: importing the provisa package pulls in the whole application's dependencies.
    spec = importlib.util.spec_from_file_location("http_listener", _LISTENER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _answer(send) -> None:
    body = str(os.getpid()).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def app(scope, receive, send):
    """Answers with the worker's pid; in its lifespan, starts the worker's own listener when
    the launcher asked for one."""
    if scope["type"] == "lifespan":
        listener = None
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                module = _load_listener()
                address = module.configured_address()
                if address is not None:
                    listener = module.WorkerHttpListener(app, *address)
                    listener.start()
                await send({"type": "lifespan.startup.complete"})
            else:
                if listener is not None:
                    await listener.stop()
                await send({"type": "lifespan.shutdown.complete"})
                return
    elif scope["type"] == "http":
        await _answer(send)


def _spread(port: int, clients: int, requests_each: int = 20) -> list[int]:
    per_conn: list[bytes | None] = [None] * clients
    gate = threading.Barrier(clients)

    def one(i: int) -> None:
        gate.wait()
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
        pids = set()
        for _ in range(requests_each):
            conn.request("GET", "/")
            pids.add(conn.getresponse().read())
        conn.close()
        assert len(pids) == 1, "a keep-alive connection stays on one worker"
        per_conn[i] = pids.pop()

    threads = [threading.Thread(target=one, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    counts = sorted(collections.Counter(per_conn).values(), reverse=True)
    return counts + [0] * (WORKERS - len(counts))


def _wait_port(port: int) -> None:
    for _ in range(600):
        with contextlib.suppress(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        time.sleep(0.1)
    raise SystemExit("server did not start")


def _launch(bind: list[str], env: dict[str, str]) -> subprocess.Popen:
    here = str(Path(__file__).parent)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "http_listener_spread_check:app", "--workers"]
        + [str(WORKERS), "--log-level", "warning", *bind],
        cwd=here,
        env={**os.environ, **env},
    )


def main() -> int:
    print(sys.platform, os.uname().release, f"- {WORKERS} workers, connections per worker")
    results = {}
    with tempfile.TemporaryDirectory() as tmp:
        for label, port, bind, env in (
            ("shared accept socket (before)", 18101, ["--port", "18101"], {}),
            (
                "SO_REUSEPORT per worker (after)",
                18102,
                ["--uds", f"{tmp}/sup.sock"],
                {"PROVISA_HTTP_LISTEN": "127.0.0.1:18102"},
            ),
        ):
            proc = _launch(bind, env)
            try:
                _wait_port(port)
                time.sleep(3)  # every worker has started its listener
                for clients in (16, 64, 256):
                    results[label, clients] = _spread(port, clients)
                    print(f"{label:<32} c={clients:<4} {results[label, clients]}", flush=True)
            finally:
                proc.terminate()  # only the server this script started
                proc.wait(timeout=60)
    if sys.platform == "linux":
        before = results["shared accept socket (before)", 64]
        after = results["SO_REUSEPORT per worker (after)", 64]
        if not max(after) < max(before):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
