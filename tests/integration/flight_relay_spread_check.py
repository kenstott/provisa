# Copyright (c) 2026 Kenneth Stott
# Canary: 1c9e4a70-6b3d-4f58-8a27-d5f0e2b6c914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Do direct Flight ``do_get`` calls on ONE port reach more than one worker process? (REQ-1900)

Four processes each run a pyarrow Flight server on a loopback port and provisa's FlightRelay on
the same shared port — what four `uvicorn --workers` processes do. 50 clients then call ``do_get``
on the shared port, and each answer names the process that served it.

Which listener gets a connection is the kernel's decision: Linux spreads connections over
SO_REUSEPORT listeners, darwin hands them all to one. So this is run where the product is
deployed, in a Linux container (needs only python + pyarrow, and this repository mounted)::

    docker run --rm -v "$PWD":/src:ro -w /src python:3.12-slim \\
        sh -c 'pip -q install pyarrow && python tests/integration/flight_relay_spread_check.py'

Exit status is non-zero on Linux when fewer than two processes served.
"""

# Requirements: REQ-1900

from __future__ import annotations

import collections
import importlib.util
import multiprocessing
import os
import socket
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as fl

WORKERS = 4
CALLS = 50
_RELAY_PATH = Path(__file__).parents[2] / "provisa" / "api" / "flight" / "relay.py"


def _load_relay():
    # By path: importing the provisa package pulls in the whole application's dependencies.
    spec = importlib.util.spec_from_file_location("flight_relay", _RELAY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _WhoAmI(fl.FlightServerBase):
    def do_get(self, context, ticket):  # noqa: ARG002 - Flight override signature
        return fl.RecordBatchStream(pa.table({"pid": [os.getpid()]}))


def _worker(port: int, ready) -> None:
    server = _WhoAmI("grpc://127.0.0.1:0")
    threading.Thread(target=server.serve, daemon=True).start()
    _load_relay().FlightRelay("0.0.0.0", port, server.port)  # noqa: S104 - same bind as the product
    ready.set()
    threading.Event().wait()


def main() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    ctx = multiprocessing.get_context("spawn")
    workers = []
    for _ in range(WORKERS):
        ready = ctx.Event()
        proc = ctx.Process(target=_worker, args=(port, ready), daemon=True)
        proc.start()
        assert ready.wait(60), "worker did not start"
        workers.append(proc)
    served: collections.Counter[int] = collections.Counter()
    guard = threading.Lock()

    def _call() -> None:
        client = fl.connect(f"grpc://127.0.0.1:{port}")
        try:
            pid = client.do_get(fl.Ticket(b"x"), fl.FlightCallOptions(timeout=30)).read_all()
        finally:
            client.close()
        with guard:
            served[pid["pid"][0].as_py()] += 1

    started = time.monotonic()
    threads = [threading.Thread(target=_call) for _ in range(CALLS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for proc in workers:  # only the processes this script started
        proc.terminate()
    print(
        f"{sys.platform} {os.uname().release}: {WORKERS} workers on port {port}, "
        f"{sum(served.values())}/{CALLS} do_get calls answered in "
        f"{time.monotonic() - started:.1f}s by {len(served)} process(es), "
        f"split {sorted(served.values(), reverse=True)}"
    )
    assert sum(served.values()) == CALLS
    assert set(served) <= {p.pid for p in workers}
    if sys.platform == "linux" and len(served) < 2:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
