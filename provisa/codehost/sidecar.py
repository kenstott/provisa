# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2a0e-8c41-4d7a-9e15-b2d4c7a91e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source's pgwire sidecar (REQ-2267 part 3): the PostgreSQL face a built host
serves (pgfront.py), run as a pure-Python process Provisa launches beside it, one per dev-mode
source, so a PostgreSQL client or a federation engine reads the code's tables.

    <python> sidecar.py <port> <bind host> <spill directory> <bounds> <fd:N,N,...>

Its worker is Provisa's own: the descriptors of ``fd:`` are connections Provisa made and kept the
other ends of, each speaking the host protocol (provisa/codesource/worker.py), and every call made
on one is run by Provisa on the source's worker in its pool (provisa/codesource/pgwire_sidecar.py).
So a read through the sidecar is the same call, on the same worker, as any other read of the
source. Its name tables (provisa/codesource/names.py) come on stdin, as JSON; ``bounds`` are the
operator's bounds on its DuckDB (``memory_mb``, ``threads``, ``spill_cap_mb``), as JSON, which
it asks Provisa for again, as they are then, before each statement it answers.

It serves plain PostgreSQL on ``<bind host>:<port>`` and checks no credentials, as the other
pgwire replicas do (provisa/federation/pgwire_replica.py): it is bound to loopback, or to the
address the deployment names for its engines alone. When Provisa goes, its connections close and
the sidecar ends (front.Worker); a SIGTERM ends it at once.
"""

# Requirements: REQ-2267
from __future__ import annotations

import json
import signal
import socket
import socketserver
import sys
from pathlib import Path
from typing import Any

#: What the sidecar writes once it is listening, with its port.
LISTENING = "pgwire sidecar listening on "


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], worker: Any, names: dict) -> None:
        self.worker = worker
        self.names = names
        super().__init__(address, _Handler)


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        import importlib

        server: Any = self.server
        importlib.import_module("pgfront").serve(self.request, server.worker, server.names)


def _connections(given: str) -> list[socket.socket]:
    if not given.startswith("fd:"):
        raise ValueError(f"the sidecar's connections are given as fd:N,..., not {given!r}")
    sockets = [socket.socket(fileno=int(fd)) for fd in given.removeprefix("fd:").split(",")]
    for sock in sockets:
        # subprocess.Popen clears FD_CLOEXEC on pass_fds so the child can receive them, but
        # socket.socket(fileno=N) does not restore it.  Mark them non-inheritable now so any
        # subprocess the sidecar or its imports start does not hold them open and block EOF.
        sock.set_inheritable(False)
    return sockets


def main(argv: list[str]) -> int:
    port, bind, spill, bounds, connections = argv
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here))  # pgfront, residual and pgcatalog, as a built host has them
    import importlib

    front = importlib.import_module("front")
    Path(spill).mkdir(parents=True, exist_ok=True)
    # The bounds it starts with; it asks for them again before each statement (settle_bounds).
    given = front.Bounds(Path(spill), spill)
    given.apply(json.loads(bounds))
    names = json.loads(sys.stdin.read())

    class _ProvisaWorker(front.Worker):
        """front's worker over the connections Provisa gave: no process of its own."""

        def __init__(self, conns: list[socket.socket]) -> None:
            self.bounds = given
            self.process = None
            self._attach(conns)

        def settle_bounds(self) -> None:
            # REQ-2267: before each statement, the operator's bounds as Provisa has them now.
            lane, own = self._lease()
            try:
                lane.send({"op": "bounds", "id": own})
                for each in self._frames(lane, own):
                    self.bounds.apply(each["bounds"])  # its one message, the end
            finally:
                self._release(lane)

    worker = _ProvisaWorker(_connections(connections))
    server = _Server((bind, int(port)), worker, names)

    def stop(_signum: int, _frame: Any) -> None:
        import os

        os._exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    sys.stderr.write(f"{LISTENING}{server.server_address[1]}\n")
    sys.stderr.flush()
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
