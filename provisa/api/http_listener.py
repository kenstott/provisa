# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1d90-8c26-4f53-a9e4-6d0c2f8b5a71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each worker's own listening socket on the public HTTP port (REQ-1900).

``uvicorn --workers N`` binds ONE socket in its supervisor and every worker accepts on it. Which
worker gets a connection is then whichever wakes first, and on Linux that is persistently the same
few: 16 workers at 16 connections served them as 7,3,2,1,1,1,1 with nine workers idle, and at 64
as 13,9,9,8,5,4,3,2,2,2,2,1,1,1,1,1. A socket per worker bound with SO_REUSEPORT — what pgwire,
gRPC, Bolt, MCP and the Flight relay already do — lets the kernel hash each connection to a
listener (same 16 workers: 3,3,2,2,1,1,1,1,1,1 and 7,7,6,5,5,5,5,4,4,4,3,3,2,2,2,0).

So when the launcher names the public address in ``PROVISA_HTTP_LISTEN`` (``host:port``), each
worker opens its own SO_REUSEPORT listener there and serves the SAME ASGI app on it, on the
worker's own event loop — the request-thread middleware, auth and every route are the app's and
are unchanged. uvicorn's own socket is then the supervisor's and is bound somewhere clients do not
dial (the launcher gives it a unix socket).

Not set: nothing here runs and uvicorn's socket is the public one, as before. That is the right
choice for one worker (there is nothing to spread) and on macOS, whose kernel hands every
connection on a SO_REUSEPORT port to ONE listener — worse than the shared socket.

The listener mirrors what the uvicorn command line gives the shared socket: proxy headers on,
``FORWARDED_ALLOW_IPS`` honoured, the keep-alive timeout from ``PROVISA_HTTP_KEEP_ALIVE``, and TLS
when ``PROVISA_HTTP_CERT``/``PROVISA_HTTP_KEY`` are set. Shutdown is uvicorn's own graceful one:
stop accepting, let in-flight requests finish, then return.
"""

# Requirements: REQ-1900

from __future__ import annotations

import asyncio
import os
import socket
from typing import Any

import uvicorn


def configured_address() -> tuple[str, int] | None:
    """The public ``(host, port)`` the launcher asked each worker to listen on, or ``None``."""
    raw = os.environ.get("PROVISA_HTTP_LISTEN")
    if not raw:
        return None
    host, _, port = raw.rpartition(":")
    return host, int(port)


def reuseport_socket(host: str, port: int) -> socket.socket:
    """A listening socket other worker processes can bind the same address beside."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind((host, port))
    sock.set_inheritable(True)
    return sock


class WorkerHttpListener:
    """This worker's HTTP server on its own SO_REUSEPORT socket, serving ``app``."""

    def __init__(self, app: Any, host: str, port: int) -> None:
        self._sock = reuseport_socket(host, port)
        cert, key = os.environ.get("PROVISA_HTTP_CERT"), os.environ.get("PROVISA_HTTP_KEY")
        with_tls = bool(cert and key)
        config = uvicorn.Config(
            app,
            # The app's lifespan is the worker's and is already running: this server must not
            # start (or, at shutdown, end) it a second time.
            lifespan="off",
            proxy_headers=True,
            timeout_keep_alive=int(os.environ.get("PROVISA_HTTP_KEEP_ALIVE", "5")),
            log_level="warning",
            ssl_certfile=cert if with_tls else None,
            ssl_keyfile=key if with_tls else None,
        )
        self._server = uvicorn.Server(config)
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start serving on the running event loop (the worker's)."""
        # `_serve`, not `serve`: `serve` installs this server's own SIGINT/SIGTERM handlers over
        # the ones uvicorn's server for this worker process installed. The worker's signals stay
        # that server's; it shuts the app down, and the app's lifespan stops this listener.
        self._task = asyncio.get_running_loop().create_task(
            self._server._serve(sockets=[self._sock]),  # noqa: SLF001 - see above
            name="provisa-http-listener",
        )

    async def stop(self) -> None:
        """Stop accepting, let in-flight requests finish, and return."""
        self._server.should_exit = True
        if self._task is not None:
            await self._task
            self._task = None
