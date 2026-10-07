# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2c5e17-4b90-4f6a-a3d8-1e7b9c0f5a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The advertised Arrow Flight port, shared by every worker process (REQ-1900).

``uvicorn --workers N`` runs N Flight servers, and pyarrow's cannot share a port: Arrow builds its
gRPC server with SO_REUSEPORT turned off and exposes no option to turn it on, so a second process
binding the same port fails. Left there, each worker takes a port of its own and the ADVERTISED
port — the one clients are given, the one a deployment publishes — reaches exactly one worker:
Flight does not scale with workers.

So the port clients dial is bound by this module instead, in every worker, with SO_REUSEPORT (the
kernel spreads incoming connections over the listeners), and each connection's bytes are relayed
to that worker's own Flight server on a loopback port nothing outside the host can reach.

The relay copies bytes and reads none of them. TLS and mTLS are negotiated end to end between the
client and the Flight server — the relay holds no key — and every Flight RPC, a direct ``do_get``
with a ticket included, works unchanged.

THREADS (REQ-1882). A relayed connection has two threads, one per direction, each blocked in
``recv`` or ``sendall`` with the GIL released. They are transport plumbing below the request, the
same role the front loop plays for HTTP ("accepts connections and relays I/O"): no request runs
on them. The Flight handler thread pyarrow gives the RPC still runs the whole request — auth,
governance, execution and the result stream — exactly as it did when the client's socket reached
the Flight server directly. A blocking copy per direction is also what gives backpressure for
free: a client that reads slowly fills its socket, ``sendall`` blocks, and the relay stops reading
from the Flight server.
"""

# Requirements: REQ-1900, REQ-1882

from __future__ import annotations

import errno
import logging
import socket
import threading

log = logging.getLogger(__name__)

_BUFFER_BYTES = 1 << 20
_UPSTREAM_CONNECT_TIMEOUT = 5.0


class FlightRelay:
    """Listens on ``(host, port)`` with SO_REUSEPORT and relays every connection to the Flight
    server on ``127.0.0.1:upstream_port``. Starts listening on construction; ``close`` stops."""

    def __init__(self, host: str, port: int, upstream_port: int) -> None:
        self._upstream = ("127.0.0.1", upstream_port)
        self._closed = False
        self._connections: set[socket.socket] = set()
        self._guard = threading.Lock()
        # reuse_port: every worker process binds this same address, and the bind fails loudly
        # (OSError) on a platform that cannot share it.
        self._listener = socket.create_server((host, port), reuse_port=True)
        self._acceptor = threading.Thread(
            target=self._accept_loop, name=f"flight-relay-accept-{port}", daemon=True
        )
        self._acceptor.start()

    def close(self) -> None:
        """Stop listening and drop every relayed connection.

        A socket is shut down or closed only while no other thread can close it: a descriptor
        closed under a thread still about to use it is reused by the next socket the process opens
        (another server's listener, another connection), and that thread's shutdown or accept then
        lands on the new one. So the listener is closed only once its accept thread has returned,
        and a relayed connection's sockets are shut down here and closed by their relay thread
        under the same lock."""
        with self._guard:
            self._closed = True
            for sock in self._connections:
                _shutdown(sock)
        # Neither close nor (on darwin) shutdown wakes a thread blocked in accept(): a connection
        # does. The accept loop sees the relay closed, drops it, and returns.
        if self._acceptor.is_alive():
            socket.create_connection(self._listener.getsockname()[:2], timeout=5).close()
        self._acceptor.join()
        self._listener.close()

    def _accept_loop(self) -> None:
        while True:
            try:
                client, _addr = self._listener.accept()
            except OSError:
                if self._closed:
                    return
                raise
            if self._closed:
                client.close()  # the connection close() made to wake this thread, or a late one
                return
            threading.Thread(
                target=self._relay, args=(client,), name="flight-relay", daemon=True
            ).start()

    def _track(self, *socks: socket.socket) -> bool:
        with self._guard:
            if self._closed:
                return False
            self._connections.update(socks)
            return True

    def _relay(self, client: socket.socket) -> None:
        """One relayed connection: this thread copies client -> server, a second copies
        server -> client; both end when either side closes."""
        try:
            upstream = socket.create_connection(self._upstream, timeout=_UPSTREAM_CONNECT_TIMEOUT)
        except OSError:
            # This worker's own Flight server is not accepting (shutting down). The client sees
            # its connection closed, which is what a stopped server looks like.
            log.exception("flight relay: local Flight server %s:%d refused", *self._upstream)
            client.close()
            return
        upstream.settimeout(None)
        for sock in (client, upstream):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if not self._track(client, upstream):
            client.close()
            upstream.close()
            return
        back = threading.Thread(
            target=_copy, args=(upstream, client), name="flight-relay-back", daemon=True
        )
        back.start()
        _copy(client, upstream)
        back.join()
        with self._guard:
            self._connections.discard(client)
            self._connections.discard(upstream)
            client.close()
            upstream.close()


def _shutdown(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError as exc:
        # Already disconnected or already closed: there is nothing left to shut down.
        if exc.errno not in (errno.ENOTCONN, errno.EBADF):
            raise


def _copy(source: socket.socket, sink: socket.socket) -> None:
    """Copy ``source`` to ``sink`` until ``source`` ends, then end both: gRPC connections do not
    half-close, so one side going away is the connection going away."""
    buffer = bytearray(_BUFFER_BYTES)
    view = memoryview(buffer)
    try:
        while received := source.recv_into(buffer):
            sink.sendall(view[:received])
    except OSError as exc:
        # A peer reset, or the relay closing the socket under this thread: the connection is
        # over, which is this function's normal end, not a failure of the relay.
        log.debug("flight relay: connection ended: %s", exc)
    finally:
        _shutdown(source)
        _shutdown(sink)
