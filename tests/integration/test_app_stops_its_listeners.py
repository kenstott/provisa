# Copyright (c) 2026 Kenneth Stott
# Canary: ebda7b04-3b5e-4885-9609-21c46f8fb55f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an app starts, it stops: its Bolt, pgwire and airport listeners end with its lifespan
(REQ-1900).

Each is bound to a port every worker shares (SO_REUSEPORT). The lifespan stopped the HTTP
listener, gRPC and the Flight relay and left these three running: an app that had shut down was
still bound and still accepting, on a thread nothing would ever join. Another worker's listener
on the same port must be untouched by one worker's stop, and the stop must not wait on it."""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest

from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# Generous for a loaded machine; a stop that depended on the other listener never returned.
_SHUTDOWN_BOUND_S = 60


def _accepts(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
    except OSError:
        return False
    return True


def _threads_named(name: str) -> int:
    return sum(1 for t in threading.enumerate() if t.name == name and t.is_alive())


async def test_shutdown_stops_the_apps_own_listeners_and_leaves_another_workers(monkeypatch):
    from provisa.api.app import create_app, state
    from provisa.api.flight.relay import FlightRelay
    from provisa.bolt.server import BoltListener
    from provisa.pgwire.server import start_pgwire_server, stop_pgwire_server

    pgwire_port, bolt_port, airport_port, spare = lease_ports(4)
    os.environ.setdefault("PG_PASSWORD", "provisa")
    monkeypatch.setenv("PROVISA_PGWIRE_PORT", str(pgwire_port))
    monkeypatch.setenv("PROVISA_BOLT_PORT", str(bolt_port))
    monkeypatch.setenv("PROVISA_AIRPORT_PORT", str(airport_port))

    app = create_app()
    others: list = []
    try:
        async with app.router.lifespan_context(app):
            pgwire, bolt, relay = state._pgwire_server, state._bolt_listener, state._airport_relay
            assert pgwire is not None and bolt is not None and relay is not None
            for port in (pgwire_port, bolt_port, airport_port):
                assert _accepts(port), port
            # Another worker's listeners on the same three ports.
            other_pgwire = start_pgwire_server("127.0.0.1", pgwire_port, None)
            other_bolt = BoltListener("127.0.0.1", bolt_port, None)
            other_relay = FlightRelay("127.0.0.1", airport_port, spare)
            others = [
                lambda: stop_pgwire_server(other_pgwire),
                other_bolt.close,
                other_relay.close,
            ]
            assert _threads_named(f"bolt-accept:{bolt_port}") == 2
            assert _threads_named(f"flight-relay-accept-{airport_port}") == 2
            began = time.monotonic()
        took = time.monotonic() - began
        assert took < _SHUTDOWN_BOUND_S, f"shutdown took {took:.0f}s with other listeners bound"

        # The app's own: no accept thread, no listening socket, nothing kept.
        assert _threads_named(f"bolt-accept:{bolt_port}") == 1  # the other worker's
        assert _threads_named(f"flight-relay-accept-{airport_port}") == 1
        assert pgwire.socket.fileno() == -1
        assert bolt._sock.fileno() == -1  # noqa: SLF001 - its listening socket
        assert relay._listener.fileno() == -1  # noqa: SLF001 - its listening socket
        assert state._pgwire_server is None and state._bolt_listener is None
        assert state._airport_relay is None and state._airport_server is None

        # The other worker's listeners still serve their ports.
        for port in (pgwire_port, bolt_port, airport_port):
            assert _accepts(port), port
    finally:
        for stop in others:
            stop()
    for port in (pgwire_port, bolt_port, airport_port):
        assert not _accepts(port), port
