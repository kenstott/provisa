# Copyright (c) 2026 Kenneth Stott
# Canary: d49b328a-bd3a-4e5d-b44b-be73810d50c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Opt-in startup hook for the airport Flight service (REQ-1120).

Gated on ``PROVISA_AIRPORT_PORT`` (mirrors the MCP/pgwire/bolt opt-in pattern).
When enabled, starts :class:`ProvisaAirportServer` on a daemon thread and logs
the mandatory ``airport server listening on ...`` startup-banner line.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.app import AppState


def start_airport_server(state: AppState, log: logging.Logger) -> None:
    """Start the airport Flight server if PROVISA_AIRPORT_PORT is set (>0)."""
    from provisa.core import settings_registry

    port = settings_registry.value("server.airport_port")  # REQ-1913; 0 = not started
    if not port:
        return

    from provisa.api.airport.server import ProvisaAirportServer

    server = ProvisaAirportServer(
        state,
        host=state.hostname,
        port=port,
    )
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    state._airport_server = server  # stopped by the lifespan at shutdown
    # REQ-1900: every worker process binds the advertised port (SO_REUSEPORT) and relays to its
    # own server. Before this, under `--workers N` every worker but the first failed to bind the
    # port and did not start. A client's connection stays with ONE worker for its whole life, and
    # this server's transactions live in that worker's memory: a client that spreads one
    # transaction's RPCs over several connections is not supported.
    from provisa.api.flight.relay import FlightRelay

    state._airport_relay = FlightRelay(  # stopped by the lifespan at shutdown
        "0.0.0.0",  # nosec B104 - the airport endpoint intentionally binds all interfaces
        port,
        server.port,
    )
    log.info("airport server listening on %s:%d", state.hostname, port)
