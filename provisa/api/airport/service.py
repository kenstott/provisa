# Copyright (c) 2026 Kenneth Stott
# Canary: d49b328a-bd3a-4e5d-b44b-be73810d50c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Opt-in startup hook for the airport Flight service (REQ-1106).

Gated on ``PROVISA_AIRPORT_PORT`` (mirrors the MCP/pgwire/bolt opt-in pattern).
When enabled, starts :class:`ProvisaAirportServer` on a daemon thread and logs
the mandatory ``airport server listening on ...`` startup-banner line.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.app import AppState


def start_airport_server(state: AppState, log: logging.Logger) -> None:
    """Start the airport Flight server if PROVISA_AIRPORT_PORT is set (>0)."""
    port = int(os.environ.get("PROVISA_AIRPORT_PORT", "0"))
    if not port:
        return

    from provisa.api.airport.server import ProvisaAirportServer

    server = ProvisaAirportServer(
        state,
        host=state.hostname,
        port=port,
        main_loop=asyncio.get_running_loop(),
    )
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    state._airport_server = server  # type: ignore[attr-defined]  # keep a reference alive
    log.info("airport server listening on %s:%d", state.hostname, port)
