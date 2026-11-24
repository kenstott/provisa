# Copyright (c) 2026 Kenneth Stott
# Canary: f4009a34-2f14-41f1-907b-7f33e6830a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Arrow Flight listener minted for one call of a local code source's function (REQ-2226).

A sandboxed worker reads its call's table arguments through a Flight listener opened for that
call: on loopback, on a port of the block ``server.minted_flight_port`` starts and
``server.minted_flight_ports`` sizes, answering only the call's token under the call's id, and
only a statement's read (:class:`provisa.api.flight.server._OneCallHeadersFactory`), as the call's
role through the main server's governed pipeline. It closes when the call ends. The worker's
sandbox may connect to that block's ports and to no other of Provisa's, so never to the main
Flight server (provisa/codesource/sandbox.py). A worker serves several calls at once, each over its
own connection (REQ-2100), so the sandbox names the block, not one call's port: a call's listener
refuses every other call's token.

A port another process holds (another worker of the deployment, under ``--workers N``) is passed
over for the next; a call that finds every port of the block taken is refused, naming the setting."""

# Requirements: REQ-2226
from __future__ import annotations

import threading
from typing import Any

#: The ports this process's listeners hold now.
_held: set[int] = set()
_lock = threading.Lock()


class ListenerUnavailable(Exception):
    """No listener could be opened for the call, said naming why."""


def ports() -> tuple[int, ...]:
    """The block of loopback ports the listeners are opened on; none when it is not configured
    (``server.minted_flight_port`` 0)."""
    from provisa.core import settings_registry

    first = settings_registry.value("server.minted_flight_port")
    if not first:
        return ()
    return tuple(range(first, first + settings_registry.value("server.minted_flight_ports")))


class CallListener:
    """A Flight listener for one call, open from construction until :meth:`close`."""

    def __init__(self, state: Any, call_id: str, token: str) -> None:
        import pyarrow as pa

        from provisa.api.flight.relay import serve_in_background
        from provisa.api.flight.server import ProvisaFlightServer

        block = ports()
        if not block:
            raise ListenerUnavailable(
                "the minted Flight listeners are not configured (server.minted_flight_port)"
            )
        refusals: list[str] = []
        for port in block:
            with _lock:
                if port in _held:
                    continue
                _held.add(port)
            try:
                server = ProvisaFlightServer(
                    state, location=f"grpc://127.0.0.1:{port}", one_call=(call_id, token)
                )
            except pa.ArrowException as exc:  # another process holds the port
                with _lock:
                    _held.discard(port)
                refusals.append(f"{port}: {exc}")
                continue
            self.port = port
            self.uri = f"grpc://127.0.0.1:{port}"
            self._server = server
            serve_in_background(server)
            return
        raise ListenerUnavailable(
            f"every port of the minted Flight listeners' block is in use "
            f"(server.minted_flight_ports): {'; '.join(refusals) or 'all held by other calls'}"
        )

    def close(self) -> None:
        """Stop answering and give the port back."""
        try:
            self._server.shutdown()
        finally:
            with _lock:
                _held.discard(self.port)
