# Copyright (c) 2026 Kenneth Stott
# Canary: f893a8c1-461a-43fe-b608-bacfe74421b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Egress byte meter (REQ-1452, REQ-1455).

Starter includes 25 GB of egress and bills $0.48/GB beyond it; the trial ends at 25 GB. Both need
the same number: bytes the org's clients received.

Counted at the transport, not at the audit seam: a streaming result is finalized before it is
drained, so a count taken where the statement completes includes rows the client never got. Each
protocol reports its own writes here.

Exact counts: HTTP (EgressMeterMiddleware), pgwire (CountingWriter), Bolt (BoltSession._send).
Approximate counts: Arrow Flight and gRPC hand payloads to C writers with no Python byte seam, so
they report Table/RecordBatch.nbytes and Message.ByteSize() — payload size, missing framing.

Reports buffer in memory and a scheduled job drains them. Per-report upserts would be too many (a
pgwire result set is thousands of writes) and pgwire's socketserver threads cannot await.

A process that dies with a full buffer loses those bytes: under-counting, never over-billing. A
failing drain raises rather than retrying.
"""

# Requirements: REQ-1452, REQ-1455

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping

log = logging.getLogger(__name__)

# Well under the quarter-hourly trial sweep, so it never reads a stale meter.
DRAIN_INTERVAL_SECONDS = 30

_LOCK = threading.Lock()
_PENDING: dict[str, int] = {}


def report(org_id: str | None, n_bytes: int) -> None:
    """Report ``n_bytes`` written to a client of ``org_id``. Callable from any thread.

    A None org (unauthenticated request, health probe, login page) is unattributable and dropped
    rather than charged to a default.
    """
    if org_id is None or n_bytes <= 0:
        return
    with _LOCK:
        _PENDING[org_id] = _PENDING.get(org_id, 0) + n_bytes


def take_pending() -> dict[str, int]:
    """Atomically remove and return everything reported since the last call."""
    with _LOCK:
        drained = _PENDING.copy()
        _PENDING.clear()
    return drained


def restore(counts: "MutableMapping[str, int]") -> None:
    """Put drained counts back after a failed write, so the bytes are metered on the next drain."""
    with _LOCK:
        for org_id, n_bytes in counts.items():
            _PENDING[org_id] = _PENDING.get(org_id, 0) + n_bytes


async def drain(pool: Any) -> None:
    """Write every pending report into the meter, restoring unwritten counts on failure."""
    from provisa.core.commerce import meter_egress

    counts = take_pending()
    if not counts:
        return
    try:
        for org_id, n_bytes in list(counts.items()):
            await meter_egress(pool, org_id, n_bytes)
            del counts[org_id]  # so a mid-loop failure restores only what was not yet written
    except BaseException:
        restore(counts)
        raise


async def drain_job() -> None:
    """The scheduled drain."""
    from provisa.api.app import state

    pool = state.admin_db
    if pool is None:
        # No control plane means no org registry and nothing to bill.
        return
    await drain(pool)


# --- the HTTP seam --------------------------------------------------------------------------- #


class EgressMeterMiddleware:
    """Count response body bytes per org (REQ-1452).

    Plain ASGI, not ``BaseHTTPMiddleware``: that class relays the body through a background task
    and never completes for an unbounded ``StreamingResponse`` (SSE subscriptions, REQ-219).

    Registered outside the auth middleware, so it also counts responses auth itself produced.
    """

    def __init__(self, app: "Callable[..., Awaitable[None]]") -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            # Bolt-over-WS meters at its own writer; counting here too would double-bill it.
            await self.app(scope, receive, send)
            return

        async def counting_send(message: dict) -> None:
            await send(message)
            if message["type"] == "http.response.body":
                body = message.get("body", b"")
                if body:
                    # Read per chunk, not once: auth resolves the org after the request begins.
                    report(scope.get("state", {}).get("active_org_id"), len(body))

        await self.app(scope, receive, counting_send)


# --- the stream-socket seam ------------------------------------------------------------------ #


class CountingWriter:
    """A binary file-like proxy that meters every byte written through it (pgwire).

    Delegates everything else to the wrapped object.
    """

    __slots__ = ("_inner", "_org_id")

    def __init__(self, inner: Any, org_id: str | None) -> None:
        self._inner = inner
        self._org_id = org_id

    def bind_org(self, org_id: str | None) -> None:
        """Attribute subsequent writes to ``org_id``. Called once the session authenticates."""
        self._org_id = org_id

    def write(self, data: Any) -> Any:
        written = self._inner.write(data)
        report(self._org_id, len(data))
        return written

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
