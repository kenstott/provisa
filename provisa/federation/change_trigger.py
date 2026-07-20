# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f2a19-6c47-4e15-9b08-2a7e3c4f81d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Data-less change-trigger receiver for the ``signal`` change_signal (REQ-1149).

A ``signal`` source is refreshed on an external DATA-LESS trigger — a Kafka control message on a
topic, or an HTTP webhook — that says "the source changed, refresh now" without carrying any rows.
The trigger only INVALIDATES the snapshot; the rows are re-pulled from the source of truth on the
next refresh (it never lands the message).

This registry is the receiver's effect AND the scheduler's change-detector input, unified as a
freshness TOKEN so the existing REQ-1141 probe machinery drives it unchanged: each trigger bumps a
per-key counter, and ``token(key)`` returns that counter. The scheduler compares stored vs. fresh
token exactly as for any probe — an unbumped counter is "unchanged" (no pull), a bump is "changed"
(re-pull), and the load-protected scheduler still owns WHEN the heavy pull runs (deferred to the
off-peak window). Process-local, in-memory: a trigger fires a pull promptly; it is not a durable log.
"""

from __future__ import annotations

import asyncio


class ChangeTriggerRegistry:  # REQ-1149
    """Per-key trigger counter. ``signal`` bumps it; ``token`` reads it for the probe comparison."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def signal(self, key: str) -> int:
        """Record a data-less change trigger for ``key`` (a Kafka control message / webhook hit).

        Returns the new counter. Idempotent per delivery only in the sense that each call is one
        trigger; duplicate deliveries each bump — the scheduler collapses many bumps between refreshes
        into a single re-pull, so at-least-once delivery never causes more than one pull per window."""
        async with self._lock:
            n = self._counts.get(key, 0) + 1
            self._counts[key] = n
            return n

    def token(self, key: str) -> str:
        """The current trigger counter for ``key`` as a freshness token (``"0"`` before any trigger).

        Never None: a ``signal`` source always has a definite token, so the scheduler lands once
        initially (stored None ≠ ``"0"``) then holds until a trigger bumps the counter."""
        return str(self._counts.get(key, 0))


# The process-wide receiver. A Kafka control-topic consumer and the HTTP webhook endpoint both call
# ``signal``; the scheduler's probe adapter reads ``token`` (REQ-1149).
_REGISTRY = ChangeTriggerRegistry()


def get_registry() -> ChangeTriggerRegistry:
    return _REGISTRY


async def receive_trigger(key: str) -> int:
    """Entry point for a trigger receiver (webhook / Kafka control message) — marks ``key`` changed."""
    return await _REGISTRY.signal(key)


def trigger_token(key: str) -> str:
    """The scheduler's probe verdict for a ``signal`` source: its current trigger token (REQ-1149)."""
    return _REGISTRY.token(key)
