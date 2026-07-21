# Copyright (c) 2026 Kenneth Stott
# Canary: 5d9e7a12-3c4b-4e8a-9f6d-1b2c3d4e5f60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The real per-input freshness reader for the periodic MV contract (REQ-961/859).

A periodic MV verifies its expected inputs are fresh-THROUGH the window boundary by a PULL against
each input's observed refresh state. This module builds that reader — ``freshness_of(node) ->
StateSubject`` — from the persisted ``node_freshness_state`` (stamped on every completed handle by
the processor). It is the collaborator the event loop injects into each periodic MV processor; the
same one the boot wiring passes through ``specs_from_config``.

An input with NO known refresh state (never landed) is itself an outage — the returned subject is
NOT fresh (``refreshed_at=None, ok=False``), never assumed fresh (REQ-961).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from provisa.events import queue
from provisa.freshness.adapters import StateSubject


def make_db_freshness_of(db: Any) -> Callable[[str], Awaitable[StateSubject]]:
    """Build the async ``freshness_of(node) -> StateSubject`` reader over ``db`` (REQ-961/859).

    Each call reads ``node``'s observed refresh state from ``node_freshness_state`` and wraps it as a
    :class:`StateSubject` (``refreshed_at`` = last_refresh_at, ``ok`` = last_refresh_ok). A node that
    never refreshed (no row, or a NULL ``last_refresh_at``) yields a NOT-fresh subject so the contract
    treats it as an outage — never a silent assume-fresh. The reader acquires its own short-lived
    connection; the contract PULL runs before the seal transaction, so this never nests a write txn."""

    async def freshness_of(node: str) -> StateSubject:
        async with db.acquire() as conn:
            state = await queue.get_node_state(conn, node)
        if state is None or state["last_refresh_at"] is None:
            return StateSubject(refreshed_at=None, ok=False)  # never refreshed → an outage, not fresh
        return StateSubject(refreshed_at=state["last_refresh_at"], ok=bool(state["last_refresh_ok"]))

    return freshness_of
