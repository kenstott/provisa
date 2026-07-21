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

Two "no persisted state" cases are DELIBERATELY split (REQ-961):

- An ALWAYS-CURRENT input (a live / query-time source served in place, not landed on a cadence) is
  current as of NOW by construction, so it is fresh-through ANY past boundary. Absence of a refresh
  stamp is EXPECTED there — it never lands — so it is treated as fresh, not an outage. The caller
  supplies the set of such nodes (derived from the federate strategy at wiring).
- A SCHEDULED/materialized input with no stamp genuinely could-not-be-verified (it was supposed to
  refresh and has not) → NOT fresh (``refreshed_at=None, ok=False``) → an outage. Never assumed
  fresh — that would silently close a period on missing data.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from provisa.events import queue
from provisa.freshness.adapters import StateSubject


def make_db_freshness_of(
    db: Any, always_current: Iterable[str] = ()
) -> Callable[[str], Awaitable[StateSubject]]:
    """Build the async ``freshness_of(node) -> StateSubject`` reader over ``db`` (REQ-961/859).

    ``always_current`` is the set of live / query-time input nodes that are current as of read time
    and therefore fresh-through any boundary (never landed, so they carry no refresh stamp — absence
    is expected, not an outage). Every other node is read from ``node_freshness_state``: a persisted
    stamp maps to its ``StateSubject``; a MISSING stamp on such a node is NOT fresh (an outage), never
    a silent assume-fresh. The reader acquires its own short-lived connection; the contract PULL runs
    before the seal transaction, so this never nests a write txn."""
    live = frozenset(always_current)

    async def freshness_of(node: str) -> StateSubject:
        if node in live:
            # live/query-time source → current as of now → fresh-through any past boundary.
            return StateSubject(refreshed_at=math.inf, ok=True)
        async with db.acquire() as conn:
            state = await queue.get_node_state(conn, node)
        if state is None or state["last_refresh_at"] is None:
            return StateSubject(refreshed_at=None, ok=False)  # never refreshed → an outage, not fresh
        return StateSubject(refreshed_at=state["last_refresh_at"], ok=bool(state["last_refresh_ok"]))

    return freshness_of
