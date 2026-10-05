# Copyright (c) 2026 Kenneth Stott
# Canary: 51d43399-43ef-4c00-a813-fdb34eeb8693
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Ripple a landing that did not run through the node's own processor (REQ-961, REQ-965).

A push listener lands each batch of change events straight into its table, and a store pipeline
lands rows without Provisa at all. Neither goes through the table node's processor, which is what
posts a node's change to the views that read it and stamps its freshness. :func:`ripple` does that
for them, the same way the processor does for its own work: one event, routed to the node's
dependents, and the node's refresh stamped, all in one transaction.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from provisa.events import queue


async def ripple(state: Any, node: str, *, event_type: str, payload: dict) -> int | None:
    """Post ``node``'s landed change to its dependents and stamp its refresh. Returns the posted
    event id. With no processor wired for ``node`` (the event loop is not running for this table),
    nothing reads it yet: only its refresh is stamped, and None is returned."""
    processor = next(
        (p for p in getattr(state, "event_loop_processors", None) or [] if p.node == node), None
    )
    async with state.tenant_db.acquire() as conn:
        async with conn.transaction():
            if processor is None:
                await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=True)
                return None
            return await processor.post_landed(conn, event_type, payload)
