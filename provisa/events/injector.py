# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The injector action (REQ-940) — what a scheduled poll job runs for one node.

A poll node registers a job with the embedded scheduler (APScheduler) at its cadence; each fire
calls ``check_node``: probe the source for change, and — only if it changed (token-gated) — post the
node's change event and fan it out to its dependents. Push nodes (native/debezium/kafka) use a true
listener that posts directly, not this. The event kind comes from the node's change_signal: a poll
signal with a watermark → ``append`` (insert the delta), else ``replace``; push/CDC → ``delta``
(upsert by PK). Unchanged → no event, so no wasted downstream work.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape
from provisa.events import queue

# landing shape (change_signal axis) → the event kind posted onto the bus.
_SHAPE_TO_EVENT = {CDC: "delta", APPEND: "append", REPLACE: "replace"}

# A probe returns (changed, token): compare the opaque token to the last stored one; changed drives
# a post, token is persisted for the next comparison (REQ-855 freshness gate).
Probe = Callable[[], Awaitable[tuple[bool, "str | None"]]]


async def check_node(
    conn: Any,
    *,
    node: str,
    change_signal: str,
    watermark_column: str | None,
    probe: Probe,
    dependents: list[str],
) -> int | None:
    """Run the injector action for one poll ``node``: probe, and if it changed post the change event
    (shape from ``change_signal``) + fan it out to ``dependents``. Returns the event id, or None when
    unchanged (token-gated — the guard against no-op events rippling the DAG)."""
    changed, token = await probe()
    if not changed:
        return None
    event_type = _SHAPE_TO_EVENT[select_landing_shape(change_signal, watermark_column)]
    event_id = await queue.post_event(
        conn, source_table=node, event_type=event_type, payload={"token": token}
    )
    await queue.fan_out(conn, event_id, dependents)
    return event_id
