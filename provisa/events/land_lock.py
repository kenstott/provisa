# Copyright (c) 2026 Kenneth Stott
# Canary: 5d9a1f37-8b2e-4c64-a7f0-3e6c2b48d1a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One land at a time per node, per process (REQ-1661): the event loop's source node and the query
path's residency prep both land the same replica, and two REPLACE lands interleaved on Snowflake
(DELETE, INSERT, DELETE, INSERT) left every row twice, confirmed live. Across processes the event
loop's lease and the store's atomic replace hold the line; inside one process this lock does."""

from __future__ import annotations

from provisa.core.connection_loop import CrossLoopLock

# REQ-1882 (amended 2026-09-29): the query path lands from pgwire/Bolt/Flight connection-thread
# loops and the event loop from the process loop, so the lock holds across loops and threads.
_locks: dict[str, CrossLoopLock] = {}


def land_lock(node: str) -> CrossLoopLock:
    """The lock for ``node`` (``events.nodes.source_node``, the registered identity both paths
    use)."""
    return _locks.setdefault(node, CrossLoopLock())
