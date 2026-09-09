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

import asyncio

_locks: dict[str, asyncio.Lock] = {}


def land_lock(node: str) -> asyncio.Lock:
    """The lock for ``node`` (``schema.table``, the registered name both paths use)."""
    return _locks.setdefault(node, asyncio.Lock())
