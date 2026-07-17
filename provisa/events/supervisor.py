# Copyright (c) 2026 Kenneth Stott
# Canary: c0ae225d-da2f-42e8-a955-22cb40247d3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The event-loop supervisor runtime (REQ-941) — the tick and reaper the scheduler drives.

Given the set of table processors (one per node), ``tick`` runs each processor's claim→handle→
complete→re-post once; ``drain`` runs ticks until quiescent so a change propagates through the whole
DAG within one catch-up; ``reap`` reclaims stale leases (dead processors). A scheduled job fires
``tick`` + ``reap`` periodically on the embedded scheduler; ``drain`` is for boot catch-up and tests.
``dependents_of`` is built from the SQLGlot lineage so a processor's re-post fans to the right nodes.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from provisa.events import lineage, queue


def dependents_of(mvs: dict[str, str], dialect: str = "postgres") -> Callable[[str], list[str]]:
    """Build the ``dependents_of(node) -> [nodes that listen]`` callable from the MV lineage graph —
    what a processor's re-post fans out to. Rejects a cyclic DAG (fan-out would never terminate)."""
    cycle = lineage.find_cycle(mvs, dialect)
    if cycle is not None:
        raise ValueError(f"MV lineage has a cycle: {' -> '.join(cycle)}")
    graph = lineage.dependents(mvs, dialect)
    return lambda node: graph.get(node, [])


async def tick(db: Any, processors: list[Any]) -> list[str]:
    """One processing tick: each processor drains its node's pending work once. Returns the nodes
    that re-posted a change (propagated downstream). A scheduled job fires this."""
    propagated: list[str] = []
    for processor in processors:
        async with db.acquire() as conn:
            posted = await processor.process_pending(conn)
        if posted is not None:
            propagated.append(processor.node)
    return propagated


async def drain(db: Any, processors: list[Any], *, max_rounds: int = 20) -> int:
    """Run ticks until quiescent (no processor re-posts) — propagates a change through the whole DAG
    within one catch-up. Returns the number of rounds run (bounded by ``max_rounds`` as a backstop
    against a mis-registered cycle). One *scheduled* tick calls ``tick`` once; ``drain`` is boot
    catch-up / tests."""
    for round_no in range(max_rounds):
        if not await tick(db, processors):
            return round_no
    return max_rounds


async def reap(
    db: Any, *, lease_seconds: float, grace_seconds: float = 0.0, now: datetime | None = None
) -> int:
    """REQ-959 reaper: reclaim work whose owner is gone or stuck. Reclaimable = heartbeat lapsed
    (older than ``lease_seconds``) OR past its per-claim deadline + ``grace_seconds`` (a stuck-but-
    alive owner the heartbeat cannot catch) — so a crashed OR wedged processor never orphans a node.
    Returns the count reclaimed."""
    _now = now or datetime.now(timezone.utc)
    heartbeat_cutoff = _now - timedelta(seconds=lease_seconds)
    async with db.acquire() as conn:
        return await queue.reclaim(
            conn, now=_now, heartbeat_cutoff=heartbeat_cutoff, grace_seconds=grace_seconds
        )
