# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Event-loop boot orchestration (REQ-941) — build the processors from config and register the
runtime jobs on the embedded scheduler.

``build_processors`` turns a list of :class:`NodeSpec` (one per node — a landed source table or an
MV, with its already-built ``handle``) into the ``TableProcessor`` set, sharing one lineage-derived
``dependents_of``. ``register_runtime`` adds the periodic tick (drain each processor's pending work)
and the reaper (reclaim stale leases) to the embedded APScheduler, plus each poll node's own
interval job / kafka listener. The app supplies the config→spec mapping and the real collaborators
(a source loader, the engine's MV ``run_query``); everything below is transport-agnostic wiring.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from provisa.core.change_signal import is_push
from provisa.events import supervisor
from provisa.events.injector import Probe
from provisa.events.processor import (
    MVTableProcessor,
    SourceTableProcessor,
    TableProcessor,
)

# The handle a node runs on its claimed events → (event_type, payload) | None (see handlers.py).
Handle = Callable[[list[dict]], Awaitable["tuple[str, dict] | None"]]


@dataclass(frozen=True)
class NodeSpec:
    """One node in the DAG: a landed source table (``kind='source'``) or an MV (``kind='mv'``), with
    its ingestion parameters and its pre-built ``handle`` (from the handlers factories)."""

    node: str
    kind: str  # "source" | "mv"
    change_signal: str
    watermark_column: str | None
    handle: Handle
    poll_seconds: int | None = None  # poll nodes: the timer cadence
    probe_factory: Callable[[], Probe] | None = None  # poll nodes: a fresh probe per fire


def build_processors(
    specs: list[NodeSpec], *, db: Any, dependents_of: Callable[[str], list[str]]
) -> list[TableProcessor]:
    """Build one ``TableProcessor`` per spec (Source or MV variant), sharing ``dependents_of`` and
    the control-plane ``db``. The lease name is unique per node."""
    processors: list[TableProcessor] = []
    for spec in specs:
        common = {
            "change_signal": spec.change_signal,
            "watermark_column": spec.watermark_column,
            "dependents_of": dependents_of,
            "db": db,
            "name": f"{spec.kind}:{spec.node}",
        }
        if spec.kind == "source":
            processors.append(SourceTableProcessor(spec.node, land=spec.handle, **common))
        elif spec.kind == "mv":
            processors.append(MVTableProcessor(spec.node, generate=spec.handle, **common))
        else:
            raise ValueError(f"unknown node kind {spec.kind!r} for {spec.node!r}")
    return processors


def register_runtime(
    scheduler: Any,
    *,
    db: Any,
    processors: list[TableProcessor],
    specs: list[NodeSpec],
    tick_seconds: int = 5,
    lease_seconds: int = 60,
) -> None:
    """Register the event-loop jobs on the embedded scheduler (APScheduler): one tick (drain all
    processors' pending work), one reaper (reclaim stale leases), and each POLL node's own interval
    job (its injector action at its cadence). Push nodes' listeners are started by the processor
    (``consume_kafka``) — the app wires the consumer; this registers only the scheduled side."""
    from apscheduler.triggers.interval import IntervalTrigger

    async def _tick() -> None:
        await supervisor.tick(db, processors)

    async def _reap() -> None:
        await supervisor.reap(db, lease_seconds=lease_seconds)

    scheduler.add_job(
        _tick,
        trigger=IntervalTrigger(seconds=tick_seconds),
        id="events:tick",
        replace_existing=True,
    )
    scheduler.add_job(
        _reap,
        trigger=IntervalTrigger(seconds=lease_seconds),
        id="events:reaper",
        replace_existing=True,
    )
    by_node = {p.node: p for p in processors}
    for spec in specs:
        if is_push(spec.change_signal):
            continue  # push → listener owned by the processor, not a scheduled job
        if spec.poll_seconds is None or spec.probe_factory is None:
            continue  # a poll node with no cadence/probe is driven only by upstream events
        by_node[spec.node].register_poll_job(
            scheduler, seconds=spec.poll_seconds, probe_factory=spec.probe_factory
        )
