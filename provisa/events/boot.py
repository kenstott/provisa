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

from provisa.core.change_signal import is_poll, is_push
from provisa.events import queue, supervisor
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


def _poll_probe_factory() -> Callable[[], Probe]:
    """A poll source re-queries its current rows each cadence and lands them (REPLACE). The TTL lapse
    IS the change, so the probe always reports changed. (Token/watermark probing to skip an unchanged
    poll is a per-source optimization for a later pass; re-querying is correct, just not minimal.)"""

    async def _probe() -> tuple[bool, str | None]:
        return True, None

    return lambda: _probe


def specs_from_config(
    *,
    sources: list[Any],
    tables: list[Any],
    mvs: list[Any],
    engine: Any,
    store_dsn: str,
    source_fetch: Callable[[Any, Any], Any],
    mv_columns: Callable[[Any], list[tuple[str, str]] | None],
    mv_run_query: Callable[[Any], Any],
) -> list[NodeSpec]:
    """Bind the config to :class:`NodeSpec`s (REQ-941). A MATERIALIZED source table (``federate`` ==
    MATERIALIZED) becomes a source spec — its landing args resolved from config, its ``fetch`` the
    source adapter's loader (injected). An MV becomes an mv spec — its output ``columns`` from a live
    engine introspection and its ``run_query`` the engine's SELECT (both injected, since neither is
    in the model). The engine classifies reachability; untyped tables are skipped (types are filled
    at registration). The three live collaborators are injected so this binder stays pure/testable."""
    from provisa.events.handlers import make_mv_generate, make_source_land
    from provisa.federation.engine import UnreachableSource
    from provisa.federation.residency import resolve_landing_args
    from provisa.federation.strategy import Strategy, federate

    src_by_id = {s.id: s for s in sources}
    specs: list[NodeSpec] = []

    for tbl in tables:
        src = src_by_id.get(tbl.source_id)
        if src is None:
            continue
        try:
            if federate(src, engine) is not Strategy.MATERIALIZED:
                continue  # live/scan federates in place — not landed, not a source processor
        except UnreachableSource:
            continue
        try:
            args = resolve_landing_args(src, tbl, platform=engine.dialect)
        except ValueError:
            continue  # a column's type is not yet resolved — reconcile skips it too
        node = f"{tbl.schema_name}.{tbl.table_name}"
        mat_table = f"{src.id}__{tbl.schema_name}__{tbl.table_name}"  # matches _mat_table_name
        handle = make_source_land(
            store_dsn,
            schema="mat",
            table=mat_table,
            columns=args.columns,
            change_signal=args.change_signal,
            watermark_column=args.watermark_column,
            pk_columns=args.pk_columns,
            fetch=source_fetch(src, tbl),
        )
        specs.append(
            NodeSpec(
                node=node,
                kind="source",
                change_signal=args.change_signal,
                watermark_column=args.watermark_column,
                handle=handle,
                poll_seconds=getattr(tbl, "cache_ttl", None),
                # Poll sources refresh on their own cadence (register_runtime schedules the injector);
                # push sources are driven by their listener. Boot lands the first copy either way.
                probe_factory=_poll_probe_factory() if is_poll(args.change_signal) else None,
            )
        )

    for mv in mvs:
        cols = mv_columns(mv)
        if not cols:
            continue  # output columns not resolvable yet (live introspection) — bound on a later pass
        node = f"{mv.target_schema}.{mv.target_table}"
        handle = make_mv_generate(
            store_dsn,
            schema=mv.target_schema,
            table=mv.target_table,
            columns=cols,
            run_query=mv_run_query(mv),
        )
        specs.append(
            NodeSpec(
                node=node,
                kind="mv",
                change_signal=mv.freshness_mode,
                watermark_column=None,
                handle=handle,
                poll_seconds=mv.refresh_interval,
            )
        )

    return specs


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

    async def _boot() -> None:
        # Design: replicas are BUILT at boot, then REFRESHED by events. Seed each source's first land
        # and drain the DAG so every replica (and its MVs) exists; the poll/push jobs keep them fresh.
        await boot_create(db, specs)
        await supervisor.drain(db, processors)

    # One-shot: run once as soon as the scheduler starts (no trigger = immediate single fire).
    scheduler.add_job(_boot, id="events:boot", replace_existing=True)
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


async def boot_create(db: Any, specs: list[NodeSpec]) -> int:
    """Seed the initial landing of every source node so all replicas exist after boot (REQ-941): the
    design is replicas are BUILT at boot, then REFRESHED by events. Posts one ``replace`` event per
    source node; the caller drains the DAG so each lands its current rows and fans out to its MVs.
    Idempotent — a ``replace`` re-lands the source's full current state, so re-running on every boot
    is safe. Returns the number of source nodes seeded."""
    seeded = 0
    for spec in specs:
        if spec.kind != "source":
            continue
        async with db.acquire() as conn:
            await queue.post_event(
                conn, source_table=spec.node, event_type="replace", payload={"bootstrap": True}
            )
        seeded += 1
    return seeded
