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

# The handle a node runs on its claimed events → (event_type, payload, content_hash) | None (REQ-981).
Handle = Callable[..., Awaitable["tuple[str, dict, str | None] | None"]]


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
    probe_type: str = "none"  # REQ-982: input-probe method (drives the injected event shape)
    debounce_quiet: float = 0.0  # REQ-963: live-MV debounce quiet window; 0 = real-time
    debounce_max_delay: float | None = None  # REQ-963: staleness-SLA cap under churn
    # REQ-961/962: the periodic deadline source (calendar boundary + lateness) — mutually exclusive
    # with debounce; its freshness contract (expected-events list) and the per-input freshness reader.
    deadline_source: Any | None = None
    expected_events: list[str] | None = None
    freshness_of: Callable[[str], Any] | None = None
    # REQ-965: the declared downstream emit-outcome SET (None = default single-shape fan-out) and the
    # demand-driven per-shape router (dependents that subscribe to each shape).
    emit_outcomes: frozenset[str] | None = None
    subscribers_of: Callable[[str, str], list[str]] | None = None


def _probe_factory(
    probe_type: str,
    *,
    query_scalar: Any | None,
    ref: str | None,
    watermark_column: str | None,
) -> Callable[[], Probe]:
    """Build a node's probe factory from its resolved ``probe_type`` (REQ-982). Each fire yields a
    fresh transport (``() -> str | None``) via :func:`probes.build_probe`; ``check_node`` compares the
    returned token to the persisted baseline. A watermark/count transport needs the SQL scalar runner
    + table ref (injected); hash/none (and any unwired transport) return a None token, degrading the
    node to its TTL cadence where the REQ-981 output hash still gates an unchanged ripple."""
    from provisa.events.probes import build_probe

    def factory() -> Probe:
        return build_probe(
            probe_type,
            query_scalar=query_scalar,
            ref=ref,
            watermark_column=watermark_column,
        )

    return factory


def _resolve_mv_deadline(
    mv: Any, calendar_registry: Any, freshness_of: Callable[[str], Any] | None, dialect: str
) -> tuple[Any | None, list[str] | None, Callable[[str], Any] | None, float, float | None]:
    """Resolve an MV's deadline source (REQ-961/962/963). A declared ``calendar`` → a periodic
    :class:`PeriodicCalendar` source with its freshness contract (declared expected-events, else all
    SQL-lineage inputs) and a required registry + freshness reader; otherwise the live debounce knobs
    flow through unchanged. Returns (deadline_source, expected_events, freshness_of, quiet, max_delay)."""
    calendar = getattr(mv, "calendar", None)
    if calendar is None:
        return None, None, None, mv.debounce_quiet, mv.debounce_max_delay
    from provisa.events.calendars import parse_grain
    from provisa.events.deadlines import PeriodicCalendar
    from provisa.events.lineage import extract_inputs

    if calendar_registry is None:
        raise ValueError(f"MV {mv.target_table!r} declares calendar {calendar!r} but no registry")
    grain = getattr(mv, "grain", None)
    if grain is None:
        raise ValueError(f"MV {mv.target_table!r}: calendar declared without a grain (REQ-962)")
    source = PeriodicCalendar(
        calendar=calendar_registry.get(calendar),
        grain=parse_grain(grain).value,
        allowed_lateness=float(getattr(mv, "allowed_lateness", 0.0)),
        business_day=bool(getattr(mv, "business_day_grain", False)),
    )
    declared = getattr(mv, "expected_events", None)
    expected = declared if declared is not None else sorted(extract_inputs(mv.sql, dialect))
    return source, expected, freshness_of, 0.0, None


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
    store_schema: str = "mat",
    probe_scalar: Callable[[Any, Any], Any] | None = None,
    subscribers_of: Callable[[str, str], list[str]] | None = None,
    calendar_registry: Any | None = None,
    freshness_of: Callable[[str], Any] | None = None,
) -> list[NodeSpec]:
    """Bind the config to :class:`NodeSpec`s (REQ-941). A MATERIALIZED source table (``federate`` ==
    MATERIALIZED) becomes a source spec — its landing args resolved from config, its ``fetch`` the
    source adapter's loader (injected). An MV becomes an mv spec — its output ``columns`` from a live
    engine introspection and its ``run_query`` the engine's SELECT (both injected, since neither is
    in the model). The engine classifies reachability; untyped tables are skipped (types are filled
    at registration). The three live collaborators are injected so this binder stays pure/testable.

    ``tables`` MUST be the design-time REGISTERED tables (semantic sql names + resolved types), not
    the raw YAML — the landed replica name (``mat_table``) has to match what the schema-currency
    reconcile created. ``store_schema`` is where the replicas live in the store (``main`` on a
    schema-less sqlite store, ``mat`` otherwise) — never assume ``mat``."""
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
        # A parameterized source (native-filter query/path-param columns) is a function f(args) ->
        # rows with no snapshot — fetched real-time at query time, never landed. Not a source node.
        if any(getattr(c, "native_filter_type", None) is not None for c in tbl.columns):
            continue
        try:
            args = resolve_landing_args(src, tbl, platform=engine.dialect)
        except ValueError:
            continue  # a column's type is not yet resolved — reconcile skips it too
        node = f"{tbl.schema_name}.{tbl.table_name}"
        mat_table = f"{src.id}__{tbl.schema_name}__{tbl.table_name}"  # matches _mat_table_name
        handle = make_source_land(
            store_dsn,
            schema=store_schema,
            table=mat_table,
            columns=args.columns,
            change_signal=args.change_signal,
            watermark_column=args.watermark_column,
            pk_columns=args.pk_columns,
            fetch=source_fetch(src, tbl),
            probe_type=args.probe_type,  # REQ-982: authoritative landing-shape selector
        )
        # REQ-982: build the poll node's probe from its resolved probe_type. watermark/count read the
        # source through the engine terminal (the SQL scalar runner + engine ref, injected); hash/none
        # degrade to the TTL cadence (a None token) where the REQ-981 output hash gates the ripple.
        from provisa.compiler.naming import source_to_catalog

        ref = f'"{source_to_catalog(src.id)}"."{tbl.schema_name}"."{tbl.table_name}"'
        factory = (
            _probe_factory(
                args.probe_type,
                query_scalar=probe_scalar(src, tbl) if probe_scalar is not None else None,
                ref=ref,
                watermark_column=args.watermark_column,
            )
            if is_poll(args.change_signal)
            else None
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
                probe_factory=factory,
                probe_type=args.probe_type,
            )
        )

    for mv in mvs:
        cols = mv_columns(mv)
        if not cols:
            continue  # output columns not resolvable yet (live introspection) — bound on a later pass
        node = f"{mv.target_schema}.{mv.target_table}"
        # REQ-965: the operator's PERSISTENCE outcome + the derived-table PK (REQ-970: declared or
        # GROUP-BY-inferred). require_pk fails loud if persist=upsert / emit=delta lacks a PK.
        from provisa.events.lineage import infer_pk
        from provisa.events.outcomes import require_pk, validate_emit

        persist = getattr(mv, "persist", "replace")
        pk_cols = list(getattr(mv, "primary_key", []) or []) or (
            infer_pk(mv.sql) if getattr(mv, "sql", None) else []
        )
        declared_emit = getattr(mv, "emit", None)
        emit_set = validate_emit(set(declared_emit)) if declared_emit is not None else None
        require_pk(persist, emit_set or set(), pk_cols or None)
        # REQ-969 (MAY): a declared incremental MV applies upstream deltas (feasibility checked in
        # make_mv_incremental — PK + incrementalizable SQL, else an explicit error). Otherwise the
        # REQ-966 recompute-to-current baseline (make_mv_generate).
        if getattr(mv, "incremental", False):
            from provisa.events.handlers import make_mv_incremental

            handle = make_mv_incremental(
                store_dsn,
                schema=mv.target_schema,
                table=mv.target_table,
                columns=cols,
                sql=mv.sql,
                run_query=mv_run_query(mv),
                pk_columns=pk_cols,
                persist=persist if persist != "replace" else "upsert",
            )
        else:
            handle = make_mv_generate(
                store_dsn,
                schema=mv.target_schema,
                table=mv.target_table,
                columns=cols,
                run_query=mv_run_query(mv),
                persist=persist,
                pk_columns=pk_cols or None,
            )
        # An MV's periodic cadence IS its poll job (register_runtime skips a poll node with no
        # probe_factory). Poll-mode MVs (ttl/probe/ttl_probe) recompute-to-current on cadence; the MV's
        # own change detection is the input-token (REQ-881) + output-hash (REQ-981) gate inside the
        # generate handler, so the node probe is always "none" (cadence-only). Without this a poll MV
        # never fires — most visibly a source-less view MV (source_tables=[]) that has no upstream
        # ripple either, so it sits STALE forever despite its TTL.
        mv_probe_factory = (
            _probe_factory("none", query_scalar=None, ref=None, watermark_column=None)
            if is_poll(mv.freshness_mode)
            else None
        )
        # REQ-961/962: a declared calendar makes the MV PERIODIC (calendar-boundary trigger + a
        # freshness contract), mutually exclusive with REQ-963 live debounce. Undeclared expected-
        # events default to ALL SQL-lineage inputs (extract_inputs, REQ-939).
        deadline_source, expected, fresh_reader, quiet, max_delay = _resolve_mv_deadline(
            mv, calendar_registry, freshness_of, engine.dialect
        )
        specs.append(
            NodeSpec(
                node=node,
                kind="mv",
                change_signal=mv.freshness_mode,
                watermark_column=None,
                handle=handle,
                poll_seconds=mv.refresh_interval,
                probe_factory=mv_probe_factory,
                probe_type="none",
                debounce_quiet=quiet,  # REQ-963 (0 when periodic)
                debounce_max_delay=max_delay,  # REQ-963
                deadline_source=deadline_source,  # REQ-961/962 periodic
                expected_events=expected,  # REQ-961 freshness contract
                freshness_of=fresh_reader,
                emit_outcomes=emit_set,  # REQ-965 (None → default single-shape fan-out)
                subscribers_of=subscribers_of if emit_set is not None else None,
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
            "probe_type": spec.probe_type,
            "debounce_quiet": spec.debounce_quiet,  # REQ-963
            "debounce_max_delay": spec.debounce_max_delay,  # REQ-963
            "deadline_source": spec.deadline_source,  # REQ-961/962 periodic
            "expected_events": spec.expected_events,  # REQ-961 freshness contract
            "freshness_of": spec.freshness_of,  # REQ-961 per-input freshness reader
        }
        if spec.kind == "source":
            processors.append(SourceTableProcessor(spec.node, land=spec.handle, **common))
        elif spec.kind == "mv":
            processors.append(
                MVTableProcessor(
                    spec.node,
                    generate=spec.handle,
                    emit_outcomes=spec.emit_outcomes,  # REQ-965
                    subscribers_of=spec.subscribers_of,  # REQ-965 demand routing
                    **common,
                )
            )
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
    seed: bool = True,
) -> None:
    """Register the event-loop jobs on the embedded scheduler (APScheduler): one tick (drain all
    processors' pending work), one reaper (reclaim stale leases), and each POLL node's own interval
    job (its injector action at its cadence). Push nodes' listeners are started by the processor
    (``consume_kafka``) — the app wires the consumer; this registers only the scheduled side.

    ``seed`` controls the one-shot boot-create (seed every source's first land + drain the DAG). True
    at boot; False on a RE-wire (e.g. after a runtime MV create) so poll jobs get (re)registered
    WITHOUT re-landing every source. Re-registration is idempotent (replace_existing throughout)."""
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

    if seed:
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
            event_id = await queue.post_event(
                conn, source_table=spec.node, event_type="replace", payload={"bootstrap": True}
            )
            # A source node is the FIRST dependent of its own change: enqueue the work item for the
            # node itself so its processor claims + lands it (it then re-posts to the MV dependents).
            # Without this the boot event has no claimable work and nothing ever lands.
            await queue.fan_out(conn, event_id, [spec.node])
        seeded += 1
    return seeded
