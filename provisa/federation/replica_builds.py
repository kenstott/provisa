# Copyright (c) 2026 Kenneth Stott
# Canary: 6431e5c9-2a91-4b96-8aba-ae2ae3b20a3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Building one replica through the data replicator, and the runner that does it (REQ-1915).

``build_replica`` is the one way a whole-table replica is built: it resolves the table's source
reader, the engine's part and the store's write face, and runs the data replicator's job for
them. Nothing else copies a whole source table into a store.

Every process that does background work runs one :class:`ReplicaRunner` per org
(``wire_replica_runner``): a scheduled pass that claims requested builds within the node's,
the engine's and the source's limits and runs each on a background worker. A read that needs a
replica asks for its build in the state store and waits on the record; it never builds on its
own thread.
"""

# Requirements: REQ-1915, REQ-1920

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from provisa.federation import replica_state
from provisa.federation.data_replicator import BuildOutcome, Progress, data_replicator
from provisa.federation.replica_runner import ReplicaRunner, engine_job_key
from provisa.federation.data_replicator import Method, SourceCaps, SourceRead, TargetLoad
from provisa.federation.replica_errors import BuildFailure
from provisa.federation.replica_converge import definition_hash, drop_retired, whole_copy
from provisa.federation.replica_source import BATCH_ROWS
from provisa.federation.replica_state import ReplicaKey

log = logging.getLogger(__name__)

#: The scheduler job that runs a process's build pass for one org (suffixed for a non-default
#: org, as the event loop's jobs are). Run by every worker, not only the scheduler's holder.
RUNNER_JOB_ID = "replica:builds"
#: How often a process looks for requested builds when nothing kicked it sooner.
_PASS_SECONDS = 2

#: This process's runner per org (None: the default org), so a read can start a pass at once.
_runners: dict[str | None, ReplicaRunner] = {}


class ReplicaTableGone(BuildFailure, LookupError):
    """A build was asked for a table the model no longer has."""

    code = "replication.table_gone"

    def __init__(self, key: ReplicaKey) -> None:
        self.key = key
        self.params = {"source": key[0], "schema": key[1], "table": key[2]}
        super().__init__(
            f"no registered table {key[1]}.{key[2]} of source {key[0]}: its replica is not built"
        )


class NoWholeCopy(BuildFailure, LookupError):
    """A build was asked for a table that has no whole copy: one replicated row by row, or one
    that is a function of its parameters."""

    code = "replication.no_whole_copy"

    def __init__(self, key: ReplicaKey) -> None:
        self.key = key
        self.params = {"source": key[0], "schema": key[1], "table": key[2]}
        super().__init__(
            f"table {key[1]}.{key[2]} of source {key[0]} has no whole copy to build: it is "
            "replicated row by row, or it has a parameter column"
        )


def store_identity(state: Any) -> str:
    """What identifies the store this engine's replicas are written into: the engine's own
    address where it is its own store, else the address of the store it reads through. A
    digest, so no credential in an address is recorded."""
    import hashlib

    from provisa.federation.engine import configured_engine_url

    engine = state.federation_engine.engine
    if engine.native_store is not None:
        where = engine_job_key(engine.name, configured_engine_url())
    else:
        where = engine.materialize_store()
    return hashlib.sha256(f"{engine.name}|{where}".encode()).hexdigest()[:32]


def _delta_rebuild_due(record: Any, delta_cfg: Any) -> bool:
    """REQ-874: whether a delta table is due for its periodic whole rebuild. True when
    ``delta.rebuild_every`` seconds have elapsed since the last completed build; False when no
    interval is declared or no build has completed yet (the first-build case is handled by
    ``delta_build_reason`` via ``has_cursor``)."""
    every = getattr(delta_cfg, "rebuild_every", None)
    if every is None or record is None or record.completed_at is None:
        return False
    return (datetime.now(UTC) - record.completed_at).total_seconds() >= every


async def _model_row(state: Any, key: ReplicaKey) -> tuple[Any, Any, list[Any]]:
    """The source and table ``key`` names, as the model in this process has them, and every
    registered source (the vault a build dials sources under is bound for all of them)."""
    from provisa.federation.registry_view import registered_sources, registered_tables

    sources = await registered_sources(state)
    source = next((s for s in sources if s.id == key[0]), None)
    table = next(
        (
            t
            for t in await registered_tables(state)
            if (t.source_id, t.schema_name, t.table_name) == key
        ),
        None,
    )
    if source is None or table is None:
        raise ReplicaTableGone(key)
    return source, table, sources


class _EngineReached:
    """A table's reader, also declared readable by the engine in place.

    Whether the engine reaches a table itself is the engine's fact: its part in the build
    (``replica_engine``) says so when it holds what a copy inside the engine needs — the
    PostgreSQL engine's own foreign table for the copy, which exists whether or not the source
    is attached live (a source the operator floors has no live attach, REQ-1912, and is still
    copied by the engine). The reader is the stream the build falls back on only when the
    store takes no statement-level copy."""

    def __init__(self, reader: Any) -> None:
        self._reader = reader
        self.caps = SourceCaps(reader.caps.reads | {SourceRead.ENGINE_REACHABLE})

    def batches(self, batch_rows: int) -> Any:
        return self._reader.batches(batch_rows)


async def build_replica(state: Any, key: ReplicaKey, progress: Progress) -> BuildOutcome:
    """Build the replica of the table ``key`` names, in the org and environment ``state``
    serves: read its source as the source allows, write the engine's store through its write
    face, replace the replica atomically. Returns what the build did."""
    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.land_lock import land_lock
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.residency import resolve_landing_args
    from provisa.federation.source_vault import org_vault

    source, table, sources = await _model_row(state, key)
    engine = state.federation_engine
    if not whole_copy(source, table, engine):
        raise NoWholeCopy(key)  # nothing asks for one; a build that ran would call a function bare
    backend = engine.engine.backend
    args = resolve_landing_args(source, table, platform=backend.dialect)
    address = backend.replica_address(
        state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
    )
    async with state.tenant_db.acquire() as conn:
        record = await replica_state.read(conn, key)

    # REQ-990/REQ-848: a SingleStore store whose write face for this origin is PIPELINE_LAND loads
    # the origin itself through a one-shot pipeline; no row passes through Provisa.
    pipelined = await _build_by_pipeline(state, key, source, table, sources, args, address)
    if pipelined is not None:
        return pipelined

    # REQ-874: a declared delta refreshes the whole-copy replica incrementally -- read only the
    # rows past the stored cursor from the SQL source and apply them by key -- when the rule allows;
    # otherwise the whole rebuild below runs and records WHY (delta_skipped). SQL sources only; the
    # config/admin guards refuse `delta` on non-SQL source types by name.
    delta_cfg = getattr(table, "delta", None)
    delta_skipped: str | None = None
    if delta_cfg is not None:
        from sqlalchemy import make_url

        from provisa.federation import delta as _delta

        eng = engine.engine
        # The store the replica is applied into is the engine's materialization store -- the same
        # store ``replica_target`` writes the whole copy to, for every engine (REQ-1912).
        store_dsn = eng.materialize_store()
        store_applies = make_url(store_dsn).get_backend_name() in ("postgresql", "duckdb")
        this_def = definition_hash(source, address, args.columns, args.pk_columns)
        cursor_raw = record.delta_cursor if record is not None else None
        has_cursor = (
            record is not None
            and cursor_raw is not None
            and record.exists_in(store_identity(state))
        )
        delta_skipped = _delta.delta_build_reason(
            has_delta=True,
            has_cursor=has_cursor,
            reason=(record.requested_reason if record else None) or replica_state.REASON_MODEL,
            definition_changed=(
                record is not None
                and record.definition_hash is not None
                and record.definition_hash != this_def
            ),
            store_applies_delta=store_applies and state.source_pools.has(source.id),
            rebuild_due=_delta_rebuild_due(record, delta_cfg),
        )
        if delta_skipped is None and cursor_raw is not None:
            import json

            async with state.tenant_db.acquire() as conn:
                await replica_state.record_started(conn, key, method="delta", load_kind="delta")
            async with land_lock(f"{address.schema}.{address.table}"):
                applied, new_cursor = await _delta.apply_sql_delta(
                    state, engine, source, table, args, address, json.loads(cursor_raw), store_dsn
                )
            async with state.tenant_db.acquire() as conn:
                await replica_state.record_delta_applied(conn, key, cursor=new_cursor)
            return BuildOutcome(
                rows_copied=applied,
                method="delta",
                changed=applied > 0,
                definition_hash=this_def,
                built_columns=[[name, ir_type] for name, ir_type in args.columns],
            )
    loader = SourceRowLoader(
        engine,
        adapter_loaders=build_adapter_loaders(state, engine),
        keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
    )
    # REQ-1695: the build dials sources, so the vault of the org they are registered in is
    # bound here, where the build runs.
    async with org_vault(state, sources):
        reader = loader.replica_source(state, source, table, args.columns)
        engine_party = backend.replica_engine(state, source, table, address=address, args=args)
        target = backend.replica_target(state, address=address, args=args, engine=engine_party)
        if engine_party.caps.reaches_source:
            reader = _EngineReached(reader)

        async def still_wanted() -> None:
            # A table deleted while its build ran: the build ends without swapping.
            await _model_row(state, key)

        job = data_replicator(
            reader,
            target,
            engine_party,
            batch_rows=BATCH_ROWS,
            still_wanted=still_wanted,
            # The last build's hash says "unchanged" only of the replica standing in THIS store.
            prior_hash=(
                record.content_hash
                if record is not None and record.exists_in(store_identity(state))
                else None
            ),
        )
        # How this build copies, recorded as it starts so an operator sees it while it runs.
        async with state.tenant_db.acquire() as conn:
            await replica_state.record_started(
                conn, key, method=job.method.value, load_kind=target.caps.load.value
            )
        # REQ-1661: never two writers on one replica in a process. The event loop's delta lands
        # take this same lock, keyed on the replica's address.
        async with land_lock(f"{address.schema}.{address.table}"):
            outcome = await job.run(progress)
        # REQ-874: a delta table that fell through to a whole rebuild records why (delta_skipped) and
        # advances the cursor to the source's current max watermark, so the next build resumes as a
        # delta from there instead of re-pulling everything.
        if delta_cfg is not None:
            from provisa.federation import delta as _delta

            new_cursor = await _delta.source_max_watermark(state, engine, source, table)
            async with state.tenant_db.acquire() as conn:
                await replica_state.record_whole_rebuild(
                    conn, key, skipped=delta_skipped or _delta.SKIP_NO_DELTA, cursor=new_cursor
                )
        # What it was built from: the next convergence compares the model with this.
        return replace(
            outcome,
            definition_hash=definition_hash(source, address, args.columns, args.pk_columns),
            built_columns=[[name, ir_type] for name, ir_type in args.columns],
        )


async def _build_by_pipeline(
    state: Any,
    key: ReplicaKey,
    source: Any,
    table: Any,
    sources: Any,
    args: Any,
    address: Any,
) -> BuildOutcome | None:
    """The build, when the store's write face for this table's origin is PIPELINE_LAND (REQ-848,
    REQ-990); None when it is not, and the build streams as every other one does."""
    from sqlalchemy import make_url

    from provisa.events.land_lock import land_lock
    from provisa.federation.materialization import WriteFace, select_write_face
    from provisa.federation.singlestore_pipeline import PIPELINE_STORE, origin_of
    from provisa.federation.singlestore_pipeline_build import build_by_pipeline
    from provisa.federation.source_vault import org_vault

    engine = state.federation_engine
    bare = engine.engine
    if bare.native_store != PIPELINE_STORE:
        return None
    origin = origin_of(source, table)
    store = make_url(bare.materialize_store()).get_backend_name()
    if select_write_face(bare, store, origin) is not WriteFace.PIPELINE_LAND:
        return None
    backend = bare.backend
    async with org_vault(state, sources):
        target = backend.replica_target(state, address=address, args=args, engine=None)
        async with state.tenant_db.acquire() as conn:
            await replica_state.record_started(
                conn,
                key,
                method=Method.STORE_PIPELINE.value,
                load_kind=TargetLoad.BULK_STREAM.value,
            )
        async with land_lock(f"{address.schema}.{address.table}"):
            outcome = await build_by_pipeline(
                source=source,
                origin=origin,
                target=target,
                columns=[name for name, _ in args.columns],
            )
    return replace(
        outcome,
        definition_hash=definition_hash(source, address, args.columns, args.pk_columns),
        built_columns=[[name, ir_type] for name, ir_type in args.columns],
    )


def _next_refresh_at(state: Any) -> Any:
    """When a replica completed now is next due, by its declared cache TTL; None for a table
    with no TTL of its own (its refresh is then only what a reader's tolerance asks for), and
    for one that left the model while it was built."""

    async def due(key: ReplicaKey, now: datetime) -> datetime | None:
        from provisa.federation.role_ttl import declared_cache_ttl

        try:
            source, table, _sources = await _model_row(state, key)
        except ReplicaTableGone:
            return None
        ttl = declared_cache_ttl(table, source)
        return now + timedelta(seconds=ttl) if ttl else None

    return due


def _source_cap(state: Any) -> Any:
    """A replica's source's live-read cap (REQ-1909), or None when the source has none."""

    async def cap(key: ReplicaKey) -> int | None:
        from provisa.federation.registry_view import registered_sources

        source = next((s for s in await registered_sources(state) if s.id == key[0]), None)
        return source.max_live_concurrency if source is not None else None

    return cap


async def run_build(state: Any, key: ReplicaKey, progress: Progress) -> BuildOutcome:
    """One build as the runner runs it: the build itself, its outcome stamped on the table's
    node in the event loop's freshness state (a materialized view that reads this table judges
    its input by that stamp), and a build that changed the replica posted to that node so its
    dependents ripple."""
    from provisa.events import queue
    from provisa.events.nodes import source_node

    node = source_node(*key)
    try:
        outcome = await build_replica(state, key, progress)
    except BaseException:
        async with state.tenant_db.acquire() as conn:
            await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
        raise
    async with state.tenant_db.acquire() as conn:
        await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=True)
        if outcome.changed and key in (getattr(state, "replica_nodes", None) or {}):
            # The replica changed: post it to the table's event-loop node, which re-posts
            # its change to the materialized views that read it. A build whose content is
            # unchanged posts nothing, so nothing ripples (REQ-981).
            event_id = await queue.post_event(
                conn,
                source_table=node,
                event_type="replace",
                payload={"built": True, "rows": outcome.rows_copied},
            )
            await queue.fan_out(conn, event_id, [node])
    return outcome


def _group_loader(state: Any, source: Any) -> Any:
    """The adapter that reads ``source``'s rows, when it gives several tables from one read
    (it carries ``replica_group`` and ``replica_group_source``); None for every other source."""
    from provisa.events.app_wiring import build_adapter_loaders

    loader = build_adapter_loaders(state, state.federation_engine).get(source.type)
    if loader is None or getattr(loader, "replica_group", None) is None:
        return None
    return loader


async def group_of(state: Any, key: ReplicaKey) -> list[ReplicaKey]:
    """The other replicas one read of ``key``'s source builds with it: the tables its adapter
    says come from the same read, that the model declares and that have a replica. Empty for a
    table read on its own, which is every table of most sources."""
    from provisa.federation.registry_view import registered_tables

    try:
        source, table, _sources = await _model_row(state, key)
    except ReplicaTableGone:
        return []
    loader = _group_loader(state, source)
    names = loader.replica_group(source, table) if loader is not None else None
    if not names:
        return []
    declared = {
        (t.source_id, t.schema_name, t.table_name)
        for t in await registered_tables(state)
        if t.source_id == key[0] and t.schema_name == key[1] and t.table_name in names
    }
    siblings: list[ReplicaKey] = []
    async with state.tenant_db.acquire() as conn:
        for sibling in sorted(declared - {key}):
            record = await replica_state.read(conn, sibling)
            if record is not None and record.retired_at is None:
                siblings.append(sibling)
    return siblings


async def build_group(
    state: Any, keys: list[ReplicaKey], progress: Any
) -> dict[ReplicaKey, "BuildOutcome | BaseException"]:
    """Build the replicas ``keys`` name from ONE read of their source (``ReplicaGroupJob``):
    what a single build does for one table -- its store address, its build table, its record
    of how it copies, the lock on its replica in this process, its stamp on the table's
    freshness node -- done for each, around one read. A table the model no longer declares is
    answered with why and the others are built. Raises when the read itself fails."""
    from contextlib import AsyncExitStack

    from provisa.events import queue
    from provisa.events.land_lock import land_lock
    from provisa.events.nodes import source_node
    from provisa.federation.data_replicator import GroupPart, ReplicaGroupJob
    from provisa.federation.residency import resolve_landing_args
    from provisa.federation.source_vault import org_vault

    engine = state.federation_engine
    backend = engine.engine.backend
    results: dict[ReplicaKey, BuildOutcome | BaseException] = {}
    prepared: dict[ReplicaKey, tuple[Any, Any, Any, Any]] = {}  # source, table, args, address
    sources: list[Any] = []
    for key in keys:
        try:
            source, table, sources = await _model_row(state, key)
            if not whole_copy(source, table, engine):
                raise NoWholeCopy(key)
        except (ReplicaTableGone, NoWholeCopy) as gone:
            results[key] = gone
            continue
        args = resolve_landing_args(source, table, platform=backend.dialect)
        address = backend.replica_address(
            state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
        )
        prepared[key] = (source, table, args, address)
    if not prepared:
        return results
    source = next(iter(prepared.values()))[0]
    loader = _group_loader(state, source)
    if loader is None:
        raise RuntimeError(f"source {source.id!r} gives no group of tables from one read")

    def _still_wanted(key: ReplicaKey) -> Any:
        async def still_wanted() -> None:
            await _model_row(state, key)  # raises when the table was deleted while it was read

        return still_wanted

    nodes = {key: source_node(*key) for key in prepared}
    try:
        # REQ-1695: the read dials the source, so the vault of the org it is registered in is
        # bound here, where the build runs.
        async with org_vault(state, sources):
            parts: dict[str, GroupPart] = {}
            for key, (src, table, args, address) in prepared.items():
                engine_party = backend.replica_engine(state, src, table, address=address, args=args)
                target = backend.replica_target(
                    state, address=address, args=args, engine=engine_party
                )
                async with state.tenant_db.acquire() as conn:
                    record = await replica_state.read(conn, key)
                parts[table.table_name] = GroupPart(
                    target,
                    engine_party,
                    # The last build's hash says "unchanged" only of the replica in THIS store.
                    prior_hash=(
                        record.content_hash
                        if record is not None and record.exists_in(store_identity(state))
                        else None
                    ),
                    still_wanted=_still_wanted(key),
                )
            by_table = {table.table_name: key for key, (_s, table, _a, _d) in prepared.items()}
            reader = loader.replica_group_source(
                source,
                tuple(by_table),
                {t.table_name: args.columns for _s, t, args, _d in prepared.values()},
            )
            job = ReplicaGroupJob(reader, parts, batch_rows=BATCH_ROWS)
            async with state.tenant_db.acquire() as conn:
                for key, (_s, table, _a, _d) in prepared.items():
                    await replica_state.record_started(
                        conn,
                        key,
                        method=job.methods[table.table_name].value,
                        load_kind=parts[table.table_name].target.caps.load.value,
                    )

            async def table_progress(table_name: str, rows_copied: int) -> None:
                await progress(by_table[table_name], rows_copied)

            # REQ-1661: never two writers on one replica in a process; the locks are taken in
            # one order (the addresses sorted) by every group build.
            async with AsyncExitStack() as held:
                for name in sorted(f"{a.schema}.{a.table}" for _s, _t, _a, a in prepared.values()):
                    await held.enter_async_context(land_lock(name))
                built = await job.run(table_progress)
    except BaseException:
        async with state.tenant_db.acquire() as conn:
            for node in nodes.values():
                await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
        raise
    async with state.tenant_db.acquire() as conn:
        for key, (src, table, args, address) in prepared.items():
            outcome = built[table.table_name]
            node = nodes[key]
            if isinstance(outcome, BaseException):
                await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
                results[key] = outcome
                continue
            await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=True)
            if outcome.changed and key in (getattr(state, "replica_nodes", None) or {}):
                # The replica changed: post it to the table's event-loop node, as a single
                # build does (run_build), so the views that read it ripple (REQ-981).
                event_id = await queue.post_event(
                    conn,
                    source_table=node,
                    event_type="replace",
                    payload={"built": True, "rows": outcome.rows_copied},
                )
                await queue.fan_out(conn, event_id, [node])
            # What it was built from: the next convergence compares the model with this.
            results[key] = replace(
                outcome,
                definition_hash=definition_hash(src, address, args.columns, args.pk_columns),
                built_columns=[[name, ir_type] for name, ir_type in args.columns],
            )
    return results


def make_runner(state: Any, org_id: str, platform_url: str) -> ReplicaRunner:
    """This process's runner for the org ``state`` serves."""
    from provisa.core import settings_registry
    from provisa.core.connection_loop import spawn_background
    from provisa.federation.engine import configured_engine_url
    from provisa.federation.replica_locks import BuildLocks

    engine = state.federation_engine.engine

    async def build(key: ReplicaKey, progress: Progress) -> BuildOutcome:
        return await run_build(state, key, progress)

    return ReplicaRunner(
        db=state.tenant_db,
        org_id=org_id,
        locks=BuildLocks(platform_url),
        engine_key=lambda: engine_job_key(engine.name, configured_engine_url()),
        build=build,
        source_cap=_source_cap(state),
        permits=state.live_permit_store,
        next_refresh_at=_next_refresh_at(state),
        store=lambda: store_identity(state),
        retry=replica_state.retry_policy,
        housekeeping=lambda locks, org: drop_retired(state, locks, org),
        builds_per_node=lambda: int(settings_registry.value("replication.builds_per_node")),
        engine_jobs=lambda: int(settings_registry.value("replication.engine_jobs")),
        spawn=spawn_background,
        group_of=lambda key: group_of(state, key),
        build_group=lambda keys, progress: build_group(state, keys, progress),
    )


def wire_replica_runner(scheduler: Any, *, state: Any, platform_url: str) -> None:
    """Register this process's build pass for the org ``state`` serves on ``scheduler``.
    Idempotent: registering again replaces the job and the runner. A process that does no
    background work (REQ-1916) registers nothing."""
    from apscheduler.triggers.interval import IntervalTrigger

    from provisa.core import process_mode
    from provisa.core.request_context import require_current_org, reset_current_org, set_current_org

    if not process_mode.runs_background_work():
        return
    org_id = require_current_org()
    runner = make_runner(state, org_id, platform_url)
    _runners[org_id] = runner

    async def _pass() -> None:
        token = set_current_org(org_id)
        try:
            await runner.run_pass()
        finally:
            reset_current_org(token)

    scheduler.add_job(
        _pass,
        trigger=IntervalTrigger(seconds=_PASS_SECONDS),
        id=f"{RUNNER_JOB_ID}:org_{org_id}",
        replace_existing=True,
        next_run_time=datetime.now(UTC),
    )


async def request_if_replicated(state: Any, table_id: int, source_id: str, reason: str) -> bool:
    """Ask for a build of the whole-table replica of a table whose rows changed at its source
    (REQ-1915): a write made through Provisa (REQ-1924), or a change its source reported
    (REQ-1861). True when the request stands -- made now, or kept on a build already running
    (``replica_state.request_build``).

    The table is judged as a read judges it (``query_residency``): it is served from a replica
    when the operator's settings put it there or the engine cannot read its source in place, and
    only a whole copy is built -- a table replicated row by row, or one with a parameter column,
    has no whole to rebuild. Nothing is asked for a table that is not."""
    from provisa.core.request_context import current_org
    from provisa.federation import replica_state
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.replica_converge import whole_copy
    from provisa.federation.strategy import engine_attaches

    engine = state.federation_engine
    source = {s.id: s for s in await registered_sources(state)}.get(source_id)
    if source is None:
        return False  # a built-in source (registry_view.registered_sources): never replicated
    table = {t.id: t for t in await registered_tables(state)}[table_id]
    served_from_replica = table.id in state.replica_routes.floored or not engine_attaches(
        engine, source.type.value
    )
    if not (served_from_replica and whole_copy(source, table, engine)):
        return False
    async with state.tenant_db.acquire() as conn:
        requested = await replica_state.request_build(
            conn, (source.id, table.schema_name, table.table_name), reason
        )
    if requested:
        kick(current_org.get(None))
    return True


def kick(org_id: str | None) -> None:
    """Start this process's build pass for ``org_id`` now, without waiting for its next
    scheduled one — a read has just asked for a build. Does nothing in a process with no
    runner for the org: another node's pass picks the request up."""
    from provisa.core.connection_loop import spawn_background

    runner = _runners.get(org_id)
    if runner is not None:
        spawn_background(runner.run_pass(), name="replica-builds:kick")
