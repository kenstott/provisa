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

#: The scheduler job that runs a process's build pass for one org environment (suffixed for a
#: non-default org, as the event loop's jobs are, and then for an environment other than prod).
#: Run by every worker, not only the scheduler's holder.
RUNNER_JOB_ID = "replica:builds"
#: How often a process looks for requested builds when nothing kicked it sooner.
_PASS_SECONDS = 2

#: This process's runner per org (None: the default org) and environment, so a read can start a
#: pass at once. Each environment's replicas have their own build state (its tenant schema), so
#: each has its own runner; one keyed by org alone was replaced by the next environment built (#273).
_runners: dict[tuple[str | None, str], ReplicaRunner] = {}
#: The orgs a build was asked of with no runner here, said once each (kick).
_no_runner_said: set[tuple[str | None, str]] = set()


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
    from provisa.federation.registry_view import (
        draft_tables,
        registered_sources,
        registered_tables,
    )

    sources = await registered_sources(state)
    source = next((s for s in sources if s.id == key[0]), None)
    table = next(
        (
            t
            # REQ-1921: a draft's build is asked for by its steward's preview; it is found here.
            for t in [*await registered_tables(state), *await draft_tables(state)]
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


def _prior_hash(state: Any, record: Any) -> str | None:
    """The last build's content hash, which says "unchanged" only of the replica standing in
    THIS store."""
    if record is not None and record.exists_in(store_identity(state)):
        return record.content_hash
    return None


async def build_replica(state: Any, key: ReplicaKey, progress: Progress) -> BuildOutcome:
    """Build the replica of the table ``key`` names, in the org and environment ``state``
    serves: read its source as the source allows, write the engine's store through its write
    face, replace the replica atomically. Returns what the build did."""
    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.land_lock import land_lock
    from provisa.events.nodes import source_node
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.replica_preflight import row_check
    from provisa.federation.residency import resolve_landing_args
    from provisa.federation.source_vault import org_vault

    source, table, sources = await _model_row(state, key)
    # REQ-2113: a source given its file by download reads the org's copy; the copy is fetched
    # again first when the table's TTL has lapsed since it was made.
    from provisa.core.source_downloads import refetched

    await refetched(source, table)
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
    # REQ-1165: the table's check runs over the rows this build streams, before its swap. A
    # table with one is built whole and streamed: no pipeline, no delta, no engine statement.
    check = row_check(
        table,
        node=source_node(*key),
        columns=args.columns,
        prior_hash=_prior_hash(state, record),
    )

    # REQ-990/REQ-848: a SingleStore store whose write face for this origin is PIPELINE_LAND loads
    # the origin itself through a one-shot pipeline; no row passes through Provisa.
    if check is None:
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
        this_def = definition_hash(
            source, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
        )
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
        if delta_skipped is None and check is not None:
            delta_skipped = _delta.SKIP_PREFLIGHT
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
        import asyncio

        from provisa.federation.pgwire_replica import serve_for_build

        # A source whose server takes minutes to start is attached only once it serves.
        await asyncio.to_thread(serve_for_build, source)
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
            prior_hash=_prior_hash(state, record),
            check=check,
        )
        # How this build copies, recorded as it starts so an operator sees it while it runs.
        async with state.tenant_db.acquire() as conn:
            await replica_state.record_started(
                conn, key, method=job.method.value, load_kind=target.caps.load.value
            )
        # REQ-1733: a table its source's CDC topic follows is kept, after this build, by a
        # listener on that topic, which starts where the build stopped reading it. Taken before
        # the source is read, so no change is missed; one made while it is read is applied again
        # by key.
        from provisa.events.push_wiring import cdc_capture
        from provisa.kafka import landed_offsets

        captured = cdc_capture(source, table)
        stopped = await landed_offsets.end_offsets(*captured) if captured else None
        # REQ-1661: never two writers on one replica in a process. The event loop's delta lands
        # take this same lock, keyed on the replica's address.
        async with land_lock(f"{address.schema}.{address.table}"):
            outcome = await job.run(progress)
        if captured and stopped is not None:
            await landed_offsets.record(
                captured[0], landed_offsets.landed_group(source.id), stopped
            )
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
            definition_hash=definition_hash(
                source, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
            ),
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
        definition_hash=definition_hash(
            source, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
        ),
        built_columns=[[name, ir_type] for name, ir_type in args.columns],
    )


async def _probe_decides(state: Any, key: ReplicaKey, source: Any, table: Any) -> bool:
    """Whether the table's own probe, and not its TTL, says when its replica is rebuilt
    (REQ-855: under ``probe`` and ``ttl_probe`` a copy is rebuilt only when the probe says the
    source changed). True for a replica a node of the event loop builds, whose change signal
    probes and whose probe has a token to ask for; its node's timer runs that probe on the
    table's TTL and asks for the build. A probe with no token to give degrades to the TTL
    (REQ-982), and so does a replica no node watches."""
    from provisa.core.change_signal import is_poll
    from provisa.core.secrets import resolve_secrets
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.events.probes import can_probe, loader_probe
    from provisa.federation.residency import resolve_landing_args

    if key not in (getattr(state, "replica_nodes", None) or {}):
        return False
    backend = state.federation_engine.engine.backend
    args = resolve_landing_args(source, table, platform=backend.dialect)
    if not is_poll(args.change_signal) or args.change_signal == "ttl":
        return False
    return can_probe(
        args.probe_type,
        watermark_column=args.watermark_column,
        sentinel_path=getattr(source, "sentinel_path", None),  # REQ-1148
        source_path=resolve_secrets(getattr(source, "path", "") or "") or None,  # REQ-788
        file_glob=getattr(table, "file_glob", None),  # REQ-788
        # The source's own adapter asks its provider (a mail source's mailboxes).
        loader_probe=loader_probe(
            build_adapter_loaders(state, state.federation_engine), source, table
        )
        is not None,
    )


def _off_peak_window(source: Any, table: Any) -> Any:
    """The off-peak window a load-protected table is refreshed in (REQ-1141), or None: one
    not load-protected, or naming no window, is refreshed whenever it is due."""
    from provisa.federation.scheduled_refresh import resolve_refresh_policy

    policy = resolve_refresh_policy(source, table)
    return policy.window if policy.load_protected else None


def _next_refresh_at(state: Any) -> Any:
    """When a replica completed now is next due, by its declared cache TTL; None for a table
    with no TTL of its own (its refresh is then only what a reader's tolerance asks for), for
    one whose probe decides (:func:`_probe_decides`), and for one that left the model while it
    was built. A load-protected table with an off-peak window and no TTL is due at the window's
    next opening: once per window (REQ-1141). A table on the ``signal`` change signal is never due
    on a clock: it is rebuilt when a signal says its source changed (REQ-1149). A table whose
    cache_ttl never expires is never due: it lands once (REQ-2031)."""

    async def due(key: ReplicaKey, now: datetime) -> datetime | None:
        from provisa.core.cache_ttl import NEVER_EXPIRES
        from provisa.core.change_signal import is_trigger
        from provisa.federation.role_ttl import declared_cache_ttl
        from provisa.federation.scheduled_refresh import next_window_open

        try:
            source, table, _sources = await _model_row(state, key)
        except ReplicaTableGone:
            return None
        signal = table.change_signal if table.change_signal is not None else source.change_signal
        if is_trigger(signal) or await _probe_decides(state, key, source, table):
            return None
        ttl = declared_cache_ttl(table, source)
        if ttl == NEVER_EXPIRES:
            return None  # REQ-2031: landed once, never due again
        if ttl:
            return now + timedelta(seconds=ttl)
        window = _off_peak_window(source, table)
        if window is None:
            return None
        return datetime.fromtimestamp(next_window_open(window, now.timestamp()), UTC)

    return due


def _deferred_until(state: Any) -> Any:
    """When a build of a replica that stands in this store may run, when it may not run now: a
    load-protected table's refresh runs only while its off-peak window is open (REQ-1141).
    None -- run now -- for any other table, inside the window, and for a replica not yet built
    here: its first build is what reads of it wait on."""

    async def until(key: ReplicaKey, now: datetime) -> datetime | None:
        from provisa.federation.scheduled_refresh import next_window_open, window_open

        try:
            source, table, _sources = await _model_row(state, key)
        except ReplicaTableGone:
            return None
        window = _off_peak_window(source, table)
        if window is None or window_open(window, now.timestamp()):
            return None
        async with state.tenant_db.acquire() as conn:
            record = await replica_state.read(conn, key)
        if record is None or not record.exists_in(store_identity(state)):
            return None
        return datetime.fromtimestamp(next_window_open(window, now.timestamp()), UTC)

    return until


def _source_cap(state: Any) -> Any:
    """A replica's source's live-read cap (REQ-1909), or None when the source has none."""

    async def cap(key: ReplicaKey) -> int | None:
        from provisa.federation.registry_view import registered_sources

        source = next((s for s in await registered_sources(state) if s.id == key[0]), None)
        return source.max_live_concurrency if source is not None else None

    return cap


async def _post_check_verdict(
    state: Any, conn: Any, key: ReplicaKey, node: str, failure: BaseException
) -> None:
    """REQ-1165: a checked build's ABORT and QUARANTINE are the event loop's own events about the
    table's node, as land() posted them when the loop checked the rows itself: ``error`` (the
    reason, fanned to the views that read the table, which it poisons) and ``quarantine`` (the
    reason, a hold, not fanned). Any other failure is the build's, recorded on its own record."""
    from provisa.events import queue
    from provisa.events.processor import PreflightQuarantine, PreprocessError

    if key not in (getattr(state, "replica_nodes", None) or {}):
        return  # the table's node is not in this process's event loop: nothing watches it here
    if isinstance(failure, PreprocessError):
        event_id = await queue.post_event(
            conn, source_table=node, event_type="error", payload={"error": str(failure)}
        )
        await queue.fan_out(conn, event_id, state.replica_dependents_of(node))
    elif isinstance(failure, PreflightQuarantine):
        await queue.post_event(
            conn, source_table=node, event_type="quarantine", payload={"reason": failure.reason}
        )


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
    except BaseException as failed:
        async with state.tenant_db.acquire() as conn:
            await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
            await _post_check_verdict(state, conn, key, node, failed)
        raise
    async with state.tenant_db.acquire() as conn:
        await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=True)
        if outcome.changed:
            await _post_built(state, conn, key, node, outcome.rows_copied)
    landed = getattr(state, "on_input_landed", None)
    if outcome.changed and landed is not None:
        # A view left off the event loop because this replica had not landed is wired now
        # (events.app_wiring._wire_views_reading).
        landed(node)
    return outcome


async def _post_built(state: Any, conn: Any, key: ReplicaKey, node: str, rows: int) -> None:
    """A build that changed the replica, told to what reads it. A table with an event-loop node
    is posted to that node, which re-posts its change to the materialized views that read it. A
    table with none -- one that lands once and is never re-read (cache_ttl -1, REQ-2031) -- is
    posted to those views directly: its one build is the only change they will hear of. A build
    whose content is unchanged posts nothing, so nothing ripples (REQ-981)."""
    from provisa.events import queue

    if key in (getattr(state, "replica_nodes", None) or {}):
        listeners = [node]
    else:
        # No event loop wired in this process (nothing to tell), or the table has no node.
        dependents_of = getattr(state, "replica_dependents_of", None)
        listeners = dependents_of(node) if dependents_of is not None else []
    if not listeners:
        return
    event_id = await queue.post_event(
        conn, source_table=node, event_type="replace", payload={"built": True, "rows": rows}
    )
    await queue.fan_out(conn, event_id, listeners)


#: The source types whose adapter reads its tables as a group (``replica_group``): a mail
#: source's several tables from one read (Microsoft 365, Google Workspace), a Logs source's one
#: table (REQ-1996), each of which may be kept in place a part at a time. Asked first, so a
#: build of any other source's table looks no adapter up.
GROUP_READ_TYPES: frozenset[str] = frozenset({"microsoft_365", "google_workspace", "logs"})


def _group_loader(state: Any, source: Any) -> Any:
    """The adapter that reads ``source``'s rows, when it gives several tables from one read
    (it carries ``replica_group`` and ``replica_group_source``); None for every other source."""
    if source.type not in GROUP_READ_TYPES:
        return None
    from provisa.events.app_wiring import build_adapter_loaders

    loader = build_adapter_loaders(state, state.federation_engine).get(source.type)
    if loader is None or getattr(loader, "replica_group", None) is None:
        return None
    return loader


async def group_of(state: Any, key: ReplicaKey) -> list[ReplicaKey] | None:
    """The other replicas one read of ``key``'s source builds with it: the tables its adapter
    says come from the same read, that the model declares and that have a replica. None for a
    table read on its own, which is every table of most sources; an empty list for a table
    of a group whose siblings are not declared or have no replica."""
    from provisa.federation.registry_view import draft_tables, registered_tables

    try:
        source, table, _sources = await _model_row(state, key)
    except ReplicaTableGone:
        return None
    loader = _group_loader(state, source)
    names = loader.replica_group(source, table) if loader is not None else None
    if not names:
        return None
    declared = {
        (t.source_id, t.schema_name, t.table_name)
        for t in [*await registered_tables(state), *await draft_tables(state)]
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
    from provisa.federation.data_replicator import GroupPart, GroupReadFailed, ReplicaGroupJob
    from provisa.federation.replica_preflight import row_check
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
            by_table = {table.table_name: key for key, (_s, table, _a, _d) in prepared.items()}
            if await _kept_in_place(state, loader, prepared):
                async with AsyncExitStack() as held:
                    for name in sorted(
                        f"{a.schema}.{a.table}" for _s, _t, _a, a in prepared.values()
                    ):
                        await held.enter_async_context(land_lock(name))
                    built = await _land_units(state, loader, prepared, by_table)
                return await _group_results(state, prepared, nodes, built, results)
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
                    prior_hash=_prior_hash(state, record),
                    still_wanted=_still_wanted(key),
                    check=row_check(
                        table,
                        node=nodes[key],
                        columns=args.columns,
                        prior_hash=_prior_hash(state, record),
                    ),
                )
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
    except BaseException as failed:
        async with state.tenant_db.acquire() as conn:
            for node in nodes.values():
                await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
        if isinstance(failed, Exception) and results:
            # Tables answered before the read keep their own reason on their records.
            raise GroupReadFailed(failed, dict(results)) from failed
        raise
    return await _group_results(state, prepared, nodes, built, results)


async def _group_results(
    state: Any,
    prepared: dict[ReplicaKey, tuple[Any, Any, Any, Any]],
    nodes: dict[ReplicaKey, Any],
    built: dict[str, "BuildOutcome | BaseException"],
    results: dict[ReplicaKey, "BuildOutcome | BaseException"],
) -> dict[ReplicaKey, "BuildOutcome | BaseException"]:
    """Each table's outcome of one read, stamped on its freshness node and posted to its
    event-loop node when the replica changed; what it was built from is added to it."""
    from provisa.events import queue

    async with state.tenant_db.acquire() as conn:
        for key, (src, table, args, address) in prepared.items():
            outcome = built[table.table_name]
            node = nodes[key]
            if isinstance(outcome, BaseException):
                await queue.record_refresh(conn, node, at=datetime.now(UTC), ok=False)
                await _post_check_verdict(state, conn, key, node, outcome)
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
                definition_hash=definition_hash(
                    src, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
                ),
                built_columns=[[name, ir_type] for name, ir_type in args.columns],
            )
    return results


async def _kept_in_place(
    state: Any, loader: Any, prepared: dict[ReplicaKey, tuple[Any, Any, Any, Any]]
) -> bool:
    """Whether the tables of this read are kept in place a part at a time
    (``federation.mail_units``) instead of copied whole and swapped in: the source's reader
    gives its changes in units, every table has a key to keep rows by (the loader's
    ``replica_key_columns``), and no table's replica was built from another definition
    (changed columns are a whole rebuild)."""
    if getattr(loader, "replica_unit_source", None) is None:
        return False
    # REQ-1165: a part kept in place is readable at once, with no swap to check before; a table
    # with a check is copied whole and checked before it is swapped in.
    if any(getattr(table, "mv_preprocess", None) for _s, table, _a, _d in prepared.values()):
        return False
    store = store_identity(state)
    async with state.tenant_db.acquire() as conn:
        for key, (src, table, args, address) in prepared.items():
            if not loader.replica_key_columns(table.table_name):
                return False
            record = await replica_state.read(conn, key)
            if (
                record is not None
                and record.exists_in(store)
                and record.definition_hash
                != definition_hash(
                    src, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
                )
            ):
                return False
    return True


#: Cached answers are dropped and an event is posted to a table's node for the parts landing
#: in it at most this often while a pass runs, and once more when it ends: a first build
#: writes thousands of parts.
_RIPPLE_EVERY_S = 5.0


async def _land_units(
    state: Any,
    loader: Any,
    prepared: dict[ReplicaKey, tuple[Any, Any, Any, Any]],
    by_table: dict[str, ReplicaKey],
) -> dict[str, "BuildOutcome | BaseException"]:
    """Keep the replicas ``prepared`` names in place from one pass over their source: every
    part the reader gives is written to them as it arrives and can be read at once. A table
    with no copy in this store becomes readable when its first part has landed, and says how
    many of the source's mailboxes have been read to their end while the pass runs."""
    import time

    from provisa.events import queue
    from provisa.events.nodes import source_node
    from provisa.federation.change_group import TableChanges
    from provisa.federation.mail_units import METHOD, Part, ReplicaUnitJob
    from provisa.federation.replica_hot import _row_count

    engine = state.federation_engine
    store = store_identity(state)
    source = next(iter(prepared.values()))[0]
    parts = {
        table.table_name: Part(
            address.schema,
            address.table,
            list(args.columns),
            tuple(loader.replica_key_columns(table.table_name)),
            loader.replica_stamp_column,
        )
        for _src, table, args, address in prepared.values()
    }
    hashes = {
        key: definition_hash(
            src, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
        )
        for key, (src, _t, args, address) in prepared.items()
    }
    first: dict[ReplicaKey, bool] = {}
    async with state.tenant_db.acquire() as conn:
        for key in prepared:
            record = await replica_state.read(conn, key)
            first[key] = not (record is not None and record.exists_in(store))
            await replica_state.record_started(
                conn, key, method=METHOD, load_kind=TargetLoad.ROW_COPY.value
            )
    # Every table is there before any part lands: a table no part ever writes to (a mailbox
    # with no attachment) is then an empty replica, not a missing one.
    await engine.apply_change_group(
        [
            TableChanges(part.schema, part.table, part.columns, list(part.keys))
            for _name, part in sorted(parts.items())
        ]
    )
    reader = loader.replica_unit_source(
        source, tuple(by_table), {name: part.columns for name, part in parts.items()}
    )
    rippled = dict.fromkeys(by_table, 0.0)

    async def first_write() -> None:
        now = datetime.now(UTC)
        async with state.tenant_db.acquire() as conn:
            for key, (_src, _table, args, _address) in prepared.items():
                if first[key]:
                    await replica_state.record_partial(
                        conn,
                        key,
                        store=store,
                        now=now,
                        definition_hash=hashes[key],
                        built_columns=[[name, ir_type] for name, ir_type in args.columns],
                    )

    async def mailbox_done(done: int) -> None:
        async with state.tenant_db.acquire() as conn:
            for key in prepared:
                await replica_state.record_parts(
                    conn, key, done=done, total=reader.total, unit=reader.unit, first=first[key]
                )

    table_ids = {table.table_name: table.id for _src, table, _a, _d in prepared.values()}

    async def ripple(names: list[str]) -> None:
        # An answer cached from the table before these parts landed is no longer what it
        # holds: dropped here, as a write through Provisa drops it (cache.tenancy).
        from provisa.cache.tenancy import invalidate_tables

        await invalidate_tables(state, [table_ids[name] for name in names])
        # And what a completed build posts (run_build): the views that read the table ripple.
        watched = getattr(state, "replica_nodes", None) or {}
        async with state.tenant_db.acquire() as conn:
            for name in names:
                key = by_table[name]
                if key not in watched:
                    continue
                node = source_node(*key)
                event_id = await queue.post_event(
                    conn, source_table=node, event_type="replace", payload={"built": True}
                )
                await queue.fan_out(conn, event_id, [node])

    behind: set[str] = set()  # tables written to since their last ripple

    async def written(unit: Any) -> None:
        now = time.monotonic()
        behind.update(name for name in parts if name in unit.rows or name in unit.removed)
        due = [name for name in sorted(behind) if now - rippled[name] >= _RIPPLE_EVERY_S]
        for name in due:
            rippled[name] = now
            behind.discard(name)
        if due:
            await ripple(due)

    landed = await ReplicaUnitJob(
        reader,
        parts,
        engine.apply_change_group,
        first_write=first_write,
        mailbox_done=mailbox_done,
        written=written,
    ).run()
    if behind:
        await ripple(sorted(behind))  # the last parts landed inside the interval
    notes = tuple(reader.notes())
    built: dict[str, BuildOutcome | BaseException] = {}
    for name, key in by_table.items():
        built[name] = BuildOutcome(
            # What the replica holds now, counted where it is read: a pass that applies a few
            # changes writes few rows, and the table's size is not that.
            rows_copied=await _row_count(state, key),
            method=METHOD,
            # The pass's last parts may have landed after the last event posted for them: the
            # completion posts once more for a pass that wrote anything.
            changed=landed.wrote,
            notes=notes,
            committed=reader.committed,
        )
    return built


async def _indexed_replica(state: Any, key: ReplicaKey) -> tuple[Any, Any, Any, Any]:
    """The replica ``key`` names as its indexes see it: its source, table, address and the
    store's indexer (None: the store keeps no secondary index)."""
    source, table, _sources = await _model_row(state, key)
    backend = state.federation_engine.engine.backend
    address = backend.replica_address(
        state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
    )
    return source, table, address, backend.replica_indexer(state, address=address)


async def plan_indexes(state: Any, key: ReplicaKey, landed_at: datetime) -> None:
    """REQ-2015: record the indexes the replica that landed at ``landed_at`` is to carry -- its
    key and the columns its declared relationships join on, as the model has them now -- for
    the runner to build after the landing."""
    from provisa.federation import replica_indexes
    from provisa.federation.residency import resolve_landing_args

    source, table, address, indexer = await _indexed_replica(state, key)
    if indexer is None:
        return
    args = resolve_landing_args(
        source, table, platform=state.federation_engine.engine.backend.dialect
    )
    pk = list(args.pk_columns or ())
    # The relationships are the model's (its store); the plan is this store's replica state.
    async with state.model_db.acquire() as conn:
        joins = await replica_indexes.join_columns(conn, key, pk)
    specs = replica_indexes.plan(
        f"{address.schema}.{address.table}", [name for name, _ in args.columns], pk, joins
    )
    async with state.tenant_db.acquire() as conn:
        await replica_indexes.record_plan(conn, key, landed_at, specs)


async def build_indexes(state: Any, item: Any) -> None:
    """REQ-2015: build the indexes ``item`` names on the standing replica."""
    from provisa.federation import replica_indexes

    _source, _table, _address, indexer = await _indexed_replica(state, item.key)
    if indexer is None:
        return
    await replica_indexes.build_due(state.tenant_db, item, indexer)


def make_runner(state: Any, org_id: str, platform_url: str) -> ReplicaRunner:
    """This process's runner for the org ``state`` serves."""
    from provisa.core import settings_registry
    from provisa.core.connection_loop import spawn_background
    from provisa.federation.engine import configured_engine_url
    from provisa.federation.replica_locks import BuildLocks

    from provisa.scheduler.table_triggers import table_changed

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
        plan_indexes=lambda key, landed_at: plan_indexes(state, key, landed_at),
        deferred_until=_deferred_until(state),
        build_indexes=lambda item: build_indexes(state, item),
        changed=lambda key: table_changed(state, key),  # REQ-2054
    )


def wire_replica_runner(scheduler: Any, *, state: Any, platform_url: str) -> None:
    """Register this process's build pass for the org ``state`` serves on ``scheduler``.
    Idempotent: registering again replaces the job and the runner. A process that does no
    background work (REQ-1916) registers nothing."""
    from apscheduler.triggers.interval import IntervalTrigger

    from provisa.core import process_mode
    from provisa.core.environments import PROD
    from provisa.core.request_context import (
        active_env,
        require_current_org,
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    if not process_mode.runs_background_work():
        return
    org_id = require_current_org()
    env = active_env()
    runner = make_runner(state, org_id, platform_url)
    _runners[(org_id, env)] = runner

    async def _pass() -> None:
        token = set_current_org(org_id)
        env_token = set_current_env(env)
        try:
            await runner.run_pass()
        finally:
            reset_current_env(env_token)
            reset_current_org(token)

    scheduler.add_job(
        _pass,
        trigger=IntervalTrigger(seconds=_PASS_SECONDS),
        id=f"{RUNNER_JOB_ID}:org_{org_id}" + ("" if env == PROD else f":env_{env}"),
        replace_existing=True,
        next_run_time=datetime.now(UTC),
    )


def stop_replica_runner(org_id: str) -> None:
    """A deleted org's build runners, one per environment, start no more builds (REQ-2006)."""
    for key in [k for k in _runners if k[0] == org_id]:
        _runners.pop(key).stop()


def stop_replica_runners() -> None:
    """Process shutdown: every build runner this process registered starts no more builds, so
    none runs after the engine it builds with has been closed."""
    for runner in _runners.values():
        runner.stop()


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
    from provisa.federation.replica_routing import owns_replica
    from provisa.federation.strategy import engine_attaches

    engine = state.federation_engine
    source = {s.id: s for s in await registered_sources(state)}.get(source_id)
    if source is None:
        return False  # a built-in source (registry_view.registered_sources): never replicated
    if not owns_replica(source):
        return False  # read where it is written (NO_REPLICA_TYPES, live-only): never replicated
    table = {t.id: t for t in await registered_tables(state)}[table_id]
    from provisa.federation.role_ttl import never_expires

    if never_expires(table, source):
        return False  # REQ-2031: landed once; a change at its source is never read again
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


def kick(org_id: str | None, env: str | None = None) -> None:
    """Start this process's build pass for ``org_id``'s environment ``env`` (the bound one when
    None) now, without waiting for its next scheduled one — a read has just asked for a build.
    Does nothing in a process with no runner for it: another node's pass picks the request up."""
    from provisa.core.connection_loop import spawn_background
    from provisa.core.request_context import active_env

    scope = (org_id, env or active_env())
    runner = _runners.get(scope)
    if runner is None:
        if scope not in _no_runner_said:
            _no_runner_said.add(scope)
            log.warning(
                "replica builds of org %s (environment %s) requested with no build runner in this "
                "process: another node's pass builds them",
                *scope,
            )
        return
    _no_runner_said.discard(scope)
    spawn_background(runner.run_pass(), name="replica-builds:kick")
