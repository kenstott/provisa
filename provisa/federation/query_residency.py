# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3f9c25-1d6e-4b80-9e4c-2f8b5d17a6c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query path's residency prep (REQ-1661): a MATERIALIZED source a query reads is landed
before the read when it has never landed or has gone stale.

The event loop lands every materialized source at boot and refreshes it on its cadence. This is
the other half of the same contract, for the query that arrives first: before the execute terminal
runs a plan, each source the plan names is checked against the persisted node freshness state
(the stamp the event loop's own lands write) and, when stale, landed through the engine's
``materialize_pending`` -- the same loaders, landing address and store write face the event loop
uses, so both paths converge on one replica.

Stale means: a table of the source the query reads has no refresh stamp (never landed), its last
land failed, or its stamp has outrun THE READER's effective TTL on that table,
max(cache_ttl, role_ttl(role)) (REQ-1907) -- each table against its own TTL, so readers with a
larger tolerance serve the existing replica and never start a land. A source with
``freshness_gate`` set is judged by its own predicate (REQ-860). A ``load_protected`` source lands
here only when it has never landed (REQ-1141: the scheduler is its sole refresher).

Concurrent stale reads of one source share one land: staleness is re-read from the persisted node
state AFTER the per-node land locks are held, so a reader that queued behind another's land sees
the fresh stamp and serves it instead of landing again (REQ-1907). The wait for the lock is an
await inside the request's own coroutine, bounded by the request's deadline.

A land that fails is stamped ``ok=False`` (so the next query retries it) and fails the query with
its own cause; the query never reads the stale replica (REQ-1661, amended 2026-09-30).
"""

# Requirements: REQ-1661, REQ-860, REQ-855, REQ-1141, REQ-1907

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from provisa.federation.replica_build import ReplicaBuilding

log = logging.getLogger(__name__)


def _node(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def _physical_node(backend: Any, state: Any, source: Any, table: Any) -> str:
    """The lock key for ``land_lock`` — the address a land actually writes to, the table's replica
    address (REQ-1912), not its registered name. ``events/boot.py``'s own poll-node wiring locks
    on this same address (its ``land_schema``/``land_table``), and
    ``EngineBackend.materialize_pending`` resolves the identical one right after this function's
    caller acquires its lock — so the two lands ``land_lock``'s own docstring promises never
    interleave key on the SAME string (REQ-1730: keyed on anything else, Oracle's REPLACE land
    interleaved across two threads and landed every row twice)."""
    address = backend.replica_address(
        state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
    )
    return _node(address.schema, address.table)


def stale_sources(
    sources: list[Any],
    tables_by_source: dict[str, list[Any]],
    states: dict[str, dict | None],
) -> tuple[dict[str, float | None], dict[str, bool]]:
    """Per source: its residency stamp (the oldest of its tables' stamps, None when any table has
    never landed) and whether every table's last land succeeded. Pure."""
    stamps: dict[str, float | None] = {}
    oks: dict[str, bool] = {}
    for source in sources:
        tables = tables_by_source.get(source.id, [])
        refreshed: list[float] = []
        ok = True
        for table in tables:
            state = states.get(_node(table.schema_name, table.table_name))
            at = state.get("last_refresh_at") if state else None
            if at is None:
                refreshed = []
                break
            refreshed.append(float(at))
            ok = ok and bool(state.get("last_refresh_ok", True))  # type: ignore[union-attr]
        stamps[source.id] = min(refreshed) if refreshed and tables else None
        oks[source.id] = ok
    return stamps, oks


async def _node_states(db: Any, queue: Any, tables: list[Any]) -> dict[str, dict | None]:
    """The persisted freshness state of each table's node (REQ-1661)."""
    async with db.acquire() as conn:
        return {
            _node(t.schema_name, t.table_name): await queue.get_node_state(
                conn, _node(t.schema_name, t.table_name)
            )
            for t in tables
        }


def _row_cache_ttls(table: Any, source: Any, reader_role: str | None) -> tuple[int, int]:
    """(effective TTL for this reader, reap horizon) for a row_materialize table (REQ-1865,
    REQ-1907). Freshness is judged per reader against each row's landed-at stamp; the stamped
    ``_row_expires_at`` is only the reaper's horizon -- the longest TTL any reader accepts -- so a
    cold-row sweep never deletes a row a long-TTL class still serves."""
    from provisa.federation.role_ttl import declared_cache_ttl, effective_ttl, max_accepted_ttl

    if declared_cache_ttl(table, source) is None:
        raise ValueError(
            f"row-materialize table {table.table_name!r}: no resolved cache_ttl at fetch time "
            "(registration should have rejected this — REQ-1865)"
        )
    return effective_ttl(table, source, reader_role), max_accepted_ttl(table, source)


# Change signals whose rows the event loop's own change path lands as they change (REQ-929): the
# CDC consumer for a pushed change, the poll node for a probe. For such a table that change path IS
# its freshness check, and the replica it keeps is current as of the last observed change.
_PUSH_SIGNALS = frozenset({"native", "debezium", "kafka"})
_PROBE_SIGNALS = frozenset({"probe", "ttl_probe"})


def change_signal_of(table: Any, source: Any) -> str:
    """The table's change signal, inheriting its source's (REQ-929)."""
    return table.change_signal if table.change_signal is not None else source.change_signal


def freshness_verdict(
    source: Any, table: Any, stamp: float | None, ok: bool, now: float
) -> bool | None:
    """The table's freshness check (REQ-1907, amended 2026-09-30): True = fresh, False = not
    fresh, None = the table has no freshness check (it replicates on its TTL alone).

    A ``freshness_gate`` source is judged by its own read-time predicate (REQ-860). A table whose
    change signal is pushed or probed is kept current by the event loop's change path (the CDC
    consumer / the poll node), so its replica is fresh as of the last observed change: role_ttl
    limits only read-triggered refreshes and never holds back that feed. A ``ttl`` table has no
    check beyond its cache_ttl (None: the TTL alone decides). A ``ttl`` / ``ttl_probe`` table with
    no table or source cache_ttl is a configuration error and raises."""
    from provisa.federation.role_ttl import require_landing_ttl

    require_landing_ttl(table, source)
    if source.freshness_gate:
        from provisa.freshness.source_gate import gate_source, source_subject

        return gate_source(source, source_subject(stamp, ok=ok), now).is_fresh
    if change_signal_of(table, source) in _PUSH_SIGNALS | _PROBE_SIGNALS:
        return True
    return None


def is_stale_of(
    sources: list[Any],
    tables_by_source: dict[str, list[Any]],
    states: dict[str, dict | None],
    now: float,
    *,
    reader_role: str | None,
    fresh_of: Any = None,
) -> Any:
    """The staleness oracle the residency plan consults (REQ-1661, REQ-1907). A source needs a
    land when any table of it the query reads:

    - was never landed, or its last land failed (always), or
    - has a replica older than the reader's effective TTL on THAT table,
      max(cache_ttl, role_ttl(role)), AND its freshness check (``fresh_of``) does not report it
      fresh. A table with no freshness check (``fresh_of`` returns None) lands on the TTL alone.

    Both halves are a gate: a reader whose TTL has not passed never lands, whatever changed
    upstream; a replica the freshness check reports fresh is never re-landed on the clock.
    ``fresh_of`` defaults to :func:`freshness_verdict`.

    Raises ValueError when a table it would judge has change_signal ttl / ttl_probe and no table or
    source cache_ttl (REQ-1907, amended 2026-09-30): such a table has no refresh clock."""
    from provisa.federation.role_ttl import effective_ttl, require_landing_ttl

    by_id = {s.id: s for s in sources}
    for s in sources:
        for t in tables_by_source.get(s.id, []):
            require_landing_ttl(t, s)
    check = fresh_of if fresh_of is not None else freshness_verdict

    def _table_stale(source: Any, table: Any) -> bool:
        state = states.get(_node(table.schema_name, table.table_name))
        at = state.get("last_refresh_at") if state else None
        ok = bool(state.get("last_refresh_ok", True)) if state else True
        if at is None or not ok:
            return True
        if now - float(at) <= float(effective_ttl(table, source, reader_role)):
            return False
        return check(source, table, float(at), ok, now) is not True

    def is_stale(source_id: str) -> bool:
        tables = tables_by_source.get(source_id, [])
        if not tables:
            return True  # a source with no registered table has nothing resident
        return any(_table_stale(by_id[source_id], t) for t in tables)

    return is_stale


def _load_protected(source: Any, tables: list[Any]) -> bool:
    """Whether this source's read is load-protected: the source's own value, or any of the tables
    the statement reads turning it on (a table value of None inherits the source's, REQ-1141)."""
    from provisa.core.replicate import resolved_load_protected

    return bool(source.load_protected) or any(resolved_load_protected(source, t) for t in tables)


def _replicated(state: Any, tables: list[Any]) -> bool:
    """Whether the operator's settings put any of the tables the statement reads on its replica
    (REQ-826): judged per table, from the floored tables published with the replica routes
    (``replica_routing.table_floor``), the same answer routing gives for the statement."""
    floored = state.replica_routes.floored
    return any(t.id in floored for t in tables)


async def ensure_resident(
    state: Any,
    source_ids: Iterable[str],
    *,
    reader_role: str | None,
    table_ids: Iterable[int],
) -> list[tuple[str, str]]:
    """Land what a query reads and is not resident (REQ-1661). Returns the (source_id, table_name)
    pairs landed. A no-op without an engine, config or tenant store, or when nothing is stale.

    ``table_ids`` are the registered tables the STATEMENT reads (REQ-826): only those are judged
    and landed. A statement that reads one table of a source neither lands nor waits on the
    source's other tables, and is not moved onto a replica by a setting on a table it does not read.

    ``reader_role`` is the governed role the query runs as (REQ-1907): staleness is judged against
    its effective TTL per table. None is a caller with no reader (it uses each table's cache_ttl).

    A table replicated ROW BY ROW (``_row_level``: the row_materialize flag on an engine that
    cannot attach its source) is never landed here. Its rows are fetched by key —
    ``ensure_rows_resident`` for a key the statement binds, ``pushdown_row_materialize`` for a key
    a join supplies — and a statement that binds neither is refused at planning (REQ-1915,
    ``pgwire._pipeline._pk_bounds``), so no read reaches this function needing the whole table.
    The whole-table path that used to live here (load the entire source table into the worker,
    upsert it one row at a time) is gone with that rule."""
    wanted = {s for s in source_ids if s}
    engine = getattr(state, "federation_engine", None)  # the EngineRuntime (write face + engine)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    config = getattr(state, "config", None)
    db = getattr(state, "tenant_db", None)
    if not wanted or engine is None or backend is None or config is None or db is None:
        return []
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.source_vault import org_vault

    # REQ-1674: the registry, not the config file — see registry_view.
    _all_sources = await registered_sources(state)
    sources = [s for s in _all_sources if s.id in wanted]
    if not sources:
        return []
    from provisa.federation.strategy import engine_attaches

    _attached_types = {s.id: engine_attaches(engine, s.type.value) for s in sources}

    def _row_level(t: Any) -> bool:
        """Whether row_materialize APPLIES to ``t`` on this engine (REQ-1865, settled): the flag
        is set AND the engine cannot attach the table's source. An engine that reads the source
        in place ignores the flag — the table is read through the attach, never landed into a
        row cache — the same rule ``active_row_materialize_tables`` applies for every other
        row-level consumer."""
        return bool(getattr(t, "row_materialize", False)) and not _attached_types[t.source_id]

    read = frozenset(table_ids)
    floored = state.replica_routes.floored

    def _lands(t: Any) -> bool:
        """Whether this statement's read of ``t`` is served from a whole-table replica (REQ-826,
        judged per table): the operator's settings put it there, or the engine cannot read its
        source in place. A table the engine reads live beside a replica-served sibling of the same
        source is left alone — it is neither landed nor asked for a replication clock."""
        return t.id in floored or not _attached_types[t.source_id]

    tables_by_source: dict[str, list[Any]] = {}
    for t in await registered_tables(state):
        if t.source_id in wanted and t.id in read and not _row_level(t) and _lands(t):
            tables_by_source.setdefault(t.source_id, []).append(t)
    # REQ-826 / REQ-1141: a table the operator's settings put on its replica moves its source's
    # read there for this statement, even where the engine could attach the source.
    replicated_by = {s.id: _replicated(state, tables_by_source.get(s.id, [])) for s in sources}
    protected_of = {s.id: _load_protected(s, tables_by_source.get(s.id, [])) for s in sources}

    from provisa.events import queue
    from provisa.federation.role_ttl import require_landing_ttl

    landed: list[tuple[str, str]] = []
    from contextlib import AsyncExitStack

    from provisa.federation.node_freshness_view import generation_of, view_for

    view = view_for(state)
    generation = generation_of(state)
    loader: Any = None
    # What a replicated source lands into: the engine's own store, or (Trino) the store it reads
    # through its connector. The plan asks for it only for a source the operator's setting moves
    # onto its replica (REQ-846), so the store is resolved only when one of these is.
    _materialization_backend = (
        engine.engine.replica_store_backend()
        if any(replicated_by.values()) or any(protected_of.values())
        else None
    )

    def _pending(source: Any, states: dict[str, dict | None] | None, now: float) -> bool:
        """Whether a read of ``source`` must land something first, given its tables' freshness
        ``states`` — None asks the question for the worst case (everything stale, nothing
        resident), where False means this engine never lands the source at all. The same decision
        the land below acts on (``EngineBackend.pending_lands``), taken without a lock or a store
        read."""
        tables = tables_by_source.get(source.id, [])
        if states is None:
            is_stale = lambda sid: True  # noqa: E731
            stamps: dict[str, float | None] = {}
        else:
            stamps, _ = stale_sources([source], {source.id: tables}, states)
            clock_stale = is_stale_of(
                [source], {source.id: tables}, states, now, reader_role=reader_role
            )
            is_stale = lambda sid: clock_stale(sid) or backend.is_first_touch(sid)  # noqa: E731
        return bool(
            backend.pending_lands(
                [source],
                is_stale=is_stale,
                replicated_of=lambda sid: replicated_by[sid],
                load_protected_of=lambda sid: protected_of[sid],
                resident_of=(None if states is None else lambda sid: stamps.get(sid) is not None),
                materialization_backend=_materialization_backend,
                # REQ-1907: a freshness_gate source's predicate is folded into ``is_stale`` (the
                # TTL AND freshness gate); the plan must not re-decide it alone.
                freshness_subject_of=None,
                now=now,
            )
        )

    for source in sources:
        # REQ-1661 (amended 2026-10-01): the staleness decision is made in memory first. A source
        # this engine reads in place never lands, whatever its state — no lock, no control-plane
        # read. A landed source whose tables' freshness state is held in memory and says FRESH is
        # left alone the same way. Only a STALE (or unknown) answer goes on to take the land locks
        # and read the persisted state, which is the truth the land is decided on.
        if not tables_by_source.get(source.id):
            # Nothing of this source lands whole for this statement: its tables are read live
            # through the engine's attach, or are replicated row by row.
            continue
        # REQ-1907 (amended 2026-09-30, direct attach is live): a source this engine reads in place
        # has no replica — cache_ttl, role_ttl and the freshness gate do not apply to it.
        if not _pending(source, None, time.time()):
            continue
        # REQ-1907 (amended 2026-09-30): a ttl / ttl_probe table that lands with no table or source
        # cache_ttl has no refresh clock — fail the read before any lock, land or refresh stamp.
        for t in tables_by_source[source.id]:
            require_landing_ttl(t, source)
        _nodes = [_node(t.schema_name, t.table_name) for t in tables_by_source.get(source.id, [])]
        _held = view.states(generation, _nodes)
        if _held is not None and not _pending(source, _held, time.time()):
            continue
        if loader is None:
            # Built for the first source that may land — not for a read that lands nothing.
            from provisa.events.app_wiring import (
                build_adapter_loaders,
                build_keyed_adapter_loaders,
            )
            from provisa.events.source_loader import SourceRowLoader

            loader = SourceRowLoader(
                engine,
                adapter_loaders=build_adapter_loaders(state, engine),
                keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
            )
        # REQ-1695: the land dials sources — this one through its loader, and every registered
        # one if the engine's attach walk runs — so the vault of the org they are registered in
        # is bound here, where the refresh runs, not left to whichever path reached it.
        async with org_vault(state, _all_sources):
            # The same per-node locks the event loop's land takes, so the boot land and a first query
            # never interleave on one replica; every node of the source is held for the source's land.
            async with AsyncExitStack() as held:
                for t in tables_by_source.get(source.id, []):
                    await _hold_land_lock(held, _physical_node(backend, state, source, t))
                # Staleness is judged with the locks held: a request that waited here for another
                # request's land of the same table reads the stamp that land wrote and finds the table
                # fresh, so one stale table is landed once, not once per waiting reader (REQ-1882,
                # REQ-1907 single-flight).
                source_tables = {source.id: tables_by_source.get(source.id, [])}
                states = await _node_states(db, queue, source_tables[source.id])
                view.read(generation, states)
                now = time.time()
                stamps, _ = stale_sources([source], source_tables, states)
                try:
                    # REQ-1907: the whole replication gate -- the reader's effective TTL AND the
                    # table's freshness check (a freshness_gate source's own predicate included) --
                    # is decided here, so the plan below consults only this oracle.
                    clock_stale = is_stale_of(
                        [source], source_tables, states, now, reader_role=reader_role
                    )
                    # REQ-1730: OR in this backend INSTANCE's own first-touch signal — see
                    # EngineBackend._landed_this_process's own doc for why the persisted, per-NODE
                    # freshness clock alone under-reports staleness for an engine with no live reach for
                    # this source type (a genuine reboot onto an engine that has never held this row
                    # reads as "fresh" purely because a DIFFERENT engine landed it recently).
                    is_stale = lambda sid: clock_stale(sid) or backend.is_first_touch(sid)  # noqa: E731
                    landed += await backend.materialize_pending(
                        state,
                        loader=loader,
                        source_ids={source.id},
                        is_stale=is_stale,
                        replicated_of=lambda sid: replicated_by[sid],
                        load_protected_of=lambda sid: protected_of[sid],
                        resident_of=lambda sid: stamps.get(sid) is not None,
                        # the engine's own store is what a replicated source's replica is written into
                        materialization_backend=_materialization_backend,
                        # REQ-1907: folded into ``is_stale`` above; not re-decided by the plan.
                        freshness_subject_of=None,
                        now=now,
                        coordination=_BuildCoordination(db, queue, view, generation),
                    )
                    backend.mark_landed(source.id)
                except ReplicaBuilding:
                    # Not a failed land: the build is still running and will stamp the node itself.
                    # Stamping it failed here would make the next read start over.
                    raise
                except Exception:  # noqa: BLE001 - the adapter's error type is its own; re-raised
                    # REQ-1661 (amended 2026-09-30): a failed land fails the query -- it never reads
                    # the stale replica. Stamp the nodes not ok first, so the next query retries.
                    await _record_refresh(
                        db,
                        queue,
                        [(source.id, t) for t in tables_by_source.get(source.id, [])],
                        ok=False,
                        seen=(view, generation),
                    )
                    raise
                await _record_refresh(
                    db,
                    queue,
                    [
                        (sid, t)
                        for sid, name in landed
                        if sid == source.id
                        for t in tables_by_source[sid]
                        if t.table_name == name
                    ],
                    ok=True,
                    seen=(view, generation),
                )
    if landed:
        log.info("query residency: landed %s before the read", landed)
    return landed


class _BuildCoordination:
    """What a replica build that outlives its request (``federation.replica_build``) needs from
    the freshness state: whether another worker built the replica while this one waited for the
    store's lock, and the stamp it writes itself when it finishes."""

    def __init__(self, db: Any, queue: Any, view: Any, generation: Any) -> None:
        self._db = db
        self._queue = queue
        self._seen = (view, generation)

    async def built_since(self, source: Any, table: Any, since: float) -> bool:
        del source
        async with self._db.acquire() as conn:
            state = await self._queue.get_node_state(
                conn, _node(table.schema_name, table.table_name)
            )
        if state is None or not state.get("last_refresh_ok", True):
            return False
        at = state.get("last_refresh_at")
        return at is not None and float(at) >= since

    async def built(self, source: Any, table: Any) -> None:
        await _record_refresh(self._db, self._queue, [(source.id, table)], ok=True, seen=self._seen)


async def _hold_land_lock(held: Any, node: str) -> None:
    """Take ``node``'s land lock for the caller's exit stack, waiting no longer than the request's
    remaining budget. The wait is for another request's (or the event loop's) land of this table."""
    from provisa.core import request_deadline
    from provisa.events.land_lock import land_lock

    budget = request_deadline.remaining()
    try:
        await asyncio.wait_for(held.enter_async_context(land_lock(node)), budget)
    except TimeoutError as exc:
        raise TimeoutError(
            f"{node}: another land of this table was still running when this request's "
            f"budget ran out"
        ) from exc


async def _record_refresh(
    db: Any, queue: Any, tables: list[tuple[str, Any]], *, ok: bool, seen: tuple[Any, Any]
) -> None:
    """Stamp each (source_id, table) node's refresh outcome in the freshness state the event loop
    reads (REQ-1661), and in this process's in-memory view of it (``seen``: the view and the
    generation it is keyed by) — so the next read decides on the outcome just written."""
    if not tables:
        return
    at = datetime.now(UTC)
    nodes = [_node(table.schema_name, table.table_name) for _sid, table in tables]
    async with db.acquire() as conn:
        for node in nodes:
            await queue.record_refresh(conn, node, at=at, ok=ok)
    view, generation = seen
    view.stamped(generation, nodes, at=at.timestamp(), ok=ok)


async def active_row_materialize_tables(state: Any) -> list[Any]:
    """The registered tables row_materialize APPLIES to (REQ-1865, amended 2026-09-30): the flag is
    set AND the bound engine declares it cannot direct-attach the table's source type.

    row_materialize is the reach for a source the engine cannot attach. When the engine DECLARES it
    can attach the source (``strategy.engine_attaches`` -- its connector reads in place), the engine
    attaches and reads the source live; the flag is ignored, by design, not as a fallback. A failed
    attach is then an error, never a detour through the row cache. Every row-materialize consumer
    (bound extraction, key pushdown, row fetch, background refresh/reap wiring) selects its tables
    here, so a declared-attach engine never pays a probe, a keyed fetch or a cache land it would
    not read."""
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.strategy import engine_attaches

    flagged = [t for t in await registered_tables(state) if getattr(t, "row_materialize", False)]
    if not flagged:
        return []
    engine = getattr(state, "federation_engine", None)
    # tables.source_id is a NOT NULL foreign key to sources.id (core/schema_org.py), so a missing
    # source is a KeyError, not a skip.
    sources_by_id = {s.id: s for s in await registered_sources(state)}
    return [
        t for t in flagged if not engine_attaches(engine, sources_by_id[t.source_id].type.value)
    ]


async def row_materialized_tables_by_name(state: Any) -> dict[str, Any]:
    """table reference name -> registered Table, restricted to ``row_materialize=True`` tables
    (REQ-1865). Used by ``pk_bounds.extract_pk_bounds`` to know which tables in a statement's AST
    are even eligible for the row cache — the registry, not the config file, same posture as every
    other residency lookup in this module (REQ-1674).

    Keyed by BOTH the bare physical ``table_name`` and the semantic alias
    (``apply_sql_name(t.alias)``, the same authority ``compiler.sql_rewrite.semantic_table_name``
    uses to build ``display_name``), when an alias is registered -- a table's own AST reference can
    appear under either spelling depending on the surface: a GraphQL-compiled query's domain.
    field_name resolution rewrites it to the alias, but a plain raw-SQL statement (pgwire, Flight)
    references it by its own bare physical name verbatim, never rewritten. Keying by only one form
    silently matched nothing for a statement using the other -- extract_pk_bounds never errors on a
    miss (by design), so this was never a crash, just a permanently-empty result: confirmed live,
    cypher_cross_engine's own literal customer_id predicate against bench_customer_node (aliased
    "Customer") resolved zero bounds under alias-only keying, since its raw SQL text names the
    table bench_customer_node directly."""
    from provisa.compiler.naming import apply_sql_name

    # REQ-1865 (amended): a plain raw-SQL statement (pgwire, Flight) references a table by its own
    # bare physical name (e.g. "bench_customer_node") -- it is never rewritten to the table's
    # semantic alias the way a GraphQL-compiled query's domain.field_name resolution is. Keying by
    # alias ONLY silently matched nothing for every such statement -- confirmed live: pk_bounds
    # came back empty for cypher_cross_engine's own literal customer_id predicate against
    # bench_customer_node, which does have an alias ("Customer"). Key by BOTH the alias (when set,
    # for the compiled/GraphQL path) and the bare table_name (for a raw-SQL statement) so
    # extract_pk_bounds matches whichever form the statement's own AST actually uses.
    out: dict[str, Any] = {}
    for t in await active_row_materialize_tables(state):
        out[apply_sql_name(t.table_name)] = t
        if t.alias:
            out[apply_sql_name(t.alias)] = t
    return out


async def _read_cached(
    conn: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> dict[tuple[Any, ...], datetime]:
    """key -> landed-at (``_row_cached_at``) for every key of ``keys`` currently present in the row
    cache -- the OLDEST stamp when a key column repeats, so a key is fresh only while every cached
    row of it is (REQ-1907). A key absent from the result is simply not cached yet (never an
    error)."""
    from sqlalchemy import select, tuple_

    if not keys:
        return {}
    pk_cols = [table.c[c] for c in pk_columns]
    cond = pk_cols[0].in_([k[0] for k in keys]) if len(pk_cols) == 1 else tuple_(*pk_cols).in_(keys)
    stmt = select(*pk_cols, table.c["_row_cached_at"]).where(cond)
    result = await conn.execute_core(stmt)
    out: dict[tuple[Any, ...], datetime] = {}
    for row in result.fetchall():
        cached_at = row[len(pk_columns)]
        # SQLite (a supported store dialect) has no true timezone-aware column type -- a
        # DateTime(timezone=True) round-trips as a naive value there. Every _row_cached_at this
        # module ever writes is UTC (land_rows stamps datetime.now(UTC)), so a naive value read
        # back is always UTC too; normalize it before comparing against an aware `now`.
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=UTC)
        key = tuple(row[: len(pk_columns)])
        out[key] = min(out[key], cached_at) if key in out else cached_at
    return out


def _row_stale(cached_at: datetime, now: datetime, reader_ttl: int, change_fed: bool) -> bool:
    """A cached row is re-fetched for a reader only when its landed-at stamp is older than the
    reader's effective TTL AND its freshness check does not report it fresh (REQ-1907). A row of a
    pushed-change table is kept current by the CDC background refresh (REQ-1865 section 5), which
    is its freshness check; any other row has none and is judged on the TTL alone."""
    return (now - cached_at).total_seconds() > reader_ttl and not change_fed


async def _tombstone_keys(
    conn: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> None:
    from sqlalchemy import tuple_

    if not keys:
        return
    pk_cols = [table.c[c] for c in pk_columns]
    cond = pk_cols[0].in_([k[0] for k in keys]) if len(pk_cols) == 1 else tuple_(*pk_cols).in_(keys)
    await conn.execute_core(table.delete().where(cond))


def _join_key_column(join: Any, target_alias: str) -> tuple[str, Any] | None:
    """For a ``JOIN ... ON target.col = other_expr`` (or reversed) equality, return
    ``(target_col_name, other_side_expr)`` -- ``None`` for anything else (composite ON, a
    non-equality, an OR, a literal on either side): never guessed. Such a join does not bind the
    table (REQ-1915): planning refuses the statement unless a key predicate binds it."""
    import sqlglot.expressions as exp

    on = join.args.get("on")
    if not isinstance(on, exp.EQ):
        return None
    left, right = on.left, on.right
    if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
        return None
    if left.table == target_alias and right.table != target_alias:
        return left.name, right
    if right.table == target_alias and left.table != target_alias:
        return right.name, left
    return None


def _pushdown_alias(table: Any) -> str:
    """The probe's output column carrying a row-level table's join keys: named for the table's
    registered name, a plain identifier whatever its replica is called."""
    return f"__pushdown_{table.table_name}"


def resolve_landing_args_for(source: Any, table: Any, dialect: str | None) -> Any:
    from provisa.federation.residency import resolve_landing_args

    return resolve_landing_args(source, table, platform=dialect)


async def pushdown_row_materialize(
    state: Any,
    physical_sql: str,
    dialect: str,
    params: list[Any] | None = None,
    *,
    reader_role: str | None,
) -> set[str]:
    """REQ-1865 key pushdown: land exactly the rows a JOIN-reached row_materialize table needs,
    without a full-table land, and without hand-walking the join graph symbolically.

    Mechanism (the ONLY AST change): flip each JOIN-reached row_materialize table's join to LEFT
    OUTER and run the query, AS WRITTEN, through the engine once. Every already-resident table's
    real rows come back unchanged (LEFT preserves the row regardless of a match); the target
    table's own join column, read off the OTHER (real) side of the row, IS the actual key set --
    no rebuilt/reduced probe query, no symbolic propagation. Keyed-fetch/land those keys, then the
    caller's already-scheduled REAL (unmodified, all-INNER) query runs normally afterward and
    finds the rows it needs.

    Iterates (bounded by the number of pending tables) because a later hop's connecting column may
    itself live on a table only resolved by an earlier pushdown pass in THIS SAME call (e.g.
    bench_product_node's join key lives on bench_contains_edge, which is itself unresolved on the
    first pass) -- each pass reverts any table landed in a prior pass back to its original join
    kind, so a real (not NULL) value flows through for the next pass's still-pending tables.

    A candidate key already fresh in the row cache (filtered by target_col, not necessarily the
    table's real PK -- the cache carries every landed column) is dropped before the fetch, mirroring
    ``ensure_rows_resident``'s own stale_or_missing check: a repeat query with the same key set does
    zero live-source work, and a query mixing fresh and new/stale keys fetches+lands only the
    reduced subset -- never the coarse "is ANY row in this table fresh" table-level skip an earlier
    version of this mechanism used, which could miss a genuinely new key while an unrelated row was
    still warm.

    Never guesses: a table whose join key can't be read from a single column-to-column ON
    equality (``_join_key_column`` returns None) is left off this pass. REQ-1915: planning
    (``pgwire._pipeline._pk_bounds_inputs``) counts a table as bound by pushdown only under the
    conditions this function acts on, and refuses a statement that binds it no other way, so a
    table left off here is one a key predicate bound and ``ensure_rows_resident`` fetched.

    ``params`` is the statement's own bind-parameter values, in bind order -- REQUIRED whenever
    ``physical_sql`` still carries a literal ``$N`` placeholder (Bolt/Cypher-transport: the
    predicate value is never inlined, unlike the SQL-transport's own literal-inlined text). The
    probe re-executes ``physical_sql`` (with one join flipped) AS WRITTEN, placeholder included --
    without binding it, an unbound ``$N`` compares as NULL and the WHERE clause never matches,
    silently returning zero rows for every statement using a bound parameter. Confirmed live: a
    Bolt query for a customer with real, confirmed data (12 rows via the SQL transport's literal-
    inlined equivalent) returned zero rows over Bolt until this was threaded through."""
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.events.app_wiring import (
        build_adapter_loaders,
        build_keyed_adapter_loaders,
        build_keyed_arrow_loaders,
    )
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.registry_view import registered_sources

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return set()

    row_level = await active_row_materialize_tables(state)
    if not row_level:
        return set()
    # REQ-1912: ``physical_sql`` addresses a row-level table at its replica, so a join is matched
    # to its table by the replica's name.
    tables_by_name = {
        backend.replica_address(
            state, source_id=t.source_id, schema_name=t.schema_name, table_name=t.table_name
        ).table: t
        for t in row_level
    }
    sources_by_id = {s.id: s for s in await registered_sources(state)}

    try:
        tree = sqlglot.parse_one(physical_sql, read=dialect)
    except Exception:
        return set()
    if not isinstance(tree, exp.Select):
        return set()

    all_joins = {j.this.name: j for j in tree.find_all(exp.Join) if j.this.name in tables_by_name}
    if not all_joins:
        return set()

    landed_this_call: set[str] = set()
    remaining = set(all_joins)
    loader = SourceRowLoader(
        engine,
        adapter_loaders=build_adapter_loaders(state, engine),
        keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
        keyed_arrow_loaders=build_keyed_arrow_loaders(engine),
    )

    for _pass in range(len(all_joins)):
        still_pending = {name for name in remaining if name not in landed_this_call}
        if not still_pending:
            break

        pass_tree = tree.copy()
        joins_by_name = {j.this.name: j for j in pass_tree.find_all(exp.Join)}
        key_cols: dict[str, tuple[str, Any]] = {}
        skip: set[str] = set()
        for name in still_pending:
            # _join_key_column matches against the join's OWN alias (e.g. "ce"), not the bare
            # table name ("bench_contains_edge") -- an aliased join's ON-condition columns are
            # always alias-qualified, never re-qualified back to the physical table name.
            kc = _join_key_column(joins_by_name[name], joins_by_name[name].this.alias_or_name)
            if kc is None:
                skip.add(name)  # never guessed -- bound by a key predicate (REQ-1915)
                continue
            key_cols[name] = kc
            joins_by_name[name].set("kind", "LEFT")
        for name in skip:
            still_pending.discard(name)
        if not still_pending:
            break

        for name, (_target_col, other_expr) in key_cols.items():
            pass_tree.select(
                exp.alias_(other_expr.copy(), _pushdown_alias(tables_by_name[name])),
                append=True,
                copy=False,
            )

        # A failed probe propagates, same as a failed keyed fetch below: breaking out left every
        # pending table unlanded with no error surfaced.
        result = await engine.execute_engine(pass_tree.sql(dialect=dialect), params)

        made_progress = False
        for name in list(still_pending):
            target_col, _ = key_cols[name]
            alias = _pushdown_alias(tables_by_name[name])
            if alias not in result.column_names:
                continue
            idx = result.column_names.index(alias)
            values = sorted({row[idx] for row in result.rows if row[idx] is not None})
            if not values:
                continue
            table = tables_by_name[name]
            source = sources_by_id.get(table.source_id)
            if source is None:
                continue
            pk_columns = [c.name for c in table.columns if c.is_primary_key]
            if len(pk_columns) != 1:
                continue
            real_pk = pk_columns[0]
            args = resolve_landing_args_for(source, table, backend.dialect)
            reader_ttl, reap_horizon = _row_cache_ttls(table, source, reader_role)
            change_fed = change_signal_of(table, source) in _PUSH_SIGNALS
            address = backend.replica_address(
                state,
                source_id=source.id,
                schema_name=table.schema_name,
                table_name=table.table_name,
            )
            schema, cache_name = address.schema, address.table
            # REQ-1865 (amended): a coarse "does this table hold ANY fresh row" gate used to
            # decide whether to probe/fetch AT ALL -- correct only for a repeat query using the
            # exact same key set, and wrong for a query needing keys the cache doesn't have yet
            # while some UNRELATED row is still fresh (that other-key case never even ran the
            # probe to discover them). Replaced with the same per-key stale_or_missing check
            # ensure_rows_resident already does for a literal PK bound -- filter target_col's
            # own candidate values against the row cache (it carries every landed column,
            # target_col included, not just real_pk) BEFORE fetching, so a query whose keys are
            # already fresh does zero live-source work, and a query with a MIX of fresh and new/
            # stale keys fetches+lands only the reduced subset, never the full candidate set.
            cache_table, cached = await _ensure_and_read_row_cache(
                engine,
                backend,
                state,
                schema,
                cache_name,
                args.columns,
                [target_col],
                [(v,) for v in values],
            )
            now = datetime.now(UTC)
            stale_or_missing = [
                v
                for v in values
                if (v,) not in cached or _row_stale(cached[(v,)], now, reader_ttl, change_fed)
            ]
            if not stale_or_missing:
                landed_this_call.add(name)
                made_progress = True
                continue
            # A failed keyed fetch propagates: swallowing it left the table unlanded and the
            # query answered from whatever the row cache already held -- confirmed live,
            # large_federated_join returned 3030 rows instead of ~3.03M with no error.
            fetched = await loader.load_keys_arrow(
                source, table, [target_col], [(v,) for v in stale_or_missing]
            )
            if fetched.num_rows == 0:
                landed_this_call.add(name)
                made_progress = True
                continue
            await _land_row_cache_arrow(
                engine,
                backend,
                state,
                schema,
                cache_name,
                cache_table,
                [real_pk],
                args.columns,
                fetched,
                reap_horizon,
            )
            landed_this_call.add(name)
            made_progress = True

        if not made_progress:
            break

    return landed_this_call


def _is_duckdb_store(backend: Any, state: Any) -> bool:
    """True when the replica STORE is an embedded DuckDB file, reached through the store broker.
    The engine's dialect does not say so: a DuckDB engine may keep its replicas in Postgres, and
    then the row cache goes through the store write face like any other engine's."""
    if getattr(backend, "dialect", None) != "duckdb":
        return False
    return _duckdb_runtime(backend, state)._store_is_duckdb()


def _duckdb_runtime(backend: Any, state: Any) -> Any:
    """The live ``DuckDBFederationRuntime`` behind ``backend`` (a ``NativeEngineBackend``) -- the
    object that actually holds the engine's single DuckDB file connection (REQ-989)."""
    return backend._runtime_for(state)


async def _ensure_row_cache_table(
    engine: Any, backend: Any, state: Any, schema: str, name: str, columns: list[tuple[str, str]]
) -> Any:
    """CREATE SCHEMA/TABLE IF NOT EXISTS for a row-materialize cache table, dispatching to DuckDB's
    single-writer native connection (REQ-989) when the store is DuckDB -- ``store_writer.
    store_connection``'s async SQLAlchemy path has no async DuckDB driver at all (confirmed live:
    crashed every row-materialize write on this engine). Returns the SQLAlchemy ``Table`` object for
    the generic (non-DuckDB) path's later ``land_rows``/``_read_cached`` calls, or ``None`` for the
    DuckDB path (those calls are dispatched separately, see ``_read_row_cache``/``_land_row_cache``/
    ``_tombstone_row_cache`` below)."""
    from provisa.federation.materialize_exec import (
        _ROW_CACHED_AT,
        _ROW_EXPIRES_AT,
        build_row_cache_table,
    )

    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        full_columns = list(columns) + [
            (_ROW_CACHED_AT, "timestamp"),
            (_ROW_EXPIRES_AT, "timestamp"),
        ]
        runtime._store_broker.ensure_row_cache_table(schema, name, full_columns)
        return None

    from provisa.core.database import create_engine_from_url
    from provisa.core.db import add_missing_columns
    from provisa.federation import store_writer
    from sqlalchemy.schema import CreateSchema

    cache_table = build_row_cache_table(schema, name, columns, (), dialect_name=backend.dialect)
    dsn = engine.engine.materialize_store()
    from provisa.federation.replica_guard import require_store_replica_table

    async with store_writer.store_connection(dsn) as conn:
        if schema and conn.capabilities.schemas:
            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
        await require_store_replica_table(conn, schema, name, action="write the row-level replica")
    # Additive reconcile (REQ-828 pattern, same as add_missing_columns's other callers): a cache
    # table already landed at this name by an OLDER row-materialize schema (missing the cache's
    # own _row_cached_at/_row_expires_at bookkeeping columns) never gets those columns from
    # CREATE TABLE IF NOT EXISTS alone -- confirmed live (UndefinedColumnError on _row_expires_at
    # against a table created before that column existed). ``add_missing_columns`` also creates
    # the table outright when absent, so this replaces the old CreateTable-only step entirely.
    reconcile_engine = create_engine_from_url(dsn, pool_size=1)
    try:
        with reconcile_engine.begin() as raw_conn:
            add_missing_columns(raw_conn, [cache_table], schema)
    finally:
        reconcile_engine.dispose()
    return cache_table


async def _ensure_and_read_row_cache(
    engine: Any,
    backend: Any,
    state: Any,
    schema: str,
    name: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> tuple[Any, dict[tuple[Any, ...], Any]]:
    """Ensure the cache table's columns and read it back. On DuckDB, does both under ONE lock
    hold (REQ-1901, ``_SyncedStore.ensure_and_read_row_cache``) to close the multi-worker race
    window a separate ensure-then-read leaves open: another worker's ordinary whole-table
    materialize land can recreate this same table (without the cache's bookkeeping columns) in
    the gap between the two calls, so the read that follows hits a table the ensure step just
    fixed and now finds broken again -- confirmed live under a real 8-worker benchmark run. The
    generic (non-DuckDB) path has no single-writer lock to hold across two round trips, so it
    simply sequences the existing two steps."""
    if _is_duckdb_store(backend, state):
        from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT

        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        full_columns = list(columns) + [
            (_ROW_CACHED_AT, "timestamp"),
            (_ROW_EXPIRES_AT, "timestamp"),
        ]
        cached = runtime._store_broker.ensure_and_read_row_cache(
            schema, name, full_columns, pk_columns, keys
        )
        return None, cached
    cache_table = await _ensure_row_cache_table(engine, backend, state, schema, name, columns)
    cached = await _read_row_cache(
        engine, backend, state, schema, name, cache_table, pk_columns, keys
    )
    return cache_table, cached


async def _read_row_cache(
    engine: Any,
    backend: Any,
    state: Any,
    schema: str,
    name: str,
    cache_table: Any,
    pk_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> dict[tuple[Any, ...], Any]:
    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        return runtime._store_broker.read_row_cache(schema, name, pk_columns, keys)
    from provisa.federation import store_writer

    dsn = engine.engine.materialize_store()
    async with store_writer.store_connection(dsn) as conn:
        return await _read_cached(conn, cache_table, pk_columns, keys)


async def _land_row_cache_arrow(
    engine: Any,
    backend: Any,
    state: Any,
    schema: str,
    name: str,
    cache_table: Any,
    pk_columns: list[str],
    columns: list[tuple[str, str]],
    data: Any,
    reap_horizon: int,
) -> None:
    """``_land_row_cache`` for an Arrow fetch: the same row stamps (``_row_cached_at`` now,
    ``_row_expires_at`` now + ``reap_horizon``, the longest TTL any reader accepts, REQ-1907),
    added as Arrow columns, and on a DuckDB store the same upsert
    by ``pk_columns`` landed columnar through the broker -- never per-row events (REQ-1865; ~3M
    keyed rows spent ~45s as Python rows live). A declared column the fetch did not return lands
    NULL, as ``_land_row_cache``'s ``row.get`` does. Stamps are naive UTC: the DuckDB store's
    TIMESTAMP carries no zone and every reader treats it as UTC."""
    import pyarrow as pa

    from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT

    now = datetime.now(UTC)
    n = data.num_rows
    arrays = [data.column(c) if c in data.column_names else pa.nulls(n) for c, _ in columns]
    stamp = pa.timestamp("us")
    arrays += [
        pa.array([now.replace(tzinfo=None)] * n, stamp),
        pa.array([(now + timedelta(seconds=reap_horizon)).replace(tzinfo=None)] * n, stamp),
    ]
    full_columns = list(columns) + [(_ROW_CACHED_AT, "timestamp"), (_ROW_EXPIRES_AT, "timestamp")]
    stamped = pa.Table.from_arrays(arrays, names=[c for c, _ in full_columns])
    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        runtime._store_broker.upsert_arrow(schema, name, full_columns, pk_columns, stamped)
        return
    await _land_row_cache(
        engine,
        backend,
        state,
        schema,
        name,
        cache_table,
        pk_columns,
        columns,
        data.to_pylist(),
        reap_horizon,
    )


async def _land_row_cache(
    engine: Any,
    backend: Any,
    state: Any,
    schema: str,
    name: str,
    cache_table: Any,
    pk_columns: list[str],
    columns: list[tuple[str, str]],
    rows: list[dict],
    reap_horizon: int,
) -> None:
    """Upsert fetched rows, stamped landed-at now and ``_row_expires_at`` now + ``reap_horizon``
    (the reaper's horizon; freshness is judged per reader off ``_row_cached_at``, REQ-1907)."""
    if not rows:
        return
    if _is_duckdb_store(backend, state):
        from provisa.federation.materialize_exec import (
            _ROW_CACHED_AT,
            _ROW_EXPIRES_AT,
            _UpsertEvent,
        )

        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=reap_horizon)
        stamped = [{**r, _ROW_CACHED_AT: now, _ROW_EXPIRES_AT: expires_at} for r in rows]
        full_columns = list(columns) + [
            (_ROW_CACHED_AT, "timestamp"),
            (_ROW_EXPIRES_AT, "timestamp"),
        ]
        runtime._store_broker.apply_cdc(
            schema, name, full_columns, pk_columns, [_UpsertEvent(r) for r in stamped]
        )
        return
    from provisa.federation import store_writer
    from provisa.federation.materialize_exec import land_rows

    dsn = engine.engine.materialize_store()
    from provisa.federation.replica_guard import require_store_replica_table

    async with store_writer.store_connection(dsn) as conn:
        await require_store_replica_table(conn, schema, name, action="write the row-level replica")
        await land_rows(conn, cache_table, pk_columns, rows, resolved_cache_ttl=reap_horizon)


async def _tombstone_row_cache(
    engine: Any,
    backend: Any,
    state: Any,
    schema: str,
    name: str,
    cache_table: Any,
    pk_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> None:
    if not keys:
        return
    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        runtime._store_broker.tombstone_row_cache(schema, name, pk_columns, keys)
        return
    from provisa.federation import store_writer

    dsn = engine.engine.materialize_store()
    from provisa.federation.replica_guard import require_store_replica_table

    async with store_writer.store_connection(dsn) as conn:
        await require_store_replica_table(conn, schema, name, action="write the row-level replica")
        await _tombstone_keys(conn, cache_table, pk_columns, keys)


async def ensure_rows_resident(
    state: Any, pk_bounds: Iterable[Any], *, reader_role: str | None, force: bool = False
) -> list[tuple[str, str, int]]:
    """Serve exactly the rows ``pk_bounds`` names from the row cache, fetching from source only the
    missing/stale ones (REQ-1865). Returns (source_id, table_name, n_rows_fetched) per bound
    touched. A no-op for a bound with no values (``extract_pk_bounds`` already filters those out) or
    when the named table is not ``row_materialize`` (defensive: a stale/mismatched bound is just
    skipped, since the compiler is the sole authority on which tables qualify). ``force=True`` (used
    only by the CDC background-refresh caller, section 5/6b) treats every ALREADY-CACHED key in the
    bound as stale regardless of its age, without ever adding a key that isn't already cached --
    the one behavioral difference between a query-driven call and a CDC-driven one.

    A cached row is fresh for ``reader_role`` while its landed-at stamp (``_row_cached_at``) is
    within the reader's effective TTL on the table, max(cache_ttl, role_ttl(role)) (REQ-1907)."""
    from contextlib import AsyncExitStack

    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.row_lock import row_lock
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.residency import resolve_landing_args

    bounds = [b for b in pk_bounds if b.values]
    if not bounds:
        return []
    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return []

    sources_by_id = {s.id: s for s in await registered_sources(state)}
    tables_by_name = {t.table_name: t for t in await registered_tables(state)}
    loader = SourceRowLoader(
        engine,
        adapter_loaders=build_adapter_loaders(state, engine),
        keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
    )

    now = datetime.now(UTC)
    results: list[tuple[str, str, int]] = []

    for bound in bounds:
        table = tables_by_name.get(bound.table_name)
        if table is None or not getattr(table, "row_materialize", False):
            continue
        source = sources_by_id.get(bound.source_id)
        if source is None:
            continue

        args = resolve_landing_args(source, table, platform=backend.dialect)
        reader_ttl, reap_horizon = _row_cache_ttls(table, source, reader_role)
        change_fed = change_signal_of(table, source) in _PUSH_SIGNALS

        address = backend.replica_address(
            state,
            source_id=source.id,
            schema_name=table.schema_name,
            table_name=table.table_name,
        )
        schema, name = address.schema, address.table
        node = _node(schema, name)
        pk_columns = list(bound.pk_columns)
        cache_table, cached = await _ensure_and_read_row_cache(
            engine, backend, state, schema, name, args.columns, pk_columns, list(bound.values)
        )

        stale_or_missing = [
            key
            for key in bound.values
            if key not in cached or force or _row_stale(cached[key], now, reader_ttl, change_fed)
        ]
        if not stale_or_missing:
            results.append((source.id, table.table_name, 0))
            continue

        async with AsyncExitStack() as held:
            # Deterministic order, not iteration order: two concurrent calls needing an
            # overlapping multi-key set (e.g. the same IN-list, or two overlapping IN-lists) must
            # acquire their shared keys in the SAME global order, or they can circular-wait on
            # each other (A holds key1 waiting on key2; B holds key2 waiting on key1) -- a real
            # deadlock, not just contention. Sorting by the tuple's own natural order is stable
            # and cheap; every PK-tuple element here already came from a single column's own
            # comparable literal type (int/str/etc, per pk_bounds._literal_value), so a stray
            # cross-type comparison never arises within one column's values.
            for key in sorted(stale_or_missing):
                await held.enter_async_context(row_lock(node, key))

            # Re-check after acquiring: a concurrent fetch for the same key(s) may have already
            # refreshed them while this call waited on the lock (section 4's re-check rule).
            recheck = await _read_row_cache(
                engine, backend, state, schema, name, cache_table, pk_columns, stale_or_missing
            )
            still_needed = [
                k
                for k in stale_or_missing
                if force or k not in recheck or _row_stale(recheck[k], now, reader_ttl, change_fed)
            ]
            if not still_needed:
                results.append((source.id, table.table_name, 0))
                continue

            fetched = await loader.load_keys(source, table, pk_columns, still_needed)
            fetched_keys = {tuple(row.get(pk) for pk in pk_columns) for row in fetched}
            # Tombstone: a requested key the source returned no row for (section 6a) -- deleted
            # synchronously, inline, here, never deferred.
            tombstoned = [k for k in still_needed if k not in fetched_keys]

            await _land_row_cache(
                engine,
                backend,
                state,
                schema,
                name,
                cache_table,
                pk_columns,
                args.columns,
                fetched,
                reap_horizon,
            )
            await _tombstone_row_cache(
                engine, backend, state, schema, name, cache_table, pk_columns, tombstoned
            )

            results.append((source.id, table.table_name, len(fetched)))

    return results


async def prepare_engine_residency(state: Any, plan: Any) -> None:
    """Land ENGINE-route residency in ONE coroutine (REQ-1887).

    ``ensure_rows_resident`` must run before ``pushdown_row_materialize`` (REQ-1865: the
    key-pushdown probe needs directly-bound row_materialize tables populated first), and
    ``pushdown_row_materialize``'s result feeds ``ensure_resident`` — a real data dependency
    chain, but not a cross-thread one: all three already run on the main loop, so folding them
    here cuts three ``run_coroutine_threadsafe``/``_run_on_loop`` dispatches to one without
    changing order or arguments.

    Shared by Flight SQL (``provisa/api/flight/server.py``) and pgwire
    (``provisa/pgwire/server.py``) — both dispatch ``plan``/``governed`` objects of the same
    ``provisa.pgwire._pipeline._Plan`` type across their own worker-thread bridge. Living here,
    in the lower-layer module both already import ``ensure_resident``/``ensure_rows_resident``/
    ``pushdown_row_materialize`` from, avoids a Flight<->pgwire cross-import (``provisa.api.flight``
    already imports ``provisa.pgwire._pipeline``; a pgwire import of ``provisa.api.flight.server``
    would create a mutual package dependency import-linter has no contract for today but the
    layering does not want)."""
    await ensure_rows_resident(state, plan.pk_bounds, reader_role=plan.role_id)
    await pushdown_row_materialize(
        state,
        plan.physical_sql,
        state.federation_engine.dialect,
        plan.exec_params,
        reader_role=plan.role_id,
    )
    await ensure_resident(state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids)
