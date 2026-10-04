# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3f9c25-1d6e-4b80-9e4c-2f8b5d17a6c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query path's residency prep (REQ-1661, REQ-1915): a table a query reads from a
whole-table replica has that replica built and fresh before the read.

Replicas are built by the build runner (``replica_builds``), which every node that does
background work runs; a build is requested when the model declares a replica. This module is
the read's backstop for the query that arrives before a build has finished or after a replica
has gone stale for its reader: before the execute terminal runs a plan, each table the
statement reads from a replica is checked against its record in the state store
(``replica_state``) and, when stale, its build is requested and the read waits on the record.
A read never copies a table itself.

Stale means: the table has no completed build, its last build failed, or its replica has
outrun THE READER's effective TTL on that table, max(cache_ttl, role_ttl(role)) (REQ-1907) —
each table against its own TTL, so readers with a larger tolerance serve the existing replica
and never ask for a build. A source with ``freshness_gate`` set is judged by its own predicate
(REQ-860). A ``load_protected`` table is built on a read only when it has never been built
(REQ-1141: the runner is its sole refresher).

Concurrent stale reads of one table share one build: the request is one conditional write in
the state store, and every reader waits on the same record. The wait is an await inside the
request's own coroutine, bounded by the request's deadline (``ReplicaBuilding``).

A build that fails is recorded on the replica and fails the query with the build's own cause;
the query never reads what the failed build left (REQ-1661, amended 2026-09-30). A read does
not ask again until ``replication.retry_interval`` has passed.
"""

# Requirements: REQ-1661, REQ-860, REQ-855, REQ-1141, REQ-1907, REQ-1915

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from provisa.federation.execution_auth import system_auth

if TYPE_CHECKING:
    from provisa.federation.replica_state import ReplicaKey


log = logging.getLogger(__name__)


def _node(table: Any) -> str:
    """The event-graph node of a registered table (``provisa.events.nodes.source_node``) — the key
    its freshness state is held under."""
    from provisa.events.nodes import source_node

    return source_node(table.source_id, table.schema_name, table.table_name)


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
        state = states.get(_node(table))
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


@dataclass(frozen=True)
class Residency:
    """What :func:`ensure_resident` did for one statement.

    ``built``: the (source_id, table_name) pairs it waited for a build of. ``replicas_read``:
    every replica the statement reads — found fresh or waited for — with the completion time
    (UTC) of the build its read is answered from; a table read live is not in it. The audit
    record's data age (REQ-1915) is read from it."""

    built: list[tuple[str, str]] = field(default_factory=list)
    replicas_read: dict[ReplicaKey, datetime] = field(default_factory=dict)


class ReplicaAgeUnknown(RuntimeError):
    """A statement reads a replica whose build has no completion time in this store. The read
    is not answered: the audit record would otherwise claim live data for a replica read."""

    def __init__(self, replica: str) -> None:
        self.replica = replica
        super().__init__(
            f"the replica of {replica} is read but its build has no completion time in this "
            "engine's store"
        )


async def require_home_replica(state: Any, table: Any, home: str) -> None:
    """Refuse a read of ``table``, kept in region ``home``, unless its replica there is built
    (REQ-1922): such a table is read only from that replica, never live. Whether it is built is
    the home region's record, read from its state store; a store that cannot be reached refuses
    the read the same way (``query.home_region_unavailable``)."""
    from sqlalchemy.exc import SQLAlchemyError

    from provisa.core.region_stores import HomeRegionUnavailable
    from provisa.federation import replica_state

    region = state.foreign_regions[home]  # bound with the org: a table names a selected region
    key = (table.source_id, table.schema_name, table.table_name)
    try:
        async with region.state_db.acquire() as conn:
            record = await replica_state.read(conn, key)
    except (SQLAlchemyError, OSError) as exc:
        raise HomeRegionUnavailable(table.table_name, home, "cannot be reached") from exc
    if record is None or not record.exists or record.retired_at is not None:
        raise HomeRegionUnavailable(table.table_name, home, "is not built")


async def ensure_resident(
    state: Any,
    source_ids: Iterable[str],
    *,
    reader_role: str | None,
    table_ids: Iterable[int],
) -> Residency:
    """Have every replica a query reads built and fresh before it reads (REQ-1661, REQ-1915).
    Returns the builds it waited for and every replica the statement reads with the completion
    time of the build it is answered from (:class:`Residency`). A no-op without an engine,
    config or tenant store, or when every replica the statement reads is fresh for its reader:
    a fresh replica's completion time is this process's copy of its record, with no
    control-plane read.

    This is the read's backstop, not where replicas are built: builds are requested when the
    model declares a replica, and run by the build runner (``replica_builds``). A read that
    finds a replica missing or stale for its reader asks for the build in the state store
    (``replica_state.request_build``) and waits on the record until the build has completed —
    it never copies a table on its own thread. A build that failed fails the read
    (:class:`ReplicaBuildFailed`); it is asked for again only after
    ``replication.retry_interval``. A read whose deadline passes while the build is still
    running raises :class:`ReplicaBuilding`; the build goes on.

    ``table_ids`` are the registered tables the STATEMENT reads (REQ-826): only those are judged
    and waited for. A statement that reads one table of a source does not wait on the source's
    other tables, and is not moved onto a replica by a setting on a table it does not read.

    ``reader_role`` is the governed role the query runs as (REQ-1907): staleness is judged against
    its effective TTL per table. None is a caller with no reader (it uses each table's cache_ttl).

    Only a WHOLE COPY is built for a read (``replica_converge.whole_copy``). A table with a
    parameter column is a function of its arguments and is never built. A table replicated ROW
    BY ROW (the row_materialize flag on an engine that cannot attach its source, REQ-1865,
    settled: an engine that reads the source in place ignores the flag)
    has no whole-table replica and is never built here. Its rows are fetched by key —
    ``ensure_rows_resident`` for a key the statement binds, ``pushdown_row_materialize`` for a key
    a join supplies — and a statement that binds neither is refused at planning (REQ-1915,
    ``pgwire._pipeline._pk_bounds``), so no read reaches this function needing the whole table.
    The whole-table path that used to live here (load the entire source table into the worker,
    upsert it one row at a time) is gone with that rule."""
    wanted = {s for s in source_ids if s}
    engine = getattr(state, "federation_engine", None)  # the EngineRuntime (write face + engine)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    config = getattr(state, "config", None)
    # REQ-1922: the registry is read from the model store (registry_view); what is built and
    # promoted is this region's state (replica_state), read and written through ``db``.
    model_db = getattr(state, "model_db", None)
    db = getattr(state, "tenant_db", None)
    if (
        not wanted
        or engine is None
        or backend is None
        or config is None
        or model_db is None
        or db is None
    ):
        return Residency()
    from provisa.federation.registry_view import registered_sources, registered_tables

    # REQ-1674: the registry, not the config file — see registry_view. A built-in source
    # (provisa-admin, ...) is never landed and is not in it: a statement's read of one of its
    # tables is not judged here.
    sources = [s for s in await registered_sources(state) if s.id in wanted]
    if not sources:
        return Residency()
    wanted = {s.id for s in sources}
    from provisa.federation.strategy import engine_attaches

    _attached_types = {s.id: engine_attaches(engine, s.type.value) for s in sources}

    read = frozenset(table_ids)
    floored = state.replica_routes.floored

    def _lands(t: Any) -> bool:
        """Whether this statement's read of ``t`` is served from a whole-table replica (REQ-826,
        judged per table): the operator's settings put it there, or the engine cannot read its
        source in place. A table the engine reads live beside a replica-served sibling of the same
        source is left alone — it is neither landed nor asked for a replication clock."""
        return t.id in floored or not _attached_types[t.source_id]

    from provisa.federation.replica_converge import builds_here, home_region, whole_copy

    by_id = {s.id: s for s in sources}
    tables_by_source: dict[str, list[Any]] = {}
    elsewhere: list[tuple[Any, str]] = []
    for t in await registered_tables(state):
        if t.source_id not in wanted or t.id not in read:
            continue
        home = home_region(by_id[t.source_id], t)
        if not builds_here(home):
            # REQ-1922: kept in another region, read from its replica there and never live; it
            # is never built here (the address seam routes the read).
            assert home is not None  # builds_here is True for a table naming no region
            elsewhere.append((t, home))
            continue
        if not _lands(t):
            continue
        # Only a whole copy is built for a read: not a row-level table's (its rows come by
        # key) and not a parameterized table's (a function of its arguments has no whole).
        if whole_copy(by_id[t.source_id], t, engine):
            tables_by_source.setdefault(t.source_id, []).append(t)
    for t, home in elsewhere:
        await require_home_replica(state, t, home)
    # REQ-826 / REQ-1141: a table the operator's settings put on its replica moves its source's
    # read there for this statement, even where the engine could attach the source.
    replicated_by = {s.id: _replicated(state, tables_by_source.get(s.id, [])) for s in sources}
    protected_of = {s.id: _load_protected(s, tables_by_source.get(s.id, [])) for s in sources}

    from provisa.core import request_deadline, settings_registry
    from provisa.core.request_context import current_org
    from provisa.federation import replica_builds, replica_state
    from provisa.federation.replica_routing import live_while_building
    from provisa.federation.replica_state_view import view_for
    from provisa.federation.role_ttl import require_landing_ttl

    # What a replicated source's replica is written into: the engine's own store, or (Trino) the
    # store it reads through its connector. The plan asks for it only for a source the
    # operator's setting moves onto its replica (REQ-846).
    _materialization_backend = (
        engine.engine.replica_store_backend()
        if any(replicated_by.values()) or any(protected_of.values())
        else None
    )

    org_id = current_org.get(None)
    view = view_for(state)

    def _plan(source: Any, is_stale: Any, resident_of: Any) -> bool:
        """The residency plan's own decision for ``source`` (``EngineBackend.pending_lands``):
        whether a read of it must have a replica built first."""
        return bool(
            backend.pending_lands(
                [source],
                is_stale=is_stale,
                replicated_of=lambda sid: replicated_by[sid],
                load_protected_of=lambda sid: protected_of[sid],
                resident_of=resident_of,
                materialization_backend=_materialization_backend,
                # REQ-1907: a freshness_gate source's predicate is folded into ``is_stale`` (the
                # TTL AND freshness gate); the plan must not re-decide it alone.
                freshness_subject_of=None,
                now=time.time(),
            )
        )

    _resolved_store: list[str] = []

    def _store() -> str:
        """The identity of the store this engine's replicas are in, resolved on first use: a
        statement that reads no replica never asks."""
        if not _resolved_store:
            _resolved_store.append(replica_builds.store_identity(state))
        return _resolved_store[0]

    def _replicates(source: Any) -> bool:
        """Whether this engine serves ``source`` from a replica for this statement at all —
        asked for the worst case (nothing built). False: the engine reads it in place and no
        replica, TTL or freshness gate applies to it (REQ-1907, amended 2026-09-30)."""
        return _plan(source, lambda sid: True, None)

    def _stale(source: Any, table: Any, record: Any) -> bool:
        """Whether ``table``'s replica must be built before this read: it was never built, its
        last build failed, or it is older than the reader's effective TTL and its freshness
        check does not report it fresh (REQ-1907; the whole gate is ``is_stale_of``). The plan
        applies the rest: a load-protected table that has a replica is never rebuilt by a read
        (REQ-1141: its refresh is the runner's alone)."""
        node = _node(table)
        key_ = (source.id, table.schema_name, table.table_name)
        # A standing replica that can no longer answer the model (convergence found a column
        # added or retyped) is one never built, until a build of the model's definition lands.
        state_ = _freshness_state(record, _store()) if view.serves(org_id, key_, record) else None
        is_stale = is_stale_of(
            [source], {source.id: [table]}, {node: state_}, time.time(), reader_role=reader_role
        )
        resident = state_ is not None and state_["last_refresh_at"] is not None
        return _plan(source, is_stale, lambda sid: resident)

    retry_interval = float(settings_registry.value("replication.retry_interval"))
    waiting: list[tuple[Any, Any, replica_state.ReplicaKey]] = []
    replicas_read: dict[ReplicaKey, datetime] = {}

    def _read_from(key: ReplicaKey, record: Any) -> None:
        """The statement reads this replica: keep the completion time of its build."""
        if record is None or not record.exists_in(_store()):
            raise ReplicaAgeUnknown(".".join(key))
        replicas_read[key] = record.completed_at

    for source in sources:
        tables = tables_by_source.get(source.id)
        if not tables:
            # Nothing of this source is served from a whole-table replica for this statement:
            # its tables are read live through the engine's attach, or replicated row by row.
            continue
        if not _replicates(source):
            continue
        # REQ-1907 (amended 2026-09-30): a ttl / ttl_probe table served from a replica with no
        # table or source cache_ttl has no refresh clock — fail the read before anything else.
        for t in tables:
            require_landing_ttl(t, source)
        for t in tables:
            key = (source.id, t.schema_name, t.table_name)
            # REQ-1661 (amended 2026-10-01): decided in memory first. A replica this process's
            # copy says is fresh for this reader is served with no control-plane read.
            known, record = view.known(org_id, key)
            if known and not _stale(source, t, record):
                _read_from(key, record)
                continue
            # Stale or unknown by the copy: re-read this ONE record, and ask for the build,
            # under the replica's lock — a burst of stale reads makes one read and one request.
            async with view.lock(org_id, key):
                known, record = view.known(org_id, key)
                if known and not _stale(source, t, record):
                    # Another reader of this process re-read it while this one waited.
                    _read_from(key, record)
                    continue
                if not (known and _joins(record, view.age(org_id, key))):
                    async with db.acquire() as conn:
                        record = await replica_state.read(conn, key)
                        if _stale(source, t, record):
                            await replica_state.request_build(
                                conn, key, replica_state.REASON_READ, retry_interval=retry_interval
                            )
                            record = await replica_state.read(conn, key)
                    view.read(org_id, key, record)
            if not _stale(source, t, record):
                _read_from(key, record)
                continue
            if record is not None and record.build_state == replica_state.FAILED:
                # Failed too recently to ask again (replication.retry_interval): the read fails
                # with the build's own error. It never reads what the failed build left.
                raise replica_state.ReplicaBuildFailed(".".join(key), record.last_error)
            if live_while_building(source, t, engine.engine):
                continue  # read live while the build runs; the build is requested, not awaited
            waiting.append((source, t, key))
    if not waiting:
        return Residency(replicas_read=replicas_read)
    replica_builds.kick(org_id)
    built = [(key[0], key[2]) for _source, _table, key in waiting]
    started = time.monotonic()
    budget = request_deadline.remaining()
    while waiting:
        still: list[tuple[Any, Any, replica_state.ReplicaKey]] = []
        async with db.acquire() as conn:
            for source, t, key in waiting:
                record = await replica_state.read(conn, key)
                view.read(org_id, key, record)
                if record is not None and record.build_state == replica_state.FAILED:
                    raise replica_state.ReplicaBuildFailed(".".join(key), record.last_error)
                if _stale(source, t, record):
                    still.append((source, t, key))
                else:
                    _read_from(key, record)
        waiting = still
        if not waiting:
            break
        waited = time.monotonic() - started
        if budget is not None and waited + _BUILD_POLL_S + _ANSWER_RESERVE_S >= budget:
            # The request answers with ReplicaBuilding before its own deadline cuts it off with
            # a timeout that says nothing about why. The build goes on.
            raise replica_state.ReplicaBuilding(".".join(waiting[0][2]), waited)
        await asyncio.sleep(_BUILD_POLL_S)
    log.info("query residency: %s built before the read", built)
    return Residency(built=built, replicas_read=replicas_read)


#: How often a read waiting for a build looks at its record.
_BUILD_POLL_S = 0.2
#: A waiting read answers with ReplicaBuilding this long before its own deadline, so the
#: answer names the cause instead of losing the race to a timeout that names nothing.
_ANSWER_RESERVE_S = 0.5


def _joins(record: Any, age: float | None) -> bool:
    """Whether a reader joins the build this process's copy already shows requested or running,
    without reading the record or asking again: the copy was read within the last poll interval
    by another reader of the same burst. An older copy is read again — a build this process did
    not watch may have finished."""
    from provisa.federation.replica_state import BUILDING, REQUESTED

    return (
        record is not None
        and record.build_state in (REQUESTED, BUILDING)
        and age is not None
        and age < _BUILD_POLL_S
    )


def _freshness_state(record: Any, store: str) -> dict | None:
    """A replica's record in the shape the staleness oracle reads: when it was last built in
    the store this engine reads (``store``) and whether its last build succeeded. None: no
    record, never built. A replica built in another store is one never built here."""
    from provisa.federation.replica_state import FAILED

    if record is None:
        return None
    return {
        "last_refresh_at": record.completed_at.timestamp() if record.exists_in(store) else None,
        "last_refresh_ok": record.build_state != FAILED,
    }


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
        result = await engine.execute_engine(
            pass_tree.sql(dialect=dialect), params, authorization=system_auth("row replication")
        )

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
        # The table's node — the key its row lock is taken under here and by the row-refresh
        # lifecycle (``events.row_materialize_lifecycle``), so the two serialize the same rows.
        node = _node(table)
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
    residency = await ensure_resident(
        state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids
    )
    plan.replicas_read = residency.replicas_read
