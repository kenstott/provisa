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

Stale means: a table of the source has no refresh stamp (never landed), or the source declares a
``cache_ttl`` the stamp has outrun. A source with ``freshness_gate`` set is judged by its own
predicate (REQ-860). A ``load_protected`` source lands here only when it has never landed
(REQ-1141: the scheduler is its sole refresher).

A land that fails is logged and stamped ``ok=False``; the read proceeds against whatever the
replica holds. The event loop applies the same rule to a node whose fetch fails: a broken adapter
withholds fresh rows, it does not withhold every query that names the table.
"""

# Requirements: REQ-1661, REQ-860, REQ-855, REQ-1141

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)


def _node(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


def _physical_node(backend: Any, engine: Any, source: Any, table: Any) -> str:
    """The lock key for ``land_lock`` — the PHYSICAL (post-fold) address a land actually writes to,
    not the registered logical one (REQ-1730). ``events/boot.py``'s own poll-node wiring locks on
    this same ``backend.landing_target(...)`` result (its ``land_schema``/``land_table``), and
    ``EngineBackend.materialize_pending`` recomputes the identical fold internally right after this
    function's caller acquires its lock — so the two lands ``land_lock``'s own docstring promises
    never interleave must key on the SAME string. Keying on the registered name instead (as this
    used to) is a no-op fold for most engines but diverges from boot.py's key for any
    ``catalog_qualified=False`` engine (pg/ClickHouse/Oracle): the query path's lock then guards a
    different node than the event loop's own scheduled land, and the two run truly concurrently —
    confirmed live, REQ-1730, 2026-09-21: Oracle's REPLACE land interleaved (DELETE, DELETE, INSERT,
    INSERT) across two OS threads, landing every row twice."""
    from provisa.federation.backend import _env_store_schema

    store_schema = _env_store_schema(engine.engine.materialize_store())
    schema, name = backend.landing_target(
        store_schema=store_schema,
        source_id=source.id,
        source_type=source.type,
        schema_name=table.schema_name,
        table_name=table.table_name,
    )
    return _node(schema, name)


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


def is_stale_of(
    sources: list[Any], stamps: dict[str, float | None], oks: dict[str, bool], now: float
) -> Any:
    """The generic staleness oracle the plan consults for an ungated source: never landed, last
    land failed, or a declared ``cache_ttl`` outrun."""
    by_id = {s.id: s for s in sources}

    def is_stale(source_id: str) -> bool:
        stamp = stamps.get(source_id)
        if stamp is None or not oks.get(source_id, True):
            return True
        ttl = getattr(by_id.get(source_id), "cache_ttl", None)
        return ttl is not None and now - stamp > float(ttl)

    return is_stale


async def ensure_resident(state: Any, source_ids: Iterable[str]) -> list[tuple[str, str]]:
    """Land what a query reads and is not resident (REQ-1661). Returns the (source_id, table_name)
    pairs landed. A no-op without an engine, config or tenant store, or when nothing is stale."""
    wanted = {s for s in source_ids if s}
    engine = getattr(state, "federation_engine", None)  # the EngineRuntime (write face + engine)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    config = getattr(state, "config", None)
    db = getattr(state, "tenant_db", None)
    if not wanted or backend is None or config is None or db is None:
        return []
    from provisa.federation.registry_view import registered_sources, registered_tables

    # REQ-1674: the registry, not the config file — see registry_view.
    _all_sources = await registered_sources(state)
    sources = [s for s in _all_sources if s.id in wanted]
    if not sources:
        return []
    tables_by_source: dict[str, list[Any]] = {}
    for t in await registered_tables(state):
        if t.source_id in wanted:
            tables_by_source.setdefault(t.source_id, []).append(t)

    from provisa.events import queue
    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.source_loader import SourceRowLoader
    from provisa.freshness.source_gate import source_subject

    async with db.acquire() as conn:
        states = {
            _node(t.schema_name, t.table_name): await queue.get_node_state(
                conn, _node(t.schema_name, t.table_name)
            )
            for tables in tables_by_source.values()
            for t in tables
        }
    now = time.time()
    stamps, oks = stale_sources(sources, tables_by_source, states)
    by_id = {s.id: s for s in sources}
    loader = SourceRowLoader(
        engine,
        adapter_loaders=build_adapter_loaders(state, engine),
        keyed_adapter_loaders=build_keyed_adapter_loaders(state),
    )

    landed: list[tuple[str, str]] = []
    from contextlib import AsyncExitStack

    from provisa.events.land_lock import land_lock

    for source in sources:
        # The same per-node locks the event loop's land takes, so the boot land and a first query
        # never interleave on one replica; every node of the source is held for the source's land.
        async with AsyncExitStack() as held:
            for t in tables_by_source.get(source.id, []):
                await held.enter_async_context(
                    land_lock(_physical_node(backend, engine, source, t))
                )
            try:
                clock_stale = is_stale_of(sources, stamps, oks, now)
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
                    prefer_materialized_of=lambda sid: bool(
                        getattr(by_id[sid], "prefer_materialized", False)
                    ),
                    load_protected_of=lambda sid: bool(
                        getattr(by_id[sid], "load_protected", False)
                    ),
                    resident_of=lambda sid: stamps.get(sid) is not None,
                    # the engine's own store is what a prefer_materialized source lands into
                    materialization_backend=getattr(
                        getattr(engine, "engine", engine), "native_store", None
                    ),
                    freshness_subject_of=lambda sid: source_subject(
                        stamps.get(sid), ok=oks.get(sid, True)
                    ),
                    now=now,
                )
                backend.mark_landed(source.id)
                ok = True
            except Exception:  # noqa: BLE001 - the adapter's error type is its own
                log.exception(
                    "query residency: landing %s failed; the read proceeds on the replica as it is",
                    source.id,
                )
                ok = False
            stamped = (
                [(source.id, t.table_name) for t in tables_by_source.get(source.id, [])]
                if not ok
                else [pair for pair in landed if pair[0] == source.id]
            )
            if stamped:
                at = datetime.now(UTC)
                async with db.acquire() as conn:
                    for sid, table_name in stamped:
                        table = next(t for t in tables_by_source[sid] if t.table_name == table_name)
                        await queue.record_refresh(
                            conn, _node(table.schema_name, table.table_name), at=at, ok=ok
                        )
    if landed:
        log.info("query residency: landed %s before the read", landed)
    return landed


async def row_materialized_tables_by_name(state: Any) -> dict[str, Any]:
    """semantic table name -> registered Table, restricted to ``row_materialize=True`` tables
    (REQ-1865). Used by ``pk_bounds.extract_pk_bounds`` to know which tables in a statement's AST
    are even eligible for the row cache — the registry, not the config file, same posture as every
    other residency lookup in this module (REQ-1674).

    Keyed by the SEMANTIC name (``apply_sql_name(t.alias or t.table_name)``, the same authority
    ``compiler.sql_rewrite.semantic_table_name`` uses to build ``display_name``) rather than the
    bare physical ``table_name`` -- ``extract_pk_bounds`` matches this key against
    ``_resolve_pk_bounds``'s ALREADY-semantic AST (``governed_semantic``), where a table with a
    registered alias (every neo4j/query_template table in the perf-bench demo: ``bench_order_node``
    -> ``order``) appears under that alias, never its physical name. Keying by physical
    table_name alone silently matched nothing for any such table -- extract_pk_bounds never errors
    on a miss (by design), so this was never a crash, just a permanently-empty result: every
    row_materialize-enabled neo4j table fell through to the pre-existing full-source land on every
    query, exactly the cost row_materialize exists to avoid. Confirmed live on the perf-bench VM."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.federation.registry_view import registered_tables

    return {
        apply_sql_name(t.alias or t.table_name): t
        for t in await registered_tables(state)
        if getattr(t, "row_materialize", False)
    }


async def _read_cached(
    conn: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> dict[tuple[Any, ...], datetime]:
    """key -> ``_row_expires_at`` for every key of ``keys`` currently present in the row cache. A
    key absent from the result is simply not cached yet (never an error)."""
    from sqlalchemy import select, tuple_

    if not keys:
        return {}
    pk_cols = [table.c[c] for c in pk_columns]
    cond = pk_cols[0].in_([k[0] for k in keys]) if len(pk_cols) == 1 else tuple_(*pk_cols).in_(keys)
    stmt = select(*pk_cols, table.c["_row_expires_at"]).where(cond)
    result = await conn.execute_core(stmt)
    out: dict[tuple[Any, ...], datetime] = {}
    for row in result.fetchall():
        expires_at = row[len(pk_columns)]
        # SQLite (a supported store dialect) has no true timezone-aware column type -- a
        # DateTime(timezone=True) round-trips as a naive value there. Every _row_expires_at this
        # module ever writes is UTC (land_rows stamps datetime.now(UTC)), so a naive value read
        # back is always UTC too; normalize it before comparing against an aware `now`.
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        out[tuple(row[: len(pk_columns)])] = expires_at
    return out


async def _tombstone_keys(
    conn: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> None:
    from sqlalchemy import tuple_

    if not keys:
        return
    pk_cols = [table.c[c] for c in pk_columns]
    cond = pk_cols[0].in_([k[0] for k in keys]) if len(pk_cols) == 1 else tuple_(*pk_cols).in_(keys)
    await conn.execute_core(table.delete().where(cond))


async def ensure_rows_resident(
    state: Any, pk_bounds: Iterable[Any], *, force: bool = False
) -> list[tuple[str, str, int]]:
    """Serve exactly the rows ``pk_bounds`` names from the row cache, fetching from source only the
    missing/stale ones (REQ-1865). Returns (source_id, table_name, n_rows_fetched) per bound
    touched. A no-op for a bound with no values (``extract_pk_bounds`` already filters those out) or
    when the named table is not ``row_materialize`` (defensive: a stale/mismatched bound is just
    skipped, since the compiler is the sole authority on which tables qualify). ``force=True`` (used
    only by the CDC background-refresh caller, section 5/6b) treats every ALREADY-CACHED key in the
    bound as stale regardless of its ``_row_expires_at``, without ever adding a key that isn't
    already cached -- the one behavioral difference between a query-driven call and a CDC-driven
    one."""
    from contextlib import AsyncExitStack

    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.row_lock import row_lock
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation import store_writer
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.materialize_exec import build_row_cache_table, land_rows
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.residency import resolve_landing_args
    from sqlalchemy.schema import CreateSchema, CreateTable

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
        keyed_adapter_loaders=build_keyed_adapter_loaders(state),
    )
    store_schema = _env_store_schema(engine.engine.materialize_store())
    dsn = engine.engine.materialize_store()

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
        resolved_ttl = table.cache_ttl if table.cache_ttl is not None else source.cache_ttl
        if resolved_ttl is None:
            raise ValueError(
                f"row-materialize table {table.table_name!r}: no resolved cache_ttl at fetch "
                "time (registration should have rejected this — REQ-1865)"
            )

        schema, name = backend.landing_target(
            store_schema=store_schema,
            source_id=source.id,
            source_type=source.type,
            schema_name=table.schema_name,
            table_name=table.table_name,
        )
        node = _node(schema, name)
        cache_table = build_row_cache_table(
            schema, name, args.columns, bound.pk_columns, dialect_name=backend.dialect
        )
        pk_columns = list(bound.pk_columns)

        async with store_writer.store_connection(dsn) as conn:
            if schema and conn.capabilities.schemas:
                await conn.execute_core(CreateSchema(schema, if_not_exists=True))
            await conn.execute_core(CreateTable(cache_table, if_not_exists=True))
            cached = await _read_cached(conn, cache_table, pk_columns, list(bound.values))

        stale_or_missing = [
            key for key in bound.values if key not in cached or force or cached[key] < now
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
            async with store_writer.store_connection(dsn) as conn:
                recheck = await _read_cached(conn, cache_table, pk_columns, stale_or_missing)
            still_needed = [
                k for k in stale_or_missing if force or k not in recheck or recheck[k] < now
            ]
            if not still_needed:
                results.append((source.id, table.table_name, 0))
                continue

            fetched = await loader.load_keys(source, table, pk_columns, still_needed)
            fetched_keys = {tuple(row.get(pk) for pk in pk_columns) for row in fetched}
            # Tombstone: a requested key the source returned no row for (section 6a) -- deleted
            # synchronously, inline, here, never deferred.
            tombstoned = [k for k in still_needed if k not in fetched_keys]

            async with store_writer.store_connection(dsn) as conn:
                if fetched:
                    await land_rows(
                        conn,
                        cache_table,
                        pk_columns,
                        fetched,
                        resolved_cache_ttl=resolved_ttl,
                        now=now,
                    )
                if tombstoned:
                    await _tombstone_keys(conn, cache_table, pk_columns, tombstoned)

            results.append((source.id, table.table_name, len(fetched)))

    return results
