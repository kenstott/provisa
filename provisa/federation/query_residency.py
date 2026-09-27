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
from datetime import UTC, datetime, timedelta
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


async def ensure_resident(
    state: Any,
    source_ids: Iterable[str],
    *,
    pk_bounds: Iterable[Any] = (),
    pushed_down: Iterable[str] = (),
) -> list[tuple[str, str]]:
    """Land what a query reads and is not resident (REQ-1661). Returns the (source_id, table_name)
    pairs landed. A no-op without an engine, config or tenant store, or when nothing is stale.

    ``pk_bounds`` is the current query's resolved PK bound set (REQ-1865, ``_resolve_pk_bounds`` /
    ``extract_pk_bounds`` — each entry has a ``.table_name``). A row_materialize table is excluded
    from this whole-table sweep ONLY when the current query actually resolved a bound against that
    table's own declared PK; per REQ-1865's own spec ("a query whose predicate does not resolve to
    a bounded PK set falls back to the table's ordinary whole-table materialize/live resolution
    unchanged"), a row_materialize table with NO bound for this query still needs the same whole-
    table land any other table would get -- a query with no filter on that table is, by
    definition, asking for the whole table. Confirmed live: cypher_cross_engine joins
    bench_contains_edge on order_id (a plain FK column, not its own contains_id PK) with no literal
    predicate on contains_id at all -- unconditionally excluding it starved the table of every row,
    since ensure_rows_resident's keyed fetch never had a contains_id bound to key off either."""
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
    _bound_tables = {getattr(b, "table_name", None) for b in pk_bounds} | set(pushed_down)
    tables_by_source: dict[str, list[Any]] = {}
    for t in await registered_tables(state):
        if t.source_id in wanted and not (
            getattr(t, "row_materialize", False) and t.table_name in _bound_tables
        ):
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
    from provisa.federation.registry_view import registered_tables

    # REQ-1865 (amended): a plain raw-SQL statement (pgwire, Flight) references a table by its own
    # bare physical name (e.g. "bench_customer_node") -- it is never rewritten to the table's
    # semantic alias the way a GraphQL-compiled query's domain.field_name resolution is. Keying by
    # alias ONLY silently matched nothing for every such statement -- confirmed live: pk_bounds
    # came back empty for cypher_cross_engine's own literal customer_id predicate against
    # bench_customer_node, which does have an alias ("Customer"). Key by BOTH the alias (when set,
    # for the compiled/GraphQL path) and the bare table_name (for a raw-SQL statement) so
    # extract_pk_bounds matches whichever form the statement's own AST actually uses.
    out: dict[str, Any] = {}
    for t in await registered_tables(state):
        if not getattr(t, "row_materialize", False):
            continue
        out[apply_sql_name(t.table_name)] = t
        if t.alias:
            out[apply_sql_name(t.alias)] = t
    return out


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


def _join_key_column(join: Any, target_alias: str) -> tuple[str, Any] | None:
    """For a ``JOIN ... ON target.col = other_expr`` (or reversed) equality, return
    ``(target_col_name, other_side_expr)`` -- ``None`` for anything else (composite ON, a
    non-equality, an OR, a literal on either side): never guessed, this table's pushdown is simply
    skipped and it falls back to ``ensure_resident``'s whole-table land instead."""
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


async def _has_fresh_cached_rows(state: Any, source: Any, table: Any) -> bool:
    """Cheap existence check: does this row_materialize table's cache already hold ANY unexpired
    row at all? Used only to decide whether the outer-join probe pass below is needed this call --
    a coarse, table-level heuristic (not a per-key verification, which ``ensure_rows_resident``'s
    own stale_or_missing check already does once real keys are known), so a repeated query within
    one cache_ttl window (this session's own benchmark's own 10-20x reruns) pays for the probe
    exactly once per TTL window rather than on every call."""
    if source is None:
        return False
    from sqlalchemy import select

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return False
    from provisa.federation import store_writer
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.materialize_exec import _ROW_EXPIRES_AT, build_row_cache_table

    store_schema = _env_store_schema(engine.engine.materialize_store())
    dsn = engine.engine.materialize_store()
    schema, name = backend.landing_target(
        store_schema=store_schema,
        source_id=source.id,
        source_type=source.type,
        schema_name=table.schema_name,
        table_name=table.table_name,
    )
    pk_columns = [c.name for c in table.columns if c.is_primary_key]
    if len(pk_columns) != 1:
        return False
    try:
        args = resolve_landing_args_for(source, table, backend.dialect)
        cache_table = build_row_cache_table(
            schema, name, args.columns, tuple(pk_columns), dialect_name=backend.dialect
        )
        async with store_writer.store_connection(dsn) as conn:
            stmt = (
                select(cache_table.c[pk_columns[0]])
                .where(cache_table.c[_ROW_EXPIRES_AT] > datetime.now(UTC))
                .limit(1)
            )
            result = await conn.execute_core(stmt)
            return result.fetchone() is not None
    except Exception:
        # No cache table yet, or any other lookup failure -- treat as "not warm", the safe default
        # (the probe pass below still runs, never worse than before pushdown existed).
        return False


def resolve_landing_args_for(source: Any, table: Any, dialect: str | None) -> Any:
    from provisa.federation.residency import resolve_landing_args

    return resolve_landing_args(source, table, platform=dialect)


async def pushdown_row_materialize(
    state: Any, physical_sql: str, dialect: str, params: list[Any] | None = None
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

    Skips a table already warm (``_has_fresh_cached_rows``) -- see that function's docstring.
    Never guesses: a table whose join key can't be read from a single column-to-column ON
    equality (``_join_key_column`` returns None) is simply left off this pass and falls back to
    ``ensure_resident``'s whole-table land instead, same as before this mechanism existed.

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

    from provisa.events.app_wiring import build_adapter_loaders, build_keyed_adapter_loaders
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.registry_view import registered_sources, registered_tables

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return set()

    tables_by_name = {
        t.table_name: t
        for t in await registered_tables(state)
        if getattr(t, "row_materialize", False)
    }
    if not tables_by_name:
        return set()
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
        keyed_adapter_loaders=build_keyed_adapter_loaders(state),
    )

    for _pass in range(len(all_joins)):
        pending = {name for name in remaining if name not in landed_this_call}
        # Drop a table already warm from cache -- the conditional rerun (skip the probe for it).
        still_pending: set[str] = set()
        for name in pending:
            table = tables_by_name[name]
            source = sources_by_id.get(table.source_id)
            if await _has_fresh_cached_rows(state, source, table):
                landed_this_call.add(name)  # treat "already warm" the same as "landed"
            else:
                still_pending.add(name)
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
                skip.add(name)  # never guessed -- falls back to whole-table land
                continue
            key_cols[name] = kc
            joins_by_name[name].set("kind", "LEFT")
        for name in skip:
            still_pending.discard(name)
        if not still_pending:
            break

        for name, (_target_col, other_expr) in key_cols.items():
            pass_tree.select(
                exp.alias_(other_expr.copy(), f"__pushdown_{name}"), append=True, copy=False
            )

        try:
            result = await engine.execute_engine(pass_tree.sql(dialect=dialect), params)
        except Exception:
            log.warning("row-materialize key-pushdown probe failed", exc_info=True)
            break

        made_progress = False
        for name in list(still_pending):
            target_col, _ = key_cols[name]
            alias = f"__pushdown_{name}"
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
            try:
                rows = await loader.load_keys(source, table, [target_col], [(v,) for v in values])
            except Exception:
                log.warning(
                    "row-materialize key-pushdown fetch failed for %s.%s",
                    name,
                    target_col,
                    exc_info=True,
                )
                continue
            if not rows:
                landed_this_call.add(name)
                made_progress = True
                continue
            args = resolve_landing_args_for(source, table, backend.dialect)
            resolved_ttl = table.cache_ttl if table.cache_ttl is not None else source.cache_ttl
            if resolved_ttl is None:
                raise ValueError(
                    f"row-materialize table {table.table_name!r}: no resolved cache_ttl at fetch "
                    "time (registration should have rejected this — REQ-1865)"
                )
            store_schema = _env_store_schema(engine.engine.materialize_store())
            schema, cache_name = backend.landing_target(
                store_schema=store_schema,
                source_id=source.id,
                source_type=source.type,
                schema_name=table.schema_name,
                table_name=table.table_name,
            )
            cache_table = await _ensure_row_cache_table(
                engine, backend, state, schema, cache_name, args.columns
            )
            await _land_row_cache(
                engine,
                backend,
                state,
                schema,
                cache_name,
                cache_table,
                [real_pk],
                args.columns,
                rows,
                resolved_ttl,
            )
            landed_this_call.add(name)
            made_progress = True

        if not made_progress:
            break

    return landed_this_call


def _is_duckdb_store(backend: Any) -> bool:
    return getattr(backend, "dialect", None) == "duckdb"


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

    if _is_duckdb_store(backend):
        from provisa.federation.store_connection import ensure_row_cache_table_duckdb_native

        runtime = _duckdb_runtime(backend, state)
        catalog = runtime.ensure_materialize_attached()
        full_columns = list(columns) + [
            (_ROW_CACHED_AT, "timestamp"),
            (_ROW_EXPIRES_AT, "timestamp"),
        ]
        ensure_row_cache_table_duckdb_native(
            runtime.connection, catalog=catalog, schema=schema, table=name, columns=full_columns
        )
        return None

    from provisa.federation import store_writer
    from sqlalchemy.schema import CreateSchema, CreateTable

    cache_table = build_row_cache_table(schema, name, columns, (), dialect_name=backend.dialect)
    dsn = engine.engine.materialize_store()
    async with store_writer.store_connection(dsn) as conn:
        if schema and conn.capabilities.schemas:
            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
        await conn.execute_core(CreateTable(cache_table, if_not_exists=True))
    return cache_table


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
    if _is_duckdb_store(backend):
        from provisa.federation.store_connection import read_row_cache_duckdb_native

        runtime = _duckdb_runtime(backend, state)
        catalog = runtime.ensure_materialize_attached()
        return read_row_cache_duckdb_native(
            runtime.connection,
            catalog=catalog,
            schema=schema,
            table=name,
            pk_columns=pk_columns,
            keys=keys,
        )
    from provisa.federation import store_writer

    dsn = engine.engine.materialize_store()
    async with store_writer.store_connection(dsn) as conn:
        return await _read_cached(conn, cache_table, pk_columns, keys)


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
    resolved_ttl: int,
) -> None:
    if not rows:
        return
    if _is_duckdb_store(backend):
        from provisa.federation.materialize_exec import (
            _ROW_CACHED_AT,
            _ROW_EXPIRES_AT,
            _UpsertEvent,
        )
        from provisa.federation.store_connection import apply_cdc_duckdb_native

        runtime = _duckdb_runtime(backend, state)
        catalog = runtime.ensure_materialize_attached()
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=resolved_ttl)
        stamped = [{**r, _ROW_CACHED_AT: now, _ROW_EXPIRES_AT: expires_at} for r in rows]
        full_columns = list(columns) + [
            (_ROW_CACHED_AT, "timestamp"),
            (_ROW_EXPIRES_AT, "timestamp"),
        ]
        apply_cdc_duckdb_native(
            runtime.connection,
            catalog=catalog,
            schema=schema,
            table=name,
            columns=full_columns,
            pk_columns=pk_columns,
            events=[_UpsertEvent(r) for r in stamped],
        )
        return
    from provisa.federation import store_writer
    from provisa.federation.materialize_exec import land_rows

    dsn = engine.engine.materialize_store()
    async with store_writer.store_connection(dsn) as conn:
        await land_rows(conn, cache_table, pk_columns, rows, resolved_cache_ttl=resolved_ttl)


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
    if _is_duckdb_store(backend):
        from provisa.federation.store_connection import tombstone_row_cache_duckdb_native

        runtime = _duckdb_runtime(backend, state)
        catalog = runtime.ensure_materialize_attached()
        tombstone_row_cache_duckdb_native(
            runtime.connection,
            catalog=catalog,
            schema=schema,
            table=name,
            pk_columns=pk_columns,
            keys=keys,
        )
        return
    from provisa.federation import store_writer

    dsn = engine.engine.materialize_store()
    async with store_writer.store_connection(dsn) as conn:
        await _tombstone_keys(conn, cache_table, pk_columns, keys)


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
    from provisa.federation.backend import _env_store_schema
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
        keyed_adapter_loaders=build_keyed_adapter_loaders(state),
    )
    store_schema = _env_store_schema(engine.engine.materialize_store())

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
        pk_columns = list(bound.pk_columns)
        cache_table = await _ensure_row_cache_table(
            engine, backend, state, schema, name, args.columns
        )
        cached = await _read_row_cache(
            engine, backend, state, schema, name, cache_table, pk_columns, list(bound.values)
        )

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
            recheck = await _read_row_cache(
                engine, backend, state, schema, name, cache_table, pk_columns, stale_or_missing
            )
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
                resolved_ttl,
            )
            await _tombstone_row_cache(
                engine, backend, state, schema, name, cache_table, pk_columns, tombstoned
            )

            results.append((source.id, table.table_name, len(fetched)))

    return results
