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

A land that fails is stamped ``ok=False`` (so the next query retries it) and fails the query with
its own cause; the query never reads the stale replica (REQ-1661, amended 2026-09-30).
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
    unbound_targets: Iterable[str] = (),
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
    definition, asking for the whole table.

    ``unbound_targets`` (REQ-1865) is the set of physical table names the CURRENT query's own SQL
    text names directly (its base FROM/JOIN tables, e.g. ``_table_names_in_sql``) -- NOT every
    table that merely shares a source with one the query reads. Without this, a row_materialize
    table's own whole-table fallback (above) cannot be told apart from an unrelated row_materialize
    table that just happens to be registered under the same ``source_ids`` the query touches for a
    different table entirely -- landing the latter is pure collateral cost row_materialize exists
    to avoid (confirmed live: a row_materialize table swept into a whole-source land triggered by
    an unrelated sibling table going stale paid the same full-table cost a keyed lookup exists to
    avoid). The fallback below only ever fires for a table both stale AND named in this set."""
    wanted = {s for s in source_ids if s}
    engine = getattr(state, "federation_engine", None)  # the EngineRuntime (write face + engine)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    config = getattr(state, "config", None)
    db = getattr(state, "tenant_db", None)
    if not wanted or engine is None or backend is None or config is None or db is None:
        return []
    from provisa.federation.registry_view import registered_sources, registered_tables

    # REQ-1674: the registry, not the config file — see registry_view.
    _all_sources = await registered_sources(state)
    sources = [s for s in _all_sources if s.id in wanted]
    if not sources:
        return []
    _bound_tables = {getattr(b, "table_name", None) for b in pk_bounds} | set(pushed_down)
    _unbound_targets = set(unbound_targets)
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
        keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
    )

    landed: list[tuple[str, str]] = []
    from contextlib import AsyncExitStack

    from provisa.federation.backend import _env_store_schema
    from provisa.events.land_lock import land_lock

    store_schema = _env_store_schema(engine.engine.materialize_store())

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
                # REQ-1865 gap: materialize_pending's own registered_tables() sweep (backend.py)
                # unconditionally excludes row_materialize tables ("governed EXCLUSIVELY by the
                # row-level cache") — but a row_materialize table with NO bound for THIS query
                # (the only reason it survived the `tables_by_source` filter above) still needs a
                # whole-table land, per this module's own documented fallback. Neither path
                # actually performed that land: confirmed live (neo4j_materialize_cold's unfiltered
                # `SELECT count(*)` against a row_materialize table hit "relation does not exist"
                # on a fresh boot — materialize_pending silently skipped it, and nothing else ever
                # created/populated its row-cache table). Land it here via the same row-cache infra
                # the keyed paths use, fetching every row instead of a key subset.
                for t in tables_by_source.get(source.id, []):
                    if (
                        not getattr(t, "row_materialize", False)
                        or t.table_name not in _unbound_targets
                        or not is_stale(source.id)
                    ):
                        continue
                    pk_columns = [c.name for c in t.columns if c.is_primary_key]
                    if len(pk_columns) != 1:
                        continue
                    args = resolve_landing_args_for(source, t, backend.dialect)
                    resolved_ttl = t.cache_ttl if t.cache_ttl is not None else source.cache_ttl
                    if resolved_ttl is None:
                        raise ValueError(
                            f"row-materialize table {t.table_name!r}: no resolved cache_ttl at "
                            "fetch time (registration should have rejected this — REQ-1865)"
                        )
                    rm_schema, rm_name = backend.landing_target(
                        store_schema=store_schema,
                        source_id=source.id,
                        source_type=source.type,
                        schema_name=t.schema_name,
                        table_name=t.table_name,
                    )
                    cache_table = await _ensure_row_cache_table(
                        engine, backend, state, rm_schema, rm_name, args.columns
                    )
                    rows = await loader.load(source, t)
                    await _land_row_cache(
                        engine,
                        backend,
                        state,
                        rm_schema,
                        rm_name,
                        cache_table,
                        pk_columns,
                        args.columns,
                        rows,
                        resolved_ttl,
                    )
                    landed.append((source.id, t.table_name))
                backend.mark_landed(source.id)
            except Exception:  # noqa: BLE001 - the adapter's error type is its own; re-raised
                # REQ-1661 (amended 2026-09-30): a failed land fails the query -- it never reads
                # the stale replica. Stamp the nodes not ok first, so the next query retries.
                await _record_refresh(
                    db,
                    queue,
                    [(source.id, t) for t in tables_by_source.get(source.id, [])],
                    ok=False,
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
            )
    if landed:
        log.info("query residency: landed %s before the read", landed)
    return landed


async def _record_refresh(db: Any, queue: Any, tables: list[tuple[str, Any]], *, ok: bool) -> None:
    """Stamp each (source_id, table) node's refresh outcome in the freshness state the event loop
    reads (REQ-1661)."""
    if not tables:
        return
    at = datetime.now(UTC)
    async with db.acquire() as conn:
        for _sid, table in tables:
            await queue.record_refresh(
                conn, _node(table.schema_name, table.table_name), at=at, ok=ok
            )


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


def resolve_landing_args_for(source: Any, table: Any, dialect: str | None) -> Any:
    from provisa.federation.residency import resolve_landing_args

    return resolve_landing_args(source, table, platform=dialect)


def table_names_in_sql(physical_sql: str, dialect: str) -> set[str]:
    """Every physical table name this statement's FROM/JOIN clauses name directly (REQ-1865) --
    the ``unbound_targets`` ``ensure_resident`` needs to tell "this query's own row_materialize
    table, no bound resolved" apart from an unrelated row_materialize table that merely shares a
    source with one the query reads (see that function's docstring). Parse-error or non-SELECT
    statements yield an empty set -- the caller's fallback then simply does not fire, same as any
    other row_materialize table this pass found nothing to do for."""
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(physical_sql, read=dialect)
    except Exception:
        return set()
    return {t.name for t in tree.find_all(exp.Table) if t.name}


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

    A candidate key already fresh in the row cache (filtered by target_col, not necessarily the
    table's real PK -- the cache carries every landed column) is dropped before the fetch, mirroring
    ``ensure_rows_resident``'s own stale_or_missing check: a repeat query with the same key set does
    zero live-source work, and a query mixing fresh and new/stale keys fetches+lands only the
    reduced subset -- never the coarse "is ANY row in this table fresh" table-level skip an earlier
    version of this mechanism used, which could miss a genuinely new key while an unrelated row was
    still warm.

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

    from provisa.events.app_wiring import (
        build_adapter_loaders,
        build_keyed_adapter_loaders,
        build_keyed_arrow_loaders,
    )
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.registry_view import registered_sources

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return set()

    tables_by_name = {t.table_name: t for t in await active_row_materialize_tables(state)}
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

        # A failed probe propagates, same as a failed keyed fetch below: breaking out left every
        # pending table unlanded with no error surfaced.
        result = await engine.execute_engine(pass_tree.sql(dialect=dialect), params)

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
            stale_or_missing = [v for v in values if (v,) not in cached or cached[(v,)] < now]
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
    async with store_writer.store_connection(dsn) as conn:
        if schema and conn.capabilities.schemas:
            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
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
    if _is_duckdb_store(backend):
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
    if _is_duckdb_store(backend):
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
    resolved_ttl: int,
) -> None:
    """``_land_row_cache`` for an Arrow fetch: the same row stamps (``_row_cached_at`` now,
    ``_row_expires_at`` now + ttl), added as Arrow columns, and on a DuckDB store the same upsert
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
        pa.array([(now + timedelta(seconds=resolved_ttl)).replace(tzinfo=None)] * n, stamp),
    ]
    full_columns = list(columns) + [(_ROW_CACHED_AT, "timestamp"), (_ROW_EXPIRES_AT, "timestamp")]
    stamped = pa.Table.from_arrays(arrays, names=[c for c, _ in full_columns])
    if _is_duckdb_store(backend):
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
        resolved_ttl,
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

        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=resolved_ttl)
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
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()  # REQ-1901: resolves runtime._store_broker
        runtime._store_broker.tombstone_row_cache(schema, name, pk_columns, keys)
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
        keyed_adapter_loaders=build_keyed_adapter_loaders(state, engine),
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
        cache_table, cached = await _ensure_and_read_row_cache(
            engine, backend, state, schema, name, args.columns, pk_columns, list(bound.values)
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
    await ensure_rows_resident(state, plan.pk_bounds)
    pushed_down = await pushdown_row_materialize(
        state, plan.physical_sql, state.federation_engine.dialect, plan.exec_params
    )
    await ensure_resident(state, plan.sources, pushed_down=pushed_down)
