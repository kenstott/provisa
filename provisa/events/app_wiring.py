# Copyright (c) 2026 Kenneth Stott
# Canary: bc41ef90-272a-4f43-b998-301a3190c1a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wire the event loop into the live app at boot (REQ-941).

Called once after the scheduler starts: builds the node specs from config + the MV registry, builds
the processors, and registers the runtime jobs (tick / reaper / poll) on the embedded APScheduler.
Fully best-effort — any failure logs and returns, never bricks boot (the app runs without the loop).

``source_fetch`` reads a source's current rows through the engine terminal (``SourceRowLoader``) for
every SQL-federatable source, and through the openapi adapter (call_api → flatten, via
``make_openapi_loader`` over the registered endpoints + api-source config in state) for openapi
sources. Other API/push types (ingest, websocket, …) still have no wired fetch, so
``SourceRowLoader`` raises ``UnsupportedSourceFetch`` and the node lands nothing (logged once) until
theirs is added. ``mv_columns`` reads each MV's output columns from a LIMIT-0 probe of its SELECT
(the engine returns typed columns), translated native→IR. ``mv_run_query`` (the engine SELECT) and
everything else are real.
"""

# complexity-gate: allow-ble=1 reason="boot boundary: wire_event_loop must never propagate into app startup — it logs and the app runs without the loop"

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from provisa.events import supervisor
from provisa.events.boot import build_processors, register_runtime, specs_from_config


async def _load_calendar_registry(db: Any) -> Any:
    """REQ-962: build the shared :class:`CalendarRegistry` from the persisted ``calendars`` table so a
    periodic MV declaring ``(calendar, grain)`` resolves its boundary source at boot. Each row →
    a versioned :class:`Calendar` (the immutable holiday/business-day set). An empty table yields an
    empty registry; an MV that then declares an unknown calendar fails loud at wiring (never a silent
    default calendar)."""
    from datetime import date

    from sqlalchemy import select

    from provisa.core.schema_org import calendars
    from provisa.events.calendars import BaseSystem, Calendar, CalendarRegistry

    registry = CalendarRegistry()
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(calendars))).fetchall()
    for r in rows:
        m = r._mapping
        anchor = m["retail_anchor"]
        registry.register(
            Calendar(
                name=m["name"],
                version=m["version"],
                base_system=BaseSystem(m["base_system"]),
                tz=m["tz"],
                fiscal_anchor=(m["fiscal_anchor_month"], m["fiscal_anchor_day"]),
                retail_anchor=(anchor if isinstance(anchor, date) else None),
                week_start=m["week_start"],
                holidays=frozenset(date.fromisoformat(d) for d in (m["holidays"] or [])),
                weekend=frozenset(m["weekend"] or [5, 6]),
            )
        )
    return registry


def _mv_pk(mv: Any) -> list[str]:
    """REQ-970: the derived table's PK — operator-declared ``primary_key`` or inferred from an
    unambiguous GROUP BY (``[]`` when neither)."""
    from provisa.events.lineage import infer_pk

    declared = list(getattr(mv, "primary_key", []) or [])
    if declared:
        return declared
    return infer_pk(mv.sql) if getattr(mv, "sql", None) else []


async def _reconcile_mv_store_schemas(
    store_dsn: str, mvs: list[Any], mv_cols: dict[str, list[tuple[str, str]]], log: Any
) -> None:
    """REQ-970: reconcile each derived node's store table to its SELECT-derived output schema through
    the existing ``reconcile_table`` machinery — created when absent, KEPT when the shape matches,
    RECREATED (drop + reland next fire) when the SELECT drifts the output columns. Best-effort per MV
    at this boot boundary: a store/table not yet reachable is skipped (the per-fire persist_land
    re-creates if absent regardless)."""
    from sqlalchemy.exc import SQLAlchemyError

    from provisa.federation import store_writer

    for mv in mvs:
        key = f"{mv.target_schema}.{mv.target_table}"
        cols = mv_cols.get(key)
        if not cols:
            continue
        try:
            await store_writer.reconcile_table(
                store_dsn,
                schema=mv.target_schema,
                table=mv.target_table,
                columns=cols,
                pk_columns=_mv_pk(mv) or None,
            )
        except SQLAlchemyError:
            log.warning("event loop: MV %s store-schema reconcile skipped (store not ready)", key)


def _build_subscribers_of(
    mvs: list[Any], dependents_of: Callable[[str], list[str]]
) -> Callable[[str, str], list[str]]:
    """REQ-965: the demand-driven per-shape emit router. A producer emits a shape ONLY to the
    dependents that subscribe to it; a dependent MV subscribes to the shapes in its ``consumes`` set
    (default ``{replace}``). ``subscribers_of(node, shape)`` = the dependents of ``node`` whose
    consumes set includes ``shape``."""
    consumes_by_node = {
        f"{m.target_schema}.{m.target_table}": set(getattr(m, "consumes", ["replace"]) or [])
        for m in mvs
    }

    def subscribers_of(node: str, shape: str) -> list[str]:
        return [
            dep for dep in dependents_of(node) if shape in consumes_by_node.get(dep, {"replace"})
        ]

    return subscribers_of


async def wire_event_loop(scheduler: Any, *, state: Any, log: Any, seed: bool = True) -> int:
    """Build + register the event loop from live state. Returns the node count registered (0 if the
    prerequisites are not ready or the loop is skipped). Best-effort — never raises into boot.

    ``seed`` is True at boot (seed every source's first land). Pass False to RE-wire after a runtime
    change (e.g. an MV created in the UI) — poll jobs get (re)registered without re-landing sources."""
    try:
        db = getattr(state, "tenant_db", None)
        engine = getattr(state, "federation_engine", None)
        config = getattr(state, "config", None)
        if db is None or engine is None or config is None:
            log.info("event loop: prerequisites not ready — skipping")
            return 0
        from provisa.federation.engine import MaterializeStoreUnconfigured

        try:
            store_dsn = engine.materialize_store_dsn()
        except MaterializeStoreUnconfigured:
            log.info("event loop: no materialization store configured — skipping")
            return 0

        registry = getattr(state, "mv_registry", None)
        mvs = registry.get_enabled() if registry is not None else []

        # dependents fan-out set from the SQLGlot lineage over each MV's SQL (join-pattern MVs w/o SQL
        # contribute no edges here). A cycle is rejected — the loop must be acyclic.
        mv_sql = {
            f"{m.target_schema}.{m.target_table}": m.sql for m in mvs if getattr(m, "sql", None)
        }
        try:
            dependents_of = supervisor.dependents_of(mv_sql)
        except ValueError:
            log.warning("event loop: MV lineage has a cycle — skipping event-loop wiring")
            return 0

        _warned: set[str] = set()
        from provisa.events.source_loader import (
            SourceRowLoader,
            UnsupportedSourceFetch,
            make_graphql_remote_loader,
            make_openapi_loader,
        )

        # openapi/graphql_remote sources have no engine table; their rows come from calling the
        # operation (their registrations live in app state). Other adapter-only types
        # (ingest/websocket/…) still raise UnsupportedSourceFetch until their fetch is wired.
        _adapter_loaders: dict[str, Any] = {}
        _api_endpoints = getattr(state, "api_endpoints", None)
        _api_sources = getattr(state, "api_sources", None)
        if _api_endpoints and _api_sources is not None:
            _adapter_loaders["openapi"] = make_openapi_loader(_api_endpoints, _api_sources)
        _gql_sources = getattr(state, "graphql_remote_sources", None)
        if _gql_sources:
            _adapter_loaders["graphql_remote"] = make_graphql_remote_loader(_gql_sources)

        # files/sharepoint/splunk on an engine with NO connector for them are landed through the
        # connector's bundled Calcite pgwire server (REQ-954): resolve+cache the bundle, start it,
        # and SELECT as generic Postgres. Registered per pgwire-replica source; a shared allocator
        # hands each server a unique port (REQ-955). Skipped on engines that reach them natively.
        from provisa.federation.pgwire_replica import (
            PortAllocator,
            make_pgwire_loader,
            needs_pgwire_replica,
        )

        _bare_engine = getattr(engine, "engine", engine)
        _port_allocator = PortAllocator()
        for _src in config.sources:
            _stype = _src.type.value if hasattr(_src.type, "value") else str(_src.type)
            if _stype in _adapter_loaders:
                continue  # a type-level loader (openapi/graphql/pgwire) is already registered
            if needs_pgwire_replica(_src, _bare_engine):
                _adapter_loaders[_stype] = make_pgwire_loader(allocator=_port_allocator)

        row_loader = SourceRowLoader(engine, adapter_loaders=_adapter_loaders)

        def source_fetch(src: Any, tbl: Any) -> Any:
            async def _fetch(_pending: list[dict]) -> list[dict]:
                try:
                    return await row_loader.load(src, tbl)
                except UnsupportedSourceFetch:
                    # API/push source with no engine-scannable table — its adapter fetch is a
                    # per-adapter follow-up. Land nothing (logged once) rather than fail the node.
                    if src.id not in _warned:
                        log.warning(
                            "event loop: %s has no engine row-scan; adapter fetch not yet wired "
                            "— landing skipped",
                            src.id,
                        )
                        _warned.add(src.id)
                    return []

            return _fetch

        # Pre-introspect each MV's output columns via a LIMIT-0 probe: the engine returns typed
        # columns (QueryResult.column_types), which we translate native→IR (REQ-846). Done here
        # (async) so the sync mv_columns callable below is a pure lookup. Best-effort per MV — one
        # whose sources are not yet reachable is skipped and binds on a later boot.
        from provisa.core.ir_types import to_ir

        _mv_cols: dict[str, list[tuple[str, str]]] = {}
        _dialect = engine.engine.dialect
        for _m in mvs:
            _sql = getattr(_m, "sql", None)
            if not _sql:
                continue
            _key = f"{_m.target_schema}.{_m.target_table}"
            try:
                _probe = await engine.execute_engine(f"SELECT * FROM ({_sql}) AS _mv_probe LIMIT 0")
            except Exception:
                log.warning("event loop: MV %s not introspectable yet — skipping", _key)
                continue
            if _probe.column_types is None:
                log.warning(
                    "event loop: engine returned no column types for MV %s — skipping", _key
                )
                continue
            try:
                _mv_cols[_key] = [
                    (n, to_ir(t, _dialect))
                    for n, t in zip(_probe.column_names, _probe.column_types)
                ]
            except ValueError:
                log.warning("event loop: MV %s has an unmapped output type — skipping", _key)

        # REQ-970: reconcile each derived node's store table to its SELECT-derived schema (existing
        # reconcile_table machinery). REQ-965: build the demand-driven per-shape emit router. Both in
        # module helpers to keep this boot function within the complexity ceiling.
        await _reconcile_mv_store_schemas(store_dsn, mvs, _mv_cols, log)
        subscribers_of = _build_subscribers_of(mvs, dependents_of)

        def mv_columns(mv: Any) -> list[tuple[str, str]] | None:
            return _mv_cols.get(f"{mv.target_schema}.{mv.target_table}")

        def mv_run_query(mv: Any) -> Any:
            async def _run() -> list[dict]:
                result = await engine.execute_engine(mv.sql)
                return [dict(zip(result.column_names, row)) for row in result.rows]

            return _run

        # REQ-1162/1166/1167: the append entry a bitemporal MV's event-loop generate calls per fire.
        # ``engine`` is the FederationEngine (execute_engine + dialect), the same handle refresh_mv
        # uses; ``system_ts`` (from the calendar window.end) makes a periodic seal deterministic.
        def mv_bitemporal_append(mv: Any) -> Any:
            async def _append(system_ts: str | None) -> None:
                from provisa.mv.refresh import apply_bitemporal_append

                await apply_bitemporal_append(engine, mv, system_ts=system_ts)

            return _append

        # Drive the loop off the design-time REGISTERED tables (semantic sql names + resolved types),
        # not the raw YAML — the landed replica name must match what the schema-currency reconcile
        # created, and the types the YAML omits are resolved in the control plane at registration.
        # Change-signal / cadence / live block stay owned by config (matched on the semantic name).
        from types import SimpleNamespace

        from provisa.api.admin.db_queries import fetch_tables
        from provisa.compiler.naming import apply_sql_name
        from urllib.parse import urlparse

        _store_scheme = urlparse(store_dsn).scheme.split("+", 1)[0]
        store_schema = "main" if _store_scheme == "sqlite" else "mat"
        _cfg_by = {(t.source_id, apply_sql_name(t.table_name)): t for t in config.tables}
        async with db.acquire() as _conn:
            _registered = await fetch_tables(_conn)
        registered_tables = []
        for _rt in _registered:
            _cfg = _cfg_by.get((_rt["source_id"], _rt["table_name"]))
            registered_tables.append(
                SimpleNamespace(
                    source_id=_rt["source_id"],
                    schema_name=_rt["schema_name"],
                    table_name=_rt["table_name"],
                    columns=[
                        SimpleNamespace(
                            name=_c["column_name"],
                            data_type=_c["data_type"],
                            is_primary_key=_c["is_primary_key"],
                            native_filter_type=_c["native_filter_type"],
                        )
                        for _c in _rt["columns"]
                    ],
                    live=getattr(_cfg, "live", None),
                    change_signal=getattr(_cfg, "change_signal", None),
                    watermark_column=getattr(_cfg, "watermark_column", None),
                    cache_ttl=getattr(_cfg, "cache_ttl", None),
                    probe_type=getattr(_cfg, "probe_type", None),  # REQ-982
                )
            )

        # REQ-982: the SQL scalar runner a watermark/count probe uses to read the source through the
        # engine terminal — the same read path as the row loader, returning the single scalar.
        def probe_scalar(_src: Any, _tbl: Any) -> Any:
            async def _scalar(sql: str) -> Any:
                result = await engine.execute_engine(sql)
                return result.rows[0][0] if result.rows else None

            return _scalar

        # REQ-962: the shared calendar registry (periodic MV boundary source). REQ-961: the real
        # per-input freshness reader the periodic contract PULLs at fire time (persisted refresh state).
        from provisa.events.freshness_reader import make_db_freshness_of
        from provisa.federation.engine import UnreachableSource
        from provisa.federation.strategy import Strategy, federate

        calendar_registry = await _load_calendar_registry(db)
        # REQ-961: a LIVE/scan input federates in place (served at query time), so it is current as of
        # now and fresh-through any snapshot boundary — it never lands, so its missing refresh stamp is
        # expected, not an outage. Collect those nodes so the freshness reader treats them as fresh;
        # a MATERIALIZED input with no stamp still fails loud (it was supposed to refresh).
        _src_by_id = {s.id: s for s in config.sources}
        always_current: set[str] = set()
        for _tbl in registered_tables:
            _src = _src_by_id.get(_tbl.source_id)
            if _src is None:
                continue
            try:
                if federate(_src, engine.engine) is not Strategy.MATERIALIZED:
                    always_current.add(f"{_tbl.schema_name}.{_tbl.table_name}")
            except UnreachableSource:
                pass  # unreachable → a frozen one-shot snapshot, not always-current
        freshness_of = make_db_freshness_of(db, always_current)

        specs = specs_from_config(
            sources=config.sources,
            tables=registered_tables,
            mvs=mvs,
            engine=engine.engine,  # the FederationEngine (federate classification)
            store_dsn=store_dsn,
            source_fetch=source_fetch,
            mv_columns=mv_columns,
            mv_run_query=mv_run_query,
            store_schema=store_schema,
            probe_scalar=probe_scalar,
            subscribers_of=subscribers_of,  # REQ-965 demand routing
            calendar_registry=calendar_registry,  # REQ-962 periodic boundary source
            freshness_of=freshness_of,  # REQ-961 per-input freshness contract reader
            mv_bitemporal_append=mv_bitemporal_append,  # REQ-1162/1166/1167 append entry
        )
        processors = build_processors(specs, db=db, dependents_of=dependents_of)
        # register_runtime schedules the tick/reaper, each poll node's job, AND a one-shot boot-create
        # job: replicas are BUILT at boot (that job lands every source + fans out to its MVs), then
        # REFRESHED by the poll/push events.
        register_runtime(scheduler, db=db, processors=processors, specs=specs, seed=seed)
        log.info(
            "event loop wired: %d node(s) on the scheduler (boot-create + refresh scheduled)",
            len(processors),
        )
        return len(processors)
    except Exception:
        log.exception("event loop wiring failed — the app runs without it")
        return 0
