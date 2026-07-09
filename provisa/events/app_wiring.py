# Copyright (c) 2026 Kenneth Stott
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

``source_fetch`` reads a source's current rows through the engine terminal (``SourceRowLoader``);
every SQL-federatable source works. Row-oriented API / push sources (openapi, ingest, …) have no
engine-scannable table — their adapter fetch is a per-adapter follow-up, so ``SourceRowLoader``
raises ``UnsupportedSourceFetch`` and the node lands nothing (logged once) until that is wired.
``mv_columns`` reads each MV's output columns from a LIMIT-0 probe of its SELECT (the engine returns
typed columns), translated native→IR. ``mv_run_query`` (the engine SELECT) and everything else are
real.
"""

# complexity-gate: allow-ble=1 reason="boot boundary: wire_event_loop must never propagate into app startup — it logs and the app runs without the loop"

from __future__ import annotations

from typing import Any

from provisa.events import supervisor
from provisa.events.boot import build_processors, register_runtime, specs_from_config


async def wire_event_loop(scheduler: Any, *, state: Any, log: Any) -> int:
    """Build + register the event loop from live state. Returns the node count registered (0 if the
    prerequisites are not ready or the loop is skipped). Best-effort — never raises into boot."""
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
        from provisa.events.source_loader import SourceRowLoader, UnsupportedSourceFetch

        row_loader = SourceRowLoader(engine)  # reads source rows via the engine terminal

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

        def mv_columns(mv: Any) -> list[tuple[str, str]] | None:
            return _mv_cols.get(f"{mv.target_schema}.{mv.target_table}")

        def mv_run_query(mv: Any) -> Any:
            async def _run() -> list[dict]:
                result = await engine.execute_engine(mv.sql)
                return [dict(zip(result.column_names, row)) for row in result.rows]

            return _run

        specs = specs_from_config(
            sources=config.sources,
            tables=config.tables,
            mvs=mvs,
            engine=engine.engine,  # the FederationEngine (federate classification)
            store_dsn=store_dsn,
            source_fetch=source_fetch,
            mv_columns=mv_columns,
            mv_run_query=mv_run_query,
        )
        processors = build_processors(specs, db=db, dependents_of=dependents_of)
        register_runtime(scheduler, db=db, processors=processors, specs=specs)
        log.info(
            "event loop wired: %d node(s) on the scheduler (source fetch + MV columns pending)",
            len(processors),
        )
        return len(processors)
    except Exception:
        log.exception("event loop wiring failed — the app runs without it")
        return 0
