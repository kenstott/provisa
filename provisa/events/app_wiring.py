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

Two live collaborators are still per-source/per-engine follow-ups and are declared here explicitly:
- ``source_fetch`` — a source's change-event → its current rows. No generic primitive exists (each
  adapter is per-operation), so this is stubbed (logs once, lands nothing) pending the per-adapter
  loader. Source *nodes* are registered; they just land nothing until the loader is wired.
- ``mv_columns`` — an MV's output column types, which are not in the model and need a live engine
  introspection of the SELECT. Returns None for now, so MV nodes are skipped until that lands.
``mv_run_query`` (the engine SELECT) and everything else are real.
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

        def source_fetch(src: Any, tbl: Any) -> Any:
            async def _fetch(_pending: list[dict]) -> list[dict]:
                if src.id not in _warned:
                    log.warning(
                        "event loop: source fetch for %s not yet wired per adapter — landing skipped",
                        src.id,
                    )
                    _warned.add(src.id)
                return []

            return _fetch

        def mv_columns(_mv: Any) -> list[tuple[str, str]] | None:
            return None  # live output-column introspection pending — MV nodes skipped until then

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
