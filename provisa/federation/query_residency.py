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
    sources = [s for s in await registered_sources(state) if s.id in wanted]
    if not sources:
        return []
    tables_by_source: dict[str, list[Any]] = {}
    for t in await registered_tables(state):
        if t.source_id in wanted:
            tables_by_source.setdefault(t.source_id, []).append(t)

    from provisa.events import queue
    from provisa.events.app_wiring import build_adapter_loaders
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
    loader = SourceRowLoader(engine, adapter_loaders=build_adapter_loaders(state, engine))

    landed: list[tuple[str, str]] = []
    from contextlib import AsyncExitStack

    from provisa.events.land_lock import land_lock

    for source in sources:
        # The same per-node locks the event loop's land takes, so the boot land and a first query
        # never interleave on one replica; every node of the source is held for the source's land.
        async with AsyncExitStack() as held:
            for t in tables_by_source.get(source.id, []):
                await held.enter_async_context(land_lock(_node(t.schema_name, t.table_name)))
            try:
                landed += await backend.materialize_pending(
                    state,
                    loader=loader,
                    source_ids={source.id},
                    is_stale=is_stale_of(sources, stamps, oks, now),
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
