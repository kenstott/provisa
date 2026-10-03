# Copyright (c) 2026 Kenneth Stott
# Canary: a2f607a5-46d3-4bee-a094-3aaaf9ed6483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registered sources and tables a landing path drives off (REQ-1674).

The config file is not the registry. A source created through the Sources page and a table
registered through Register Table live in the control plane and never in ``state.config``, so a
landing path that read ``config.sources``/``config.tables`` — the event-loop wiring, the query-time
residency check, the pre-read land — saw none of them: the query reached the landed-replica name
before anything had landed it. These two readers give every landing path the same view: the
control plane's rows, with the config's per-source/per-table settings (passwords are secret refs
only the config carries; change signal, cadence, live block) laid over them where the config
declares the same id.
"""

from __future__ import annotations

from types import SimpleNamespace
from collections.abc import Iterable
from typing import Any

from provisa.core.models import BUILT_IN_SOURCE_IDS, Source


def _source_from_row(row: dict) -> Source:
    """A Source model from a control-plane row: the row's columns that are model fields, set.

    REQ-1695: including its password, which the row carries as the ``password_ref`` reference.
    """
    from provisa.core.repositories.source import source_from_row

    return source_from_row(row)


async def registered_sources(state: Any, conn: Any | None = None) -> list[Source]:  # REQ-1674
    """Every registered source: the config's Source where the config declares the id (it carries
    the operator's settings), else the control-plane row -- which since REQ-1695 carries its own
    password reference too. Built-in sources (provisa-admin, provisa-otel, the derived-view source)
    are never landed and stay out.

    REQ-1892: cached (TTL + schema-generation-keyed) when called on the pool-acquire path
    (``conn`` unset) -- this is read 2-3 times per governed call (`ensure_rows_resident`,
    `ensure_resident`, the replica build) with no cache before this, the same shape of finding
    REQ-1882 already fixed for `registered_tables` in this file. A caller supplying its own
    ``conn`` (already inside an explicit transaction) bypasses the cache, unchanged from before."""
    from provisa.core.repositories import source as source_repo
    from provisa.core.request_context import current_org
    from provisa.federation.registered_sources_cache import get_cache_for

    config = getattr(state, "config", None)
    by_id: dict[str, Source] = {s.id: s for s in (getattr(config, "sources", None) or [])}
    db = getattr(state, "tenant_db", None)
    if db is None:
        return list(by_id.values())
    if conn is not None:
        rows = await source_repo.list_all(conn)
        return _merge_source_rows(by_id, rows)

    generation = (
        current_org.get(None),
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    rs_cache = get_cache_for(state)
    cached = rs_cache.get(generation)
    if cached is not None:
        return cached
    async with db.acquire() as _conn:
        rows = await source_repo.list_all(_conn)
    out = _merge_source_rows(by_id, rows)
    rs_cache.put(generation, out)
    return out


def operator_floor(state: Any, table_ids: Iterable[int]) -> dict[str, str]:  # REQ-030, REQ-826
    """{source_id: operator setting} for the sources a statement may not read live, judged by the
    tables the STATEMENT reads (``table_ids``: the registered tables the pipeline resolved for
    it) — the floor ``decide_route`` enforces (REQ-030, amended 2026-09-30).

    A source is in the map when one of the statement's tables of it is put on its replica by the
    operator's settings (``replica_routing.table_floor``: load_protected, ``replicate: 0``, a
    promoted table, or a source floored as a whole). A statement that reads no such table keeps
    its direct route even when its source has other replicated tables. Read from the floored
    tables published with the replica routes at schema build, the same registry read the
    replicas are reconciled from, so routing and replication never disagree about a table."""
    floored = state.replica_routes.floored
    floor: dict[str, str] = {}
    for table_id in table_ids:
        entry = floored.get(table_id)
        if entry is not None:
            source_id, setting = entry
            floor.setdefault(source_id, setting)
    return floor


def _merge_source_rows(by_id: dict[str, Source], rows: list[dict]) -> list[Source]:
    """The control-plane rows merged over `by_id` (config-declared sources), factored out so both
    the cached (pool-acquire) and uncached (caller-supplied ``conn``) paths build identically."""
    by_id = dict(by_id)
    for row in rows:
        sid = row["id"]
        if sid in by_id or sid in BUILT_IN_SOURCE_IDS:
            continue
        by_id[sid] = _source_from_row(row)
    return list(by_id.values())


async def registered_tables(state: Any, conn: Any | None = None) -> list[Any]:  # REQ-1674
    """Every registered table, in the shape the landing paths read: the control plane's semantic
    sql name and resolved column types, with the config table's landing settings (live block, change
    signal, watermark, cadence, probe) where the config declares the same source + table.

    REQ-1882: cached (TTL + schema-generation-keyed) when called on the pool-acquire path
    (``conn`` unset) — this is read on every single governed query's residency/pk-bounds step with
    no cache before this, and was one of the blocking-work sources a live py-spy dump caught
    running in-line on the shared event loop under concurrent load. A caller supplying its own
    ``conn`` (already inside an explicit transaction) bypasses the cache, unchanged from before."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.request_context import current_org
    from provisa.federation.registered_tables_cache import get_cache_for

    config = getattr(state, "config", None)
    cfg_by = {
        (t.source_id, apply_sql_name(t.table_name)): t
        for t in (getattr(config, "tables", None) or [])
    }
    db = getattr(state, "tenant_db", None)
    if db is None:
        return []
    if conn is not None:
        registered = await fetch_tables(conn)
        return _build_registered_tables(registered, cfg_by)

    generation = (
        current_org.get(None),
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    rt_cache = get_cache_for(state)
    cached = rt_cache.get(generation)
    if cached is not None:
        return cached
    async with db.acquire() as _conn:
        registered = await fetch_tables(_conn)
    out = _build_registered_tables(registered, cfg_by)
    rt_cache.put(generation, out)
    return out


def _build_registered_tables(registered: list[dict], cfg_by: dict) -> list[Any]:
    """The SimpleNamespace-shaping loop `registered_tables` runs over `fetch_tables`' rows,
    factored out so both the cached (pool-acquire) and uncached (caller-supplied ``conn``) paths
    build identically-shaped rows."""
    out: list[Any] = []
    for rt in registered:
        cfg = cfg_by.get((rt["source_id"], rt["table_name"]))
        out.append(
            SimpleNamespace(
                id=rt["id"],
                source_id=rt["source_id"],
                schema_name=rt["schema_name"],
                table_name=rt["table_name"],
                columns=[
                    SimpleNamespace(
                        name=c["column_name"],
                        data_type=c["data_type"],
                        is_primary_key=c["is_primary_key"],
                        native_filter_type=c["native_filter_type"],
                    )
                    for c in rt["columns"]
                ],
                live=getattr(cfg, "live", None),
                # REQ-929: the table's own change signal is on its row — saved there by the config
                # load and by the admin alike — so a table registered at runtime (no config entry)
                # is judged by its own signal, not its source's. NULL = it sets none.
                change_signal=rt["change_signal"],
                watermark_column=getattr(cfg, "watermark_column", None),
                # REQ-1730: a table registered dynamically (through the UI, no YAML `tables:`
                # entry) has no `cfg` at all, so `getattr(cfg, "cache_ttl", None)` alone was ALWAYS
                # None for it, regardless of the Cache TTL an operator saved on it — TableEditForm
                # writes straight to `registered_tables.cache_ttl` (schema_mutation.py's
                # update_table), which `rt` (fetch_tables' own row, above) already carries. Prefer
                # that DB value; fall back to the static config only when the DB column is unset
                # (a config-declared table with no per-table override, the ORIGINAL case this
                # function's static-only lookup covered fine). Reproduced live: wire_new_poll_jobs
                # (app_wiring.py) could never wire a poll job for a UI-registered rss/poll table on
                # any backend it hadn't ALSO handled the Cache-TTL save on — `poll_seconds` came
                # back None every time, permanently (state.poll_jobs_registered marks a node
                # visited on the very first attempt, whether a job was actually wired or not).
                cache_ttl=(
                    rt["cache_ttl"]
                    if rt.get("cache_ttl") is not None
                    else getattr(cfg, "cache_ttl", None)
                ),
                # REQ-1907: the operator's per-role TTLs live on the registry row (config upsert
                # and the admin mutation both write it there).
                role_ttl=dict(rt["role_ttl"]),
                # REQ-826/REQ-1141 per-table settings (None = inherit the source's): a table they
                # put on its replica is read there even on an attach-capable engine.
                replicate=rt["replicate"],
                load_protected=rt["load_protected"],
                probe_type=getattr(cfg, "probe_type", None),  # REQ-982
                # REQ-1443: a checker table's rows are the results of running its contract, so
                # the registered contract rides with the table into make_dq_loader.
                dq_contract=rt["dq_contract"],
                # REQ-1865: fetch_tables already SELECTs this column; it was simply never carried
                # onto the returned object here, so every getattr(t, "row_materialize", False)
                # check downstream (ensure_resident, the replica build,
                # row_materialized_tables_by_name) silently saw False for every table regardless of
                # its real registration -- the row-level cache could never actually engage through
                # the registry view. Confirmed live: a row_materialize=True neo4j table's residency
                # was still being decided entirely by the whole-source path.
                row_materialize=bool(rt.get("row_materialize", False)),
                # REQ-1865: row_materialized_tables_by_name keys on this (apply_sql_name(t.alias or
                # t.table_name)) to match the SEMANTIC AST a query compiles to -- also never
                # surfaced here before. Dead code until row_materialize (above) actually started
                # returning True; would have raised AttributeError the moment it did.
                alias=rt.get("alias"),
            )
        )
    return out
