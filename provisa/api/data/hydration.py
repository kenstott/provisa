# Copyright (c) 2026 Kenneth Stott
# Canary: 8cecaf23-1646-4926-8b18-37a14a446d6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""API-table hydration for the /data/graphql endpoint (REQ-140, REQ-848, REQ-1915).

Before the engine executes, the calls an API-backed table of the request needs (its
collection with this request's arguments, a batch of parent keys, one call per parent key)
are made and their answers kept as fills in the store's API cache schema
(``api_source.fill_cache``) — never in the control plane. Extracted from endpoint.py; leaf
module.
"""

# complexity-gate: allow-ble=1 reason="a parent that is not an API table is read from the tenant plane, where it exists only when its table lives in that database; a failed read of it is logged and its dependents are not fetched for, as before the fills moved to the store. A fill's own failure (the remote, the store) is not caught: it fails the request (REQ-1661)"

from __future__ import annotations

import logging
import time as _time


log = logging.getLogger(__name__)


# Source-level hydration expiry: source_id -> monotonic expiry.
# When set, the entire source is skipped (no pool acquire, no PG queries).
_source_hydration_expiry: dict[str, float] = {}


def _ident(name: str) -> str:
    """``name`` as a double-quoted SQL identifier for the control-plane connection."""
    if "\x00" in name:
        raise ValueError(f"identifier contains a NUL character: {name!r}")
    return '"' + name.replace('"', '""') + '"'


async def _parent_keys(state, parent_table_meta, parent_join_col: str, child: str) -> list | None:
    """The distinct keys of the parent a dependent API table is fetched for, or None when
    there is nothing to fetch from (logged).

    An API parent's rows are its own fills, in the store. Any other parent is read where it
    was read before this function moved the fills: from the tenant plane, under its registered
    schema — a read, and one that only finds a parent whose table lives in that database."""
    from provisa.api_source import fill_cache

    p_table = parent_table_meta.table_name
    parent_ep = state.api_endpoints.get(p_table)
    if parent_ep is not None:
        table = fill_cache.fill_table(
            state, parent_ep, (state.api_sources or {}).get(parent_ep.source_id)
        )
        if parent_join_col not in table.data_columns:
            log.warning(
                "%s joins %s on %r, which is not a response column of it: nothing to fetch for",
                child,
                p_table,
                parent_join_col,
            )
            return None
        with state.federation_engine.isolated_sync() as conn:
            return fill_cache.distinct_values(conn, table, parent_join_col)
    if state.tenant_db is None:
        log.warning("no tenant plane to read the keys of %s from, for %s", p_table, child)
        return None
    async with state.tenant_db.acquire() as pg_conn:
        column = _ident(parent_join_col)
        relation = (
            f"{_ident(parent_table_meta.schema_name)}.{_ident(p_table)}"
            if pg_conn.capabilities.schemas
            else _ident(p_table)
        )
        try:
            rows = await pg_conn.fetch(
                f"SELECT DISTINCT {column} FROM {relation} WHERE {column} IS NOT NULL"
            )
        except Exception as exc:
            log.warning("failed to fetch parent keys of %s for %s: %s", p_table, child, exc)
            return None
    return [r[0] for r in rows]


async def _hydrate_dataloader(
    src,
    endpoint,
    ttl,
    source_id,
    dataloader_col,
    dataloader_parent_join_col,
    dataloader_parent_table_meta,
    state,
    hydration_rows: dict,
) -> None:
    """DataLoader branch: one call for the batch of parent keys, as a query-parameter list."""
    from provisa.api_source import fill_cache

    keys = await _parent_keys(
        state, dataloader_parent_table_meta, dataloader_parent_join_col, endpoint.table_name
    )
    if not keys:
        return
    param_name = dataloader_col.param_name or dataloader_col.name
    n = await fill_cache.fill(state, endpoint, src, [{param_name: keys}], ttl)
    hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n


async def _hydrate_collection(
    src,
    endpoint,
    ttl,
    source_id,
    compiled,
    state,
    hydration_rows: dict,
    cache_hit_sources: set,
) -> None:
    """Collection branch: the call for this request's arguments, unless its fill is fresh."""
    from provisa.api_source import fill_cache

    param_name_map = {
        c.name: (c.param_name or c.name) for c in endpoint.columns if c.param_type is not None
    }
    raw_params = compiled.api_args or {}
    query_params = {param_name_map.get(k, k): v for k, v in raw_params.items()}
    if fill_cache.is_mem_fresh(fill_cache.fill_table(state, endpoint, src), query_params):
        cache_hit_sources.add(source_id)
        return
    n = await fill_cache.fill(state, endpoint, src, [query_params], ttl)
    hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n


async def _hydrate_path_param(
    src,
    endpoint,
    ttl,
    source_id,
    path_col,
    ctx,
    state,
    hydration_rows: dict,
) -> bool:
    """Path-parameter branch: one call per parent key.

    Returns False if parent join is missing (caller should skip this table).
    """
    from provisa.api_source import fill_cache

    pg_table = endpoint.table_name
    path_param_name = path_col.param_name or path_col.name
    parent_join_col = None
    parent_table_meta = None
    for (src_type, _), join_meta in ctx.joins.items():
        if join_meta.target.table_name == pg_table:
            parent_join_col = join_meta.source_column
            for tbl_meta in ctx.tables.values():
                if tbl_meta.type_name == src_type:
                    parent_table_meta = tbl_meta
                    break
            break

    if parent_table_meta is None or parent_join_col is None:
        log.warning("No parent join for path-param table %s — skipping hydration", pg_table)
        return False

    keys = await _parent_keys(state, parent_table_meta, parent_join_col, pg_table)
    if keys:
        n = await fill_cache.fill(
            state, endpoint, src, [{path_param_name: str(key)} for key in keys], ttl
        )
        hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n
    return True


async def _hydrate_api_tables_before_engine(
    compiled, ctx, state
) -> tuple[set, dict[str, float], dict[str, int], set]:
    """Ensure the fills an API-backed table of this request needs are in the store before the
    engine executes (``api_source.fill_cache``: TTL-aware, keyed by the hash of the arguments).

    For each API source in compiled.sources:
    - Non-path-param: the call for this request's arguments, or one call for the batch of
      parent keys when a query parameter is the target of a join.
    - Path-param (returns single object per call): one call per parent key.

    Returns (dataloader_sources, hydration_times_ms, hydration_rows, cache_hit_sources).
    """
    from provisa.api_source.models import ParamType

    dataloader_sources: set = set()
    hydration_times: dict[str, float] = {}
    hydration_rows: dict[str, int] = {}
    cache_hit_sources: set = set()
    if not hasattr(state, "api_endpoints") or not state.api_endpoints:
        return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources
    # REQ-1865: a table replicated row by row (row_materialize) has no whole-table API cache table
    # to fill — its rows live in the row-level replica, filled by key (ensure_rows_resident). The
    # same rule _materialize_api_to_engine_cache applies on the raw-SQL/compiled path. Filling it
    # here read a "default" cache table that does not exist and failed every GraphQL query that
    # touched the source.
    _row_level_tables = {
        t.get("table_name")
        for t in (getattr(state, "tables", None) or [])
        if t.get("row_materialize")
    }

    for source_id in compiled.sources:
        _t_src = _time.perf_counter()
        if _source_hydration_expiry.get(source_id, 0) > _time.monotonic():
            hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
            cache_hit_sources.add(source_id)
            continue
        src = (state.api_sources or {}).get(source_id)
        if src is None:
            continue
        _min_ttl = None
        for table_name, endpoint in state.api_endpoints.items():
            if endpoint.source_id != source_id:
                continue
            if table_name in _row_level_tables:
                continue
            pg_table = table_name
            ttl = endpoint.ttl
            _min_ttl = ttl if _min_ttl is None else min(_min_ttl, ttl)

            path_cols = [c for c in endpoint.columns if c.param_type == ParamType.path]

            # DataLoader candidate: a query param column that is the FK target of a join.
            dataloader_col = None
            dataloader_parent_join_col = None
            dataloader_parent_table_meta = None
            for (src_type, _), join_meta in ctx.joins.items():
                if join_meta.target.table_name == pg_table:
                    target_col = next(
                        (
                            c
                            for c in endpoint.columns
                            if c.name == join_meta.target_column and c.param_type == ParamType.query
                        ),
                        None,
                    )
                    if target_col:
                        dataloader_col = target_col
                        dataloader_parent_join_col = join_meta.source_column
                        for tbl_meta in ctx.tables.values():
                            if tbl_meta.type_name == src_type:
                                dataloader_parent_table_meta = tbl_meta
                                break
                        break

            if dataloader_col is not None and dataloader_parent_table_meta is not None:
                dataloader_sources.add(source_id)
                await _hydrate_dataloader(
                    src,
                    endpoint,
                    ttl,
                    source_id,
                    dataloader_col,
                    dataloader_parent_join_col,
                    dataloader_parent_table_meta,
                    state,
                    hydration_rows,
                )
            elif not path_cols:
                await _hydrate_collection(
                    src,
                    endpoint,
                    ttl,
                    source_id,
                    compiled,
                    state,
                    hydration_rows,
                    cache_hit_sources,
                )
            else:
                await _hydrate_path_param(
                    src,
                    endpoint,
                    ttl,
                    source_id,
                    path_cols[0],
                    ctx,
                    state,
                    hydration_rows,
                )

        hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
        if _min_ttl is not None:
            _source_hydration_expiry[source_id] = _time.monotonic() + _min_ttl

    return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources
