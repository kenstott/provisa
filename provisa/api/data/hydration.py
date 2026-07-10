# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""API-table hydration for the /data/graphql endpoint (REQ-140, REQ-848).

Pre-engine hydration of API/dataloader/collection/path-param source tables into
the local cache before engine execution. Extracted from endpoint.py; leaf module.
"""

# complexity-gate: allow-ble=2 reason="hydration paths relocated verbatim from endpoint.py; the broad excepts make per-source pre-engine hydration best-effort (a hydration failure falls back to engine execution, never fails the request)"

from __future__ import annotations

import logging
import time as _time


log = logging.getLogger(__name__)


# Source-level hydration expiry: source_id -> monotonic expiry.
# When set, the entire source is skipped (no pool acquire, no PG queries).
_source_hydration_expiry: dict[str, float] = {}


async def _hydrate_dataloader(
    src,
    endpoint,
    pg_table,
    pg_schema,
    ttl,
    source_id,
    dataloader_col,
    dataloader_parent_join_col,
    dataloader_parent_table_meta,
    state,
    hydration_rows: dict,
) -> None:
    """DataLoader branch: batch-fetch via query param list from parent PKs."""
    from provisa.openapi.pg_cache import fill_api_table

    async with state.tenant_db.acquire() as pg_conn:
        p_table = dataloader_parent_table_meta.table_name
        p_schema = (
            "default"
            if p_table in state.api_endpoints
            else dataloader_parent_table_meta.schema_name
        )
        try:
            rows = await pg_conn.fetch(
                f'SELECT DISTINCT "{dataloader_parent_join_col}" FROM "{p_schema}"."{p_table}"'
                f' WHERE "{dataloader_parent_join_col}" IS NOT NULL'
            )
            pk_values = [r[0] for r in rows]
        except Exception as exc:
            log.warning("DataLoader: failed to fetch parent PKs for %s: %s", pg_table, exc)
            return
        if pk_values:
            param_name = dataloader_col.param_name or dataloader_col.name
            n = await fill_api_table(
                src.base_url,
                endpoint.path,
                {param_name: pk_values},
                pg_conn,
                pg_schema,
                pg_table,
                ttl,
                endpoint.response_root,
                endpoint.error_path,
                endpoint.pk_column,
            )
            hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n


async def _hydrate_collection(
    src,
    endpoint,
    pg_table,
    pg_schema,
    ttl,
    source_id,
    compiled,
    state,
    hydration_rows: dict,
    cache_hit_sources: set,
) -> None:
    """Collection endpoint branch: skip if mem-fresh, else fill_api_table."""
    from provisa.openapi.pg_cache import fill_api_table, is_mem_fresh

    param_name_map = {
        c.name: (c.param_name or c.name) for c in endpoint.columns if c.param_type is not None
    }
    raw_params = compiled.api_args or {}
    query_params = {param_name_map.get(k, k): v for k, v in raw_params.items()}
    if is_mem_fresh("default", pg_table, query_params):
        cache_hit_sources.add(source_id)
        return
    async with state.tenant_db.acquire() as pg_conn:
        n = await fill_api_table(
            src.base_url,
            endpoint.path,
            query_params,
            pg_conn,
            pg_schema,
            pg_table,
            ttl,
            endpoint.response_root,
            endpoint.error_path,
            endpoint.pk_column,
        )
        hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n


async def _hydrate_path_param(
    src,
    endpoint,
    pg_table,
    pg_schema,
    ttl,
    source_id,
    path_col,
    ctx,
    state,
    hydration_rows: dict,
) -> bool:
    """Path-param branch: fetch one row per parent PK.

    Returns False if parent join is missing (caller should skip this table).
    """
    from provisa.openapi.pg_cache import fetch_pk_row

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

    async with state.tenant_db.acquire() as pg_conn:
        p_table = parent_table_meta.table_name
        p_schema = "default" if p_table in state.api_endpoints else parent_table_meta.schema_name
        try:
            rows = await pg_conn.fetch(
                f'SELECT DISTINCT "{parent_join_col}" FROM "{p_schema}"."{p_table}"'
                f' WHERE "{parent_join_col}" IS NOT NULL'
            )
            pk_values = [r[0] for r in rows]
        except Exception as exc:
            log.warning("Failed to fetch parent PKs for %s: %s", pg_table, exc)
            return True

        for pk in pk_values:
            n = await fetch_pk_row(
                src.base_url,
                endpoint.path,
                path_param_name,
                pk,
                pg_conn,
                pg_schema,
                pg_table,
                ttl,
                endpoint.response_root,
                endpoint.error_path,
            )
            hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n
    return True


async def _hydrate_api_tables_before_engine(
    compiled, ctx, state
) -> tuple[set, dict[str, float], dict[str, int], set]:
    """Ensure API-backed PG cache tables are populated before the engine executes.

    For each openapi source in compiled.sources:
    - Non-path-param: call fill_api_table (TTL-aware, keyed by params hash).
    - Path-param (returns single object per call): fetch one row per parent PK value
      via fetch_pk_row (TTL-aware, hash IS the PK for single-object responses).

    Returns (dataloader_sources, hydration_times_ms, hydration_rows, cache_hit_sources).
    """
    from provisa.api_source.models import ParamType

    dataloader_sources: set = set()
    hydration_times: dict[str, float] = {}
    hydration_rows: dict[str, int] = {}
    cache_hit_sources: set = set()
    if not hasattr(state, "api_endpoints") or not state.api_endpoints:
        return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources
    if state.tenant_db is None:
        return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources

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
            pg_schema = "default"
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
                    pg_table,
                    pg_schema,
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
                    pg_table,
                    pg_schema,
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
                    pg_table,
                    pg_schema,
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
