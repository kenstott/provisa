# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""API-source materialization for the /data/graphql endpoint (REQ-848, REQ-941).

Fetch + schema-less-store of API / graphql_remote / gRPC source rows into the
engine cache (VALUES-CTE rewrites). Extracted from endpoint.py; leaf module.
"""

# complexity-gate: allow-cc=32 allow-ble=4 reason="materialization fetch/store paths relocated verbatim from endpoint.py; the broad excepts make API-source landing best-effort (fall back to live execution, never fail the query) per REQ-848/REQ-941; per-route split is separately-tracked debt"

from __future__ import annotations

import asyncio
import json
import logging


log = logging.getLogger(__name__)


def _lookup_ep(state, table_name: str):
    """Find API endpoint by table name."""
    ep_map: dict = getattr(state, "api_endpoints", {})
    return ep_map.get(table_name)


def _lookup_gql_remote_table(state, table_name: str):
    """Find a graphql_remote table registration by SQL table name (snake_case or camelCase)."""
    from provisa.compiler.naming import apply_sql_name as _asn

    normalised = _asn(table_name)
    gql_srcs = getattr(state, "graphql_remote_sources", {})
    for reg in gql_srcs.values():
        for tbl in reg.get("tables", []):
            if tbl["sql_name"] == table_name or tbl["sql_name"] == normalised:
                return reg, tbl
    return None, None


async def _promote_joined_from_pg(
    state, ep, tn, hot_mgr, col_names, meta_cols, cache_loc, hot_threshold
) -> None:
    """Fetch joined API table rows from PG and store in hot_mgr for next-request Values CTE."""
    import json as _json

    try:
        async with state.tenant_db.acquire() as _pg_conn:
            _raw = await _pg_conn.fetch(f'SELECT * FROM "default"."{ep.table_name}"')
        _col_set = set(col_names)
        rows = []
        for r in _raw:
            row = {}
            for k, v in dict(r).items():
                if k in meta_cols or k not in _col_set:
                    continue
                if isinstance(v, (dict, list)):
                    row[k] = _json.dumps(v)
                elif v is None:
                    row[k] = None
                elif not isinstance(v, (int, float, bool)):
                    row[k] = str(v)
                else:
                    row[k] = v
            rows.append(row)
        if 0 < len(rows) <= hot_threshold:
            from provisa.cache.hot_tables import HotTableEntry

            hot_mgr._hot_tables[tn] = HotTableEntry(
                table_name=tn,
                catalog=cache_loc.catalog,
                schema=cache_loc.schema,
                pk_column=col_names[0] if col_names else "id",
                rows=rows,
                column_names=col_names,
                is_api=True,
            )
            log.warning(
                "[MAT] promoted %s → hot_mgr (%d rows) for next-request Values CTE", tn, len(rows)
            )
    except Exception as exc:
        log.warning("[MAT] _promote_joined_from_pg failed for %s: %s", tn, exc)


def _normalize_mat_value(v):
    """Normalize a value for materialization into the engine cache (VARCHAR/scalar types)."""
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    return str(v)


async def _fetch_gql_remote_rows(
    gql_reg, gql_tbl, col_selections, variables, gql_to_sql, max_items
):
    """Fetch a graphql_remote field (with its native-filter args) and remap each row's GQL field
    keys to the sql column names the store lands under. A single-record field returns null (→ [None])
    when nothing matches — drop non-dict rows so the caller lands an empty result, not a crash."""
    from provisa.graphql_remote.executor import execute_remote

    rows = await execute_remote(
        url=gql_reg["url"],
        auth=gql_reg.get("auth"),
        field_name=gql_tbl.get("field_name") or gql_tbl["name"],
        columns=col_selections,
        variables=variables or None,
        required_args=gql_tbl.get("required_args") or None,
        limit=max_items,
        pagination=gql_tbl.get("pagination"),
    )
    return [
        {gql_to_sql.get(k, k): v for k, v in row.items()} for row in rows if isinstance(row, dict)
    ]


async def _mat_gql_remote_table(
    tn: str,
    gql_reg: dict,
    gql_tbl: dict,
    state,
    hot_mgr,
    _hot_threshold: int,
    cache_rewrites: dict,
    values_cte_entries: dict,
    extra_selections: dict[str, str] | None = None,
    variables: dict | None = None,
) -> None:
    """Materialize a graphql_remote-backed table into the engine cache or VALUES CTE."""
    from provisa.api_source.engine_cache import (
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        land_api_cache,
        resolved_cache_catalog,
        schedule_drop,
        table_known_live,
    )
    from provisa.cache.hot_tables import HotTableEntry
    from provisa.executor.redirect import RedirectConfig
    from dataclasses import dataclass as _dc

    @_dc
    class _GCol:
        name: str
        type: str

    _GQL_TYPE_MAP = {
        "text": "string",
        "integer": "integer",
        "numeric": "number",
        "boolean": "boolean",
        "jsonb": "jsonb",
    }
    col_dicts = list(gql_tbl.get("columns", []))
    if extra_selections:
        _existing_names = {c["name"] for c in col_dicts}
        for _fname, _gql_sel in extra_selections.items():
            if _fname not in _existing_names:
                col_dicts.append({"name": _fname, "type": "jsonb", "gql_selection": _gql_sel})
    _gql_srcs = getattr(state, "graphql_remote_sources", {})
    _governed_gql_types = {
        tbl.get("gql_type_name")
        for reg in _gql_srcs.values()
        for tbl in reg.get("tables", [])
        if tbl.get("gql_type_name")
    }
    if _governed_gql_types:
        _governed_excluded = {
            c["name"]
            for c in col_dicts
            if c.get("gql_object_type")
            and not c.get("gql_is_list", False)
            and c["gql_object_type"] in _governed_gql_types
        }
        if _governed_excluded:
            col_dicts = [c for c in col_dicts if c["name"] not in _governed_excluded]
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    def _sel_from_obj_fields(fname: str, sub_fields: list) -> str:
        parts = []
        for sf in sub_fields or []:
            if sf.get("fields"):
                parts.append(_sel_from_obj_fields(sf["name"], sf["fields"]))
            else:
                parts.append(sf["name"])
        return f"{fname} {{ {' '.join(parts)} }}" if parts else fname

    # Synthesize gql_selection from gql_object_fields when not explicitly set
    for _c in col_dicts:
        if not _c.get("gql_selection") and _c.get("gql_object_fields"):
            _c["gql_selection"] = _sel_from_obj_fields(_c["name"], _c["gql_object_fields"])

    # Map raw GQL field name → SQL name (snake_case) so CTE headers match SQL column refs
    _gql_to_sql = {c["name"]: _apply_sql_name(c["name"]) for c in col_dicts}
    col_names = [_gql_to_sql[c["name"]] for c in col_dicts]
    col_selections = [c.get("gql_selection", c["name"]) for c in col_dicts]
    col_objs = [
        _GCol(name=_gql_to_sql[c["name"]], type=_GQL_TYPE_MAP.get(c.get("type", "text"), "string"))
        for c in col_dicts
    ]

    _org_id = getattr(state, "org_id", "default")
    _cache_cat = resolved_cache_catalog(state.federation_engine)
    gql_cache_loc = cache_location(gql_reg["source_id"], _cache_cat, f"org_{_org_id}_gql_cache")
    _cache_hash: dict = {"cols": sorted(col_selections)}
    if variables:
        _cache_hash.update(variables)
    gql_cache_tbl = cache_table_name(gql_reg["source_id"], tn, _cache_hash)

    redirect_config = RedirectConfig.from_env()

    # A schema-less materialization store (SQLite) has no separate cache schema to CREATE, so the
    # engine-cache path (ensure_cache_schema → CREATE SCHEMA) is unavailable. Fetch fresh from the
    # remote and inject the result INLINE as a VALUES CTE — the parameterized fetch is bounded by
    # max_list_items, and an empty result still injects an empty CTE (query returns []).
    from urllib.parse import urlparse as _urlparse

    _store_scheme = _urlparse(state.federation_engine.materialize_store_dsn()).scheme.split("+", 1)[
        0
    ]
    _max_items = state.config.graphql_remote.max_list_items
    if _store_scheme == "sqlite":
        gql_rows = await _fetch_gql_remote_rows(
            gql_reg, gql_tbl, col_selections, variables, _gql_to_sql, _max_items
        )
        # Inline THIS query only — never register in hot_mgr: a parameterized fetch is keyed by its
        # arg, so caching it under the bare table name would serve one arg's rows for another.
        values_cte_entries[tn] = HotTableEntry(
            table_name=tn,
            catalog=gql_cache_loc.catalog,
            schema="main",
            pk_column=col_names[0] if col_names else "id",
            rows=gql_rows,
            column_names=col_names,
            is_api=True,
        )
        return

    # Cache hit — only trust in-process table_known_live
    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, gql_cache_loc)
    if table_known_live(gql_cache_loc, gql_cache_tbl):
        cache_rewrites[tn] = (gql_cache_loc, gql_cache_tbl)
        return

    # Cache miss — fetch from remote
    try:
        gql_rows = await _fetch_gql_remote_rows(
            gql_reg, gql_tbl, col_selections, variables, _gql_to_sql, _max_items
        )
    except Exception as fetch_exc:
        raise RuntimeError(f"GQL remote fetch failed for {tn!r}: {fetch_exc}") from fetch_exc

    # Hydrate to the engine cache (best-effort)
    try:
        await land_api_cache(
            state.federation_engine, gql_cache_loc, gql_cache_tbl, gql_rows, col_objs
        )
        asyncio.create_task(
            schedule_drop(
                state.federation_engine, gql_cache_loc, gql_cache_tbl, 300, redirect_config
            )
        )
    except Exception as cache_exc:
        log.warning("[GQL REMOTE] cache write failed for %s: %s", tn, cache_exc)

    # Inline as VALUES CTE if below threshold; else use cache rewrite
    if 0 < len(gql_rows) <= _hot_threshold:
        entry = HotTableEntry(
            table_name=tn,
            catalog=gql_cache_loc.catalog,
            schema=gql_cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            rows=gql_rows,
            column_names=col_names,
            is_api=True,
        )
        if hot_mgr is not None:
            hot_mgr._hot_tables[tn] = entry
        values_cte_entries[tn] = entry
        log.warning("[GQL REMOTE] VALUES CTE inline for %s (%d rows)", tn, len(gql_rows))
    else:
        cache_rewrites[tn] = (gql_cache_loc, gql_cache_tbl)
        log.warning(
            "[GQL REMOTE] %d rows → the engine cache %s.%s.%s",
            len(gql_rows),
            gql_cache_loc.catalog,
            gql_cache_loc.schema,
            gql_cache_tbl,
        )


async def _mat_fetch_rows_from_pg(ep, col_names: list, _META_COLS: set, state) -> tuple[list, bool]:
    """Fetch rows for an API endpoint from the PG cache table.

    Returns (rows, pg_ok).
    """
    rows: list[dict] = []
    pg_ok = False
    if getattr(state, "tenant_db", None) is None:
        return rows, pg_ok
    try:
        async with state.tenant_db.acquire() as _pg_conn:
            _raw = await _pg_conn.fetch(f'SELECT * FROM "default"."{ep.table_name}"')
        col_set = set(col_names)
        for r in _raw:
            row = {
                k: _normalize_mat_value(v)
                for k, v in dict(r).items()
                if k not in _META_COLS and k in col_set
            }
            rows.append(row)
        pg_ok = True
    except Exception as exc:
        log.warning("[MAT] PG read failed for %s: %s — trying REST", ep.table_name, exc)
    return rows, pg_ok


async def _mat_fetch_rows_from_rest(
    ep,
    col_names: list,
    engine,
    api_source,
    source_id,
    state,
    _cache_loc,
    cache_tbl,
    cache_rewrites: dict,
) -> list | None:
    """Fetch rows for an API endpoint from REST fallback.

    Returns rows list, or None if the table is in cache_rewrites (already handled).
    Raises on unrecoverable REST failure.
    """
    from provisa.api_source.router_integration import handle_api_query

    rest_result = await handle_api_query(
        ep,
        {},
        engine,
        source=api_source,
        source_ttl=getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
        global_ttl=getattr(state, "response_cache_default_ttl", None),
        loc=_cache_loc,
    )
    log.warning(
        "[MAT] REST fallback for %s: from_cache=%s rows=%d",
        ep.table_name,
        rest_result.from_cache,
        len(rest_result.rows),
    )
    if rest_result.from_cache:
        cache_rewrites[ep.table_name] = (_cache_loc, cache_tbl)
        return None
    col_set = set(col_names)
    return [
        {k: _normalize_mat_value(v) for k, v in r.items() if k in col_set} for r in rest_result.rows
    ]


def _mat_store_rows(
    tn: str,
    rows: list,
    col_names: list,
    _cache_loc,
    cache_tbl: str,
    _hot_threshold: int,
    hot_mgr,
    response_cols: list,
    engine,
    ttl,
    redirect_config,
    cache_rewrites: dict,
    values_cte_entries: dict,
    all_ep_col_names: list | None = None,
) -> None:
    """ALWAYS persist rows to the materialization store (the durable source of truth), then inline a
    small table as a VALUES CTE for this query — the hot cache is a rebuildable projection of the
    store, so an inlined small table survives a restart (re-promoted from the store, not re-fetched)."""
    from provisa.api_source.engine_cache import create_and_insert, schedule_drop
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    # Column names must match the compiler's snake_case output.
    _name_map = {c.name: _apply_sql_name(c.name) for c in response_cols}
    _snake_cols = [c.model_copy(update={"name": _apply_sql_name(c.name)}) for c in response_cols]
    _snake_rows = [{_name_map.get(k, k): v for k, v in r.items()} for r in rows]
    with engine.isolated_sync() as _c:
        create_and_insert(_c, _cache_loc, cache_tbl, _snake_rows, _snake_cols)
    asyncio.create_task(schedule_drop(engine, _cache_loc, cache_tbl, ttl, redirect_config))
    log.warning("[MAT] persisted %d rows → store %s", len(rows), cache_tbl)

    if 0 < len(rows) <= _hot_threshold:
        from provisa.cache.hot_tables import HotTableEntry

        # Small + not hot → promote to the hot cache and inline for THIS query. Include all endpoint
        # columns (response + params) so generated SQL referencing a param column resolves to NULL.
        hot_col_names = all_ep_col_names if all_ep_col_names else col_names
        entry = HotTableEntry(
            table_name=tn,
            catalog=_cache_loc.catalog,
            schema=_cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            rows=rows,
            column_names=hot_col_names,
            is_api=True,
        )
        if hot_mgr is not None:
            hot_mgr._hot_tables[tn] = entry
        values_cte_entries[tn] = entry
        log.warning("[MAT] + hot VALUES CTE inline for %s (%d rows)", tn, len(rows))
    else:
        cache_rewrites[tn] = (_cache_loc, cache_tbl)


async def _mat_api_ep_table(
    tn: str,
    ep,
    state,
    hot_mgr,
    _hot_threshold: int,
    _META_COLS: set,
    cache_rewrites: dict,
    values_cte_entries: dict,
) -> None:
    """Materialize a REST API endpoint-backed table into the engine cache or VALUES CTE."""
    from provisa.api_source.engine_cache import (
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        table_exists,
        table_known_live,
    )
    from provisa.executor.redirect import RedirectConfig

    source_id = ep.source_id
    api_source = getattr(state, "api_sources", {}).get(source_id)

    _cc = getattr(api_source, "cache_catalog", None) if api_source else None
    _org_id = getattr(state, "org_id", "default")
    _cs = (
        getattr(api_source, "cache_schema", f"org_{_org_id}_api_cache")
        if api_source
        else f"org_{_org_id}_api_cache"
    )
    _cache_loc = cache_location(source_id, _cc, _cs, engine=state.federation_engine)
    cache_tbl = cache_table_name(source_id, tn, {})
    ttl = (
        getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl")
        or getattr(state, "response_cache_default_ttl", None)
        or ep.ttl
    )
    from provisa.compiler.naming import apply_sql_name

    response_cols = [c for c in ep.columns if c.param_type is None]
    col_names = [c.name for c in response_cols]
    all_ep_col_names = [apply_sql_name(c.name) for c in ep.columns]
    redirect_config = RedirectConfig.from_env()

    if not response_cols:
        log.warning("[MAT] %s has no response columns — skipping", tn)
        return

    # Priority 2: the engine cache hit
    if table_known_live(_cache_loc, cache_tbl):
        log.warning(
            "[MAT] the engine cache hit for %s → %s.%s.%s",
            tn,
            _cache_loc.catalog,
            _cache_loc.schema,
            cache_tbl,
        )
        cache_rewrites[tn] = (_cache_loc, cache_tbl)
        if hot_mgr is not None and getattr(state, "tenant_db", None) is not None:
            asyncio.create_task(
                _promote_joined_from_pg(
                    state, ep, tn, hot_mgr, col_names, _META_COLS, _cache_loc, _hot_threshold
                )
            )
        return

    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, _cache_loc)
        _cache_hit = table_exists(_c, _cache_loc, cache_tbl, ttl=ttl)
    if _cache_hit:
        log.warning(
            "[MAT] the engine cache hit for %s → %s.%s.%s",
            tn,
            _cache_loc.catalog,
            _cache_loc.schema,
            cache_tbl,
        )
        cache_rewrites[tn] = (_cache_loc, cache_tbl)
        if hot_mgr is not None and getattr(state, "tenant_db", None) is not None:
            asyncio.create_task(
                _promote_joined_from_pg(
                    state, ep, tn, hot_mgr, col_names, _META_COLS, _cache_loc, _hot_threshold
                )
            )
        return

    # Priority 3: cache miss — hydrate from PG then REST fallback
    rows, pg_ok = await _mat_fetch_rows_from_pg(ep, col_names, _META_COLS, state)

    if not pg_ok or not rows:
        if any(c.param_type == "path" for c in ep.columns):
            log.warning("[MAT] %s requires path params — skipping", tn)
            return
        try:
            rows = await _mat_fetch_rows_from_rest(
                ep,
                col_names,
                state.federation_engine,
                api_source,
                source_id,
                state,
                _cache_loc,
                cache_tbl,
                cache_rewrites,
            )
        except Exception as rest_exc:
            log.warning("[MAT] REST fallback failed for %s: %s — skipping", tn, rest_exc)
            return
        if rows is None:
            return  # already written to cache_rewrites by _mat_fetch_rows_from_rest

    _mat_store_rows(
        tn,
        rows,
        col_names,
        _cache_loc,
        cache_tbl,
        _hot_threshold,
        hot_mgr,
        response_cols,
        state.federation_engine,
        ttl,
        redirect_config,
        cache_rewrites,
        values_cte_entries,
        all_ep_col_names=all_ep_col_names,
    )


async def _materialize_api_to_engine_cache(
    exec_sql: str,
    state,
    gql_remote_extra_selections: dict | None = None,
    nf_args: dict | None = None,
) -> tuple[dict, dict, list[str]]:
    """Materialize API-backed tables into the engine cache (VARCHAR columns) before the engine SQL runs.

    Avoids INVALID_CAST_ARGUMENT: the engine's PG connector exposes JSONB as json type;
    cache tables store all columns as VARCHAR/scalar types instead.

    Reads from the PG cache populated by _hydrate_api_tables_before_engine — no HTTP call.
    Returns (cache_rewrites, values_cte_entries, dropped_tables):
      cache_rewrites: {physical_table_name: (CacheLocation, cache_tbl)}
      values_cte_entries: {physical_table_name: HotTableEntry} — inlined as VALUES CTEs
      dropped_tables: table names whose UNION branches should be dropped (unreachable remotes)
    """
    from provisa.compiler.nf_extractor import find_api_table_names

    cache_rewrites: dict = {}
    values_cte_entries: dict = {}
    dropped_tables: list[str] = []
    hot_mgr = getattr(state, "hot_manager", None)
    table_names = find_api_table_names(exec_sql)
    if not table_names:
        return cache_rewrites, values_cte_entries, dropped_tables

    _has_pg_pool = getattr(state, "tenant_db", None) is not None
    _META_COLS = {"_params_hash", "_cached_at"}
    _hot_threshold = hot_mgr.auto_threshold if hot_mgr is not None else 500

    for tn in table_names:
        # Hot cache: inline rows as VALUES CTE — avoids cross-catalog JOIN entirely
        if hot_mgr is not None and hot_mgr.is_hot(tn):
            entry = hot_mgr.get_entry(tn)
            if entry is not None:
                values_cte_entries[tn] = entry
                log.warning("[MAT] hot VALUES CTE for %s (%d rows inline)", tn, len(entry.rows))
                continue

        ep = _lookup_ep(state, tn)
        if ep is None:
            gql_reg, gql_tbl = _lookup_gql_remote_table(state, tn)
            if gql_reg is not None:
                assert gql_tbl is not None
                assert isinstance(gql_tbl, dict)
            if gql_reg is not None and gql_tbl is not None:
                req_args = gql_tbl.get("required_args") or []
                if req_args:
                    # required_args carry the REMOTE arg name (e.g. ``name``, ``breedName``); the
                    # extracted nf_args are keyed by the GraphQL schema arg, which Provisa prefixes
                    # with ``_`` when it collides with a scalar field and stores in sql convention
                    # (``breedName`` → ``_breed_name``). Match through the naming authority: both
                    # sides reduce to the same sql name once the disambiguation ``_`` is dropped.
                    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

                    _nf_canon = {
                        _apply_sql_name(k.lstrip("_")): v for k, v in (nf_args or {}).items()
                    }
                    resolved = {}
                    missing = []
                    for a in req_args:
                        canon = _apply_sql_name(a["name"].lstrip("_"))
                        if canon in _nf_canon:
                            resolved[a["name"]] = _nf_canon[canon]
                        else:
                            missing.append(a["name"])
                    if missing:
                        # Required filter(s) absent — exclude the object (drop its union branch) so a
                        # broad sweep (graph counts, multi-label union) skips it instead of erroring.
                        log.warning("[MAT] %s requires filter(s) %s — dropping branch", tn, missing)
                        dropped_tables.append(tn)
                    else:
                        try:
                            await _mat_gql_remote_table(
                                tn,
                                gql_reg,
                                gql_tbl,
                                state,
                                hot_mgr,
                                _hot_threshold,
                                cache_rewrites,
                                values_cte_entries,
                                extra_selections=(gql_remote_extra_selections or {}).get(tn),
                                variables=resolved,
                            )
                        except RuntimeError as _gql_err:
                            log.warning("[MAT] GQL remote unreachable for %s: %s", tn, _gql_err)
                            dropped_tables.append(tn)
                else:
                    try:
                        await _mat_gql_remote_table(
                            tn,
                            gql_reg,
                            gql_tbl,
                            state,
                            hot_mgr,
                            _hot_threshold,
                            cache_rewrites,
                            values_cte_entries,
                            extra_selections=(gql_remote_extra_selections or {}).get(tn),
                        )
                    except RuntimeError as _gql_err:
                        log.warning(
                            "[MAT] GQL remote unreachable for %s — dropping union branch: %s",
                            tn,
                            _gql_err,
                        )
                        dropped_tables.append(tn)
            continue

        if not _has_pg_pool:
            log.warning("[MAT] tenant_db is None — skipping API table %s", tn)
            continue

        await _mat_api_ep_table(
            tn,
            ep,
            state,
            hot_mgr,
            _hot_threshold,
            _META_COLS,
            cache_rewrites,
            values_cte_entries,
        )
        if tn not in cache_rewrites and tn not in values_cte_entries:
            log.warning("[MAT] %s could not be materialized — dropping union branch", tn)
            dropped_tables.append(tn)

    return cache_rewrites, values_cte_entries, dropped_tables
