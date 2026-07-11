# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source and route executors for the /data endpoint.

Executes compiled queries against a source (engine-standard, API, gRPC-remote,
nodes) and drives the redirect/CTAS/inline route paths plus response-cache
stores. Depends on endpoint_helpers; never calls back into the route handlers.
"""

# complexity-gate: allow-ble=1 reason="the gRPC-remote cache write (land_api_cache + schedule_drop) is best-effort and non-fatal BY DESIGN: any failure is logged and the query still returns correct results via the inline VALUES-CTE fallback. Landing touches the federation write-face which can fail many ways, none of which should fail the user's query. Mirrors the established convention at materialization.py:275 for the same call."

from __future__ import annotations

import asyncio
import logging
import time as _time


from fastapi import HTTPException

from provisa.cache.middleware import store_result
from provisa.compiler.sql_rewrite import (
    rewrite_semantic_to_catalog_physical,
    rewrite_semantic_to_physical,
)
from provisa.executor.serialize import (
    serialize_aggregate,
    serialize_group_by,
)
from provisa.executor import stats as _qs_mod
from provisa.transpiler.router import Route
from provisa.transpiler.transpile import transpile
from provisa.api.data.hydration import _hydrate_api_tables_before_engine
from provisa.api.data.materialization import (
    _materialize_api_to_engine_cache,
)
from provisa.api.data.endpoint_helpers import (
    _append_mermaid,
    _build_mermaid,
    _format_response,
    _grpc_cache_type,
    _inject_probe_limit,
    _record_per_source_stats,
)

log = logging.getLogger(__name__)


async def _execute_api_source(compiled, ctx, state, source_id, root_field, output_format):
    """Execute a query against an API source in two phases.

    Phase 1 — REST call: native filter args (api_args) build the URL.
              On cache miss, rows are materialized into the source's cache table
              (postgresql backend by default; iceberg if configured).

    Phase 2 — the engine SQL: the compiled WHERE/ORDER BY/LIMIT are applied by the engine
              against the cached table.  Same-source JOINs are pushed down to the
              source database when using the postgresql backend.
    """
    from provisa.api_source.router_integration import handle_api_query
    from provisa.api_source.engine_cache import (
        cache_location,
        ensure_cache_schema,
        rewrite_from_cache,
    )

    # Find the API endpoint matching this query's table
    table_meta = None
    for meta in state.contexts.values():
        tm = meta.tables.get(root_field)
        if tm:
            table_meta = tm
            break

    table_name = table_meta.table_name if table_meta else root_field
    endpoint = state.api_endpoints.get(table_name)
    if endpoint is None:
        raise HTTPException(
            status_code=400,
            detail=f"No API endpoint registered for table {table_name!r}",
        )

    api_source = state.api_sources.get(source_id)
    _cc = getattr(api_source, "cache_catalog", None) if api_source else None
    _org_id = getattr(state, "org_id", "default")
    _cs = (
        getattr(api_source, "cache_schema", f"org_{_org_id}_api_cache")
        if api_source
        else f"org_{_org_id}_api_cache"
    )
    _cache_loc = cache_location(source_id, _cc, _cs, engine=state.federation_engine)

    # Resolve native filter args (path/query params) — may be "_"-prefixed on collision.
    url_params: dict = compiled.api_args.copy() if compiled.api_args else {}
    param_name_map: dict = {}
    for c in endpoint.columns:
        if c.param_name:
            param_name_map[c.name] = c.param_name
            param_name_map[f"_{c.name}"] = c.param_name
    url_params = {param_name_map.get(k, k): v for k, v in url_params.items()}

    # Hot table bypass: skip REST + the engine materialization entirely
    hot_mgr = getattr(state, "hot_manager", None)
    if hot_mgr is not None and hot_mgr.is_hot(table_name):
        from provisa.cache.hot_tables import build_values_cte_sql
        from provisa.compiler.nf_extractor import extract_nf_args

        entry = hot_mgr.get_entry(table_name)
        _exec_sql, _exec_params, _ = extract_nf_args(compiled.sql, compiled.params)
        _exec_sql = rewrite_semantic_to_catalog_physical(_exec_sql, ctx)
        hot_sql = build_values_cte_sql(_exec_sql, table_name, entry)
        physical_sql = state.federation_engine.transpile_physical(hot_sql)
        log.info("[HOT TABLE] hit — %s (%d rows inline)", table_name, len(entry.rows))
        _loop = asyncio.get_running_loop()
        _t0 = _time.perf_counter()
        engine_result = await _loop.run_in_executor(
            None, lambda: state.federation_engine.execute_engine_sync(physical_sql, _exec_params)
        )
        phase2_ms = (_time.perf_counter() - _t0) * 1000
        response_data = _format_response(
            engine_result.rows,
            compiled.columns,
            root_field,
            output_format,
            result_limit=compiled.result_limit,
        )
        field_rows = (
            response_data.get("data", {}).get(root_field, [])
            if isinstance(response_data, dict)
            else response_data
        )
        return field_rows, response_data, 0.0, phase2_ms, physical_sql, True

    # --- Phase 1: materialize if cache miss ---
    from provisa.api_source.engine_cache import (
        cache_table_name as _cache_table_name,
        table_known_live,
    )

    _loop = asyncio.get_running_loop()
    _engine = state.federation_engine

    # Fast path: in-process cache hit — skip ensure_cache_schema + handle_api_query entirely
    _probe_tbl = _cache_table_name(endpoint.source_id, endpoint.table_name, url_params)
    if table_known_live(_cache_loc, _probe_tbl):
        cache_tbl = _probe_tbl
        _cache_miss = False
        phase1_ms = 0.0
        log.info("[API CACHE] in-process hit — %s", cache_tbl)
    else:
        _t_phase1 = _time.perf_counter()

        def _ensure() -> None:
            with _engine.isolated_sync() as _c:
                ensure_cache_schema(_c, _cache_loc)

        await _loop.run_in_executor(None, _ensure)
        # handle_api_query owns cache key derivation and the full create/schedule_drop lifecycle.
        result = await handle_api_query(
            endpoint=endpoint,
            params=url_params,
            engine=_engine,
            source=api_source,
            source_ttl=state.source_cache.get(source_id, {}).get("cache_ttl"),
            global_ttl=state.response_cache_default_ttl,
            loc=_cache_loc,
        )
        cache_tbl = result.cache_table
        _cache_miss = not result.from_cache
        if result.from_cache:
            log.warning("[API CACHE] hit — %s", cache_tbl)
        else:
            log.warning(
                "[API CACHE] miss — %d rows from REST, materialized as %s",
                len(result.rows),
                cache_tbl,
            )
            if hot_mgr is not None and result.rows:
                asyncio.create_task(hot_mgr.maybe_promote_dicts(table_name, result.rows))
        phase1_ms = (_time.perf_counter() - _t_phase1) * 1000

    # --- Phase 2: apply WHERE/ORDER BY/LIMIT vithe engine ---
    from provisa.compiler.nf_extractor import extract_nf_args

    exec_sql, exec_params, _ = extract_nf_args(compiled.sql, compiled.params)
    exec_sql = rewrite_semantic_to_catalog_physical(exec_sql, ctx)
    assert cache_tbl is not None, "cache_tbl must be set before Phase 2"
    rewritten_sql = rewrite_from_cache(exec_sql, _cache_loc, cache_tbl)
    # Rewrite any joined API table refs → VALUES CTE (hot) or the engine cache
    _join_rewrites, _join_values_ctes, _join_dropped = await _materialize_api_to_engine_cache(
        rewritten_sql, state, compiled.gql_remote_extra_selections
    )
    if _join_dropped:
        from provisa.compiler.nf_extractor import drop_union_branches_for_table

        for _dtn in _join_dropped:
            rewritten_sql = drop_union_branches_for_table(rewritten_sql, _dtn)
    if _join_values_ctes:
        from provisa.cache.hot_tables import build_values_cte_sql

        for _tn, _entry in _join_values_ctes.items():
            rewritten_sql = build_values_cte_sql(rewritten_sql, _tn, _entry)
    if _join_rewrites:
        from provisa.api_source.engine_cache import rewrite_all_from_cache

        rewritten_sql = rewrite_all_from_cache(rewritten_sql, _join_rewrites)
    physical_sql = state.federation_engine.transpile_physical(rewritten_sql)
    log.warning("[API P2] physical_sql=%s", physical_sql[:500])
    _t_phase2 = _time.perf_counter()
    engine_result = await _loop.run_in_executor(
        None, lambda: _engine.execute_engine_sync(physical_sql, exec_params)
    )
    phase2_ms = (_time.perf_counter() - _t_phase2) * 1000

    response_data = _format_response(
        engine_result.rows, compiled.columns, root_field, output_format
    )
    if isinstance(response_data, dict):
        field_rows = response_data.get("data", {}).get(root_field, [])
    else:
        field_rows = response_data

    return field_rows, response_data, phase1_ms, phase2_ms, physical_sql, not _cache_miss


async def _execute_grpc_remote_source(compiled, ctx, state, source_id, root_field, output_format):
    """Execute a gRPC remote query method.

    Calls the remote gRPC endpoint with _nf_ args, injects result rows as a
    VALUES CTE, then applies WHERE/ORDER BY/LIMIT vithe engine (Phase 2 only).
    """
    from provisa.compiler.nf_extractor import extract_nf_args
    from provisa.cache.hot_tables import HotTableEntry, build_values_cte_sql
    from provisa.source_adapters import grpc_remote_adapter

    reg = getattr(state, "grpc_remote_sources", {}).get(source_id)
    if reg is None:
        raise HTTPException(
            status_code=400, detail=f"gRPC remote source {source_id!r} not registered"
        )

    table_meta = None
    for meta in state.contexts.values():
        tm = meta.tables.get(root_field)
        if tm:
            table_meta = tm
            break
    table_name = table_meta.table_name if table_meta else root_field

    namespace = reg.get("namespace", "")
    prefix = f"{namespace}__" if namespace else ""
    grpc_query = next(
        (q for q in reg.get("queries", []) if f"{prefix}{q.service}__{q.method}" == table_name),
        None,
    )
    if grpc_query is None:
        raise HTTPException(
            status_code=400, detail=f"No gRPC query registered for table {table_name!r}"
        )

    nf_args: dict = compiled.api_args.copy() if compiled.api_args else {}

    # REQ-327: materialize gRPC-remote results to the PG cache table (like graphql_remote),
    # then inline small results as a VALUES CTE and reference the cache table for large
    # ones. A repeat query hits the PG cache table (no re-fetch). The VALUES CTE is the
    # safe fallback if any cache step fails.
    from collections import namedtuple as _nt

    from provisa.api_source.engine_cache import (
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        land_api_cache,
        resolved_cache_catalog,
        rewrite_from_cache,
        schedule_drop,
        table_known_live,
    )
    from provisa.cache.store import NoopCacheStore
    from provisa.executor.redirect import RedirectConfig

    _org_id = getattr(state, "org_id", "default")
    _cache_cat = resolved_cache_catalog(state.federation_engine)
    cache_loc = cache_location(source_id, _cache_cat, f"org_{_org_id}_grpc_cache")
    cache_tbl = cache_table_name(source_id, table_name, nf_args)  # SHA-256(source+method+args)
    redirect_config = RedirectConfig.from_env()
    hot_mgr = getattr(state, "hot_manager", None)
    _hot_threshold = hot_mgr.auto_threshold if hot_mgr is not None else 500

    exec_sql, exec_params, _ = extract_nf_args(compiled.sql, compiled.params)
    exec_sql = rewrite_semantic_to_catalog_physical(exec_sql, ctx)

    with state.federation_engine.isolated_sync() as _cache_conn:
        ensure_cache_schema(_cache_conn, cache_loc)

    phase1_ms = 0.0
    final_sql: str | None = None
    if table_known_live(cache_loc, cache_tbl):
        final_sql = rewrite_from_cache(exec_sql, cache_loc, cache_tbl)  # None on failure

    if final_sql is None:
        _t0 = _time.perf_counter()
        rows = await grpc_remote_adapter.fetch(
            source_id=source_id,
            full_method_path=grpc_query.full_method_path,
            input_message_name=grpc_query.input_message,
            output_message_name=grpc_query.output_message,
            pb2=reg["pb2"],
            args=nf_args,
            grpc_remote_sources=getattr(state, "grpc_remote_sources", {}),
            response_cache_store=NoopCacheStore(),  # the PG cache table is the cache, not Redis
            ttl=reg.get("cache_ttl", 300),
            server_streaming=grpc_query.server_streaming,
        )
        phase1_ms = (_time.perf_counter() - _t0) * 1000

        col_names = (
            [c.name for c in grpc_query.columns]
            if grpc_query.columns
            else (list(rows[0].keys()) if rows else [])
        )

        _Col = _nt("_Col", ["name", "type"])
        cache_cols = (
            [_Col(name=c.name, type=_grpc_cache_type(c.type)) for c in grpc_query.columns]
            if grpc_query.columns
            else [_Col(name=n, type="string") for n in col_names]
        )
        materialized = False
        if rows:
            try:
                await land_api_cache(
                    state.federation_engine, cache_loc, cache_tbl, rows, cache_cols
                )
                asyncio.create_task(
                    schedule_drop(
                        state.federation_engine,
                        cache_loc,
                        cache_tbl,
                        reg.get("cache_ttl", 300),
                        redirect_config,
                    )
                )
                materialized = True
            except Exception as cache_exc:
                log.warning("[GRPC REMOTE] cache write failed for %s: %s", table_name, cache_exc)

        if materialized and len(rows) > _hot_threshold:
            final_sql = rewrite_from_cache(exec_sql, cache_loc, cache_tbl)
        if final_sql is None:  # small result, or rewrite failed → inline VALUES CTE
            entry = HotTableEntry(
                table_name=table_name,
                catalog="",
                schema="",
                pk_column="",
                rows=rows,
                column_names=col_names,
                is_api=True,
            )
            final_sql = build_values_cte_sql(exec_sql, table_name, entry)

    physical_sql = state.federation_engine.transpile_physical(final_sql)

    _loop = asyncio.get_running_loop()
    _t2 = _time.perf_counter()
    engine_result = await _loop.run_in_executor(
        None, lambda: state.federation_engine.execute_engine_sync(physical_sql, exec_params)
    )
    phase2_ms = (_time.perf_counter() - _t2) * 1000

    response_data = _format_response(
        engine_result.rows, compiled.columns, root_field, output_format
    )
    field_rows = (
        response_data.get("data", {}).get(root_field, [])
        if isinstance(response_data, dict)
        else response_data
    )

    return field_rows, response_data, phase1_ms, phase2_ms, physical_sql, False


async def _execute_engine_standard(
    compiled, ctx, state, role_id, root_field, probe_limit, query_session_props, query_text
):
    """Execute the engine federated path.

    Returns (result, physical_sql, engine_ms, per_source_ms, dataloader_srcs, hydration_ms,
             hydration_rows, hydration_cache_hits).
    """
    from provisa.cache.hot_tables import build_values_cte_sql
    from provisa.api_source.engine_cache import rewrite_all_from_cache
    from provisa.compiler.hints import extract_hints

    if not state.federation_engine.is_connected():
        raise HTTPException(status_code=503, detail="the engine not connected")

    (
        _dataloader_srcs,
        _hydration_ms,
        _hydration_rows,
        _hydration_cache_hits,
    ) = await _hydrate_api_tables_before_engine(compiled, ctx, state)

    exec_sql = rewrite_semantic_to_catalog_physical(compiled.sql, ctx)
    if probe_limit is not None:
        exec_sql = _inject_probe_limit(exec_sql, probe_limit)

    # A PARAMETERIZED (native-filter) table is a function f(args) -> rows with no snapshot: pull its
    # _nf_ predicates out of the SQL and resolve the args, so _materialize_api_to_engine_cache FETCHES
    # the source real-time with them (openapi / grpc_remote / graphql_remote) and injects the rows as
    # a VALUES CTE — instead of scanning a replica whose _nf_ column was stripped (a binder error).
    # Mirrors the openapi Phase-2 and Cypher (cypher_router) native-filter paths.
    from provisa.compiler.nf_extractor import extract_nf_args

    exec_sql, exec_params, _nf_args = extract_nf_args(exec_sql, compiled.params)

    # Materialize API-backed tables into the engine cache to avoid INVALID_CAST_ARGUMENT
    _api_cache_rewrites, _api_values_ctes, _api_dropped = await _materialize_api_to_engine_cache(
        exec_sql, state, compiled.gql_remote_extra_selections, nf_args=_nf_args
    )
    if _api_dropped:
        from provisa.compiler.nf_extractor import drop_union_branches_for_table

        for _dtn in _api_dropped:
            exec_sql = drop_union_branches_for_table(exec_sql, _dtn)
    for _tn, _entry in _api_values_ctes.items():
        exec_sql = build_values_cte_sql(exec_sql, _tn, _entry)
    if _api_cache_rewrites:
        exec_sql = rewrite_all_from_cache(exec_sql, _api_cache_rewrites)

    # AL5/AL3: extract comment hints, merge source federation hints
    exec_sql, comment_hints = extract_hints(exec_sql)
    from provisa.compiler.directives import translate_federation_hints

    session_hints: dict[str, str] = {}
    for sid in compiled.sources:
        src_hints = getattr(state, "source_federation_hints", {}).get(sid, {})
        # REQ-281: source hints use the Provisa-branded @provisa vocabulary; translate to
        # the engine session props here (the single translation layer) before they reach SET SESSION.
        session_hints.update(translate_federation_hints(src_hints))
    session_hints.update(query_session_props or {})
    session_hints.update(comment_hints)

    physical_sql = state.federation_engine.transpile_physical(exec_sql)
    _t_engine = _time.perf_counter()
    _engine_ck = getattr(state, "engine_conn_kwargs", None)
    _root_meta = ctx.tables.get(compiled.canonical_field or root_field)
    _root_name = (
        (_root_meta.original_table_name or _root_meta.table_name) if _root_meta else ""
    ) or root_field
    _root_domain = _root_meta.domain_id if _root_meta else ""
    _span_attrs: dict[str, str] = {
        "provisa.table": _root_name,
        "provisa.domain": _root_domain,
        "provisa.role": role_id or "",
    }
    if query_text is not None:
        _span_attrs["provisa.query_text"] = query_text

    result = await state.federation_engine.execute_engine(
        physical_sql,
        exec_params,
        session_hints=session_hints or None,
        conn_kwargs=_engine_ck,
        span_attrs=_span_attrs,
    )
    _engine_ms = (_time.perf_counter() - _t_engine) * 1000
    _per_source_ms: dict[str, float] = {
        src_id: _hydration_ms.get(src_id, _engine_ms)
        if (state.source_types or {}).get(src_id) == "openapi"
        else _engine_ms
        for src_id in compiled.sources
    }
    # Lazy hot-table promotion
    _hot_mgr = getattr(state, "hot_manager", None)
    if _hot_mgr is not None:
        _tbl = compiled.canonical_field or root_field
        asyncio.create_task(_hot_mgr.maybe_promote(_tbl, result.rows, result.column_names))

    return (
        result,
        physical_sql,
        _engine_ms,
        _per_source_ms,
        _dataloader_srcs,
        _hydration_ms,
        _hydration_rows,
        _hydration_cache_hits,
        session_hints,
    )


async def _exec_nodes_query(compiled, ctx, state, decision):
    """Execute the aggregate nodes sub-query (plain-SELECT companion).

    Returns the nodes_result.
    """
    engine = state.federation_engine
    if decision.route == Route.DIRECT and decision.source_id:
        nodes_target_sql = transpile(
            rewrite_semantic_to_physical(compiled.nodes_sql, ctx),
            decision.dialect or "postgres",
        )
        return await engine.execute_native(
            state.source_pools,
            decision.source_id,
            nodes_target_sql,
            compiled.nodes_params,
        )
    nodes_physical_sql = state.federation_engine.transpile_physical(
        rewrite_semantic_to_catalog_physical(compiled.nodes_sql, ctx)
    )
    return await engine.execute_engine(
        nodes_physical_sql,
        compiled.nodes_params,
        conn_kwargs=getattr(state, "engine_conn_kwargs", None),
    )


async def _store_response_cache(
    state,
    ck: str,
    response_data: dict,
    root_field: str,
    ctx,
    compiled,
    response_cache_ttl: int | None,
    no_cache: bool,
    org_id: str | None = None,
) -> None:
    """Store response_data in the response cache if TTL allows."""
    from provisa.cache.policy import resolve_policy

    source_id = next(iter(compiled.sources), None)
    src_cache = state.source_cache.get(source_id, {}) if source_id else {}
    # Resolve the root table by its ctx.tables key. canonical_field is the pre-alias schema
    # field (variant keys like …GroupBy/…_aggregate are registered too); root_field may be a
    # client alias not present in ctx.tables, so canonical_field takes precedence.
    _root_meta = ctx.tables.get(compiled.canonical_field or root_field)
    table_ids = {_root_meta.table_id} if _root_meta is not None else set()
    table_id = next(iter(table_ids), None)
    tbl_cache_ttl = state.table_cache.get(table_id) if table_id else None
    _, resolved_ttl = resolve_policy(
        stable_id=None,
        cache_ttl=response_cache_ttl,
        default_ttl=state.response_cache_default_ttl,
        source_cache_enabled=src_cache.get("cache_enabled", True),
        source_cache_ttl=src_cache.get("cache_ttl"),
        table_cache_ttl=tbl_cache_ttl,
    )
    if resolved_ttl > 0 and not no_cache:
        await store_result(
            state.response_cache_store,
            ck,
            response_data,
            ttl=resolved_ttl,
            table_ids=table_ids,
            org_id=org_id,
        )


async def _store_api_source_cache(
    state,
    ck: str,
    response_data: dict,
    root_field: str,
    canonical_field: str,
    ctx,
    source_id: str,
    response_cache_ttl: int | None,
    no_cache: bool,
    org_id: str | None = None,
) -> None:
    """Store API-source response_data in the response cache if TTL allows."""
    from provisa.cache.policy import resolve_policy

    _src_cache = state.source_cache.get(source_id, {})
    # Resolve by ctx.tables key. canonical_field is the pre-alias schema field (variant keys
    # like …GroupBy are registered too); root_field may be a client alias absent from ctx.tables.
    _root_meta = ctx.tables.get(canonical_field or root_field)
    _table_ids = {_root_meta.table_id} if _root_meta is not None else set()
    _table_id = next(iter(_table_ids), None)
    _tbl_cache_ttl = state.table_cache.get(_table_id) if _table_id else None
    _, _resolved_ttl = resolve_policy(
        stable_id=None,
        cache_ttl=response_cache_ttl,
        default_ttl=state.response_cache_default_ttl,
        source_cache_enabled=_src_cache.get("cache_enabled", True),
        source_cache_ttl=_src_cache.get("cache_ttl"),
        table_cache_ttl=_tbl_cache_ttl,
    )
    if _resolved_ttl > 0 and not no_cache:
        await store_result(
            state.response_cache_store,
            ck,
            response_data,
            ttl=_resolved_ttl,
            table_ids=_table_ids,
            org_id=org_id,
        )


async def _exec_api_route(
    compiled,
    ctx,
    state,
    decision,
    root_field,
    output_format,
    ck,
    response_cache_ttl,
    no_cache,
    org_id: str | None = None,
):
    """Execute Route.API path.

    Returns (root_field, field_rows, None, ck, None).
    """
    _api_source_type = getattr(state, "source_types", {}).get(decision.source_id, "")
    try:
        if _api_source_type == "grpc_remote":
            (
                field_rows,
                response_data,
                _phase1_ms,
                _phase2_ms,
                _api_physical_sql,
                _api_cache_hit,
            ) = await _execute_grpc_remote_source(
                compiled, ctx, state, decision.source_id, root_field, output_format
            )
            _api_cache_hit = False
        else:
            (
                field_rows,
                response_data,
                _phase1_ms,
                _phase2_ms,
                _api_physical_sql,
                _api_cache_hit,
            ) = await _execute_api_source(
                compiled, ctx, state, decision.source_id, root_field, output_format
            )
    except HTTPException:
        raise
    except (MemoryError, ConnectionError) as e:
        log.error("Query resource error for %s: %s", root_field, e)
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        log.exception("API source execution failed for %s", root_field)
        raise HTTPException(status_code=500, detail=str(e))
    _api_rows = len(field_rows) if isinstance(field_rows, list) else 0
    _qs_mod.record(
        field=root_field,
        source=decision.source_id,
        strategy="hydration",
        elapsed_ms=_phase1_ms,
        rows=0,
        cache_hit=_api_cache_hit,
    )
    _qs_mod.record(
        field=root_field,
        source=decision.source_id,
        strategy=f"api:{_api_source_type}" if _api_source_type else "api",
        elapsed_ms=_phase2_ms,
        rows=_api_rows,
        cache_hit=False,
        physical_sql=_api_physical_sql,
    )
    _qs = _qs_mod.current()
    if _qs is not None:
        _source_types = getattr(state, "source_types", {})
        _hydration_ms_api = {decision.source_id: _phase1_ms}
        _root_meta = ctx.tables.get(compiled.canonical_field or root_field)
        _join_fields: list[tuple[str, str, bool]] = []
        # Only relationships the query actually federates across sources (see _append_mermaid).
        if _root_meta:
            for (_tn, _rf), _jm in (ctx.joins or {}).items():
                _t = _jm.target.source_id
                if (
                    _tn == _root_meta.type_name
                    and _t in compiled.sources
                    and _t != decision.source_id
                ):
                    _join_fields.append((_rf, _t, True))
        _engine_ms_for_mermaid = _phase2_ms if _join_fields else None
        _src_obj = getattr(state, "api_sources", {}).get(decision.source_id)
        _cc = getattr(_src_obj, "cache_catalog", None) if _src_obj else None
        _api_mermaid = _build_mermaid(
            compiled.sources,
            _source_types,
            _hydration_ms_api,
            _engine_ms_for_mermaid,
            _api_rows,
            root_field,
            join_fields=_join_fields or None,
            root_source_id=decision.source_id,
            cache_catalog=_cc,
        )
        _qs.mermaid = f"{_qs.mermaid}\n\n{_api_mermaid}" if _qs.mermaid else _api_mermaid
    if isinstance(response_data, dict):
        await _store_api_source_cache(
            state,
            ck,
            response_data,
            root_field,
            compiled.canonical_field,
            ctx,
            decision.source_id,
            response_cache_ttl,
            no_cache,
            org_id=org_id,
        )
    return root_field, field_rows, None, ck, None


async def _exec_ctas_route(compiled, ctx, state, effective_redirect_format, redirect_config):
    """Execute CTAS redirect path.

    Returns redirect_info dict on success, or raises.
    """
    from provisa.executor.redirect import presign_ctas_result, schedule_s3_cleanup

    _, _, _, _ = await _hydrate_api_tables_before_engine(compiled, ctx, state)
    _ctas_exec_sql = rewrite_semantic_to_catalog_physical(compiled.sql, ctx)
    _ctas_rewrites, _ctas_values_ctes, _ctas_dropped = await _materialize_api_to_engine_cache(
        _ctas_exec_sql, state, compiled.gql_remote_extra_selections
    )
    if _ctas_dropped:
        from provisa.compiler.nf_extractor import drop_union_branches_for_table

        for _dtn in _ctas_dropped:
            _ctas_exec_sql = drop_union_branches_for_table(_ctas_exec_sql, _dtn)
    if _ctas_values_ctes:
        from provisa.cache.hot_tables import build_values_cte_sql

        for _tn, _entry in _ctas_values_ctes.items():
            _ctas_exec_sql = build_values_cte_sql(_ctas_exec_sql, _tn, _entry)
    if _ctas_rewrites:
        from provisa.api_source.engine_cache import rewrite_all_from_cache

        _ctas_exec_sql = rewrite_all_from_cache(_ctas_exec_sql, _ctas_rewrites)
    physical_sql = state.federation_engine.transpile_physical(_ctas_exec_sql)
    ctas_result = state.federation_engine.ctas_redirect(physical_sql, effective_redirect_format)
    url = await presign_ctas_result(ctas_result["s3_prefix"], redirect_config)
    # Do NOT drop the Iceberg table here — DROP TABLE on the JDBC catalog purges
    # S3 data files immediately, invalidating the presigned URL before the user
    # can download. The background task deletes S3 objects after TTL expires.
    asyncio.create_task(schedule_s3_cleanup(ctas_result["s3_prefix"], redirect_config))
    content_type = {"parquet": "application/vnd.apache.parquet", "orc": "application/x-orc"}.get(
        effective_redirect_format, "application/octet-stream"
    )
    return {
        "redirect_url": url,
        "row_count": ctas_result["row_count"],
        "expires_in": redirect_config.ttl,
        "content_type": content_type,
    }


async def _exec_probe_redirect(
    compiled,
    ctx,
    state,
    decision,
    session_hints,
    effective_redirect_format,
    redirect_config,
    role_id=None,
):
    """Re-execute without probe limit then upload-and-presign.

    Returns redirect_info dict on success, or raises.
    """
    from provisa.executor.redirect import upload_and_presign

    engine = state.federation_engine
    if decision.route == Route.DIRECT and decision.source_id:
        target_sql = transpile(
            rewrite_semantic_to_physical(compiled.sql, ctx), decision.dialect or "postgres"
        )
        full_result = await engine.execute_native(
            state.source_pools, decision.source_id, target_sql, compiled.params
        )
    else:
        full_physical_sql = state.federation_engine.transpile_physical(
            rewrite_semantic_to_catalog_physical(compiled.sql, ctx)
        )
        full_result = await engine.execute_engine(
            full_physical_sql,
            compiled.params,
            session_hints=session_hints or None,
            conn_kwargs=getattr(state, "engine_conn_kwargs", None),
        )
    return await upload_and_presign(
        full_result,
        redirect_config,
        output_format=effective_redirect_format,
        columns=compiled.columns,
        role=role_id,
    )


async def _exec_inline_result(
    compiled,
    ctx,
    state,
    decision,
    root_field,
    result,
    output_format,
    ck,
    response_cache_ttl,
    no_cache,
    t0,
    _dataloader_srcs,
    _per_source_ms,
    _engine_ms,
    _hydration_rows,
    _hydration_cache_hits,
    physical_sql,
):
    """Build inline response, cache it, record stats, and return (root_field, field_rows, None, ck, None)."""
    if compiled.nodes_sql is not None:
        try:
            nodes_result = await _exec_nodes_query(compiled, ctx, state, decision)
        except (MemoryError, ConnectionError) as e:
            log.error("Nodes query resource error for %s: %s", root_field, e)
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            log.exception("Nodes query execution failed for %s", root_field)
            raise HTTPException(status_code=500, detail=str(e))
        if compiled.is_group_by:
            response_data = serialize_group_by(
                result.rows,
                compiled.columns,
                nodes_result.rows,
                compiled.nodes_columns,
                root_field,
            )
        else:
            response_data = serialize_aggregate(
                result.rows,
                compiled.columns,
                nodes_result.rows,
                compiled.nodes_columns,
                root_field,
                agg_alias=compiled.agg_alias,
            )
    else:
        response_data = _format_response(
            result.rows,
            compiled.columns,
            root_field,
            output_format,
            result_limit=compiled.result_limit,
        )

    if isinstance(response_data, dict):
        field_rows = response_data.get("data", {}).get(root_field, [])
    else:
        field_rows = response_data

    if isinstance(response_data, dict):
        await _store_response_cache(
            state, ck, response_data, root_field, ctx, compiled, response_cache_ttl, no_cache
        )

    _n_rows = len(field_rows) if isinstance(field_rows, list) else 0
    _record_per_source_stats(
        root_field,
        compiled.sources,
        (_time.perf_counter() - t0) * 1000,
        _n_rows,
        ctx,
        state,
        decision,
        _dataloader_srcs,
        _per_source_ms,
        _engine_ms,
        _hydration_rows,
        field_rows if isinstance(field_rows, list) else None,
        physical_sql or None,
        _hydration_cache_hits,
    )
    qs = _qs_mod.current()
    if qs is not None and len(compiled.sources) >= 1:
        _append_mermaid(
            qs,
            compiled,
            ctx,
            root_field,
            _per_source_ms,
            _engine_ms,
            _n_rows,
            _hydration_cache_hits,
        )
    return root_field, field_rows, None, ck, None
