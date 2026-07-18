# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher execution backends (Phase AU, REQ-345–353).

Role/label resolution, API + graphql-remote lookups, and the federation/API/
gql-remote execution paths invoked by the /query/cypher pipeline. Extracted
from cypher_router.py; leaf module (no route handlers).
"""

# complexity-gate: allow-cc=31 allow-ble=1 reason="_execute_with_api and its one broad except (API-source execution error surfaced to the client) relocated verbatim from cypher_router.py; per-stage split is separately-tracked debt"

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import Request

if TYPE_CHECKING:
    from provisa.cypher.label_map import CypherLabelMap  # noqa: F401
    from provisa.api.app import AppState  # noqa: F401
    from provisa.compiler.sql_gen import CompilationContext  # noqa: F401
    from provisa.core.database import Connection  # noqa: F401


from provisa.api.rest.registered_call import (
    _detect_procedure,  # noqa: F401 — re-exported for tests
    _handle_procedure,  # noqa: F401 — re-exported for tests
)


log = logging.getLogger(__name__)


def _span_attrs_from_semantic_sql(
    semantic_sql: str,
    role_id: str,
    query_text: str | None = None,
) -> dict[str, str]:
    """Extract normalized table names from semantic SQL for OTel span attributes.

    Semantic SQL uses domain_to_sql_name(domain_id) as schema, e.g. pet_store.pets.
    sqlglot Table nodes expose .db (schema/domain) and .name (table).
    """
    import sqlglot
    import sqlglot.expressions as _sge

    tables: set[str] = set()
    domains: set[str] = set()
    try:
        parsed = sqlglot.parse_one(semantic_sql, dialect="postgres")
        # Walk only the primary FROM clause — LATERAL joins (ops/meta traversal) live in
        # parsed.args["joins"] and must be excluded so provisa.table is a single root
        # table name rather than a comma list that never matches the exact-match join condition.
        from_node = parsed.args.get("from")
        if from_node:
            for tbl in from_node.find_all(_sge.Table):
                db = tbl.db
                name = tbl.name
                if db:
                    tables.add(f"{db}.{name}")
                    domains.add(db)
                elif name:
                    tables.add(name)
    except Exception:
        pass
    attrs: dict[str, str] = {
        "provisa.table": ", ".join(sorted(tables)) or "cypher",
        "provisa.domain": ", ".join(sorted(domains)) or "cypher",
        "provisa.role": role_id,
    }
    if query_text is not None:
        attrs["provisa.query_text"] = query_text
    return attrs


def _resolve_role_id(request: Request, state: AppState) -> str:
    """Resolve the role_id from the X-Provisa-Role header, else the configured default_role.

    A no-header request must NEVER escalate to admin — that would grant an unauthenticated caller
    full visibility. Resolve the auth config's ``default_role`` (a deliberately-scoped role, like
    the MCP path in api/mcp/server.py which refuses to escalate), falling back to a deterministic
    first role only when no default is configured. Admins see everything, but a caller must present
    an admin role via header/token to be one."""
    roles: dict = getattr(state, "roles", {})
    header_role = request.headers.get("x-provisa-role") or request.headers.get("X-Provisa-Role")
    if header_role and header_role in roles:
        return header_role
    if not roles:
        return "default"
    auth_config = getattr(state, "auth_config", None) or {}
    default_role = auth_config.get("default_role") if isinstance(auth_config, dict) else None
    if default_role and default_role in roles:
        return default_role
    return next(iter(roles))


def _build_label_map(ctx: CompilationContext, role_id: str, state: AppState) -> CypherLabelMap:
    """Build CypherLabelMap with cross-domain traversal nodes for the given role."""
    from provisa.cypher.label_map import CypherLabelMap

    role = getattr(state, "roles", {}).get(role_id, {})
    cache = getattr(state, "schema_build_cache", {})
    return CypherLabelMap.from_schema(
        ctx,
        domain_access=role.get("domain_access"),
        all_tables=cache.get("tables"),
        all_relationships=cache.get("relationships"),
        all_column_types=cache.get("column_types"),
        source_catalogs=getattr(state, "source_catalogs", None),
    )


def _lookup_api_endpoint(state: AppState, table_name: str):
    """Look up an API endpoint by table name."""
    ep_map: dict = getattr(state, "api_endpoints", {})
    return ep_map.get(table_name)


def _lookup_gql_remote_table(state: AppState, table_name: str) -> dict | None:
    """Look up graphql_remote source info by SQL table name (snake_case)."""
    for reg in getattr(state, "graphql_remote_sources", {}).values():
        for t in reg.get("tables", []):
            if t["sql_name"] == table_name:
                return {
                    "source_id": reg["source_id"],
                    "url": reg["url"],
                    "auth": reg.get("auth"),
                    "field_name": t.get("field_name", t["name"]),
                    "columns": t.get("columns", []),
                    "required_args": t.get("required_args", []),
                    "cache_ttl": reg.get("cache_ttl", 300),
                    "cache_catalog": reg.get("cache_catalog", "provisa_admin"),
                }
    return None


async def _execute_with_api(
    exec_sql: str,
    params: list,
    nf_args: dict,
    state: Any,
    span_attrs: dict[str, str] | None = None,
) -> list[dict]:
    """Phase 1 (REST) + Phase 2 (the engine) execution for ALL API-backed tables in the query.

    For each API-backed table referenced in FROM/JOIN clauses:
      1. Derive URL params from nf_args columns that match the endpoint's native params.
      2. Materialize into the engine cache (cache miss) or reuse (cache hit).
      3. Rewrite all API table references in the SQL to their respective cache tables.
    This ensures json-typed JSONB columns are always exposed as VARCHAR in the cache.
    """
    import asyncio
    from provisa.api_source.router_integration import handle_api_query
    from provisa.api_source.engine_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.compiler.nf_extractor import find_api_table_names

    table_names = find_api_table_names(exec_sql)
    api_endpoints_in_sql: list[tuple[str, Any]] = []
    for tn in table_names:
        ep = _lookup_api_endpoint(state, tn)
        if ep is not None:
            api_endpoints_in_sql.append((tn, ep))

    if not api_endpoints_in_sql:
        raise RuntimeError(f"No API endpoint found for tables: {table_names}")

    hot_mgr = getattr(state, "hot_manager", None)

    # Hot table bypass: only applies when there is exactly one API table and it is hot.
    if len(api_endpoints_in_sql) == 1:
        table_name, endpoint = api_endpoints_in_sql[0]
        if hot_mgr is not None and hot_mgr.is_hot(table_name):
            from provisa.cache.hot_tables import build_values_cte_sql

            entry = hot_mgr.get_entry(table_name)
            hot_sql = build_values_cte_sql(exec_sql, table_name, entry)
            physical_sql = state.federation_engine.transpile_physical(hot_sql)
            log.info("[HOT TABLE] hit — %s (%d rows inline)", table_name, len(entry.rows))
            engine_result = await state.federation_engine.execute_engine(
                physical_sql, params, span_attrs=span_attrs
            )
            return [dict(zip(engine_result.column_names, row)) for row in engine_result.rows]

    from provisa.executor.redirect import RedirectConfig

    redirect_config = RedirectConfig.from_env()

    # Materialize every API-backed table into its the engine cache slot.
    cache_rewrites: dict[str, tuple] = {}  # physical table name → (CacheLocation, cache_tbl)
    for table_name, endpoint in api_endpoints_in_sql:
        _response_cols = [c for c in endpoint.columns if c.param_type is None]
        if not _response_cols:
            from provisa.compiler.nf_extractor import drop_union_branches_for_table

            exec_sql = drop_union_branches_for_table(exec_sql, table_name)
            log.warning(
                "[API CACHE] %s has no response columns — dropping union branch", table_name
            )
            continue
        source_id: Any = getattr(endpoint, "source_id", None)
        api_source = getattr(state, "api_sources", {}).get(source_id)

        # Filter nf_args to columns belonging to this endpoint.
        param_name_map: dict = {}
        valid_nf_keys: set = set()
        for c in endpoint.columns:
            if c.param_name:
                param_name_map[c.name] = c.param_name
                param_name_map[f"_{c.name}"] = c.param_name
                valid_nf_keys.add(c.name)
                valid_nf_keys.add(f"_{c.name}")
        url_params = {param_name_map.get(k, k): v for k, v in nf_args.items() if k in valid_nf_keys}

        _unfilled_path_params = [
            c.param_name or c.name
            for c in endpoint.columns
            if c.param_type is not None
            and c.param_type.value == "path"
            and (c.param_name or c.name) not in url_params
        ]
        if _unfilled_path_params:
            from provisa.compiler.nf_extractor import drop_union_branches_for_table

            exec_sql = drop_union_branches_for_table(exec_sql, table_name)
            log.warning(
                "[API CACHE] %s missing path params %s — dropping union branch",
                table_name,
                _unfilled_path_params,
            )
            continue

        _cc = getattr(api_source, "cache_catalog", None) if api_source else None
        _default_cs = f"org_{getattr(state, 'org_id', 'default')}_api_cache"
        _cs = getattr(api_source, "cache_schema", _default_cs) if api_source else _default_cs
        _cache_loc = cache_location(source_id, _cc, _cs)
        cache_tbl = cache_table_name(source_id, table_name, url_params)
        cache_rewrites[table_name] = (_cache_loc, cache_tbl)

        loop = asyncio.get_event_loop()

        def _check_schema_and_exists() -> bool:
            with state.federation_engine.isolated_sync() as conn:
                ensure_cache_schema(conn, _cache_loc)
                return table_exists(conn, _cache_loc, cache_tbl)

        hit = await loop.run_in_executor(None, _check_schema_and_exists)
        if not hit:
            result = await handle_api_query(
                endpoint=endpoint,
                params=url_params,
                engine=state.federation_engine,
                source=api_source,
                source_ttl=getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
                global_ttl=getattr(state, "response_cache_default_ttl", None),
                loc=_cache_loc,
            )

            ttl = (
                getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl")
                or getattr(state, "response_cache_default_ttl", None)
                or endpoint.ttl
            )
            asyncio.create_task(
                schedule_drop(state.federation_engine, _cache_loc, cache_tbl, ttl, redirect_config)
            )

            if hot_mgr is not None and result.rows:
                asyncio.create_task(hot_mgr.maybe_promote_dicts(table_name, result.rows))
        else:
            log.info("[API CACHE] hit — %s", cache_tbl)

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites)
    physical_sql = state.federation_engine.transpile_physical(rewritten_sql)

    result = await state.federation_engine.execute_engine(
        physical_sql, params, fresh=True, span_attrs=span_attrs
    )
    return [dict(zip(result.column_names, row)) for row in result.rows]


async def _execute_with_gql_remote(
    exec_sql: str,
    params: list,
    nf_args: dict,
    state: Any,
    span_attrs: dict[str, str] | None = None,
) -> list[dict]:
    """Materialize graphql_remote tables into the engine cache and execute the query."""
    import asyncio
    from dataclasses import dataclass
    from provisa.graphql_remote.executor import execute_remote
    from provisa.api_source.engine_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        create_and_insert,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.compiler.nf_extractor import (
        find_api_table_names,
        drop_joined_table,
        drop_union_branches_for_table,
        where_referenced_tables,
    )
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    @dataclass
    class _Col:
        name: str
        type: str

    _GQL_TO_CACHE_TYPE = {
        "text": "string",
        "integer": "integer",
        "numeric": "number",
        "boolean": "boolean",
        "jsonb": "jsonb",
    }

    table_names = find_api_table_names(exec_sql)
    where_tables = where_referenced_tables(exec_sql)
    cache_rewrites: dict[str, tuple] = {}
    gql_remote_skipped: set[str] = set()

    for tn in table_names:
        info = _lookup_gql_remote_table(state, tn)
        if info is None:
            continue

        required_args: list[dict] = info.get("required_args", [])
        # Build variables for this table from nf_args keyed by arg name
        gql_vars = {a["name"]: nf_args[a["name"]] for a in required_args if a["name"] in nf_args}
        missing = [a["name"] for a in required_args if a["name"] not in nf_args]
        if missing:
            if tn not in where_tables:
                # Not explicitly filtered — drop JOIN or UNION branch.
                exec_sql = drop_joined_table(exec_sql, tn)
                exec_sql = drop_union_branches_for_table(exec_sql, tn)
                gql_remote_skipped.add(tn)
                continue
            raise ValueError(
                f"Table '{tn}' requires argument(s) {missing} — "
                f"add a WHERE clause, e.g. WHERE n.{missing[0]} = <value>"
            )

        cache_loc = cache_location(
            info["source_id"],
            # The bound engine's cache catalog (native DuckDB → attached materialization store, which
            # isolated_sync attaches below; Trino → provisa_admin). Hardcoding provisa_admin
            # binder-errors on a native engine that never attaches it.
            state.federation_engine.cache_catalog() or info["cache_catalog"],
            f"org_{getattr(state, 'org_id', 'default')}_gql_cache",
        )
        cache_tbl = cache_table_name(info["source_id"], tn, gql_vars)
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        _info_columns: list = info["columns"]

        loop = asyncio.get_event_loop()

        def _check_or_create_cache(fetch_rows: list | None) -> bool:
            with state.federation_engine.isolated_sync() as conn:
                ensure_cache_schema(conn, cache_loc)
                if table_exists(conn, cache_loc, cache_tbl):
                    return True
                if fetch_rows is not None:
                    col_objs = [
                        _Col(
                            name=_apply_sql_name(c["name"]),
                            type=_GQL_TO_CACHE_TYPE.get(c.get("type", "text"), "string"),
                        )
                        for c in _info_columns
                    ]
                    _gql_to_sql = {c["name"]: _apply_sql_name(c["name"]) for c in _info_columns}
                    fetch_rows = [
                        {_gql_to_sql.get(k, k): v for k, v in row.items()} for row in fetch_rows
                    ]
                    create_and_insert(conn, cache_loc, cache_tbl, fetch_rows, col_objs)
                return False

        hit = await loop.run_in_executor(None, _check_or_create_cache, None)
        if not hit:
            col_selections = [c.get("gql_selection", c["name"]) for c in info["columns"]]
            fetched_rows = await execute_remote(
                url=info["url"],
                auth=info["auth"],
                field_name=info["field_name"],
                columns=col_selections,
                variables=gql_vars or None,
                required_args=required_args or None,
            )
            await loop.run_in_executor(None, _check_or_create_cache, fetched_rows)
            asyncio.create_task(
                schedule_drop(state.federation_engine, cache_loc, cache_tbl, info["cache_ttl"])
            )
        else:
            log.info("[GQL CACHE] hit — %s", cache_tbl)

    # If skipped gql_remote tables (missing required args) weren't dropped from the main FROM,
    # they still reference an uncacheable catalog — return empty instead of failing in the engine.
    if gql_remote_skipped and not cache_rewrites:
        still_present = gql_remote_skipped & set(find_api_table_names(exec_sql))
        if still_present:
            return []

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites) if cache_rewrites else exec_sql
    physical_sql = state.federation_engine.transpile_physical(rewritten_sql)

    result = await state.federation_engine.execute_engine(
        physical_sql, params, fresh=True, span_attrs=span_attrs
    )
    return [dict(zip(result.column_names, row)) for row in result.rows]


async def _execute(
    sql: str, params: list, state: Any, span_attrs: dict[str, str] | None = None
) -> list[dict]:
    """Execute SQL against the federation engine and return rows as dicts."""
    if not state.federation_engine.is_connected():
        raise RuntimeError("Federation engine not connected")
    result = await state.federation_engine.execute_engine(sql, params or [], span_attrs=span_attrs)
    return [dict(zip(result.column_names, row)) for row in result.rows]


async def _execute_call_body(
    call_body: Any,
    label_map: Any,
    params: dict,
    state: Any,
    ctx: Any,
    role_id: str = "default",
) -> tuple[list[dict], dict]:
    """Full pipeline execution for a single CALL subquery body."""
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    sql_ast, ordered_params, graph_vars = cypher_to_sql(call_body, label_map, params)
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    sql_str = sql_ast.sql(dialect="postgres")
    semantic_sql = make_semantic_sql(sql_str, ctx)
    _cb_span_attrs: dict[str, str] = _span_attrs_from_semantic_sql(semantic_sql, role_id)
    resolved_params = [params.get(name) for name in ordered_params]

    plan = await _govern_and_route_compiled(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
    )
    exec_sql = plan.exec_sql or ""
    physical_sql = plan.physical_sql or ""

    clean_exec_sql, clean_params, nf_args = extract_nf_args(exec_sql, resolved_params)
    api_table_names = find_api_table_names(exec_sql)
    has_api = any(_lookup_api_endpoint(state, tn) is not None for tn in api_table_names)
    has_gql_remote = any(_lookup_gql_remote_table(state, tn) is not None for tn in api_table_names)

    if nf_args or has_api:
        rows = await _execute_with_api(clean_exec_sql, clean_params, nf_args, state, _cb_span_attrs)
    elif has_gql_remote:
        rows = await _execute_with_gql_remote(
            exec_sql, resolved_params, nf_args, state, _cb_span_attrs
        )
    elif physical_sql:
        rows = await _execute(physical_sql, resolved_params, state, _cb_span_attrs)
    else:
        from provisa.pgwire._pipeline import _execute_plan as _exec_plan

        qr = await _exec_plan(plan, state)
        rows = [dict(zip(qr.column_names, row)) for row in qr.rows]

    return rows, graph_vars
