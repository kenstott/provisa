# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""POST /query/cypher — Cypher query endpoint (Phase AU, REQ-345–353).

Five-stage pipeline:
  1. Cypher parser + translator → SQLGlot AST (physical refs)
  2. Graph type rewriter → CAST(ROW(...) AS JSON) for node/edge columns
  3. make_semantic_sql → semantic refs; apply_governance → RLS/masking/visibility
  4. rewrite_semantic_to_trino_physical → catalog-qualified refs
  5. Federation executor → flat rows → assembler → typed response
"""

from __future__ import annotations

import logging
import time as _time
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

if TYPE_CHECKING:
    from provisa.cypher.label_map import CypherLabelMap  # noqa: F401

import re as _re

import trino.exceptions as _trino_exc

from provisa.executor import stats as _qs_mod

log = logging.getLogger(__name__)

router = APIRouter()

_PROC_RE = _re.compile(
    r"^\s*CALL\s+(db\.labels|db\.relationshipTypes|db\.propertyKeys)\s*\(\s*\)\s*$", _re.IGNORECASE
)


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


def _federation_error(exc: Exception) -> str:
    """Format execution errors without leaking the Trino backend name."""
    if isinstance(exc, _trino_exc.TrinoQueryError):
        parts = [f"type={exc.error_type}", f"name={exc.error_name}", f'message="{exc.message}"']
        if exc.query_id:
            parts.append(f"query_id={exc.query_id}")
        return "FederationUserError(" + ", ".join(parts) + ")"
    return str(exc)


def _detect_procedure(query: str) -> str | None:
    m = _PROC_RE.match(query.strip())
    return m.group(1).lower() if m else None


class CypherRequest(BaseModel):
    query: str
    params: dict[str, Any] = {}


@router.post("/data/cypher")
async def cypher_query(
    body: CypherRequest,
    request: Request,
    query_id: str | None = Query(None),
    x_provisa_stats: str | None = Header(None),
) -> Response:
    """Execute a Cypher read query and return typed rows."""
    from provisa.api.app import state

    if query_id:
        from provisa.api.data.endpoint_dev import QueryRequest, unified_query_endpoint

        queries: list[dict] = (getattr(state, "schema_build_cache", {}) or {}).get(
            "approved_queries", []
        )
        matched = next((q for q in queries if q.get("stable_id") == query_id), None)
        if matched is None:
            return JSONResponse(
                status_code=404, content={"error": f"Approved query not found: {query_id!r}"}
            )
        query_req = QueryRequest(
            query=matched.get("query_text") or "", role=_resolve_role_id(request, state)
        )
        result = await unified_query_endpoint(request, query_req, x_provisa_role=None)
        return result  # type: ignore[return-value]

    try:
        from provisa.cypher.parser import parse_cypher, CypherParseError
        from provisa.cypher.translator import (
            cypher_to_sql,
            CypherCrossSourceError,
            CypherTranslateError,
        )
        from provisa.cypher.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.params import collect_param_names, bind_params, CypherParamError
        from provisa.cypher.assembler import assemble_rows, to_serializable
        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
        from provisa.compiler.stage2 import apply_governance, build_governance_context
    except Exception as exc:
        log.exception("Cypher imports failed")
        return JSONResponse(status_code=500, content={"error": f"Import failed: {exc}"})

    # Resolve role → use default role_id
    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    # Intercept Neo4j-compatible schema procedures before parse
    _proc = _detect_procedure(body.query)
    if _proc is not None:
        label_map = _build_label_map(ctx, role_id, state)
        if _proc == "db.labels":
            # Return individual domain labels + table labels (multi-label nodes).
            # Each node contributes up to two labels; deduplicate and sort.
            all_labels: set[str] = set()
            for nm in label_map.nodes.values():
                if nm.domain_label:
                    all_labels.add(nm.domain_label)
                all_labels.add(nm.table_label)
            rows = [{"label": lbl} for lbl in sorted(all_labels)]
            return JSONResponse(content={"columns": ["label"], "rows": rows})
        if _proc == "db.relationshiptypes":
            rows = [
                {"relationshipType": r.rel_type}
                for r in sorted(label_map.relationships.values(), key=lambda x: x.rel_type)
            ]
            return JSONResponse(content={"columns": ["relationshipType"], "rows": rows})
        if _proc == "db.propertykeys":
            keys: set[str] = set()
            for nm in label_map.nodes.values():
                keys.update(nm.properties.keys())
            rows = [{"propertyKey": k} for k in sorted(keys)]
            return JSONResponse(content={"columns": ["propertyKey"], "rows": rows})

    # Stage 1: Parse
    try:
        ast = parse_cypher(body.query)
    except CypherParseError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Build label map
    label_map = _build_label_map(ctx, role_id, state)

    # Validate and bind params
    param_names = collect_param_names(body.query)
    try:
        bind_params(param_names, body.params)
    except CypherParamError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Multi-CALL pattern: independent (non-correlated) CALL blocks with no outer MATCH.
    # Translate and execute each body independently, then Python CROSS JOIN the results.
    _non_corr_calls = [cs for cs in ast.call_subqueries if not cs.imported_vars]
    if _non_corr_calls and not ast.match_clauses:
        try:
            rls = state.rls_contexts.get(role_id, RLSContext.empty())
            gov_ctx = build_governance_context(
                role_id, rls, state.masking_rules, ctx, getattr(state, "tables", [])
            )
        except Exception as exc:
            log.exception("Cypher governance setup failed")
            return JSONResponse(
                status_code=500, content={"error": f"Governance setup failed: {exc}"}
            )

        all_rows: list[list[dict]] = []
        merged_graph_vars: dict = {}
        for call_sq in _non_corr_calls:
            try:
                rows_i, gvars_i = await _execute_call_body(
                    call_sq.body, label_map, body.params, state, gov_ctx, ctx, role_id
                )
                all_rows.append(rows_i)
                merged_graph_vars.update(gvars_i)
            except Exception as exc:
                log.exception("Cypher multi-CALL execution failed")
                return JSONResponse(status_code=500, content={"error": f"Execution failed: {exc}"})

        combined: list[dict] = [{}]
        for rs in all_rows:
            combined = [{**a, **b} for a in combined for b in (rs or [{}])]

        try:
            assembled = assemble_rows(combined, merged_graph_vars)
        except Exception as exc:
            return JSONResponse(status_code=500, content={"error": f"Assembly failed: {exc}"})
        try:
            columns = list(combined[0].keys()) if combined else []
            serializable_rows = [to_serializable(r) for r in assembled]
        except Exception as exc:
            log.exception("Cypher serialization failed")
            return JSONResponse(status_code=500, content={"error": f"Serialization failed: {exc}"})
        return JSONResponse(content={"columns": columns, "rows": serializable_rows})

    # Stage 1: Translate to SQLGlot (physical catalog.schema.table refs)
    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, body.params)
    except CypherCrossSourceError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except CypherTranslateError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Stage 2: Graph type rewriter
    try:
        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    except Exception as exc:
        log.exception("Cypher graph rewrite failed")
        return JSONResponse(status_code=500, content={"error": f"Graph rewrite failed: {exc}"})

    # Render to SQL string (postgres dialect; make_semantic_sql handles catalog-qualified refs)
    try:
        import sqlglot

        sql_str = sql_ast.sql(dialect="postgres")
        if ast.comments:
            prefix = "\n".join(f"-- {c}" for c in ast.comments)
            sql_str = f"{prefix}\n{sql_str}"
    except Exception as exc:
        log.exception("Cypher SQL render failed")
        return JSONResponse(status_code=500, content={"error": f"SQL generation failed: {exc}"})

    # Stage 3: Governance — semantic SQL → apply RLS/masking/visibility
    try:
        rls = state.rls_contexts.get(role_id, RLSContext.empty())
        gov_ctx = build_governance_context(
            role_id, rls, state.masking_rules, ctx, getattr(state, "tables", [])
        )
        semantic_sql = make_semantic_sql(sql_str, ctx)

        # Validate against role-scoped GraphQL-equivalent rules
        from provisa.compiler.sql_validator import validate_sql

        _role_dict = state.roles.get(role_id) or {}
        _violations = validate_sql(
            semantic_sql,
            ctx,
            gov_ctx,
            _role_dict,
            getattr(state, "tables", []),
            bypass_relationship_guard=True,  # Cypher semantic layer is authoritative on joins
            bypass_uncovered_relationships=True,
        )
        if _violations:
            return JSONResponse(
                status_code=403,
                content={
                    "violations": [{"code": v.code, "message": v.message} for v in _violations]
                },
            )

        governed_sql = apply_governance(semantic_sql, gov_ctx)
    except Exception as exc:
        log.exception("Cypher governance failed")
        return JSONResponse(status_code=500, content={"error": f"Governance failed: {exc}"})

    # Stage 4: Rewrite to Trino-physical (catalog.schema.table)
    try:
        exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)
        # Inline non-materialized views — their refs resolve to the synthetic
        # '__provisa__' source which is not a Trino catalog. expand into the
        # view's (already physical) defining SQL, same as the GraphQL/SQL path.
        if state.view_sql_map:
            from provisa.compiler.view_expand import expand_view_refs

            exec_sql = expand_view_refs(exec_sql, state.view_sql_map)
    except Exception as exc:
        log.exception("Cypher physical rewrite failed")
        return JSONResponse(status_code=500, content={"error": f"Physical rewrite failed: {exc}"})

    # Transpile to Trino dialect
    try:
        trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    except Exception as exc:
        log.exception("Cypher SQL transpile failed")
        return JSONResponse(status_code=500, content={"error": f"Transpile failed: {exc}"})

    # Resolve ordered parameter values
    resolved_params = [body.params.get(name) for name in ordered_params]

    # Stage 5: Execute — extract _nf_* native filter args before Trino execution
    from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names

    clean_exec_sql, clean_params, nf_args = extract_nf_args(exec_sql, resolved_params)

    # Route through REST+cache whenever any table is API-backed, even with no nf_args.
    # This ensures JSONB columns (exposed as json by Trino's PG connector) are always
    # accessed as VARCHAR from the Trino cache, avoiding INVALID_CAST_ARGUMENT errors.
    # Scan the expanded exec_sql, not governed_sql: a view's body inlines API/graphql_remote
    # tables that the outer (view-name-only) governed_sql never names.
    _api_table_names = find_api_table_names(clean_exec_sql)
    _has_api_tables = any(_lookup_api_endpoint(state, tn) is not None for tn in _api_table_names)
    _has_gql_remote = any(
        _lookup_gql_remote_table(state, tn) is not None for tn in _api_table_names
    )

    # Build span attrs from semantic SQL table refs (already normalized: pet_store.pets)
    _cypher_span_attrs: dict[str, str] = _span_attrs_from_semantic_sql(
        semantic_sql, role_id, body.query
    )

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()
    _t0 = _time.perf_counter()

    import asyncio as _asyncio
    from provisa.api.data.endpoint import _request_timeout

    _timeout = _request_timeout()
    log.info("Cypher final SQL: %s", trino_sql)
    try:
        if _has_gql_remote:
            rows = await _asyncio.wait_for(
                _execute_with_gql_remote(
                    clean_exec_sql, clean_params, nf_args, state, _cypher_span_attrs
                ),
                timeout=_timeout,
            )
        elif nf_args or _has_api_tables:
            rows = await _asyncio.wait_for(
                _execute_with_api(clean_exec_sql, clean_params, nf_args, state, _cypher_span_attrs),
                timeout=_timeout,
            )
        else:
            rows = await _asyncio.wait_for(
                _execute(trino_sql, resolved_params, state, _cypher_span_attrs),
                timeout=_timeout,
            )
    except _asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={"error": f"Query timed out after {_timeout:.0f}s", "sql": trino_sql},
        )
    except Exception as exc:
        log.exception("Cypher execution failed: %s", trino_sql)
        return JSONResponse(
            status_code=500,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
        )

    # Assemble
    try:
        assembled = assemble_rows(rows, graph_vars)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Assembly failed: {exc}"})

    try:
        columns = list(rows[0].keys()) if rows else []
        serializable_rows = [to_serializable(r) for r in assembled]
    except Exception as exc:
        log.exception("Cypher serialization failed")
        return JSONResponse(status_code=500, content={"error": f"Serialization failed: {exc}"})

    content: dict = {"columns": columns, "rows": serializable_rows}
    if stats_enabled:
        _qs_mod.record(
            field="cypher",
            source="trino",
            strategy="federated",
            elapsed_ms=(_time.perf_counter() - _t0) * 1000,
            rows=len(serializable_rows),
            physical_sql=trino_sql,
        )
        qs = _qs_mod.current()
        if qs is not None:
            content["provisa_stats"] = qs.to_dict()
    return JSONResponse(content=content)


@router.get("/data/graph-schema")
async def graph_schema(request: Request) -> JSONResponse:
    """Return node labels and relationship types for the current role."""
    from provisa.api.app import state

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = _build_label_map(ctx, role_id, state)
    all_tables: list[dict] = getattr(state, "schema_build_cache", {}).get("tables", [])
    from provisa.cypher.label_map import _table_label_from_table_name

    cluster_by_name: dict[str, dict] = {
        _table_label_from_table_name(t["table_name"], t.get("domain_id")): {
            "scl1": t.get("l1_cluster"),
            "scl2": t.get("l2_cluster"),
            "scl3": t.get("l3_cluster"),
        }
        for t in all_tables
    }
    return JSONResponse(
        content={
            "node_labels": [
                {
                    "label": n.label,
                    "domain_label": n.domain_label,
                    "domain_id": n.domain_id,
                    "table_label": n.table_label,
                    "properties": list(n.properties.keys()),
                    "pk_columns": n.pk_columns,
                    "id_column": n.id_column,
                    "native_filter_columns": sorted(n.native_filter_columns),
                    "traversal_only": n.traversal_only,
                    **cluster_by_name.get(
                        n.table_label, {"scl1": None, "scl2": None, "scl3": None}
                    ),
                }
                for n in label_map.nodes.values()
            ],
            "relationship_types": [
                {
                    "type": r.rel_type,
                    "source": r.source_label,
                    "target": r.target_label,
                }
                for r in label_map.relationships.values()
            ],
        }
    )


def _resolve_role_id(request: Request, state: object) -> str:
    """Resolve the role_id from X-Provisa-Role header, falling back to the first registered role."""
    roles: dict = getattr(state, "roles", {})
    header_role = request.headers.get("x-provisa-role") or request.headers.get("X-Provisa-Role")
    if header_role and header_role in roles:
        return header_role
    if roles:
        return next(iter(roles))
    return "default"


def _build_label_map(ctx: object, role_id: str, state: object) -> CypherLabelMap:
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


def _lookup_api_endpoint(state: object, table_name: str):
    """Look up an API endpoint by table name, tolerating camelCase/snake_case mismatch.

    Physical SQL uses tables.table_name (may be snake_case after compiler normalization).
    state.api_endpoints is keyed by api_endpoints.table_name (raw operationId, may be camelCase).
    Try exact match first, then snake_case conversion of the key, then camelCase conversion.
    """
    from provisa.compiler.naming import to_snake_case

    ep_map: dict = getattr(state, "api_endpoints", {})
    ep = ep_map.get(table_name)
    if ep is not None:
        return ep
    # Try matching keys by comparing their snake_case forms
    snake_tn = to_snake_case(table_name)
    for key, val in ep_map.items():
        if to_snake_case(key) == snake_tn:
            return val
    return None


def _lookup_gql_remote_table(state: object, table_name: str) -> dict | None:
    """Look up graphql_remote source info by physical table name."""
    for reg in getattr(state, "graphql_remote_sources", {}).values():
        for t in reg.get("tables", []):
            if t["name"] == table_name or t.get("field_name") == table_name:
                return {
                    "source_id": reg["source_id"],
                    "url": reg["url"],
                    "auth": reg.get("auth"),
                    "field_name": t["field_name"],
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
    """Phase 1 (REST) + Phase 2 (Trino) execution for ALL API-backed tables in the query.

    For each API-backed table referenced in FROM/JOIN clauses:
      1. Derive URL params from nf_args columns that match the endpoint's native params.
      2. Materialize into Trino cache (cache miss) or reuse (cache hit).
      3. Rewrite all API table references in the SQL to their respective cache tables.
    This ensures json-typed JSONB columns are always exposed as VARCHAR in the cache.
    """
    import asyncio
    from provisa.api_source.router_integration import handle_api_query
    from provisa.api_source.trino_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino
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
            trino_sql = transpile_to_trino(hot_sql)
            log.info("[HOT TABLE] hit — %s (%d rows inline)", table_name, len(entry.rows))
            trino_result = execute_trino(state.trino_conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(trino_result.column_names, row)) for row in trino_result.rows]

    from provisa.executor.redirect import RedirectConfig

    redirect_config = RedirectConfig.from_env()

    # Materialize every API-backed table into its Trino cache slot.
    cache_rewrites: dict[str, tuple] = {}  # physical table name → (CacheLocation, cache_tbl)
    for table_name, endpoint in api_endpoints_in_sql:
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

        _cc = getattr(api_source, "cache_catalog", None) if api_source else None
        _cs = getattr(api_source, "cache_schema", "api_cache") if api_source else "api_cache"
        _cache_loc = cache_location(source_id, _cc, _cs)
        cache_tbl = cache_table_name(source_id, table_name, url_params)
        cache_rewrites[table_name] = (_cache_loc, cache_tbl)

        loop = asyncio.get_event_loop()

        def _check_schema_and_exists() -> bool:
            import trino as _trino

            conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
            try:
                ensure_cache_schema(conn, _cache_loc)
                return table_exists(conn, _cache_loc, cache_tbl)
            finally:
                conn.close()

        hit = await loop.run_in_executor(None, _check_schema_and_exists)
        if not hit:
            result = await handle_api_query(
                endpoint=endpoint,
                params=url_params,
                conn=state.trino_conn,
                source=api_source,
                source_ttl=getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
                global_ttl=getattr(state, "response_cache_default_ttl", None),
            )

            ttl = (
                getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl")
                or getattr(state, "response_cache_default_ttl", None)
                or endpoint.ttl
            )
            asyncio.create_task(
                schedule_drop(state.trino_conn, _cache_loc, cache_tbl, ttl, redirect_config)
            )

            if hot_mgr is not None and result.rows:
                asyncio.create_task(hot_mgr.maybe_promote_dicts(table_name, result.rows))
        else:
            log.info("[API CACHE] hit — %s", cache_tbl)

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites)
    trino_sql = transpile_to_trino(rewritten_sql)

    def _run_query() -> list[dict]:
        import trino as _trino

        conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
        try:
            result = execute_trino(conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(result.column_names, row)) for row in result.rows]
        finally:
            conn.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run_query)


async def _execute_with_gql_remote(
    exec_sql: str,
    params: list,
    nf_args: dict,
    state: Any,
    span_attrs: dict[str, str] | None = None,
) -> list[dict]:
    """Materialize graphql_remote tables into Trino cache and execute the query."""
    import asyncio
    from dataclasses import dataclass
    from provisa.graphql_remote.executor import execute_remote
    from provisa.api_source.trino_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        create_and_insert,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino
    from provisa.compiler.nf_extractor import find_api_table_names

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
    cache_rewrites: dict[str, tuple] = {}

    for tn in table_names:
        info = _lookup_gql_remote_table(state, tn)
        if info is None:
            continue

        required_args: list[dict] = info.get("required_args", [])
        # Build variables for this table from nf_args keyed by arg name
        gql_vars = {a["name"]: nf_args[a["name"]] for a in required_args if a["name"] in nf_args}
        missing = [a["name"] for a in required_args if a["name"] not in nf_args]
        if missing:
            raise ValueError(
                f"Table '{tn}' requires argument(s) {missing} — "
                f"add a WHERE clause, e.g. WHERE n.{missing[0]} = <value>"
            )

        cache_loc = cache_location(info["source_id"], info["cache_catalog"], "gql_cache")
        cache_tbl = cache_table_name(info["source_id"], tn, gql_vars)
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        _info_columns: list = info["columns"]

        loop = asyncio.get_event_loop()

        def _check_or_create_cache(fetch_rows: list | None) -> bool:
            import trino as _trino

            conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
            try:
                ensure_cache_schema(conn, cache_loc)
                if table_exists(conn, cache_loc, cache_tbl):
                    return True
                if fetch_rows is not None:
                    col_objs = [
                        _Col(
                            name=c["name"],
                            type=_GQL_TO_CACHE_TYPE.get(c.get("type", "text"), "string"),
                        )
                        for c in _info_columns
                    ]
                    create_and_insert(conn, cache_loc, cache_tbl, fetch_rows, col_objs)
                return False
            finally:
                conn.close()

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
                schedule_drop(state.trino_conn, cache_loc, cache_tbl, info["cache_ttl"])
            )
        else:
            log.info("[GQL CACHE] hit — %s", cache_tbl)

    if not cache_rewrites:
        raise RuntimeError(f"No graphql_remote table found for: {table_names}")

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites)
    trino_sql = transpile_to_trino(rewritten_sql)

    def _run_query() -> list[dict]:
        import trino as _trino

        conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
        try:
            result = execute_trino(conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(result.column_names, row)) for row in result.rows]
        finally:
            conn.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run_query)


async def _execute(
    sql: str, params: list, state: Any, span_attrs: dict[str, str] | None = None
) -> list[dict]:
    """Execute SQL against the federation engine and return rows as dicts."""
    from provisa.executor.trino import execute_trino

    trino_conn = getattr(state, "trino_conn", None)
    if trino_conn is None:
        raise RuntimeError("Federation engine not connected")

    import asyncio

    def _run() -> list[dict]:
        result = execute_trino(trino_conn, sql, params or [], span_attrs=span_attrs)
        return [dict(zip(result.column_names, row)) for row in result.rows]

    return await asyncio.get_event_loop().run_in_executor(None, _run)


async def _execute_call_body(
    call_body: Any,
    label_map: Any,
    params: dict,
    state: Any,
    gov_ctx: Any,
    ctx: Any,
    role_id: str = "default",
) -> tuple[list[dict], dict]:
    """Full pipeline execution for a single CALL subquery body."""
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
    from provisa.compiler.stage2 import apply_governance
    from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names
    import sqlglot

    sql_ast, ordered_params, graph_vars = cypher_to_sql(call_body, label_map, params)
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    sql_str = sql_ast.sql(dialect="postgres")
    semantic_sql = make_semantic_sql(sql_str, ctx)
    _cb_span_attrs: dict[str, str] = _span_attrs_from_semantic_sql(semantic_sql, role_id)
    governed_sql = apply_governance(semantic_sql, gov_ctx)
    exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)
    trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    resolved_params = [params.get(name) for name in ordered_params]

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
    else:
        rows = await _execute(trino_sql, resolved_params, state, _cb_span_attrs)

    return rows, graph_vars
