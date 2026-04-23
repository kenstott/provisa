# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/graphql endpoint (REQ-043).

Pipeline: parse -> compile -> MV rewrite -> sampling -> make_semantic_sql
  -> governance (RLS/masking/visibility) -> cache check -> route
  -> rewrite_to_physical -> transpile -> execute -> cache store -> serialize.
Mutations: parse -> compile_mutation -> RLS inject -> direct execute (never Trino).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time as _time

import httpx

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from graphql import GraphQLSyntaxError, OperationType
from pydantic import BaseModel

from provisa.cache.key import cache_key
from provisa.cache.middleware import build_cache_headers, check_cache, store_result
from provisa.compiler.hints import extract_graphql_hints, graphql_hints_to_session_props
from provisa.compiler.directives import extract_directives, extract_directives_from_sql_comments, merge_directives
from provisa.compiler.mutation_gen import (
    compile_mutation,
    inject_rls_into_mutation,
)
from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sampling import apply_sampling_if_needed
from provisa.compiler.sql_gen import (
    compile_query, make_semantic_sql,
    rewrite_semantic_to_physical, rewrite_semantic_to_trino_physical,
)
from provisa.executor.direct import execute_direct
from provisa.executor.serialize import serialize_aggregate, serialize_rows
from provisa.executor.trino import execute_trino
from provisa.executor import stats as _qs_mod
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability, InsufficientRightsError, check_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)

# Source-level hydration expiry: source_id → monotonic expiry.
# When set, the entire source is skipped (no pool acquire, no PG queries).
_source_hydration_expiry: dict[str, float] = {}

router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str | None = None
    variables: dict | None = None
    role: str = "admin"  # test mode: role passed in request
    extensions: dict | None = None  # APQ: {"persistedQuery": {"sha256Hash": "..."}}
    queryId: str | None = None  # Governed Query stable_id (Phase AN)


_ACCEPT_MAP = {
    "application/json": "json",
    "application/x-ndjson": "ndjson",
    "text/csv": "csv",
    "application/vnd.apache.parquet": "parquet",
    "application/vnd.apache.arrow.stream": "arrow",
}


def _parse_accept(accept: str | None) -> str:
    """Parse Accept header to output format name. Defaults to json."""
    if not accept:
        return "json"
    for mime, fmt in _ACCEPT_MAP.items():
        if mime in accept:
            return fmt
    return "json"


def _format_response(rows, columns, root_field, output_format):
    """Serialize query results in the requested output format."""
    if output_format == "json":
        return serialize_rows(rows, columns, root_field)

    if output_format == "ndjson":
        from provisa.executor.formats.ndjson import rows_to_ndjson
        content = rows_to_ndjson(rows, columns)
        return Response(content=content, media_type="application/x-ndjson")

    if output_format == "csv":
        from provisa.executor.formats.tabular import rows_to_csv
        content = rows_to_csv(rows, columns)
        return Response(content=content, media_type="text/csv")

    if output_format == "parquet":
        from provisa.executor.formats.tabular import rows_to_parquet
        content = rows_to_parquet(rows, columns)
        return Response(content=content, media_type="application/vnd.apache.parquet")

    if output_format == "arrow":
        from provisa.executor.formats.arrow import rows_to_arrow_ipc
        content = rows_to_arrow_ipc(rows, columns)
        return Response(content=content, media_type="application/vnd.apache.arrow.stream")

    return serialize_rows(rows, columns, root_field)


import re as _re

def _inject_probe_limit(sql: str, limit: int) -> str:
    """Inject or tighten a LIMIT clause for threshold probing.

    If the query already has a literal LIMIT, use the smaller of the two.
    If the query already has a parameterized LIMIT ($N), leave it unchanged.
    """
    # Parameterized limit already present — user-supplied, leave as-is
    if _re.search(r"\bLIMIT\s+\$\d+", sql, _re.IGNORECASE):
        return sql
    limit_match = _re.search(r"\bLIMIT\s+(\d+)", sql, _re.IGNORECASE)
    if limit_match:
        existing = int(limit_match.group(1))
        effective = min(existing, limit)
        return sql[:limit_match.start()] + f"LIMIT {effective}" + sql[limit_match.end():]
    return sql + f" LIMIT {limit}"


@router.get("/graphql")
async def graphql_get_endpoint(
    raw_request: Request,
    queryId: str,
    role: str = "admin",
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
):
    """Execute a governed query by stable_id over GET.

    GET /data/graphql?queryId=<stable_id>&role=<role>

    GET is safe and idempotent — responses are CDN/proxy-cacheable.
    Only approved governed queries are permitted; ad-hoc queries require POST.
    """
    request = GraphQLRequest(queryId=queryId, role=role)
    return await graphql_endpoint(
        raw_request=raw_request,
        request=request,
        x_provisa_role=x_provisa_role,
        accept=accept,
        x_provisa_redirect=None,
        x_provisa_redirect_threshold=None,
        x_provisa_redirect_format=None,
        query_id=None,
    )


@router.post("/graphql")
async def graphql_endpoint(
    raw_request: Request,
    request: GraphQLRequest,
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
    x_provisa_redirect: str | None = Header(None),
    x_provisa_redirect_threshold: int | None = Header(None),
    x_provisa_redirect_format: str | None = Header(None),
    x_provisa_stats: str | None = Header(None),
    query_id: str | None = Query(None),
):
    """Execute a GraphQL query or mutation. Content negotiation via Accept header.

    Redirect behavior:
    - X-Provisa-Redirect: true — force redirect regardless of row count
    - X-Provisa-Redirect-Threshold: N — override server threshold (rows)
    - X-Provisa-Redirect-Format: <mime> — format for redirected file
      (defaults to server config or parquet)

    When result rows exceed the threshold, the response is JSON with a redirect
    URL to the file on S3 in the requested redirect format.  Below threshold,
    the inline response uses the Accept header format (default JSON).
    """
    from provisa.api.app import state

    # Auth middleware role takes precedence, then header, then request body
    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role or request.role

    if role_id not in state.schemas:
        raise HTTPException(
            status_code=400,
            detail=f"No schema available for role {role_id!r}",
        )

    # Rights check
    role = state.roles.get(role_id)
    if role:
        try:
            check_capability(role, Capability.QUERY_DEVELOPMENT)
        except InsufficientRightsError as e:
            raise HTTPException(status_code=403, detail=str(e))

    # --- Governed Query path (queryId, Phase AN) ---
    # URL ?queryId= takes precedence over body field for HTTP GET-style approved lookups
    if query_id:
        request = GraphQLRequest(queryId=query_id, variables=request.variables, role=request.role)
    if request.queryId:
        if state.pg_pool is None:
            raise HTTPException(status_code=503, detail="Database pool not available")
        async with state.pg_pool.acquire() as conn:
            from provisa.registry.store import get_by_stable_id
            record = await get_by_stable_id(conn, request.queryId)
        if record is None or record.get("status") != "approved":
            raise HTTPException(
                status_code=404,
                detail=f"Governed query {request.queryId!r} not found or not approved",
            )
        request = GraphQLRequest(
            query=record["query_text"],
            variables=request.variables,
            role=request.role,
        )

    # --- APQ (Automatic Persisted Queries, Phase AN) ---
    apq_hash: str | None = None
    if request.extensions:
        pq = request.extensions.get("persistedQuery", {})
        apq_hash = pq.get("sha256Hash")

    if apq_hash and not request.query:
        # Hash-only request: look up in APQ cache
        apq_cache = getattr(state, "apq_cache", None)
        cached_query = await apq_cache.get(apq_hash) if apq_cache else None
        if cached_query is None:
            return JSONResponse(
                status_code=200,
                content={
                    "errors": [{"message": "PersistedQueryNotFound",
                                "extensions": {"code": "PERSISTED_QUERY_NOT_FOUND"}}]
                },
            )
        request = GraphQLRequest(
            query=cached_query,
            variables=request.variables,
            role=request.role,
        )
    elif apq_hash and request.query:
        # Hash + query: validate hash only here; cache AFTER successful execution (REQ-291)
        from provisa.apq.cache import compute_apq_hash
        expected = compute_apq_hash(request.query)
        if expected != apq_hash:
            raise HTTPException(status_code=400, detail="APQ hash mismatch")
        # apq_hash is preserved; we store after execution succeeds (see bottom of handler)

    if not request.query:
        raise HTTPException(status_code=400, detail="query is required")

    # Legacy comment hints (kept for backwards compat) — merge with directive hints below
    _legacy_hints = extract_graphql_hints(request.query)

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    # Parse and validate
    try:
        document = parse_query(schema, request.query, request.variables)
    except GraphQLValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except GraphQLSyntaxError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Extract directives: legacy comments < SQL comments < GraphQL directives
    _comment_directives = extract_directives_from_sql_comments(request.query)
    _ast_directives = extract_directives(document)
    directives = merge_directives(_comment_directives, _ast_directives)
    # Fall back to legacy @provisa comment hints if no directive route set
    if directives.steward_hint is None and _legacy_hints.get("route"):
        raw = _legacy_hints["route"]
        directives.route = "FEDERATED" if raw == "federated" else "DIRECT" if raw == "direct" else None

    steward_hint = directives.steward_hint

    # Apply variable defaults declared in the operation for any missing variables (§6.4.1)
    effective_variables = coerce_variable_defaults(document, request.variables)

    # Detect introspection queries (__schema, __type) and execute them
    # directly against the GraphQL schema instead of compiling to SQL.
    from graphql import execute as gql_execute
    from graphql.language.ast import OperationDefinitionNode
    introspection_fields = {"__schema", "__type", "__typename"}
    is_introspection = False
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode) and defn.selection_set:
            field_names = {
                sel.name.value
                for sel in defn.selection_set.selections
                if hasattr(sel, "name")
            }
            if field_names and field_names <= introspection_fields:
                is_introspection = True
                break

    if is_introspection:
        result = gql_execute(schema, document, variable_values=effective_variables)
        return JSONResponse({"data": result.data})

    # Detect operation type
    is_mut = any(
        hasattr(d, "operation") and d.operation == OperationType.MUTATION
        for d in document.definitions
    )
    is_sub = any(
        hasattr(d, "operation") and d.operation == OperationType.SUBSCRIPTION
        for d in document.definitions
    )

    if is_sub:
        from provisa.api.data.subscription_sse import handle_subscription_sse
        return await handle_subscription_sse(
            document, ctx, rls, state, effective_variables, role, role_id, raw_request,
            directives=directives,
        )

    output_format = _parse_accept(accept)

    # @redirect directive overrides HTTP headers; headers take precedence if both present
    directive_redirect_format = _parse_accept(directives.redirect_format) if directives.redirect_format else None
    redirect_format = (
        _parse_accept(x_provisa_redirect_format) if x_provisa_redirect_format
        else directive_redirect_format
    )
    directive_redirect_threshold = directives.redirect_threshold

    # Redirect-Format without a threshold implies force redirect.
    # Redirect-Format with a threshold is conditional on row count.
    # X-Provisa-Redirect: true is still supported as an explicit force.
    force_redirect = (x_provisa_redirect or "").lower() == "true"
    effective_threshold = x_provisa_redirect_threshold or directive_redirect_threshold
    if redirect_format and effective_threshold is None:
        force_redirect = True

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()

    if is_mut:
        response = await _handle_mutation(
            document, ctx, rls, state, effective_variables, role_id, raw_request,
        )
    else:
        response = await _handle_query(
            document, ctx, rls, state, effective_variables, role, output_format, role_id,
            force_redirect=force_redirect,
            redirect_threshold=effective_threshold,
            redirect_format=redirect_format,
            steward_hint=steward_hint,
            query_session_props=directives.to_session_props(),
        )

    if stats_enabled:
        qs = _qs_mod.current()
        if qs is not None:
            stats_dict = qs.to_dict()
            if isinstance(response, JSONResponse):
                body = json.loads(response.body)
                body.setdefault("extensions", {})["provisa_stats"] = stats_dict
                skip = {"content-length", "content-type"}
                extra = {k: v for k, v in response.headers.items() if k.lower() not in skip}
                response = JSONResponse(content=body, headers=extra)
            elif isinstance(response, dict):
                response.setdefault("extensions", {})["provisa_stats"] = stats_dict

    # AN (REQ-291): store APQ hash only after successful execution — never cache rejected queries
    if apq_hash and request.query and response is not None:
        apq_cache = getattr(state, "apq_cache", None)
        if apq_cache:
            await apq_cache.set(apq_hash, request.query)

    return response


async def _prepare_compiled(compiled, ctx, rls, state, role_id, role, fresh_mvs):
    """Apply governance, MV rewrite, Kafka filters, and sampling to a compiled query."""
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    if state.view_sql_map:
        from provisa.compiler.view_expand import expand_views
        compiled = expand_views(compiled, state.view_sql_map)

    # ABAC approval hook (Phase AE)
    if hasattr(state, "approval_hook") and state.approval_hook is not None:
        from provisa.auth.approval_hook import ApprovalRequest, should_check
        table_ids = {m.table_id for m in ctx.tables.values() if m.field_name == compiled.root_field}
        source_ids = compiled.sources
        hook_config = state.approval_hook_config
        table_hooks = getattr(state, "table_approval_hooks", {})
        source_hooks = getattr(state, "source_approval_hooks", {})
        if should_check(table_ids, source_ids, hook_config, table_hooks, source_hooks):
            req = ApprovalRequest(
                user=role_id, roles=[role_id] if role_id else [],
                tables=list(compiled.sources), columns=compiled.columns,
                operation="query",
            )
            resp = await state.approval_hook.evaluate(req)
            if not resp.approved:
                raise HTTPException(status_code=403, detail=f"Approval denied: {resp.reason}")

    original_sources = set(compiled.sources)
    compiled = rewrite_if_mv_match(compiled, fresh_mvs)
    mv_used = compiled.sources != original_sources
    if mv_used:
        log.info(
            "[QUERY %s] MV optimization applied — sources changed: %s → %s",
            compiled.root_field, original_sources, compiled.sources,
        )
    else:
        log.debug("[QUERY %s] No MV match, using original sources: %s",
                  compiled.root_field, compiled.sources)

    if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
        from provisa.kafka.window import inject_kafka_filters
        compiled = inject_kafka_filters(
            compiled, ctx, state.source_types, state.kafka_table_configs,
        )

    compiled = apply_sampling_if_needed(compiled, role)

    # Governance: compile → semantic SQL → apply RLS/masking/visibility
    gov_ctx = build_governance_context(
        role_id, rls, state.masking_rules, ctx, getattr(state, "tables", [])
    )
    compiled.sql = apply_governance(make_semantic_sql(compiled.sql, ctx), gov_ctx)
    if compiled.nodes_sql is not None:
        compiled.nodes_sql = apply_governance(make_semantic_sql(compiled.nodes_sql, ctx), gov_ctx)

    return compiled, mv_used


async def _hydrate_api_tables_before_trino(compiled, ctx, state) -> tuple[set, dict[str, float], dict[str, int]]:
    """Ensure API-backed PG cache tables are populated before Trino executes.

    For each openapi source in compiled.sources:
    - Non-path-param: call fill_api_table (TTL-aware, keyed by params hash).
    - Path-param (returns single object per call): fetch one row per parent PK value
      via fetch_pk_row (TTL-aware, hash IS the PK for single-object responses).

    Returns (dataloader_sources, hydration_times_ms, hydration_rows).
    """
    from provisa.api_source.models import ParamType
    from provisa.openapi.pg_cache import fetch_pk_row, fill_api_table, is_mem_fresh

    dataloader_sources: set = set()
    hydration_times: dict[str, float] = {}
    hydration_rows: dict[str, int] = {}
    if not hasattr(state, "api_endpoints") or not state.api_endpoints:
        return dataloader_sources, hydration_times, hydration_rows
    if state.pg_pool is None:
        return dataloader_sources, hydration_times, hydration_rows

    for source_id in compiled.sources:
        _t_src = _time.perf_counter()
        if _source_hydration_expiry.get(source_id, 0) > _time.monotonic():
            hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
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
            # Collect all parent PKs and issue one batch call instead of N path-param calls.
            dataloader_col = None
            dataloader_parent_join_col = None
            dataloader_parent_table_meta = None
            for (src_type, _), join_meta in ctx.joins.items():
                if join_meta.target.table_name == pg_table:
                    target_col = next(
                        (c for c in endpoint.columns
                         if c.name == join_meta.target_column and c.param_type == ParamType.query),
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
                # DataLoader: one batch call with all parent PKs as a query param list.
                # Always needs PG to fetch parent PKs — can't skip connection.
                dataloader_sources.add(source_id)
                async with state.pg_pool.acquire() as pg_conn:
                    p_table = dataloader_parent_table_meta.table_name
                    p_schema = "default" if p_table in state.api_endpoints else dataloader_parent_table_meta.schema_name
                    try:
                        rows = await pg_conn.fetch(
                            f'SELECT DISTINCT "{dataloader_parent_join_col}" FROM "{p_schema}"."{p_table}"'
                            f' WHERE "{dataloader_parent_join_col}" IS NOT NULL'
                        )
                        pk_values = [r[0] for r in rows]
                    except Exception as exc:
                        log.warning("DataLoader: failed to fetch parent PKs for %s: %s", pg_table, exc)
                        continue
                    if pk_values:
                        param_name = dataloader_col.param_name or dataloader_col.name
                        n = await fill_api_table(
                            src.base_url, endpoint.path, {param_name: pk_values},
                            pg_conn, pg_schema, pg_table, ttl,
                            endpoint.response_root, endpoint.error_path, endpoint.pk_column,
                        )
                        hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n
            elif not path_cols:
                # Collection endpoint: params known upfront — skip pool entirely if mem-fresh.
                param_name_map = {
                    c.name: (c.param_name or c.name)
                    for c in endpoint.columns if c.param_type is not None
                }
                raw_params = compiled.api_args or {}
                query_params = {param_name_map.get(k, k): v for k, v in raw_params.items()}
                if is_mem_fresh("default", pg_table, query_params):
                    continue
                async with state.pg_pool.acquire() as pg_conn:
                    n = await fill_api_table(src.base_url, endpoint.path, query_params, pg_conn, pg_schema, pg_table, ttl, endpoint.response_root, endpoint.error_path, endpoint.pk_column)
                    hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n
            else:
                # Path-param: needs PG to fetch parent PKs — can't skip connection.
                path_col = path_cols[0]
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
                    continue

                async with state.pg_pool.acquire() as pg_conn:
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
                        continue

                    for pk in pk_values:
                        n = await fetch_pk_row(
                            src.base_url, endpoint.path, path_param_name, pk,
                            pg_conn, pg_schema, pg_table, ttl,
                            endpoint.response_root, endpoint.error_path,
                        )
                        hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n

        hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
        if _min_ttl is not None:
            _source_hydration_expiry[source_id] = _time.monotonic() + _min_ttl

    return dataloader_sources, hydration_times, hydration_rows


def _count_rows_per_source(field_rows: list, ctx) -> dict[str, int]:
    """Count matched rows per source_id using join cardinality in the result.

    For one-to-many joins, sums the nested array lengths across all parent rows.
    For many-to-one / one-to-one joins, counts the non-null joined objects.
    The root source row count is NOT included here — callers use len(field_rows) for that.
    """
    counts: dict[str, int] = {}
    if not field_rows or not ctx or not hasattr(ctx, "joins"):
        return counts
    for (_, join_field), join_meta in ctx.joins.items():
        src_id = join_meta.target.source_id
        if join_meta.cardinality == "one-to-many":
            total = sum(
                len(row.get(join_field, []) or [])
                for row in field_rows
                if isinstance(row, dict)
            )
        else:
            total = sum(
                1 for row in field_rows
                if isinstance(row, dict) and row.get(join_field) is not None
            )
        counts[src_id] = counts.get(src_id, 0) + total
    return counts


def _build_mermaid(
    sources: set,
    source_types: dict,
    ctx,
    hydration_ms: dict[str, float],
    trino_ms: float | None,
    result_rows: int,
    root_field: str,
) -> str:
    """Build a Mermaid flowchart LR diagram for the federated query execution DAG."""
    def _node_id(s: str) -> str:
        return s.replace("-", "_").replace(".", "_")

    lines = ["flowchart LR"]

    for src_id in sorted(sources):
        src_type = source_types.get(src_id, "")
        nid = _node_id(src_id)
        if src_type == "openapi":
            h_ms = hydration_ms.get(src_id, 0.0)
            cache_label = "cache hit" if h_ms < 5 else f"hydration {round(h_ms)}ms"
            lines.append(f'    {nid}["{src_id}\\n({src_type})"]')
            lines.append(f'    pg_{nid}["PG cache\\n{src_id}"]')
            lines.append(f'    {nid} -->|"{cache_label}"| pg_{nid}')
            trino_label = f"{round(trino_ms)}ms" if trino_ms is not None else ""
            lines.append(f'    pg_{nid} -->|"federated {trino_label}"| trino')
        else:
            trino_label = f"{round(trino_ms)}ms" if trino_ms is not None else ""
            lines.append(f'    {nid}["{src_id}\\n({src_type})"]')
            lines.append(f'    {nid} -->|"federated {trino_label}"| trino')

    lines.append(f'    trino{{"Virtual\\nJoin"}}')
    lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
    lines.append('    trino --> result')

    # Add join-column annotations from ctx
    join_edges: set[tuple[str, str, str]] = set()
    for (src_type, tgt_type), join_meta in (ctx.joins or {}).items():
        src_id = next((s for s in sources if source_types.get(s) and src_type.lower() in s.lower()), None)
        tgt_id = next((s for s in sources if source_types.get(s) and tgt_type.lower() in s.lower()), None)
        if src_id and tgt_id:
            label = f"{join_meta.source_column}={join_meta.target_column}"
            join_edges.add((_node_id(src_id), _node_id(tgt_id), label))

    return "\n".join(lines)


async def _execute_api_source(compiled, state, source_id, root_field, ck, output_format):
    """Execute a query against an API source in two phases.

    Phase 1 — REST call: native filter args (api_args) build the URL.
              The full API response is materialized as Parquet in Trino Iceberg
              (results.api_cache) so subsequent queries with the same native args
              hit Trino directly without re-fetching.

    Phase 2 — Trino SQL: the compiled WHERE/ORDER BY/LIMIT are applied by Trino
              against the cached Parquet table.  The FROM clause is rewritten
              from the logical table ref to the cache table.
    """
    from provisa.api_source.router_integration import handle_api_query
    from provisa.api_source.trino_cache import (
        cache_table_name, ensure_cache_schema, table_exists,
        create_and_insert, rewrite_from_cache, schedule_drop,
    )
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino

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

    # Resolve native filter args (path/query params) — may be "_"-prefixed on collision.
    url_params: dict = compiled.api_args.copy() if compiled.api_args else {}
    param_name_map: dict = {}
    for c in endpoint.columns:
        if c.param_name:
            param_name_map[c.name] = c.param_name
            param_name_map[f"_{c.name}"] = c.param_name
    url_params = {param_name_map.get(k, k): v for k, v in url_params.items()}

    # Compute stable cache table name from the REST call signature.
    cache_tbl = cache_table_name(source_id, table_name, url_params)

    # --- Phase 1: materialize if cache miss ---
    ensure_cache_schema(state.trino_conn)
    if not table_exists(state.trino_conn, cache_tbl):
        result = await handle_api_query(
            endpoint=endpoint,
            params=url_params,
            conn=state.trino_conn,
            source=api_source,
            source_ttl=state.source_cache.get(source_id, {}).get("cache_ttl"),
            global_ttl=state.cache_default_ttl,
        )

        if result.from_cache:
            # handle_api_query hit its own path-based cache — reuse that table directly
            # rather than creating an empty table_name-based cache.
            cache_tbl = result.cache_table
            log.info("[API CACHE] path-cache hit — reusing %s", cache_tbl)
        else:
            log.info("[API CACHE] miss — %d rows from REST, materializing", len(result.rows))
            create_and_insert(state.trino_conn, cache_tbl, result.rows, endpoint.columns)

            # Schedule TTL cleanup
            ttl = (
                state.source_cache.get(source_id, {}).get("cache_ttl")
                or state.cache_default_ttl
                or endpoint.ttl
            )
            from provisa.executor.redirect import RedirectConfig
            redirect_config = RedirectConfig.from_env()
            asyncio.create_task(
                schedule_drop(state.trino_conn, cache_tbl, ttl, redirect_config)
            )
    else:
        log.info("[API CACHE] hit — %s", cache_tbl)

    # --- Phase 2: apply WHERE/ORDER BY/LIMIT via Trino ---
    # Strip _nf_* conditions before executing against the cache table (they were
    # injected into SQL for display/CQL purposes only; api_args already drove Phase 1).
    from provisa.compiler.nf_extractor import extract_nf_args
    exec_sql, exec_params, _ = extract_nf_args(compiled.sql, compiled.params)
    rewritten_sql = rewrite_from_cache(exec_sql, cache_tbl)
    trino_sql = transpile_to_trino(rewritten_sql)
    trino_result = execute_trino(state.trino_conn, trino_sql, exec_params)

    from provisa.compiler.sql_gen import ColumnRef
    columns = [
        ColumnRef(alias=None, column=name, field_name=name, nested_in=None)
        for name in trino_result.column_names
    ]

    response_data = _format_response(trino_result.rows, columns, root_field, output_format)
    if isinstance(response_data, dict):
        field_rows = response_data.get("data", {}).get(root_field, [])
    else:
        field_rows = response_data

    return field_rows, response_data


async def _execute_one_field(
    compiled, ctx, rls, state, role, role_id,
    fresh_mvs, output_format,
    *, force_redirect, redirect_config, effective_redirect_format, probe_limit,
    steward_hint: str | None = None,
    query_session_props: dict | None = None,
):
    """Execute a single compiled query field through the full pipeline.

    Returns (root_field, field_rows, redirect_info_or_None, cache_key, cached_entry_or_None).
    field_rows is the row data for inline responses.
    redirect_info is the redirect dict for redirect responses.
    cached_entry is the CachedResult on cache hit, None otherwise.
    """
    from provisa.executor.redirect import upload_and_presign
    from provisa.executor.trino_write import is_trino_native_format

    root_field = compiled.root_field
    _t0 = _time.perf_counter()

    def _record_per_source(
        sources: set,
        elapsed_ms: float,
        rows: int,
        decision=None,
        dataloader_sources: set | None = None,
        per_source_ms: dict[str, float] | None = None,
        trino_ms: float | None = None,
        hydration_rows: dict[str, int] | None = None,
        field_rows: list | None = None,
    ) -> None:
        """Emit FieldStat entries per source.

        For openapi sources in a federated join: emits two entries —
        one for hydration (HTTP fetch → PG write) and one for the Trino join.
        For all other sources: one entry with Trino execution time.
        Per-source row counts use join cardinality from the result for one-to-many joins.
        """
        from provisa.transpiler.router import Route
        joined_rows = _count_rows_per_source(field_rows or [], ctx) if field_rows else {}
        for src_id in (sources or set()):
            source_type = getattr(state, "source_types", {}).get(src_id, "")
            if decision is None or decision.route != Route.DIRECT:
                prefix = "federated"
            else:
                prefix = "direct"
            strategy = f"{prefix}:{source_type}" if source_type else prefix
            if dataloader_sources and src_id in dataloader_sources:
                strategy += ":dataloader"

            src_rows = joined_rows.get(src_id, rows)

            if source_type == "openapi" and per_source_ms is not None and trino_ms is not None:
                hydration = per_source_ms.get(src_id, 0.0)
                h_rows = (hydration_rows or {}).get(src_id, 0)
                _qs_mod.record(field=root_field, source=src_id, strategy="hydration",
                               elapsed_ms=hydration, rows=h_rows)
                _qs_mod.record(field=root_field, source=src_id, strategy=strategy,
                               elapsed_ms=trino_ms, rows=src_rows)
            else:
                src_ms = per_source_ms.get(src_id, elapsed_ms) if per_source_ms else elapsed_ms
                _qs_mod.record(field=root_field, source=src_id, strategy=strategy,
                               elapsed_ms=src_ms, rows=src_rows)
    # Cache check
    rls_rules_for_key = rls.rules if rls.has_rules() else {}
    ck = cache_key(compiled.sql, compiled.params, role_id, rls_rules_for_key)
    cached = await check_cache(state.cache_store, ck)
    if cached is not None:
        cached_data = json.loads(cached.data)
        field_rows = cached_data.get("data", {}).get(root_field, [])
        _qs_mod.record(field=root_field, source="cache", strategy="cache",
                       elapsed_ms=(_time.perf_counter() - _t0) * 1000,
                       rows=len(field_rows) if isinstance(field_rows, list) else 0,
                       cache_hit=True)
        return root_field, field_rows, None, ck, cached

    # Route decision
    has_json_extract = "->>" in compiled.sql
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=steward_hint,
        has_json_extract=has_json_extract,
    )
    log.info(
        "[QUERY %s] Route: %s | source=%s | reason: %s",
        root_field, decision.route.value,
        decision.source_id or "(trino)", decision.reason,
    )

    # --- API source path: fetch from external API via router_integration ---
    if decision.route == Route.API and decision.source_id:
        try:
            field_rows, response_data = await _execute_api_source(
                compiled, state, decision.source_id, root_field, ck, output_format,
            )
        except HTTPException:
            raise
        except Exception as e:
            log.exception("API source execution failed for %s", root_field)
            raise HTTPException(status_code=500, detail=str(e))
        source_type = getattr(state, "source_types", {}).get(decision.source_id, "")
        _qs_mod.record(field=root_field, source=decision.source_id,
                       strategy=f"api:{source_type}" if source_type else "api",
                       elapsed_ms=(_time.perf_counter() - _t0) * 1000,
                       rows=len(field_rows) if isinstance(field_rows, list) else 0)
        return root_field, field_rows, None, ck, None

    # --- CTAS path: force redirect with Trino-native format ---
    if (
        force_redirect
        and is_trino_native_format(effective_redirect_format)
        and state.trino_conn is not None
    ):
        try:
            from provisa.executor.trino_write import (
                execute_ctas_redirect, presign_ctas_result,
                cleanup_result_table, schedule_s3_cleanup,
            )
            _ctas_hydration_ms: dict[str, float]
            _, _ctas_hydration_ms, _ = await _hydrate_api_tables_before_trino(compiled, ctx, state)
            trino_sql = transpile_to_trino(rewrite_semantic_to_trino_physical(compiled.sql, ctx))
            ctas_result = execute_ctas_redirect(
                state.trino_conn, trino_sql, effective_redirect_format,
            )
            url = await presign_ctas_result(ctas_result["s3_prefix"], redirect_config)
            cleanup_result_table(state.trino_conn, ctas_result["table_name"])
            asyncio.create_task(
                schedule_s3_cleanup(ctas_result["s3_prefix"], redirect_config),
            )
            content_type = {
                "parquet": "application/vnd.apache.parquet",
                "orc": "application/x-orc",
            }.get(effective_redirect_format, "application/octet-stream")
            redirect_info = {
                "redirect_url": url,
                "row_count": ctas_result["row_count"],
                "expires_in": redirect_config.ttl,
                "content_type": content_type,
            }
            _record_per_source(compiled.sources, (_time.perf_counter() - _t0) * 1000,
                               ctas_result["row_count"])
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("CTAS redirect failed for %s, falling back", root_field)

    # --- Standard execution ---
    try:
        if decision.route == Route.DIRECT and decision.source_id and state.source_pools.has(decision.source_id):
            exec_sql = rewrite_semantic_to_physical(compiled.sql, ctx)
            if probe_limit is not None:
                exec_sql = _inject_probe_limit(exec_sql, probe_limit)
            target_sql = transpile(exec_sql, decision.dialect or "postgres")
            result = await execute_direct(
                state.source_pools, decision.source_id, target_sql, compiled.params,
            )
        else:
            if state.trino_conn is None:
                raise HTTPException(status_code=503, detail="Trino not connected")
            _dataloader_srcs, _hydration_ms, _hydration_rows = await _hydrate_api_tables_before_trino(compiled, ctx, state)
            exec_sql = rewrite_semantic_to_trino_physical(compiled.sql, ctx)
            if probe_limit is not None:
                exec_sql = _inject_probe_limit(exec_sql, probe_limit)

            # AL5: extract comment hints from SQL; AL3: merge source-level federation_hints
            from provisa.compiler.hints import extract_hints
            exec_sql, comment_hints = extract_hints(exec_sql)
            session_hints: dict[str, str] = {}
            for sid in compiled.sources:
                src_hints = getattr(state, "source_federation_hints", {}).get(sid, {})
                session_hints.update(src_hints)
            session_hints.update(query_session_props or {})  # @provisa hints
            session_hints.update(comment_hints)  # SQL /*+ */ hints take precedence

            trino_sql = transpile_to_trino(exec_sql)
            _t_trino = _time.perf_counter()
            result = execute_trino(
                state.trino_conn, trino_sql, compiled.params,
                session_hints=session_hints or None,
            )
            _trino_ms = (_time.perf_counter() - _t_trino) * 1000
            # API sources: charge their hydration time; DB sources: charge Trino execution time
            _per_source_ms: dict[str, float] = {
                src_id: _hydration_ms.get(src_id, _trino_ms)
                if (state.source_types or {}).get(src_id) == "openapi"
                else _trino_ms
                for src_id in compiled.sources
            }
            # Lazy hot-table promotion: if result is small, cache it for future JOINs
            _hot_mgr = getattr(state, "hot_manager", None)
            if _hot_mgr is not None:
                _tbl = compiled.canonical_field or root_field
                asyncio.create_task(
                    _hot_mgr.maybe_promote(_tbl, result.rows, result.column_names)
                )
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Query execution failed for %s", root_field)
        raise HTTPException(status_code=500, detail=str(e))

    # --- Check if probe exceeded threshold → redirect ---
    if probe_limit is not None and len(result.rows) >= probe_limit:
        log.info(
            "[QUERY %s] Probe returned %d rows (threshold %d) — redirecting",
            root_field, len(result.rows), redirect_config.threshold,
        )
        try:
            # Re-execute without probe limit
            if decision.route == Route.DIRECT and decision.source_id:
                target_sql = transpile(rewrite_semantic_to_physical(compiled.sql, ctx), decision.dialect or "postgres")
                full_result = await execute_direct(
                    state.source_pools, decision.source_id, target_sql, compiled.params,
                )
            else:
                full_trino_sql = transpile_to_trino(rewrite_semantic_to_trino_physical(compiled.sql, ctx))
                full_result = execute_trino(
                    state.trino_conn, full_trino_sql, compiled.params,
                    session_hints=session_hints or None,
                )
            redirect_info = await upload_and_presign(
                full_result, redirect_config,
                output_format=effective_redirect_format,
                columns=compiled.columns,
            )
            _record_per_source(compiled.sources, (_time.perf_counter() - _t0) * 1000,
                               redirect_info.get("row_count", 0), decision)
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("Redirect upload failed for %s, returning inline", root_field)

    # --- Force redirect (non-threshold, non-CTAS) ---
    if force_redirect:
        try:
            redirect_info = await upload_and_presign(
                result, redirect_config,
                output_format=effective_redirect_format,
                columns=compiled.columns,
            )
            _record_per_source(compiled.sources, (_time.perf_counter() - _t0) * 1000,
                               redirect_info.get("row_count", 0), decision)
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("Redirect upload failed for %s, returning inline", root_field)

    # --- Inline result ---
    # Aggregate queries with nodes: execute the plain-SELECT nodes query and
    # merge it with the aggregate result via serialize_aggregate.
    if compiled.nodes_sql is not None:
        try:
            if decision.route == Route.DIRECT and decision.source_id:
                nodes_target_sql = transpile(rewrite_semantic_to_physical(compiled.nodes_sql, ctx), decision.dialect or "postgres")
                nodes_result = await execute_direct(
                    state.source_pools, decision.source_id, nodes_target_sql, compiled.nodes_params,
                )
            else:
                nodes_trino_sql = transpile_to_trino(rewrite_semantic_to_trino_physical(compiled.nodes_sql, ctx))
                nodes_result = execute_trino(state.trino_conn, nodes_trino_sql, compiled.nodes_params)
        except Exception as e:
            log.exception("Nodes query execution failed for %s", root_field)
            raise HTTPException(status_code=500, detail=str(e))
        response_data = serialize_aggregate(
            result.rows, compiled.columns,
            nodes_result.rows, compiled.nodes_columns,
            root_field,
            agg_alias=compiled.agg_alias,
        )
    else:
        response_data = _format_response(result.rows, compiled.columns, root_field, output_format)

    # Extract rows from serialized response for merging
    if isinstance(response_data, dict):
        field_rows = response_data.get("data", {}).get(root_field, [])
    else:
        # Binary format (parquet/arrow/csv) — can't merge, return as-is
        field_rows = response_data

    # Cache store with hierarchical TTL resolution
    if isinstance(response_data, dict):
        table_ids = {
            meta.table_id for meta in ctx.tables.values()
            if meta.field_name == root_field
        }
        # Resolve per-source/per-table cache policy
        from provisa.cache.policy import resolve_policy, CachePolicy
        source_id = next(iter(compiled.sources), None)
        src_cache = state.source_cache.get(source_id, {}) if source_id else {}
        table_id = next(iter(table_ids), None)
        tbl_cache_ttl = state.table_cache.get(table_id) if table_id else None
        _policy, resolved_ttl = resolve_policy(
            stable_id=None,  # ad-hoc queries still cached in test mode
            cache_ttl=None,
            default_ttl=state.cache_default_ttl,
            source_cache_enabled=src_cache.get("cache_enabled", True),
            source_cache_ttl=src_cache.get("cache_ttl"),
            table_cache_ttl=tbl_cache_ttl,
        )
        if resolved_ttl > 0:
            await store_result(
                state.cache_store, ck, response_data,
                ttl=resolved_ttl, table_ids=table_ids,
            )

    _n_rows = len(field_rows) if isinstance(field_rows, list) else 0
    _record_per_source(
        compiled.sources,
        (_time.perf_counter() - _t0) * 1000,
        _n_rows,
        decision,
        dataloader_sources=locals().get("_dataloader_srcs"),
        per_source_ms=locals().get("_per_source_ms"),
        trino_ms=locals().get("_trino_ms"),
        hydration_rows=locals().get("_hydration_rows"),
        field_rows=field_rows if isinstance(field_rows, list) else None,
    )
    qs = _qs_mod.current()
    if qs is not None and len(compiled.sources) > 1:
        new_mermaid = _build_mermaid(
            compiled.sources,
            getattr(state, "source_types", {}),
            ctx,
            locals().get("_per_source_ms") or {},
            locals().get("_trino_ms"),
            _n_rows,
            root_field,
        )
        qs.mermaid = f"{qs.mermaid}\n\n{new_mermaid}" if qs.mermaid else new_mermaid
    return root_field, field_rows, None, ck, None



async def _handle_query(document, ctx, rls, state, variables, role, output_format="json", role_id="admin", *, force_redirect=False, redirect_threshold=None, redirect_format=None, steward_hint: str | None = None, query_session_props: dict | None = None):
    """Handle a GraphQL query operation with content negotiation.

    Pipeline per root field: compile → RLS → masking → MV rewrite → sampling
      → cache check → route → transpile → execute → cache store → serialize.
    Multiple root fields are executed independently and merged.
    """
    action_sels, regular_names = _split_action_fields(document, state)

    if action_sels and not regular_names:
        data = {}
        for sel in action_sels:
            data[sel.name.value] = await _execute_action_field(sel.name.value, sel, state, variables, ctx=ctx)
        return JSONResponse(content={"data": data}, headers=build_cache_headers(None))

    if action_sels and regular_names:
        raise HTTPException(status_code=400, detail="Cannot mix action fields with table queries")

    compiled_queries = compile_query(document, ctx, variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()

    # Prepare all compiled queries (RLS, masking, MV rewrite, sampling)
    prepared = []
    for cq in compiled_queries:
        prepped, _mv_used = await _prepare_compiled(cq, ctx, rls, state, role_id, role, fresh_mvs)
        prepared.append(prepped)

    # Determine redirect config
    from provisa.executor.redirect import RedirectConfig
    from provisa.executor.trino_write import is_trino_native_format
    redirect_config = RedirectConfig.from_env()
    if redirect_threshold is not None:
        redirect_config = RedirectConfig(
            enabled=True,
            threshold=redirect_threshold,
            bucket=redirect_config.bucket,
            endpoint_url=redirect_config.endpoint_url,
            access_key=redirect_config.access_key,
            secret_key=redirect_config.secret_key,
            ttl=redirect_config.ttl,
            region=redirect_config.region,
            default_format=redirect_config.default_format,
        )
    effective_redirect_format = redirect_format or redirect_config.default_format or "parquet"

    probe_limit = None
    if not force_redirect and redirect_config.enabled and redirect_config.threshold > 0:
        probe_limit = redirect_config.threshold + 1

    # --- Single root field: preserve existing behavior for binary formats ---
    if len(prepared) == 1:
        root_field, field_rows, redirect_info, ck, cached_entry = await _execute_one_field(
            prepared[0], ctx, rls, state, role, role_id,
            fresh_mvs, output_format,
            force_redirect=force_redirect,
            redirect_config=redirect_config,
            effective_redirect_format=effective_redirect_format,
            probe_limit=probe_limit,
            steward_hint=steward_hint,
            query_session_props=query_session_props,
        )
        if cached_entry is not None:
            headers = build_cache_headers(cached_entry)
            return JSONResponse(
                content={"data": {root_field: field_rows}},
                headers=headers,
            )
        if redirect_info is not None:
            return {"data": {root_field: None}, "redirect": redirect_info}
        # Binary format passthrough (parquet/arrow/csv single-field)
        if not isinstance(field_rows, list):
            return field_rows
        headers = build_cache_headers(None)
        return JSONResponse(
            content={"data": {root_field: field_rows}},
            headers=headers,
        )

    # --- Multiple root fields: execute each independently, merge results ---
    merged_data: dict = {}
    merged_redirects: dict = {}

    for compiled in prepared:
        root_field, field_rows, redirect_info, ck, cached_entry = await _execute_one_field(
            compiled, ctx, rls, state, role, role_id,
            fresh_mvs, "json",  # multi-field always uses JSON
            force_redirect=force_redirect,
            redirect_config=redirect_config,
            effective_redirect_format=effective_redirect_format,
            probe_limit=probe_limit,
            steward_hint=steward_hint,
            query_session_props=query_session_props,
        )
        if redirect_info is not None:
            merged_data[root_field] = None
            merged_redirects[root_field] = redirect_info
        else:
            merged_data[root_field] = field_rows

    response = {"data": merged_data}
    if merged_redirects:
        response["redirects"] = merged_redirects

    headers = build_cache_headers(None)
    return JSONResponse(content=response, headers=headers)


def _check_writable_by(table_meta, columns: list[str], role_id: str):
    """Raise 403 if any column restricts write access and the role is not allowed."""
    table_cols = {c["column_name"]: c for c in table_meta.columns} if hasattr(table_meta, "columns") else {}
    if not table_cols:
        # Fall back to dict-style access (from state.tables)
        table_cols = {c.get("column_name", c.get("name", "")): c for c in getattr(table_meta, "columns", [])}
    for col_name in columns:
        col_meta = table_cols.get(col_name)
        if not col_meta:
            continue
        writable_by = col_meta.get("writable_by", []) if isinstance(col_meta, dict) else getattr(col_meta, "writable_by", [])
        if role_id not in writable_by:
            raise HTTPException(
                status_code=403,
                detail=f"Role {role_id!r} does not have write access to column {col_name!r}",
            )


_ACTION_FILTER_ARGS = {"where", "order_by", "limit", "offset"}


async def _resolve_action_relationships(
    rows: list[dict],
    selection_set,
    return_type_name: str,
    ctx,
    state,
) -> list[dict]:
    """Batch-resolve nested relationship fields on action result rows."""
    from graphql import FieldNode as _FieldNode
    from provisa.executor.serialize import _convert_value

    for sel in selection_set.selections:
        if not isinstance(sel, _FieldNode):
            continue
        rel_field = sel.name.value
        join_key = (return_type_name, rel_field)
        if join_key not in ctx.joins:
            continue

        join_meta = ctx.joins[join_key]
        src_col = join_meta.source_column
        tgt_col = join_meta.target_column
        tgt = join_meta.target

        nested_cols = []
        if sel.selection_set:
            for ns in sel.selection_set.selections:
                if isinstance(ns, _FieldNode):
                    nested_cols.append(ns.name.value)
        if not nested_cols:
            for r in rows:
                r[rel_field] = None if join_meta.cardinality == "many-to-one" else []
            continue

        src_values = list({r[src_col] for r in rows if r.get(src_col) is not None})
        if not src_values or not state.source_pools.has(tgt.source_id):
            for r in rows:
                r[rel_field] = None if join_meta.cardinality == "many-to-one" else []
            continue

        select_cols = list({tgt_col} | set(nested_cols))
        col_list = ", ".join(f'"{c}"' for c in select_cols)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(src_values)))
        sql = (
            f'SELECT {col_list} FROM "{tgt.schema_name}"."{tgt.table_name}"'
            f' WHERE "{tgt_col}" IN ({placeholders})'
        )
        result = await state.source_pools.execute(tgt.source_id, sql, src_values)
        rel_cols = result.column_names
        rel_rows = [{c: _convert_value(v) for c, v in zip(rel_cols, r)} for r in result.rows]

        if join_meta.cardinality == "many-to-one":
            rel_index = {rr[tgt_col]: {k: rr[k] for k in nested_cols if k in rr} for rr in rel_rows}
            for r in rows:
                r[rel_field] = rel_index.get(r.get(src_col))
        else:
            from collections import defaultdict
            rel_index_multi: dict = defaultdict(list)
            for rr in rel_rows:
                child = {k: rr[k] for k in nested_cols if k in rr}
                rel_index_multi[rr[tgt_col]].append(child)
            for r in rows:
                r[rel_field] = rel_index_multi.get(r.get(src_col), [])

    return rows


def _apply_action_filters(rows: list[dict], args: dict) -> list[dict]:
    """Apply where/order_by/limit/offset post-processing to action result rows."""
    where = args.get("where")
    if where and isinstance(where, dict):
        def _matches(row: dict) -> bool:
            for field, condition in where.items():
                val = row.get(field)
                if isinstance(condition, dict):
                    for op, cmp in condition.items():
                        if op == "_eq" and val != cmp:
                            return False
                        elif op == "_neq" and val == cmp:
                            return False
                        elif op == "_gt" and not (val is not None and val > cmp):
                            return False
                        elif op == "_gte" and not (val is not None and val >= cmp):
                            return False
                        elif op == "_lt" and not (val is not None and val < cmp):
                            return False
                        elif op == "_lte" and not (val is not None and val <= cmp):
                            return False
                        elif op == "_in" and val not in (cmp or []):
                            return False
                        elif op == "_nin" and val in (cmp or []):
                            return False
                        elif op == "_like" and not (isinstance(val, str) and _like_match(val, cmp)):
                            return False
                        elif op == "_ilike" and not (isinstance(val, str) and _like_match(val.lower(), (cmp or "").lower())):
                            return False
                else:
                    if val != condition:
                        return False
            return True
        rows = [r for r in rows if _matches(r)]

    order_by = args.get("order_by")
    if order_by and isinstance(order_by, list):
        import re
        sort_keys = []
        for spec in order_by:
            if isinstance(spec, str):
                m = re.match(r"^(\w+)\s*(asc|desc)?$", spec.strip(), re.IGNORECASE)
                if m:
                    sort_keys.append((m.group(1), (m.group(2) or "asc").lower() == "desc"))
            elif isinstance(spec, dict):
                for col, direction in spec.items():
                    sort_keys.append((col, str(direction).lower() == "desc"))
        for col, reverse in reversed(sort_keys):
            rows = sorted(rows, key=lambda r, c=col: (r.get(c) is None, r.get(c)), reverse=reverse)

    offset = args.get("offset")
    if offset:
        rows = rows[int(offset):]

    limit = args.get("limit")
    if limit is not None:
        rows = rows[:int(limit)]

    return rows


def _like_match(value: str, pattern: str) -> bool:
    import re
    regex = re.escape(pattern).replace(r"\%", ".*").replace(r"\_", ".")
    return bool(re.fullmatch(regex, value, re.DOTALL))


async def _execute_action_field(field_name: str, field_node, state, variables: dict | None, *, ctx=None) -> list:
    """Execute a tracked function or webhook field, return rows list."""
    from provisa.compiler.sql_gen import _extract_value
    raw_args: dict = {}
    if hasattr(field_node, "arguments") and field_node.arguments:
        for arg in field_node.arguments:
            raw_args[arg.name.value] = _extract_value(arg.value, variables)

    filter_args = {k: raw_args.pop(k) for k in list(raw_args) if k in _ACTION_FILTER_ARGS}
    args = raw_args

    fn = state.tracked_functions.get(field_name)
    if fn:
        src_id = fn["source_id"]
        schema = fn["schema_name"]
        fn_name = fn["function_name"]
        if not state.source_pools.has(src_id):
            raise HTTPException(status_code=503, detail=f"Source '{src_id}' not connected")
        if args:
            params = list(args.values())
            placeholders = ", ".join(f"${i + 1}" for i in range(len(params)))
            sql = f'SELECT * FROM "{schema}"."{fn_name}"({placeholders})'
        else:
            sql = f'SELECT * FROM "{schema}"."{fn_name}"()'
            params = []
        result = await state.source_pools.execute(src_id, sql, params)
        from provisa.executor.serialize import _convert_value
        cols = result.column_names
        rows = [{c: _convert_value(v) for c, v in zip(cols, r)} for r in result.rows]
        rows = await _maybe_resolve_relationships(rows, field_node, fn.get("returns", ""), ctx, state)
        return _apply_action_filters(rows, filter_args)

    wh = state.tracked_webhooks.get(field_name)
    if wh:
        url = wh["url"]
        method = wh["method"].upper()
        timeout = wh["timeout_ms"] / 1000
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, json=args)
        body = resp.json()
        rows = body if isinstance(body, list) else [body]
        rows = await _maybe_resolve_relationships(rows, field_node, wh.get("returns", ""), ctx, state)
        return _apply_action_filters(rows, filter_args)

    raise HTTPException(status_code=400, detail=f"Unknown action field: {field_name!r}")


async def _maybe_resolve_relationships(rows, field_node, returns_str: str, ctx, state) -> list:
    """Resolve nested relationship fields on action rows if ctx and return type are known."""
    if not ctx or not rows or not field_node.selection_set or not returns_str:
        return rows
    if "." not in returns_str:
        return rows
    parts = returns_str.split(".", 1)
    ret_schema, ret_table = parts[0], parts[-1]
    return_type_name = None
    for meta in ctx.tables.values():
        if meta.schema_name == ret_schema and meta.table_name == ret_table:
            return_type_name = meta.type_name
            break
    if return_type_name:
        rows = await _resolve_action_relationships(rows, field_node.selection_set, return_type_name, ctx, state)
    return rows


def _split_action_fields(document, state) -> tuple[list, list]:
    """Return (action_sel_list, regular_field_names) from document root selections."""
    action_sels = []
    regular_names = []
    for defn in document.definitions:
        if not hasattr(defn, "selection_set"):
            continue
        for sel in defn.selection_set.selections:
            from graphql import FieldNode as _FieldNode
            if not isinstance(sel, _FieldNode):
                continue
            fname = sel.name.value
            if fname in state.tracked_functions or fname in state.tracked_webhooks:
                action_sels.append(sel)
            else:
                regular_names.append(fname)
    return action_sels, regular_names


async def _handle_mutation(document, ctx, rls, state, variables, role_id, request=None):
    """Handle a GraphQL mutation operation."""
    action_sels, regular_names = _split_action_fields(document, state)

    # Pure action mutation(s)
    if action_sels and not regular_names:
        data = {}
        for sel in action_sels:
            data[sel.name.value] = await _execute_action_field(sel.name.value, sel, state, variables)
        return {"data": data}

    # Mixed action + regular fields — not supported
    if action_sels and regular_names:
        raise HTTPException(status_code=400, detail="Cannot mix action fields with table mutations")

    headers = dict(request.headers) if request else None
    try:
        mutations = compile_mutation(
            document, ctx, state.source_types, variables, headers,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not mutations:
        raise HTTPException(status_code=400, detail="No mutation fields found")

    results = []
    for mutation in mutations:
        # Look up by DB table name (ctx keys are GraphQL field names which may have domain prefix)
        table_meta = ctx.tables.get(mutation.table_name)
        if table_meta is None:
            for meta in ctx.tables.values():
                if meta.table_name == mutation.table_name:
                    table_meta = meta
                    break

        # Enforce writable_by column permissions
        if table_meta and mutation.mutation_type in ("insert", "update"):
            _check_writable_by(table_meta, mutation.returning_columns, role_id)

        # Inject RLS into UPDATE/DELETE
        if table_meta and rls.has_rules():
            mutation = inject_rls_into_mutation(
                mutation, table_meta.table_id, rls.rules,
            )

        # Mutations always route direct
        source_id = mutation.source_id
        if not state.source_pools.has(source_id):
            raise HTTPException(
                status_code=503,
                detail=f"No connection pool for source {source_id!r}",
            )

        dialect = state.source_dialects.get(source_id, "postgres")
        target_sql = transpile(mutation.sql, dialect)

        try:
            result = await execute_direct(
                state.source_pools, source_id, target_sql, mutation.params,
            )
            results.append({
                "affected_rows": len(result.rows),
            })
            # Invalidate cache for mutated table (REQ-080)
            if table_meta:
                await state.cache_store.invalidate_by_table(table_meta.table_id)
                # Mark affected MVs as stale (REQ-084)
                state.mv_registry.mark_stale(table_meta.table_name)
                # Emit dataset change event (REQ-172)
                from provisa.kafka.change_events import emit_change_event
                emit_change_event(mutation.table_name, source_id)
                # Trigger Kafka sinks for this table (REQ-176, fire-and-forget)
                from provisa.kafka.sink_executor import trigger_sinks_for_table
                asyncio.create_task(
                    trigger_sinks_for_table(mutation.table_name, state),
                )
                # Invalidate and reload hot table if applicable (Phase AD6)
                if state.hot_manager is not None:
                    from provisa.cache.hot_tables import HotTableManager
                    hot_mgr = state.hot_manager
                    assert isinstance(hot_mgr, HotTableManager)
                    if hot_mgr.is_hot(table_meta.table_name):
                        await hot_mgr.invalidate(table_meta.table_name)
                        entry = hot_mgr.get_entry(table_meta.table_name)
                        if entry is None:
                            # Find table config for reload
                            _tbl_schema = table_meta.schema_name
                            _tbl_catalog = table_meta.catalog_name
                            _pk = "id"  # default PK
                            await hot_mgr.load_table(
                                state.trino_conn,
                                table_meta.table_name,
                                _tbl_schema,
                                _tbl_catalog,
                                _pk,
                            )
        except Exception as e:
            log.exception("Mutation execution failed")
            raise HTTPException(status_code=500, detail=str(e))

    # Return first mutation result (single mutation support for now)
    mutation_name = None
    for d in document.definitions:
        if hasattr(d, "selection_set"):
            for sel in d.selection_set.selections:
                mutation_name = sel.name.value
                break

    return {"data": {mutation_name: results[0] if results else None}}


# Dev endpoints (compile, submit, proto, sql) have been moved to endpoint_dev.py
