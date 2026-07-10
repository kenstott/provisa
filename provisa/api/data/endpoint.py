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
Mutations: parse -> compile_mutation -> RLS inject -> direct execute (never the engine).
"""

# complexity-gate: allow-loc=2930 allow-cc=45 reason="REQ-848 api-cache landing on the SQLAlchemy write face; REQ-941/REQ-392 route parameterized (native-filter) graphql_remote tables to a real-time fetch + schema-less-store VALUES-CTE path; endpoint.py breakup into per-route modules is separately-tracked debt (already flagged by the gate)"

# Requirements: REQ-001, REQ-002, REQ-027, REQ-028, REQ-029, REQ-032, REQ-033,
#               REQ-034, REQ-035, REQ-036, REQ-038, REQ-040, REQ-043, REQ-047,
#               REQ-049, REQ-137, REQ-140, REQ-161, REQ-172, REQ-173, REQ-174,
#               REQ-176, REQ-196, REQ-203, REQ-204, REQ-205, REQ-208, REQ-209,
#               REQ-262, REQ-263, REQ-288, REQ-289, REQ-290, REQ-291, REQ-300,
#               REQ-360, REQ-361, REQ-362

from __future__ import annotations

import asyncio
import json
import logging
import time as _time


from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from graphql import GraphQLSyntaxError, OperationType
from pydantic import BaseModel

from provisa.cache.key import cache_key, is_cacheable
from provisa.cache.middleware import build_cache_headers, check_cache, store_result
from provisa.compiler.hints import extract_graphql_hints
from provisa.compiler.directives import (
    extract_directives,
    extract_directives_from_sql_comments,
    merge_directives,
)
from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import (
    make_semantic_sql,
    rewrite_semantic_to_catalog_physical,
    rewrite_semantic_to_physical,
)
from provisa.executor.serialize import (
    serialize_aggregate,
    serialize_group_by,
    serialize_rows,
    shape_transform,
)
from provisa.executor import stats as _qs_mod
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability, InsufficientRightsError, check_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile
from provisa.api.data.hydration import _hydrate_api_tables_before_engine
from provisa.api.data.mutations import (
    _execute_action_field,
    _handle_mutation,
    _split_action_fields,
)
from provisa.api.data.materialization import (
    _materialize_api_to_engine_cache,
)

import os as _os
import re as _re

log = logging.getLogger(__name__)


def _request_timeout() -> float:
    try:
        from provisa.api.app import state

        return state.server_limits.get(
            "request_timeout", float(_os.environ.get("PROVISA_REQUEST_TIMEOUT", "60"))
        )
    except Exception:
        return float(_os.environ.get("PROVISA_REQUEST_TIMEOUT", "60"))


router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str | None = None
    variables: dict | None = None
    role: str = "admin"  # test mode: role passed in request
    extensions: dict | None = None  # APQ: {"persistedQuery": {"sha256Hash": "..."}}


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


def _format_response(rows, columns, root_field, output_format, result_limit: int | None = None):
    """Serialize query results in the requested output format."""
    if output_format == "json":
        result = serialize_rows(rows, columns, root_field, result_limit=result_limit)
        return shape_transform(result, columns)

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

    return serialize_rows(rows, columns, root_field, result_limit=result_limit)


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
        return sql[: limit_match.start()] + f"LIMIT {effective}" + sql[limit_match.end() :]
    return sql + f" LIMIT {limit}"


async def _resolve_apq(
    request: GraphQLRequest,
    apq_hash: str | None,
    state,
    tenant_id: str | None = None,
) -> GraphQLRequest | JSONResponse:
    """Handle APQ lookup (hash-only) or validation (hash+query).

    Returns updated request on success, or a JSONResponse on cache-miss,
    or raises HTTPException on hash mismatch.
    """
    if apq_hash and not request.query:
        apq_cache = getattr(state, "apq_cache", None)
        cached_query = await apq_cache.get(apq_hash, tenant_id=tenant_id) if apq_cache else None
        if cached_query is None:
            return JSONResponse(
                status_code=200,
                content={
                    "errors": [
                        {
                            "message": "PersistedQueryNotFound",
                            "extensions": {"code": "PERSISTED_QUERY_NOT_FOUND"},
                        }
                    ]
                },
            )
        return GraphQLRequest(
            query=cached_query,
            variables=request.variables,
            role=request.role,
        )
    if apq_hash and request.query:
        from provisa.apq.cache import compute_apq_hash

        expected = compute_apq_hash(request.query)
        if expected != apq_hash:
            raise HTTPException(status_code=400, detail="APQ hash mismatch")
    return request


def _check_role_capability(role, capability: Capability) -> None:
    """Raise HTTPException(403) if role lacks the given capability."""
    if role is None:
        return
    try:
        check_capability(role, capability)
    except InsufficientRightsError as e:
        raise HTTPException(status_code=403, detail=str(e))


def _detect_introspection(document) -> bool:
    """Return True if all root selections are introspection fields."""
    from graphql.language.ast import OperationDefinitionNode

    introspection_fields = {"__schema", "__type", "__typename"}
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode) and defn.selection_set:
            from graphql.language.ast import FieldNode as _FieldNode

            field_names = {
                sel.name.value
                for sel in defn.selection_set.selections
                if isinstance(sel, _FieldNode)
            }
            if field_names and field_names <= introspection_fields:
                return True
    return False


def _build_directives_with_legacy(request_query: str, document, legacy_hints: dict):
    """Build merged directives, falling back to legacy @provisa route hints."""
    _comment_directives = extract_directives_from_sql_comments(request_query)
    _ast_directives = extract_directives(document)
    directives = merge_directives(_comment_directives, _ast_directives)
    if directives.steward_hint is None and legacy_hints.get("route"):
        raw = legacy_hints["route"]
        directives.route = (
            "FEDERATED" if raw == "federated" else "DIRECT" if raw == "direct" else None
        )
    return directives


def _build_redirect_params(
    x_provisa_redirect: str | None,
    x_provisa_redirect_threshold: int | None,
    x_provisa_redirect_format: str | None,
    directives,
) -> tuple[str | None, int | None, bool]:
    """Return (redirect_format, effective_threshold, force_redirect) from headers + directives."""
    directive_redirect_format = (
        _parse_accept(directives.redirect_format) if directives.redirect_format else None
    )
    redirect_format = (
        _parse_accept(x_provisa_redirect_format)
        if x_provisa_redirect_format
        else directive_redirect_format
    )
    effective_threshold = x_provisa_redirect_threshold or directives.redirect_threshold
    force_redirect = (x_provisa_redirect or "").lower() == "true"
    if redirect_format and effective_threshold is None:
        force_redirect = True
    return redirect_format, effective_threshold, force_redirect


def _inject_stats_into_response(response, stats_dict: dict):
    """Inject provisa_stats extension into a JSON response/dict."""
    if isinstance(response, JSONResponse):
        body = json.loads(bytes(response.body))
        body.setdefault("extensions", {})["provisa_stats"] = stats_dict
        skip = {"content-length", "content-type"}
        extra = {k: v for k, v in response.headers.items() if k.lower() not in skip}
        return JSONResponse(content=body, headers=extra)
    if isinstance(response, dict):
        response.setdefault("extensions", {})["provisa_stats"] = stats_dict
    return response


class CompileRequest(BaseModel):
    query: str
    variables: dict | None = None


@router.post("/compile")
async def compile_endpoint(  # REQ-161, REQ-163
    raw_request: Request,
    request: CompileRequest,
    x_provisa_role: str | None = Header(None),
):
    """REQ-161: compile-only — return governed SQL / route / sources / params without executing.

    The REST companion to the GraphQL `compileQuery` mutation; the role is the authenticated
    role (header used only when unauthenticated).
    """
    from provisa.api.admin.dev_queries import compile_query as _compile_only
    from provisa.api.app import state

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role
    if not role_id or role_id not in state.contexts:
        raise HTTPException(status_code=403, detail="No accessible schema for role")
    try:
        results = await _compile_only(role_id, request.query, request.variables)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse({"compiled": results})


async def _handle_normalized(document, ctx, rls, state, variables, role_id, role):
    """REQ-049: emit one governed, deduplicated relational table per entity via per-table CTAS.

    Each entity's scoped SELECT DISTINCT is governed identically to the normal path, written
    to S3 by the engine CTAS (the denormalized product never forms), and returned as a manifest of
    presigned URLs. A computed-join query that cannot be normalized returns 400.
    """
    from provisa.compiler.normalize import NormalizeError, compile_normalized
    from provisa.executor.redirect import RedirectConfig
    from provisa.executor.redirect import presign_ctas_result, schedule_s3_cleanup

    try:
        ntables = compile_normalized(document, ctx, variables, use_catalog=True)
    except NormalizeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    redirect_config = RedirectConfig.from_env()
    fresh_mvs = state.mv_registry.get_fresh()
    manifest: list[dict] = []
    for nt in ntables:
        # Govern each per-table query exactly like the normal path (RLS/masking/visibility).
        await _prepare_compiled(nt.compiled, ctx, rls, state, role_id, role, fresh_mvs)
        exec_sql = rewrite_semantic_to_catalog_physical(nt.compiled.sql, ctx)
        physical_sql = state.federation_engine.transpile_physical(exec_sql)
        ctas = state.federation_engine.ctas_redirect(physical_sql, "parquet")
        url = await presign_ctas_result(ctas["s3_prefix"], redirect_config)
        asyncio.create_task(schedule_s3_cleanup(ctas["s3_prefix"], redirect_config))
        manifest.append(
            {
                "table": nt.table_name,
                "path": list(nt.path),
                "url": url,
                "rowCount": ctas["row_count"],
            }
        )
    return JSONResponse({"normalized": manifest})


@router.post("/graphql")
async def graphql_endpoint(  # REQ-001, REQ-002, REQ-043, REQ-047, REQ-049, REQ-288, REQ-289, REQ-290, REQ-291, REQ-300
    raw_request: Request,
    request: GraphQLRequest,
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
    x_provisa_redirect: str | None = Header(None),
    x_provisa_redirect_threshold: int | None = Header(None),
    x_provisa_redirect_format: str | None = Header(None),
    x_provisa_stats: str | None = Header(None),
    x_provisa_normalized: str | None = Header(None),
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

    role = state.roles.get(role_id)
    _check_role_capability(role, Capability.QUERY_DEVELOPMENT)

    # --- APQ (Automatic Persisted Queries, Phase AN) ---
    apq_hash: str | None = None
    if request.extensions:
        pq = request.extensions.get("persistedQuery", {})
        apq_hash = pq.get("sha256Hash")

    apq_result = await _resolve_apq(
        request, apq_hash, state, tenant_id=getattr(raw_request.state, "tenant_id", None)
    )
    if isinstance(apq_result, JSONResponse):
        return apq_result
    request = apq_result

    if not request.query:
        raise HTTPException(status_code=400, detail="query is required")

    # Legacy comment hints (kept for backwards compat)
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

    directives = _build_directives_with_legacy(request.query, document, _legacy_hints)
    steward_hint = directives.steward_hint
    effective_variables = coerce_variable_defaults(document, request.variables)

    # Introspection: execute directly against GraphQL schema
    if _detect_introspection(document):
        from graphql import execute as gql_execute
        from graphql.execution.execute import ExecutionResult as _ExecutionResult
        from typing import cast as _cast

        result = _cast(
            _ExecutionResult,
            gql_execute(schema, document, variable_values=effective_variables),
        )
        return JSONResponse({"data": result.data})

    from graphql.language.ast import OperationDefinitionNode as _ODN

    is_mut = any(
        isinstance(d, _ODN) and d.operation == OperationType.MUTATION for d in document.definitions
    )
    is_sub = any(
        isinstance(d, _ODN) and d.operation == OperationType.SUBSCRIPTION
        for d in document.definitions
    )

    if is_sub:
        from provisa.api.data.subscription_sse import handle_subscription_sse

        return await handle_subscription_sse(
            document,
            ctx,
            rls,
            state,
            effective_variables,
            role,
            role_id,
            raw_request,
            directives=directives,
        )

    output_format = _parse_accept(accept)
    redirect_format, effective_threshold, force_redirect = _build_redirect_params(
        x_provisa_redirect, x_provisa_redirect_threshold, x_provisa_redirect_format, directives
    )

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()

    # REQ-049: X-Provisa-Normalized returns one governed, deduplicated relational table per
    # entity (PK/FK preserved) as a manifest of S3 URLs, instead of the denormalized result.
    if (x_provisa_normalized or "").lower() == "true" and not is_mut:
        return await _handle_normalized(
            document, ctx, rls, state, effective_variables, role_id, role
        )

    if is_mut:
        response = await _handle_mutation(
            document,
            ctx,
            rls,
            state,
            effective_variables,
            role_id,
            raw_request,
        )
    else:
        response = await _handle_query(
            document,
            ctx,
            rls,
            state,
            effective_variables,
            role,
            output_format,
            role_id,
            force_redirect=force_redirect,
            redirect_threshold=effective_threshold,
            redirect_format=redirect_format,
            steward_hint=steward_hint,
            query_session_props=directives.to_session_props(),
            cache_ttl=directives.cache_ttl,
            no_cache=directives.no_cache,
            query_text=request.query,
            org_id=getattr(raw_request.state, "tenant_id", None),
        )

    if stats_enabled:
        qs = _qs_mod.current()
        if qs is not None:
            log.info("[STATS] entries=%d fields=%s", len(qs.entries), [e.field for e in qs.entries])
            response = _inject_stats_into_response(response, qs.to_dict())

    # AN (REQ-291): store APQ hash only after successful execution
    if apq_hash and request.query and response is not None:
        apq_cache = getattr(state, "apq_cache", None)
        if apq_cache:
            await apq_cache.set(
                apq_hash,
                request.query,
                tenant_id=getattr(raw_request.state, "tenant_id", None),
            )

    return response


async def _prepare_compiled(
    compiled, ctx, rls, state, role_id, role, fresh_mvs
):  # REQ-002, REQ-038, REQ-040, REQ-203, REQ-204, REQ-262, REQ-263
    """Apply governance, MV rewrite, Kafka filters, and sampling to a compiled query."""
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    if state.view_sql_map:
        from provisa.compiler.view_expand import expand_views

        compiled = expand_views(compiled, state.view_sql_map)

    original_sources = set(compiled.sources)
    compiled = rewrite_if_mv_match(compiled, fresh_mvs)
    mv_used = compiled.sources != original_sources
    if mv_used:
        log.info(
            "[QUERY %s] MV optimization applied — sources changed: %s → %s",
            compiled.root_field,
            original_sources,
            compiled.sources,
        )
    else:
        log.debug(
            "[QUERY %s] No MV match, using original sources: %s",
            compiled.root_field,
            compiled.sources,
        )

    if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
        from provisa.kafka.window import inject_kafka_filters

        compiled = inject_kafka_filters(
            compiled,
            ctx,
            state.source_types,
            state.kafka_table_configs,
        )

    # Governance: compile → semantic SQL → apply RLS/masking/visibility
    gov_ctx = build_governance_context(
        role_id, rls, state.masking_rules, ctx, getattr(state, "tables", []), role=role
    )

    # Validate semantic SQL — V002 (join relationship check) is always skipped for
    # GraphQL because the SDL defines valid relationships by design.
    from provisa.compiler.sql_validator import validate_sql

    semantic_sql_for_validation = make_semantic_sql(compiled.sql, ctx)
    _violations = validate_sql(
        semantic_sql_for_validation,
        ctx,
        gov_ctx,
        role or {},
        getattr(state, "tables", []),
        bypass_relationship_guard=True,
    )
    if _violations:
        raise HTTPException(
            status_code=403,
            detail={"violations": [{"code": v.code, "message": v.message} for v in _violations]},
        )

    compiled.sql = apply_governance(semantic_sql_for_validation, gov_ctx)
    if compiled.nodes_sql is not None:
        compiled.nodes_sql = apply_governance(make_semantic_sql(compiled.nodes_sql, ctx), gov_ctx)

    # ABAC approval hook (Phase AE, REQ-203) — evaluated AFTER RLS injection and
    # BEFORE execution. May deny the operation or return an additional filter that is
    # ANDed into the governed WHERE clause.
    if getattr(state, "approval_hook", None) is not None:
        from provisa.auth.approval_hook import ApprovalRequest, should_check
        from provisa.compiler.rls import _inject_where

        table_ids = {m.table_id for m in ctx.tables.values() if m.field_name == compiled.root_field}
        if should_check(
            list(table_ids),
            list(original_sources),
            state.approval_hook_config,
            table_hooks=getattr(state, "table_approval_hooks", {}),
            source_hooks=getattr(state, "source_approval_hooks", {}),
        ):
            req = ApprovalRequest(
                user=role_id,
                roles=[role_id] if role_id else [],
                tables=sorted(str(t) for t in table_ids),
                columns=[c.column for c in compiled.columns],
                operation="query",
                session_vars=dict((role or {}).get("session_vars", {})),
            )
            resp = await state.approval_hook.evaluate(req)
            if not resp.approved:
                raise HTTPException(status_code=403, detail=f"Approval denied: {resp.reason}")
            if resp.additional_filter:
                compiled.sql = _inject_where(compiled.sql, f"({resp.additional_filter})")

    return compiled, mv_used


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
                len(row.get(join_field, []) or []) for row in field_rows if isinstance(row, dict)
            )
        else:
            total = sum(
                1 for row in field_rows if isinstance(row, dict) and row.get(join_field) is not None
            )
        counts[src_id] = counts.get(src_id, 0) + total
    return counts


def _build_mermaid(
    sources: set,
    source_types: dict,
    hydration_ms: dict[str, float],
    engine_ms: float | None,
    result_rows: int,
    root_field: str,
    join_fields: list | None = None,
    root_source_id: str | None = None,
    cache_catalog: str | None = None,
) -> str:
    """Build a Mermaid flowchart LR diagram for the federated query execution DAG.

    join_fields: list of (rel_field_name, source_id, is_cache_hit) for JOIN targets.
    root_source_id: when set, only this source_id is rendered in the main loop; other
                    sources in `sources` that appear as join targets are rendered via join_fields.
    cache_catalog: the engine catalog used for API cache — shown on cache node label.
    """
    _cache_label = f"{cache_catalog or root_source_id or 'pg'} cache"

    def _node_id(s: str) -> str:
        return s.replace("-", "_").replace(".", "_")

    lines = ["flowchart LR"]

    has_joins = bool(join_fields)
    single = len(sources) == 1 and not has_joins

    # When root_source_id is set, only render that source in the main loop.
    # Other sources (join targets from a different source) are handled via join_fields.
    render_sources = (
        {s for s in sources if s == root_source_id or root_source_id is None}
        if root_source_id is not None
        else sources
    )

    for src_id in sorted(render_sources):
        src_type = source_types.get(src_id, "")
        nid = _node_id(src_id)
        if src_type == "openapi":
            h_ms = hydration_ms.get(src_id, 0.0)
            cache_label = "cache hit" if h_ms < 5 else f"{round(h_ms)}ms"
            lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
            lines.append(f'    pg_{nid}["{_cache_label}\\n{root_field}"]')
            lines.append(f'    {nid} -->|"{cache_label}"| pg_{nid}')
            if single:
                elapsed_label = f"{round(h_ms)}ms" if h_ms >= 5 else ""
                lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
                lines.append(
                    f'    pg_{nid} -->|"{elapsed_label}"| result'
                    if elapsed_label
                    else f"    pg_{nid} --> result"
                )
            else:
                engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    pg_{nid} -->|"federated {engine_label}"| engine')
        else:
            if single:
                elapsed_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
                lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
                lines.append(
                    f'    {nid} -->|"federated {elapsed_label}"| result'
                    if elapsed_label
                    else f"    {nid} --> result"
                )
            else:
                engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
                lines.append(f'    {nid} -->|"federated {engine_label}"| engine')

    # Render JOIN target nodes — separate node per join target, even if same source as root.
    if join_fields:
        engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
        for rel_field, jt_src_id, is_hit in join_fields:
            jt_type = source_types.get(jt_src_id, "")
            jnid = _node_id(rel_field)
            if jt_type == "openapi":
                hit_label = "cache hit" if is_hit else "fetched"
                lines.append(f'    {jnid}["{rel_field}\\n({jt_src_id})"]')
                lines.append(f'    pg_{jnid}["{_cache_label}\\n{rel_field}"]')
                lines.append(f'    {jnid} -->|"{hit_label}"| pg_{jnid}')
                lines.append(f'    pg_{jnid} -->|"federated {engine_label}"| engine')
            else:
                lines.append(f'    {jnid}["{rel_field}\\n({jt_src_id})"]')
                lines.append(f'    {jnid} -->|"federated {engine_label}"| engine')

    if not single:
        lines.append('    engine{"Virtual\\nJoin"}')
        lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
        lines.append("    engine --> result")

    return "\n".join(lines)


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


def _grpc_cache_type(sql_type: str) -> str:
    """Map a gRPC ColumnDef SQL type to the cache-table type vocabulary (REQ-327)."""
    t = (sql_type or "").upper()
    if "INT" in t:
        return "integer"
    if t in ("DOUBLE", "REAL", "FLOAT") or "DECIMAL" in t or "NUMERIC" in t:
        return "number"
    if "BOOL" in t:
        return "boolean"
    return "string"


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


def _record_per_source_stats(
    root_field: str,
    sources: set,
    elapsed_ms: float,
    rows: int,
    ctx,
    state,
    decision=None,
    dataloader_sources: set | None = None,
    per_source_ms: dict[str, float] | None = None,
    engine_ms: float | None = None,
    hydration_rows: dict[str, int] | None = None,
    field_rows: list | None = None,
    physical_sql: str | None = None,
    hydration_cache_hits: set | None = None,
) -> None:
    """Emit FieldStat entries per source.

    For openapi sources in a federated join: emits two entries —
    one for hydration (HTTP fetch → PG write) and one for the engine join.
    For all other sources: one entry with the engine execution time.
    Per-source row counts use join cardinality from the result for one-to-many joins.
    """
    joined_rows = _count_rows_per_source(field_rows or [], ctx) if field_rows else {}
    for src_id in sources or set():
        source_type = getattr(state, "source_types", {}).get(src_id, "")
        if decision is None or decision.route != Route.DIRECT:
            prefix = "federated"
        else:
            prefix = "direct"
        strategy = f"{prefix}:{source_type}" if source_type else prefix
        if dataloader_sources and src_id in dataloader_sources:
            strategy += ":dataloader"

        src_rows = joined_rows.get(src_id, rows)

        if source_type == "openapi" and per_source_ms is not None and engine_ms is not None:
            hydration = per_source_ms.get(src_id, 0.0)
            h_rows = (hydration_rows or {}).get(src_id, 0)
            h_hit = hydration_cache_hits is not None and src_id in hydration_cache_hits
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy="hydration",
                elapsed_ms=hydration,
                rows=h_rows,
                cache_hit=h_hit,
            )
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy=strategy,
                elapsed_ms=engine_ms,
                rows=src_rows,
                physical_sql=physical_sql,
            )
        else:
            src_ms = per_source_ms.get(src_id, elapsed_ms) if per_source_ms else elapsed_ms
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy=strategy,
                elapsed_ms=src_ms,
                rows=src_rows,
                physical_sql=physical_sql,
            )


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
    _root_meta = ctx.tables.get(root_field)
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
    table_ids = {meta.table_id for meta in ctx.tables.values() if meta.field_name == root_field}
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
    ctx,
    source_id: str,
    response_cache_ttl: int | None,
    no_cache: bool,
    org_id: str | None = None,
) -> None:
    """Store API-source response_data in the response cache if TTL allows."""
    from provisa.cache.policy import resolve_policy

    _src_cache = state.source_cache.get(source_id, {})
    _table_ids = {meta.table_id for meta in ctx.tables.values() if meta.field_name == root_field}
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


def _append_mermaid(
    qs, compiled, ctx, root_field, per_source_ms, engine_ms, n_rows, hydration_cache_hits
):
    """Build and append a Mermaid diagram for the standard query path to qs."""
    _st = getattr(ctx, "source_types", None) or {}
    _root_meta2 = ctx.tables.get(root_field)
    _root_src_id = _root_meta2.source_id if _root_meta2 else None
    _jf2: list[tuple[str, str, bool]] = []
    _hch = hydration_cache_hits or set()
    # Only cross-source joins the query touched (compiled.sources, target != root).
    if _root_meta2:
        for (_tn2, _rf2), _jm2 in (ctx.joins or {}).items():
            _tgt = _jm2.target.source_id
            if _tn2 == _root_meta2.type_name and _tgt in compiled.sources and _tgt != _root_src_id:
                _jf2.append((_rf2, _tgt, _tgt in _hch))
    new_mermaid = _build_mermaid(
        compiled.sources,
        _st,
        per_source_ms or {},
        engine_ms,
        n_rows,
        root_field,
        join_fields=_jf2 or None,
        root_source_id=_root_src_id,
    )
    qs.mermaid = f"{qs.mermaid}\n\n{new_mermaid}" if qs.mermaid else new_mermaid


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
        _root_meta = ctx.tables.get(root_field)
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


async def _execute_one_field(
    compiled,
    ctx,
    rls,
    state,
    role_id,
    output_format,
    *,
    force_redirect,
    redirect_config,
    effective_redirect_format,
    probe_limit,
    steward_hint: str | None = None,
    query_session_props: dict | None = None,
    response_cache_ttl: int | None = None,
    no_cache: bool = False,
    query_text: str | None = None,
    org_id: str | None = None,
):  # REQ-027, REQ-028, REQ-029, REQ-137, REQ-140, REQ-196
    """Execute a single compiled query field through the full pipeline.

    Returns (root_field, field_rows, redirect_info_or_None, cache_key, cached_entry_or_None).
    """
    from provisa.executor.redirect import upload_and_presign
    from provisa.executor.redirect import is_engine_native_format

    root_field = compiled.root_field
    _t0 = _time.perf_counter()

    # Cache check. REQ-866 fail-closed: when the identity is not fully resolved into
    # the key (empty RLS filter, or a current_setting-dependent predicate), the query
    # is not cacheable — never read or written — so a per-session value can't leak.
    _rls = rls.rules if rls.has_rules() else {}
    ck = cache_key(compiled.sql, compiled.params, role_id, _rls)
    _cache_off = no_cache or output_format != "json" or not is_cacheable(compiled.sql, _rls)[0]
    cached = None if _cache_off else await check_cache(state.response_cache_store, ck, org_id)

    # Route decision — the result cache is the first candidate route (REQ-865),
    # so a hit is served as Route.CACHE instead of a hidden pre-routing step.
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=steward_hint,
        has_json_extract="->>" in compiled.sql,
        source_dsns=state.source_dsns,
        cache_hit=cached is not None,
        no_cache=_cache_off,
    )
    log.warning(
        "[QUERY %s] Route: %s | source=%s | reason: %s",
        root_field,
        decision.route.value,
        decision.source_id or "(engine)",
        decision.reason,
    )

    if decision.route == Route.CACHE and cached is not None:
        cached_data = json.loads(cached.data)
        field_rows = cached_data.get("data", {}).get(root_field, [])
        _qs_mod.record(
            field=root_field,
            source="cache",
            strategy="cache",
            elapsed_ms=(_time.perf_counter() - _t0) * 1000,
            rows=len(field_rows) if isinstance(field_rows, list) else 0,
            cache_hit=True,
        )
        return root_field, field_rows, None, ck, cached

    if decision.route == Route.API and decision.source_id:
        return await _exec_api_route(
            compiled,
            ctx,
            state,
            decision,
            root_field,
            output_format,
            ck,
            response_cache_ttl,
            no_cache,
            org_id=org_id,
        )

    if (
        force_redirect
        and is_engine_native_format(effective_redirect_format)
        and state.engine_conn is not None
    ):
        try:
            redirect_info = await _exec_ctas_route(
                compiled, ctx, state, effective_redirect_format, redirect_config
            )
            _record_per_source_stats(
                root_field,
                compiled.sources,
                (_time.perf_counter() - _t0) * 1000,
                redirect_info["row_count"],
                ctx,
                state,
            )
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("CTAS redirect failed for %s, falling back", root_field)

    # Standard execution
    session_hints: dict[str, str] = {}
    _dataloader_srcs: set = set()
    _hydration_rows: dict[str, int] = {}
    _hydration_cache_hits: set = set()
    _per_source_ms: dict[str, float] = {}
    _engine_ms: float = 0.0
    physical_sql: str = ""

    try:
        if (
            decision.route == Route.DIRECT
            and decision.source_id
            and state.source_pools.has(decision.source_id)
        ):
            exec_sql = rewrite_semantic_to_physical(compiled.sql, ctx)
            if probe_limit is not None:
                exec_sql = _inject_probe_limit(exec_sql, probe_limit)
            result = await state.federation_engine.execute_native(
                state.source_pools,
                decision.source_id,
                transpile(exec_sql, decision.dialect or "postgres"),
                compiled.params,
            )
        else:
            (
                result,
                physical_sql,
                _engine_ms,
                _per_source_ms,
                _dataloader_srcs,
                _,
                _hydration_rows,
                _hydration_cache_hits,
                session_hints,
            ) = await _execute_engine_standard(
                compiled,
                ctx,
                state,
                role_id,
                root_field,
                probe_limit,
                query_session_props,
                query_text,
            )
    except HTTPException:
        raise
    except (MemoryError, ConnectionError) as e:
        log.error("Query resource error for %s: %s", root_field, e)
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        log.exception("Query execution failed for %s", root_field)
        raise HTTPException(status_code=500, detail=str(e))

    if probe_limit is not None and len(result.rows) >= probe_limit:
        log.info(
            "[QUERY %s] Probe returned %d rows (threshold %d) — redirecting",
            root_field,
            len(result.rows),
            redirect_config.threshold,
        )
        try:
            redirect_info = await _exec_probe_redirect(
                compiled,
                ctx,
                state,
                decision,
                session_hints,
                effective_redirect_format,
                redirect_config,
                role_id,
            )
            _record_per_source_stats(
                root_field,
                compiled.sources,
                (_time.perf_counter() - _t0) * 1000,
                redirect_info.get("row_count", 0),
                ctx,
                state,
                decision,
            )
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("Redirect upload failed for %s, returning inline", root_field)

    if force_redirect:
        try:
            redirect_info = await upload_and_presign(
                result,
                redirect_config,
                output_format=effective_redirect_format,
                columns=compiled.columns,
                role=role_id,
            )
            _record_per_source_stats(
                root_field,
                compiled.sources,
                (_time.perf_counter() - _t0) * 1000,
                redirect_info.get("row_count", 0),
                ctx,
                state,
                decision,
            )
            return root_field, None, redirect_info, ck, None
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Redirect upload failed: {e}")

    return await _exec_inline_result(
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
        _t0,
        _dataloader_srcs,
        _per_source_ms,
        _engine_ms,
        _hydration_rows,
        _hydration_cache_hits,
        physical_sql,
    )


async def _handle_query(
    document,
    ctx,
    rls,
    state,
    variables,
    role,
    output_format="json",
    role_id="admin",
    *,
    force_redirect=False,
    redirect_threshold=None,
    redirect_format=None,
    steward_hint: str | None = None,
    query_session_props: dict | None = None,
    cache_ttl: int | None = None,
    no_cache: bool = False,
    query_text: str | None = None,
    org_id: str | None = None,
):  # REQ-001, REQ-027, REQ-028, REQ-029, REQ-043, REQ-047, REQ-049, REQ-137, REQ-140, REQ-196
    """Handle a GraphQL query operation with content negotiation.

    Pipeline per root field: compile → RLS → masking → MV rewrite → sampling
      → cache check → route → transpile → execute → cache store → serialize.
    Multiple root fields are executed independently and merged.
    """
    action_sels, regular_names = _split_action_fields(document, state)

    if action_sels and not regular_names:
        data = {}
        for sel in action_sels:
            data[sel.name.value] = await _execute_action_field(
                sel.name.value, sel, state, variables, ctx=ctx, role_id=role_id
            )
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
        prepped, _ = await _prepare_compiled(cq, ctx, rls, state, role_id, role, fresh_mvs)
        prepared.append(prepped)

    # Determine redirect config
    from provisa.executor.redirect import RedirectConfig

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
        try:
            root_field, field_rows, redirect_info, _, cached_entry = await asyncio.wait_for(
                _execute_one_field(
                    prepared[0],
                    ctx,
                    rls,
                    state,
                    role_id,
                    output_format,
                    force_redirect=force_redirect,
                    redirect_config=redirect_config,
                    effective_redirect_format=effective_redirect_format,
                    probe_limit=probe_limit,
                    steward_hint=steward_hint,
                    query_session_props=query_session_props,
                    response_cache_ttl=cache_ttl,
                    no_cache=no_cache,
                    query_text=query_text,
                    org_id=org_id,
                ),
                timeout=_request_timeout(),
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504, detail=f"Query timed out after {_request_timeout():.0f}s"
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

    # --- Multiple root fields: execute in parallel, merge results ---
    merged_data: dict = {}
    merged_redirects: dict = {}

    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    _execute_one_field(
                        compiled,
                        ctx,
                        rls,
                        state,
                        role_id,
                        "json",  # multi-field always uses JSON
                        force_redirect=force_redirect,
                        redirect_config=redirect_config,
                        effective_redirect_format=effective_redirect_format,
                        probe_limit=probe_limit,
                        steward_hint=steward_hint,
                        query_session_props=query_session_props,
                        response_cache_ttl=cache_ttl,
                        no_cache=no_cache,
                        query_text=query_text,
                        org_id=org_id,
                    )
                    for compiled in prepared
                ]
            ),
            timeout=_request_timeout(),
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504, detail=f"Query timed out after {_request_timeout():.0f}s"
        )

    for root_field, field_rows, redirect_info, _, cached_entry in results:
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


@router.post("/touch/{table}", status_code=204)
async def touch_table(  # REQ-174
    table: str,
    request: Request,
    x_provisa_role: str | None = Header(None),
):
    """Emit a change event for a table without mutating any data (REQ-174).

    Useful for triggering downstream sinks or SSE subscribers when an external
    system has modified a table that Provisa tracks.
    """
    from provisa.api.app import state
    from provisa.kafka.change_events import emit_change_event

    # Find the table in config
    table_obj = next(
        (t for t in state.config.tables if t.table_name == table),
        None,
    )
    if table_obj is None:
        raise HTTPException(status_code=404, detail=f"Table {table!r} not found")

    emit_change_event(table_obj.table_name, table_obj.source_id, "touch")
    return Response(status_code=204)


# Dev endpoints (compile, submit, proto, sql) have been moved to endpoint_dev.py
