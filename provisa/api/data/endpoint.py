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
from provisa.cache.middleware import build_cache_headers, check_cache
from provisa.compiler.hints import extract_graphql_hints
from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import (
    make_semantic_sql,
    rewrite_semantic_to_catalog_physical,
    rewrite_semantic_to_physical,
)
from provisa.executor import stats as _qs_mod
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile
from provisa.api.data.mutations import (
    _execute_action_field,
    _handle_mutation,
    _split_action_fields,
)
from provisa.api.data.endpoint_helpers import (
    _build_directives_with_legacy,
    _build_redirect_params,
    _check_role_capability,
    _detect_introspection,
    _inject_probe_limit,
    _inject_stats_into_response,
    _parse_accept,
    _record_per_source_stats,
    _request_timeout,
)
from provisa.api.data.endpoint_executors import (
    _exec_api_route,
    _exec_ctas_route,
    _exec_inline_result,
    _exec_probe_redirect,
    _execute_engine_standard,
)


log = logging.getLogger(__name__)


router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str | None = None
    variables: dict | None = None
    role: str = "admin"  # test mode: role passed in request
    extensions: dict | None = None  # APQ: {"persistedQuery": {"sha256Hash": "..."}}


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

        # Resolve the root table by its ctx.tables key. canonical_field is the pre-alias schema
        # field; variant keys (…GroupBy/…_aggregate) are registered too. root_field/root_field
        # alias would miss because meta.field_name is always the base field.
        _root_meta = ctx.tables.get(compiled.canonical_field or compiled.root_field)
        table_ids = {_root_meta.table_id} if _root_meta is not None else set()
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
            raise HTTPException(status_code=502, detail=f"Redirect upload failed: {e}") from e

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
