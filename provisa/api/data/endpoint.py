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
import logging
import time as _time
from typing import Any


from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from graphql import GraphQLSyntaxError, OperationType
from pydantic import BaseModel

from provisa.core import request_deadline
from provisa.core.statement_warnings import collecting
from provisa.api.errors import ApiError
from provisa.cache.key import cache_key, is_cacheable
from provisa.cache.middleware import build_cache_headers, check_cache, decode_cached_result
from provisa.federation.replica_state import ReplicaBuilding
from provisa.cache.store import CachedResult
from provisa.cache.tenancy import cache_tenant
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
from provisa.observability.request_facts import TimedJSONResponse as JSONResponse  # REQ-1910
from provisa.audit.context import note_request_route, note_request_rows
from provisa.observability.request_facts import observe_cache_hit
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
    _inject_warnings_into_response,
    _parse_accept,
    _record_per_source_stats,
)
from provisa.api.data.endpoint_executors import (
    _exec_api_route,
    _exec_ctas_route,
    _exec_inline_result,
    _exec_probe_redirect,
    _execute_engine_standard,
)
from provisa.federation.engine_wake import ensure_engine_awake, readdress_lost_coordinator
from provisa.federation.registry_view import operator_floor


log = logging.getLogger(__name__)


router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str | None = None
    variables: dict | None = None
    role: str = "org_admin"  # test mode: role passed in request
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
        # The body's role is carried over only when the client sent one (REQ-273: a role the
        # client did not name is not one to check against the acting role).
        if "role" in request.model_fields_set:
            return GraphQLRequest(
                query=cached_query, variables=request.variables, role=request.role
            )
        return GraphQLRequest(query=cached_query, variables=request.variables)
    if apq_hash and request.query:
        from provisa.apq.cache import compute_apq_hash

        expected = compute_apq_hash(request.query)
        if expected != apq_hash:
            raise ApiError(400, "data.apq_hash_mismatch", "APQ hash mismatch")
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
        raise ApiError(403, "data.no_accessible_schema_for_role", "No accessible schema for role")
    from dataclasses import asdict

    try:
        results = await _compile_only(role_id, request.query, request.variables)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    serializable_results = [{**r, "enforcement": asdict(r["enforcement"])} for r in results]
    return JSONResponse({"compiled": serializable_results})


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
        ctas = state.federation_engine.ctas_redirect(
            physical_sql, "parquet", nt.compiled.params or None
        )
        url = await presign_ctas_result(ctas["s3_prefix"], redirect_config)
        schedule_s3_cleanup(ctas["s3_prefix"], redirect_config)
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
    x_provisa_as_of: str | None = Header(None),  # REQ-1163: read bitemporal MVs as of this time
    x_provisa_trace: str | None = Header(None),  # REQ-1910: "debug" asks for a debug trace
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

    # REQ-273: the acting role is the auth layer's (identity, or X-Provisa-Role when unsecured);
    # a body `role` that differs from it is refused.
    from provisa.api.acting_role import acting_role, sent_role

    role_id = acting_role(raw_request, x_provisa_role, sent_role(request), "org_admin")

    if role_id not in state.schemas:
        raise ApiError(
            400,
            "data.no_schema_available_for_role",
            f"No schema available for role {role_id!r}",
            role_id=role_id,
        )

    from provisa.security.rights import require_role

    role = require_role(state.roles, role_id)
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
        raise ApiError(400, "data.query_required", "query is required")

    # Legacy comment hints (kept for backwards compat)
    _legacy_hints = extract_graphql_hints(request.query)

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    # REQ-1877 (amended 2026-09-30): a plain query whose governed plan is already cached skips
    # parse, validation, the complexity guard, compilation and governance — the plan is their
    # result. A normalized read needs the document itself, so it is not offered the plan path.
    from provisa.api.data.graphql_plan import PlanRequest

    plan_request = PlanRequest(
        state,
        role_id=role_id,
        role=role,
        schema=schema,
        query=request.query,
        variables=request.variables,
        as_of=x_provisa_as_of,
        fresh_mvs=state.mv_registry.get_fresh(),
        eligible=(x_provisa_normalized or "").lower() != "true",
    )
    plan = plan_request.cached()

    if plan is not None:
        document = None
        directives = plan.directives
        effective_variables = None
    else:
        # Parse and validate
        try:
            document = parse_query(schema, request.query, request.variables)
        except GraphQLValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except GraphQLSyntaxError as e:
            raise HTTPException(status_code=400, detail=str(e))

        directives = _build_directives_with_legacy(request.query, document, _legacy_hints)
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

    steward_hint = directives.steward_hint

    # REQ-1910: request entry for GraphQL over HTTP — the same resolution the pipeline's other
    # entries make: the operator's debug-trace windows for this org and role, and the request's
    # own hint (``@debugTrace``, a `-- @provisa trace=debug` comment, or the X-Provisa-Trace
    # header), which is rejected for a role the operator has not permitted.
    from provisa.compiler.directives import debug_trace_from_header
    from provisa.pgwire._pipeline import resolve_trace_scope

    await resolve_trace_scope(
        state,
        role_id,
        hint=directives.debug_trace or debug_trace_from_header(x_provisa_trace),
    )

    # REQ-1448: the wake belongs to every executing surface, not only the SQL pipeline's
    # _execute_plan. Introspection returned above without touching the engine; everything below
    # dispatches to it, so this is the point where the active org's shard must be serving and
    # holding its catalogs. Without it a shard that had scaled to zero was never woken by GraphQL
    # and every query dialed the retired coordinator's address until some other surface ran.
    await ensure_engine_awake(state)

    from graphql.language.ast import OperationDefinitionNode as _ODN

    # A cached plan is only ever recorded for a plain query (see _handle_query).
    _definitions = document.definitions if document is not None else ()
    is_mut = any(
        isinstance(d, _ODN) and d.operation == OperationType.MUTATION for d in _definitions
    )
    is_sub = any(
        isinstance(d, _ODN) and d.operation == OperationType.SUBSCRIPTION for d in _definitions
    )

    # REQ-074/REQ-1386: one audit record per request — a query, a plan- or response-cache hit, a
    # mutation, a normalized read, a subscription being opened — handed to the audit writer (an
    # in-memory enqueue: no parse, no database).
    from provisa.audit.graphql import audit_graphql_request

    if is_sub:
        from provisa.api.data.subscription_sse import handle_subscription_sse

        # The row records the subscription being opened; the stream outlives the request.
        audit_graphql_request(state, role_id, request.query, ctx, 200, _time.monotonic())
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

    # REQ-1163: a request-level as-of validated to a safe SQL timestamp literal (400 on malformed).
    _as_of = None
    if x_provisa_as_of:
        from provisa.mv.bitemporal import parse_as_of

        try:
            _as_of = parse_as_of(x_provisa_as_of)
        except ValueError as exc:
            raise ApiError(
                400, "data.invalid_as_of", f"invalid X-Provisa-As-Of: {exc}", error=str(exc)
            )

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()

    # REQ-049: X-Provisa-Normalized returns one governed, deduplicated relational table per
    # entity (PK/FK preserved) as a manifest of S3 URLs, instead of the denormalized result.
    from provisa.audit.context import bind_request_audit

    _audit_started = _time.monotonic()
    # What the request's terminal notes for that record: the route(s), the rows, and whether a
    # statement of the request already wrote its own row (an action field).
    _audit_outcome = bind_request_audit()
    # REQ-1350: one collector for the request: whatever its statements find to say about their
    # answers (an API answer cut at max_pages) goes into extensions.warnings below.
    with collecting() as _request_warnings:
        try:
            if (x_provisa_normalized or "").lower() == "true" and not is_mut:
                response = await _handle_normalized(
                    document, ctx, rls, state, effective_variables, role_id, role
                )
            elif is_mut:
                response = await _handle_mutation(
                    document,
                    ctx,
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
                    as_of=_as_of,  # REQ-1163
                    steward_hint=steward_hint,
                    query_session_props=directives.to_session_props(),
                    cache_ttl=directives.cache_ttl,
                    cache_opt_in=directives.cache_opt_in,  # REQ-544 (amended): per-request opt-in
                    query_text=request.query,
                    # REQ-595: the response-cache tenant every surface shares (cache.tenancy) — the
                    # one write paths invalidate under.
                    org_id=cache_tenant(state),
                    plan=plan,
                    plan_request=plan_request,
                    directives=directives,
                )
        except Exception as exc:
            # The refusal or failure is the fact the row records (policy_denials reads the 403s).
            audit_graphql_request(
                state,
                role_id,
                request.query,
                ctx,
                getattr(exc, "status_code", 500),
                _audit_started,
                _audit_outcome,
            )
            raise
    audit_graphql_request(
        state,
        role_id,
        request.query,
        ctx,
        # A handler that returned a body rather than a Response completed normally.
        response.status_code if isinstance(response, Response) else 200,
        _audit_started,
        _audit_outcome,
    )
    if _request_warnings:
        response = _inject_warnings_into_response(response, _request_warnings)
    if (x_provisa_normalized or "").lower() == "true" and not is_mut:
        return response

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
    compiled, ctx, rls, state, role_id, role, fresh_mvs, as_of=None
):  # REQ-002, REQ-038, REQ-040, REQ-203, REQ-204, REQ-262, REQ-263, REQ-1163
    """Apply governance, MV rewrite, Kafka filters, and sampling to a compiled query.

    ``as_of`` (REQ-1163): a validated SQL timestamp literal. When set, bitemporal materialized views
    in the query are read AS OF that system time — their inline-expansion entries are overlaid with an
    as-of reconstruction over each one's append log (default, without it, reads current state)."""
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    # The role's governance, built first: it decides what a view reference becomes (below) and
    # then governs the whole statement.
    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role,
        relationships=getattr(state, "relationships", None),
    )

    if state.view_sql_map:
        from provisa.compiler.view_expand import expand_views
        from provisa.mv.view_read import split_for_whole_statement_governance

        _vmap = state.view_sql_map
        if as_of and getattr(state, "bitemporal_view_reads", None):
            from provisa.mv.bitemporal import as_of_view_map

            _vmap = as_of_view_map(state.view_sql_map, state.bitemporal_view_reads, as_of)
        # Each view reference becomes what THIS reader may read of it (mv/view_read.py): the
        # view's SQL now — the statement's validation and governance below reach the tables
        # inside — or, for a materialized view this reader may read whole, its stored rows,
        # substituted once the statement has been validated and governed.
        _views_now, _views_stored = split_for_whole_statement_governance(
            compiled.sql, _vmap, state, gov_ctx
        )
        compiled = expand_views(compiled, _views_now)
    else:
        _views_stored = {}

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

    # Governance: compile → semantic SQL → apply RLS/masking/visibility (gov_ctx built above)
    # Validate semantic SQL — V002 (join relationship check) is always skipped for
    # GraphQL because the SDL defines valid relationships by design.
    from provisa.compiler.sql_validator import validate_sql

    semantic_sql_for_validation = make_semantic_sql(compiled.sql, ctx)
    _violations = validate_sql(
        semantic_sql_for_validation,
        ctx,
        gov_ctx,
        role,
        getattr(state, "tables", []),
        bypass_relationship_guard=True,
    )
    if _violations:
        raise HTTPException(
            status_code=403,
            detail={"violations": [{"code": v.code, "message": v.message} for v in _violations]},
        )

    # REQ-1174: the complexity guard, on the semantic statement and before it is governed -- the
    # same check the pipeline's other two governing stages make (pgwire._pipeline). 413: the
    # query asks for too much, not for something the role may not see.
    import sqlglot

    from provisa.compiler.complexity import ComplexityLimitExceeded, guard_complexity

    try:
        guard_complexity(
            sqlglot.parse_one(semantic_sql_for_validation, read="postgres"), gov_ctx, ctx, role
        )
    except ComplexityLimitExceeded as too_complex:
        raise HTTPException(status_code=413, detail=str(too_complex)) from too_complex

    compiled.sql = apply_governance(semantic_sql_for_validation, gov_ctx)
    # REQ-1682: session-variable predicates resolve to the request's literals on every route —
    # nothing SETs them on a direct Postgres connection, so a native current_setting would raise.
    from provisa.core.request_context import session_vars_for as _session_vars_for
    from provisa.pgwire._pipeline import _resolve_session_settings

    compiled.sql = _resolve_session_settings(compiled.sql, _session_vars_for(role), "postgres")
    if compiled.nodes_sql is not None:
        compiled.nodes_sql = _resolve_session_settings(
            apply_governance(make_semantic_sql(compiled.nodes_sql, ctx), gov_ctx),
            _session_vars_for(role),
            "postgres",
        )
    if _views_stored:
        from provisa.compiler.view_expand import expand_view_refs

        compiled.sql = expand_view_refs(compiled.sql, _views_stored)
        if compiled.nodes_sql is not None:
            compiled.nodes_sql = expand_view_refs(compiled.nodes_sql, _views_stored)

    # ABAC approval hook (Phase AE, REQ-203) — evaluated AFTER RLS injection and
    # BEFORE execution. May deny the operation or return an additional filter that is
    # ANDed into the governed WHERE clause.
    if getattr(state, "approval_hook", None) is not None:
        from provisa.auth.approval_hook import ApprovalRequest, should_check
        from provisa.core.request_context import session_vars_for
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
                session_vars=session_vars_for(role),  # REQ-1682
            )
            resp = await state.approval_hook.evaluate(req)
            if not resp.approved:
                raise ApiError(
                    403,
                    "data.approval_denied",
                    f"Approval denied: {resp.reason}",
                    reason=str(resp.reason),
                )
            if resp.additional_filter:
                compiled.sql = _inject_where(compiled.sql, f"({resp.additional_filter})")

    return compiled, mv_used


def cached_field_rows(cached: CachedResult, compiled: Any) -> Any:  # REQ-544, REQ-1896
    """The rows a GraphQL Route.CACHE hit serves for the field ``compiled`` reads. The MISS stored
    the field's rows alias-free (``{"data": {<field>: rows}}``, ``response_cache_entry``) through
    the typed codec: the key decides the rows (every alias below the root is in the SQL), so two
    reads of one field under different aliases share the entry, and the caller places the rows
    under its own alias. Indexed, not ``.get(..., [])``: a missing key is a writer/reader shape
    mismatch, and defaulting it served an empty result for every hit."""
    cached_data, _ = decode_cached_result(cached)  # REQ-1896: typed binary, not lossy JSON
    return cached_data["data"][compiled.canonical_field]


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
    response_cache_ttl: int | None,
    cache_opt_in: bool,
    query_text: str | None = None,
    org_id: str | None = None,
):  # REQ-027, REQ-028, REQ-029, REQ-137, REQ-140, REQ-196
    """Execute a single compiled query field through the full pipeline.

    Returns (root_field, field_rows, redirect_info_or_None, cache_key, cached_entry_or_None).
    """
    from provisa.executor.redirect import upload_and_presign

    root_field = compiled.root_field
    _t0 = _time.perf_counter()

    # REQ-1910: this field's sources are known — a debug-trace window on one of them covers it.
    from provisa.pgwire._pipeline import extend_trace_scope_to_sources

    await extend_trace_scope_to_sources(state, role_id, frozenset(compiled.sources))

    # REQ-1915: a field that reads a row-level table without binding its key is refused here —
    # before the cache and whichever route the field would take — by the pipeline's own decision
    # point (``_pk_bounds``), as on every other surface.
    from provisa.pgwire._pipeline import _resolve_pk_bounds

    await _resolve_pk_bounds(compiled.sql, state, compiled.params)

    # Cache check. REQ-544 (amended 2026-09-30): the response cache is per-request OPT-IN — no
    # @cached / `-- @provisa cache` hint, no read and no write. REQ-866 fail-closed: when the
    # identity is not fully resolved into the key (empty RLS filter, or a current_setting-dependent
    # predicate), the query is not cacheable either, so a per-session value can't leak.
    _rls = rls.rules if rls.has_rules() else {}
    ck = cache_key(compiled.sql, compiled.params, role_id, _rls)
    _cache_off = (
        not cache_opt_in
        or not state.response_cache_store.stores_results  # caching disabled: nothing to read
        or force_redirect
        or output_format != "json"
        or not is_cacheable(compiled.sql, _rls)[0]
    )
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
        cache_opt_in=not _cache_off,
        operator_floor=operator_floor(state, compiled.table_ids),
    )
    # REQ-074: the route this field is answered by, for the request's audit row.
    note_request_route(
        "cache"
        if decision.route == Route.CACHE and cached is not None
        else decision.route.name.lower()
    )
    if decision.route == Route.CACHE and cached is not None:
        field_rows = cached_field_rows(cached, compiled)
        _qs_mod.record(
            field=root_field,
            source="cache",
            strategy="cache",
            elapsed_ms=(_time.perf_counter() - _t0) * 1000,
            rows=len(field_rows) if isinstance(field_rows, list) else 0,
            cache_hit=True,
        )
        observe_cache_hit(  # REQ-1910
            sources=compiled.sources,
            rows=len(field_rows) if isinstance(field_rows, list) else 0,
            started=_t0,
        )
        return root_field, field_rows, None, ck, cached

    # REQ-1909: a capped source this field reads live (DIRECT, an engine attach, or an API
    # source's upstream) is held to its concurrency cap for everything below that touches it.
    from provisa.federation.live_concurrency import acquire_for_route

    _live_permits = await acquire_for_route(
        state, decision.route, decision.source_id or "", compiled.sources, compiled.table_ids
    )
    try:
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
                cache_opt_in=not _cache_off,
                org_id=org_id,
                role_id=role_id,
            )

        if force_redirect and state.federation_engine.writes_result(effective_redirect_format):
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
            except (asyncio.TimeoutError, HTTPException):
                raise  # a timeout or an already-shaped error is not a redirect failure
            except Exception as failed:
                # REQ-1194: the caller asked for the result in the results store. A redirect that
                # fails is that request failing, by name -- never an inline answer in its place
                # (the rows it asked not to receive, with no word that the redirect failed).
                raise ApiError(
                    502,
                    "data.redirect_failed",
                    f"The result could not be written to the results store: {failed}",
                    error=str(failed),
                ) from failed

        # Standard execution
        session_hints: dict[str, str] = {}
        _dataloader_srcs: set = set()
        _hydration_rows: dict[str, int] = {}
        _hydration_cache_hits: set = set()
        _per_source_ms: dict[str, float] = {}
        _engine_ms: float = 0.0
        physical_sql: str = ""

        async def _dispatch():
            if (
                decision.route == Route.DIRECT
                and decision.source_id
                and state.source_pools.has(decision.source_id)
            ):
                return (
                    await state.federation_engine.execute_native(
                        state.source_pools,
                        decision.source_id,
                        _direct_exec_sql(state, role_id, compiled.sql, ctx, decision, probe_limit),
                        compiled.params,
                    ),
                    "",
                    0.0,
                    {},
                    set(),
                    {},
                    set(),
                    {},
                )
            (
                _result,
                _physical_sql,
                _eng_ms,
                _psms,
                _dl_srcs,
                _,
                _hyd_rows,
                _hyd_hits,
                _hints,
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
            return (
                _result,
                _physical_sql,
                _eng_ms,
                _psms,
                _dl_srcs,
                _hyd_rows,
                _hyd_hits,
                _hints,
            )

        try:
            try:
                (
                    result,
                    physical_sql,
                    _engine_ms,
                    _per_source_ms,
                    _dataloader_srcs,
                    _hydration_rows,
                    _hydration_cache_hits,
                    session_hints,
                ) = await _dispatch()
            except Exception as exc:
                # REQ-1448: the coordinator this query dialed may have been replaced while the wake's
                # recheck window still recorded its address. Re-resolve; redispatch only if it moved.
                if not await readdress_lost_coordinator(exc, state):
                    raise
                (
                    result,
                    physical_sql,
                    _engine_ms,
                    _per_source_ms,
                    _dataloader_srcs,
                    _hydration_rows,
                    _hydration_cache_hits,
                    session_hints,
                ) = await _dispatch()
        except HTTPException:
            raise
        except (MemoryError, ConnectionError) as e:
            log.error("Query resource error for %s: %s", root_field, e)
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            # REQ-1905: the request's deadline passing mid-statement is the request timing out, not
            # a server fault — it goes up as the timeout it is, and the caller's handler answers 504
            # naming the transport and the setting. Any other timeout (a source's own) stays a 500.
            _deadline = request_deadline.current()
            if isinstance(e, TimeoutError) and _deadline is not None and _deadline.fired:
                raise
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
            except (asyncio.TimeoutError, HTTPException):
                # The request's deadline passing while the redirect's query runs is a timeout, and an
                # error already shaped for the client is that error: neither is a redirect failure.
                raise
            except Exception as e:
                # A redirect that cannot be delivered fails the request (REQ-171), as the forced
                # redirect below does — it does not fall through to an inline result.
                raise ApiError(
                    502, "data.redirect_upload_failed", f"Redirect upload failed: {e}", error=str(e)
                ) from e

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
            except (asyncio.TimeoutError, HTTPException):
                raise  # a timeout or an already-shaped error is not a redirect failure (see above)
            except Exception as e:
                raise ApiError(
                    502, "data.redirect_upload_failed", f"Redirect upload failed: {e}", error=str(e)
                ) from e

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
            not _cache_off,
            _t0,
            _dataloader_srcs,
            _per_source_ms,
            _engine_ms,
            _hydration_rows,
            _hydration_cache_hits,
            physical_sql,
            org_id=org_id,
        )
    finally:
        _live_permits.release()


def _direct_exec_sql(
    state: Any, role_id: str, governed_sql: str, ctx: Any, decision: Any, probe_limit: int | None
) -> str:
    """The statement a DIRECT read sends to its source: the governed SQL lowered to the source's
    physical names, probe-limited when asked, and transpiled to its dialect.

    REQ-1877: that is a function of the governed text, the role's compilation context, the
    destination (source, dialect) and the probe limit — not of the request — so it is kept with
    the plans (``governed_plan.PlanSlot``: schema generation, role, acting-role set and the
    governance objects' identity) and a repeated request re-derives nothing. Bound values travel
    separately as parameters; a value inlined into the governed text is part of the key."""
    from provisa.pgwire.governed_plan import PlanSlot

    dialect = decision.dialect or "postgres"
    # The key's body is everything the derived text depends on beyond what PlanSlot already keys
    # (generation, role, acting-role set, governance objects). REQ-1912: the address a read is
    # sent to — replica or live — joins this body when a statement's text comes to depend on it.
    slot = PlanSlot(
        state, "graphql.direct_sql", role_id, governed_sql, decision.source_id, dialect, probe_limit
    )
    kept = slot.cached()
    if kept is not None:
        return kept
    exec_sql = rewrite_semantic_to_physical(governed_sql, ctx)
    if probe_limit is not None:
        exec_sql = _inject_probe_limit(exec_sql, probe_limit)
    exec_sql = transpile(exec_sql, dialect)
    slot.keep(exec_sql)
    return exec_sql


def _note_field_outcome(field_rows: Any, cached_entry: Any) -> None:
    """Note one executed root field on the request's audit outcome (REQ-074): its rows, and the
    cache route when it was served from the response cache (an executed field notes its own route
    where it is decided, in ``_execute_one_field``)."""
    if cached_entry is not None:
        note_request_route("cache")
    if isinstance(field_rows, list):
        note_request_rows(len(field_rows))


async def _handle_query(
    document,
    ctx,
    rls,
    state,
    variables,
    role,
    output_format="json",
    role_id="org_admin",
    *,
    force_redirect=False,
    redirect_threshold=None,
    redirect_format=None,
    as_of: str | None = None,  # REQ-1163: validated as-of SQL timestamp literal (or None)
    steward_hint: str | None = None,
    query_session_props: dict | None = None,
    cache_ttl: int | None,
    cache_opt_in: bool,
    query_text: str | None = None,
    org_id: str | None = None,
    plan=None,
    plan_request=None,
    directives=None,
):  # REQ-001, REQ-027, REQ-028, REQ-029, REQ-043, REQ-047, REQ-049, REQ-137, REQ-140, REQ-196
    """Handle a GraphQL query operation with content negotiation.

    Pipeline per root field: compile → RLS → masking → MV rewrite → sampling
      → cache check → route → transpile → execute → cache store → serialize.
    Multiple root fields are executed independently and merged.

    ``plan`` (REQ-1877): the request's cached governed plan — its compiled fields are executed as
    they are and the compile/governance stages are skipped. ``plan_request`` records the plan this
    call builds when there was none; ``directives`` is recorded with it.
    """
    # REQ-1174: cap execution wall-time at the tighter of this transport's request timeout
    # (REQ-1905: GraphQL's own value, else the default) and the role's max_query_time_ms (None →
    # the transport's only). Applied to every wait_for below.
    from provisa.compiler.limits import role_max_query_time_ms
    from provisa.core.limits import request_timeout_for, request_timeout_setting

    _transport_timeout = request_timeout_for("graphql")
    _rt_ms = role_max_query_time_ms(role)
    _role_timeout = (
        _transport_timeout if _rt_ms is None else min(_transport_timeout, _rt_ms / 1000.0)
    )
    # What a timed-out request's error names (REQ-1905): the setting its timeout came from.
    _timeout_setting = (
        f"role {role_id!r} max_query_time_ms"
        if _rt_ms is not None and _rt_ms / 1000.0 < _transport_timeout
        else request_timeout_setting("graphql")
    )

    if plan is not None:
        prepared = plan.prepared_copies()
    else:
        action_sels, regular_names = _split_action_fields(document, state)

        if action_sels and not regular_names:
            data = {}
            for sel in action_sels:
                data[sel.name.value] = await _execute_action_field(
                    sel.name.value, sel, state, variables, ctx=ctx, role_id=role_id
                )
            return JSONResponse(content={"data": data}, headers=build_cache_headers(None))

        if action_sels and regular_names:
            raise ApiError(
                400,
                "data.mixed_action_and_table_queries",
                "Cannot mix action fields with table queries",
            )

        compiled_queries = compile_query(document, ctx, variables)
        if not compiled_queries:
            raise ApiError(400, "data.no_query_fields", "No query fields found")

        # The plan is keyed on the fresh-MV set the request looked it up with, so the same set
        # drives the MV rewrite here.
        fresh_mvs = (
            plan_request.fresh_mvs if plan_request is not None else state.mv_registry.get_fresh()
        )

        # Prepare all compiled queries (RLS, masking, MV rewrite, sampling)
        prepared = []
        for cq in compiled_queries:
            prepped, _ = await _prepare_compiled(
                cq, ctx, rls, state, role_id, role, fresh_mvs, as_of=as_of
            )
            prepared.append(prepped)
        if plan_request is not None:
            plan_request.record(directives, prepared)

    # Determine redirect config
    from provisa.executor.redirect import request_redirect_config

    # REQ-029: a request threshold may only lower the operator's (the platform's floor).
    redirect_config = request_redirect_config(redirect_threshold)
    effective_redirect_format = redirect_format or redirect_config.default_format or "parquet"

    probe_limit = None
    if not force_redirect and redirect_config.enabled and redirect_config.threshold > 0:
        probe_limit = redirect_config.threshold + 1

    # --- Single root field: preserve existing behavior for binary formats ---
    if len(prepared) == 1:
        try:
            # The deadline is bound for the work, as on the multi-field path below: without it
            # nothing under this call knows how long the request may wait (REQ-1882).
            with request_deadline.within(_role_timeout):
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
                        cache_opt_in=cache_opt_in,
                        query_text=query_text,
                        org_id=org_id,
                    ),
                    timeout=_role_timeout,
                )
        except ReplicaBuilding as exc:
            # Not a slow query: the table's replica is still being built, and the message says so.
            raise ApiError(504, "data.replica_building", str(exc), replica=exc.replica)
        except asyncio.TimeoutError:
            raise ApiError(
                504,
                "data.query_timeout",
                f"graphql request timed out after {_role_timeout:g}s ({_timeout_setting})",
                timeout_s=f"{_role_timeout:g}",
                transport="graphql",
                setting=_timeout_setting,
            )
        _note_field_outcome(field_rows, cached_entry)  # REQ-074: the request's audit row
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

    # --- Multiple root fields: execute one after another on this request's thread, merge ---
    # REQ-1882 (amended 2026-09-29): sequential, not asyncio.gather. The request runs on its own
    # thread, whose loop executes blocking work inline; concurrent sibling tasks there gain no
    # parallelism, and one blocked inline would starve a sibling holding an engine connection or
    # lock — the two-tasks-on-one-connection-loop deadlock. Parallelism is across requests.
    merged_data: dict = {}
    merged_redirects: dict = {}

    async def _execute_fields() -> list:
        return [
            await _execute_one_field(
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
                cache_opt_in=cache_opt_in,
                query_text=query_text,
                org_id=org_id,
            )
            for compiled in prepared
        ]

    try:
        # The deadline watchdog cancels an in-flight blocking statement; wait_for alone cannot
        # fire while inline driver work holds the request thread (REQ-1882).
        with request_deadline.within(_role_timeout):
            results = await asyncio.wait_for(_execute_fields(), timeout=_role_timeout)
    except ReplicaBuilding as exc:
        # Not a slow query: the table's replica is still being built, and the message says so.
        raise ApiError(504, "data.replica_building", str(exc), replica=exc.replica)
    except asyncio.TimeoutError:
        raise ApiError(
            504,
            "data.query_timeout",
            f"graphql request timed out after {_role_timeout:g}s ({_timeout_setting})",
            timeout_s=f"{_role_timeout:g}",
            transport="graphql",
            setting=_timeout_setting,
        )

    for root_field, field_rows, redirect_info, _, cached_entry in results:
        _note_field_outcome(field_rows, cached_entry)  # REQ-074: the request's audit row
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
        raise ApiError(404, "data.table_not_found", f"Table {table!r} not found", table=table)

    emit_change_event(table_obj.table_name, table_obj.source_id, "touch")
    return Response(status_code=204)


# Dev endpoints (compile, submit, proto, sql) have been moved to endpoint_dev.py
