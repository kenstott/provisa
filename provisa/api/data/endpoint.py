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
import copy
import logging
import time as _time
from typing import Any, cast


from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from graphql import GraphQLSyntaxError, OperationType
from pydantic import BaseModel

from provisa.core import request_deadline
from provisa.core.region_stores import HomeRegionUnavailable
from provisa.core.statement_warnings import collecting
from provisa.api.errors import ApiError
from provisa.cache.middleware import build_cache_headers
from provisa.federation.replica_state import ReplicaBuilding
from provisa.compiler.hints import extract_graphql_hints
from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.executor import stats as _qs_mod
from provisa.observability.request_facts import TimedJSONResponse as JSONResponse  # REQ-1910
from provisa.audit.context import note_request_route, note_request_rows
from provisa.security.rights import Capability
from provisa.transpiler.router import Route, RouteDecision
from provisa.api.data.mutations import (
    _execute_action_field,
    _handle_mutation,
    _split_action_fields,
)
from provisa.executor.serialize import serialize_aggregate, serialize_group_by
from provisa.api.data.endpoint_helpers import (
    _format_response,
    _append_mermaid,
    _build_directives_with_legacy,
    _build_redirect_params,
    _check_role_capability,
    _detect_introspection,
    _inject_stats_into_response,
    _inject_warnings_into_response,
    _parse_accept,
    _record_per_source_stats,
)
from provisa.federation.engine_wake import ensure_engine_awake
from provisa.compiler.complexity import ComplexityLimitExceeded
from provisa.core.operator_floor import OperatorFloorError


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


async def _handle_normalized(document, ctx, state, variables, role_id):
    """REQ-049: emit one governed, deduplicated relational table per entity.

    Each entity's scoped SELECT DISTINCT is read through the one pipeline with a forced delivery:
    governed as any read is, and landed by the pipeline's materialize stage (the denormalized
    product never forms). The answer is a manifest of the delivered files. A computed-join query
    that cannot be normalized returns 400.
    """
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.compiler.normalize import NormalizeError, compile_normalized
    from provisa.executor.redirect import delivery_from_request
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

    try:
        ntables = compile_normalized(document, ctx, variables, use_catalog=True)
    except NormalizeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    manifest: list[dict] = []
    for nt in ntables:
        delivery = delivery_from_request(
            force_redirect=True, redirect_format="parquet", threshold=None, role=role_id
        )
        plan = await _govern_and_route_compiled(
            nt.compiled.sql,
            role_id,
            exec_params=nt.compiled.params or None,
            state=state,
            deliver=delivery,
            cache_hint=NO_CACHE_HINT,
            compiled=nt.compiled,
            sdl_joins=True,
        )
        handle = (await _execute_plan(plan, state)).redirect
        assert handle is not None, "a forced delivery answers with its handle"
        manifest.append(
            {
                "table": nt.table_name,
                "path": list(nt.path),
                "url": handle["redirect_url"],
                "rowCount": handle["row_count"],
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
            document = parse_query(schema, request.query, request.variables, ctx=ctx)
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

    trace_hint = directives.debug_trace or debug_trace_from_header(x_provisa_trace)
    await resolve_trace_scope(state, role_id, hint=trace_hint)

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
            try:
                if (x_provisa_normalized or "").lower() == "true" and not is_mut:
                    response = await _handle_normalized(
                        document, ctx, state, effective_variables, role_id
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
                        # REQ-1910: the request's own trace hint, which each field's pipeline
                        # entry resolves again with the request's org and role.
                        debug_trace=trace_hint,
                        plan=plan,
                        plan_request=plan_request,
                        directives=directives,
                    )
                if isinstance(response, dict):
                    # A handler's dict body (a mutation's) is encoded by orjson here, never by
                    # FastAPI's jsonable_encoder pass over a returned dict (REQ-1867).
                    response = JSONResponse(content=response)
            except PermissionError as exc:
                # A read's refusal by the pipeline — validation, governance, the approval hook — is
                # the caller's: 403, never the global handler's 500. A refusal the app answers
                # itself (the complexity guard's 413, the operator floor's own code) and a
                # mutation's keep their own answers.
                if is_mut or isinstance(exc, (ComplexityLimitExceeded, OperatorFloorError)):
                    raise
                raise _forbidden(exc) from exc
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


def _forbidden(exc: PermissionError) -> Exception:
    """The 403 a pipeline refusal answers: the approval hook's denial keeps its own code and
    reason (REQ-203); any other refusal is named by its message."""
    from provisa.pgwire._pipeline import ApprovalDenied

    if isinstance(exc, ApprovalDenied):
        return ApiError(403, exc.code, str(exc), reason=exc.reason)
    return HTTPException(status_code=403, detail=str(exc))


async def _executed(plan, state, root_field: str):
    """``_execute_plan`` with what its failure answers the caller: a delivery that fails is the
    request failing by name (REQ-1194 / REQ-171, never an inline answer in its place); a resource
    error is a 503; the request's deadline passing goes up as the timeout it is (REQ-1905); any
    other failure is a 500 naming its cause. A refusal or an already-shaped error is unchanged."""
    from provisa.executor.redirect import DeliveryFailed
    from provisa.pgwire._pipeline import _execute_plan

    try:
        return await _execute_plan(plan, state)
    except DeliveryFailed as failed:
        if failed.forced:
            raise ApiError(
                502,
                "data.redirect_failed",
                f"The result could not be written to the results store: {failed}",
                error=str(failed),
            ) from failed
        raise ApiError(
            502,
            "data.redirect_upload_failed",
            f"Redirect upload failed: {failed}",
            error=str(failed),
        ) from failed
    except (HTTPException, PermissionError, ReplicaBuilding, HomeRegionUnavailable):
        raise  # HomeRegionUnavailable (REQ-1922): refused by name; the app answers it (503)
    except (MemoryError, ConnectionError) as exc:
        log.error("Query resource error for %s: %s", root_field, exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        deadline = request_deadline.current()
        if isinstance(exc, TimeoutError) and deadline is not None and deadline.fired:
            raise
        log.exception("Query execution failed for %s", root_field)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


async def _execute_one_field(  # REQ-027, REQ-028, REQ-029, REQ-137, REQ-140, REQ-196
    compiled,
    ctx,
    state,
    role_id,
    output_format,
    *,
    delivery,
    as_of: str | None,
    steward_hint: str | None,
    query_session_props: dict | None,
    cache_hint,
):
    """One root field through the ONE compiled pipeline (``_govern_and_route_compiled`` →
    ``_execute_plan``): validation, governance, the approval hook, the response cache, routing,
    API-table hydration, residency and the materialize stage are the pipeline's. This only shapes
    the rows it gets back into the field's answer.

    Returns (root_field, field_rows, redirect_or_None, cache_hit_record_or_None).
    """
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    root_field = compiled.root_field
    _t0 = _time.perf_counter()
    common = {
        "state": state,
        "cache_hint": cache_hint,
        "serve_cached": delivery is None,
        "as_of": as_of,
        "steward_hint": steward_hint,
        "session_props": query_session_props,
    }
    plan = await _govern_and_route_compiled(
        compiled.sql,
        role_id,
        exec_params=compiled.params or None,
        deliver=delivery,
        # REQ-1224: a buffered transport — the terminal inlines a result under the threshold and
        # lands one over it. An aggregate's answer is always small.
        buffered=compiled.nodes_sql is None,
        compiled=compiled,
        **common,
        sdl_joins=True,
    )
    route = cast(Route, plan.route)
    hit = plan.cache_hit[1] if route == Route.CACHE and plan.cache_hit else None
    note_request_route(route.name.lower())  # REQ-074: the route this field is answered by
    result = await _executed(plan, state, root_field)
    if result.redirect is not None:
        return root_field, None, result.redirect, hit

    nodes_rows = None
    if compiled.nodes_sql is not None:
        nodes_plan = await _govern_and_route_compiled(
            compiled.nodes_sql,
            role_id,
            exec_params=compiled.nodes_params or None,
            api_args=compiled.api_args or None,
            extra_selections=compiled.gql_remote_extra_selections or None,
            **common,
            sdl_joins=True,
        )
        nodes_rows = (await _executed(nodes_plan, state, root_field)).rows

    if compiled.nodes_sql is not None and compiled.is_group_by:
        response = serialize_group_by(
            result.rows, compiled.columns, nodes_rows, compiled.nodes_columns, root_field
        )
    elif compiled.nodes_sql is not None:
        response = serialize_aggregate(
            result.rows,
            compiled.columns,
            nodes_rows,
            compiled.nodes_columns,
            root_field,
            agg_alias=compiled.agg_alias,
        )
    else:
        response = _format_response(
            result.rows,
            compiled.columns,
            root_field,
            output_format,
            result_limit=compiled.result_limit,
        )
    field_rows = (
        response.get("data", {}).get(root_field, []) if isinstance(response, dict) else response
    )
    _elapsed_ms = (_time.perf_counter() - _t0) * 1000
    _n_rows = len(field_rows) if isinstance(field_rows, list) else 0
    _record_per_source_stats(
        root_field,
        set(plan.sources or ()),
        _elapsed_ms,
        _n_rows,
        ctx,
        state,
        RouteDecision(
            route=route,
            source_id=plan.source_id or None,
            dialect=plan.dialect,
            reason=plan.route_reason or "",
        ),
        field_rows=field_rows if isinstance(field_rows, list) else None,
        physical_sql=plan.sql,
    )
    qs = _qs_mod.current()
    if qs is not None and compiled.sources:
        _append_mermaid(qs, compiled, ctx, root_field, None, _elapsed_ms, _n_rows, None)
    return root_field, field_rows, None, hit


def _note_field_outcome(field_rows: Any) -> None:
    """Note one executed root field's rows on the request's audit outcome (REQ-074); its route,
    the cache's included, is noted where the pipeline decides it, in ``_execute_one_field``."""
    if isinstance(field_rows, list):
        note_request_rows(len(field_rows))


async def _handle_query(
    document,
    ctx,
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
    debug_trace: bool,
    plan=None,
    plan_request=None,
    directives=None,
):  # REQ-001, REQ-027, REQ-028, REQ-029, REQ-043, REQ-047, REQ-049, REQ-137, REQ-140, REQ-196
    """Handle a GraphQL query operation with content negotiation.

    Each root field is compiled here and then read through the ONE compiled pipeline
    (``_execute_one_field``); multiple root fields are executed one after another and merged.

    ``plan`` (REQ-1877): the request's kept compiled fields — compiling is skipped; governance is
    the pipeline's, kept there. ``plan_request`` records the fields this call compiles when there
    was none; ``directives`` is recorded with it.
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

        prepared = compile_query(document, ctx, variables)
        if not prepared:
            raise ApiError(400, "data.no_query_fields", "No query fields found")
        if plan_request is not None:
            plan_request.record(directives, copy.deepcopy(prepared))

    # REQ-1194/REQ-1224: the X-Provisa-Redirect* headers name a forced delivery, handled by the
    # pipeline's materialize stage like every transport's; without one the field is read as a
    # buffered result, inlined under the operator's threshold and landed over it. REQ-029: a
    # request threshold may only lower the operator's (the platform's floor).
    from provisa.compiler.directives import CacheHint
    from provisa.executor.redirect import delivery_from_request

    delivery = delivery_from_request(
        force_redirect=force_redirect,
        redirect_format=redirect_format,
        threshold=redirect_threshold,
        role=role_id,
    )
    _cache_hint = CacheHint(
        opt_in=cache_opt_in,
        ttl=cache_ttl,
        debug_trace=debug_trace,
    )
    _field_args = {
        "delivery": delivery,
        "as_of": as_of,
        "steward_hint": steward_hint,
        "query_session_props": query_session_props,
        "cache_hint": _cache_hint,
    }

    # --- Single root field: preserve existing behavior for binary formats ---
    if len(prepared) == 1:
        try:
            # The deadline is bound for the work, as on the multi-field path below: without it
            # nothing under this call knows how long the request may wait (REQ-1882).
            with request_deadline.within(_role_timeout):
                root_field, field_rows, redirect_info, cached_entry = await asyncio.wait_for(
                    _execute_one_field(
                        prepared[0], ctx, state, role_id, output_format, **_field_args
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
        _note_field_outcome(field_rows)  # REQ-074: the request's audit row
        if cached_entry is not None:
            headers = build_cache_headers(cached_entry)
            return JSONResponse(
                content={"data": {root_field: field_rows}},
                headers=headers,
            )
        if redirect_info is not None:
            return JSONResponse(content={"data": {root_field: None}, "redirect": redirect_info})
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
            # multi-field always answers JSON
            await _execute_one_field(compiled, ctx, state, role_id, "json", **_field_args)
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

    for root_field, field_rows, redirect_info, cached_entry in results:
        _note_field_outcome(field_rows)  # REQ-074: the request's audit row
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
