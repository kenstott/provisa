# Copyright (c) 2025 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/graphql endpoint (REQ-043).

Pipeline: parse -> compile -> RLS inject -> masking -> MV rewrite -> sampling
  -> cache check -> route -> transpile -> execute -> cache store -> serialize.
Mutations: parse -> compile_mutation -> RLS inject -> direct execute (never Trino).
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from graphql import GraphQLSyntaxError, OperationType
from pydantic import BaseModel

from provisa.cache.key import cache_key
from provisa.cache.middleware import build_cache_headers, check_cache, store_result
from provisa.compiler.mask_inject import inject_masking
from provisa.compiler.mutation_gen import (
    compile_mutation,
    inject_rls_into_mutation,
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query
from provisa.executor.direct import execute_direct
from provisa.executor.serialize import serialize_rows
from provisa.executor.trino import execute_trino
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability, InsufficientRightsError, check_capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str
    variables: dict | None = None
    role: str = "admin"  # test mode: role passed in request


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



@router.post("/graphql")
async def graphql_endpoint(
    request: GraphQLRequest,
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
    x_provisa_redirect: str | None = Header(None),
    x_provisa_redirect_threshold: int | None = Header(None),
    x_provisa_redirect_format: str | None = Header(None),
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

    role_id = x_provisa_role or request.role

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

    # Detect introspection queries (__schema, __type) and execute them
    # directly against the GraphQL schema instead of compiling to SQL.
    from graphql import execute as gql_execute
    from graphql.language.ast import OperationDefinitionNode
    introspection_fields = {"__schema", "__type"}
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
        result = gql_execute(schema, document, variable_values=request.variables)
        return JSONResponse({"data": result.data})

    # Detect mutation vs query
    is_mut = any(
        hasattr(d, "operation") and d.operation == OperationType.MUTATION
        for d in document.definitions
    )

    output_format = _parse_accept(accept)
    redirect_format = _parse_accept(x_provisa_redirect_format) if x_provisa_redirect_format else None

    # Redirect-Format without a threshold implies force redirect.
    # Redirect-Format with a threshold is conditional on row count.
    # X-Provisa-Redirect: true is still supported as an explicit force.
    force_redirect = (x_provisa_redirect or "").lower() == "true"
    if redirect_format and x_provisa_redirect_threshold is None:
        force_redirect = True

    if is_mut:
        return await _handle_mutation(
            document, ctx, rls, state, request.variables, role_id,
        )
    return await _handle_query(
        document, ctx, rls, state, request.variables, role, output_format, role_id,
        force_redirect=force_redirect,
        redirect_threshold=x_provisa_redirect_threshold,
        redirect_format=redirect_format,
    )


async def _handle_query(document, ctx, rls, state, variables, role, output_format="json", role_id="admin", *, force_redirect=False, redirect_threshold=None, redirect_format=None):
    """Handle a GraphQL query operation with content negotiation.

    Pipeline: compile → RLS → masking → MV rewrite → sampling
      → cache check → route → transpile → execute → cache store → serialize.
    """
    compiled_queries = compile_query(document, ctx, variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    compiled = compiled_queries[0]

    # Apply RLS and masking (before MV rewrite — MV has unfiltered data)
    compiled = inject_rls(compiled, ctx, rls)
    compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

    # MV rewrite — may change sources, so re-route after
    fresh_mvs = state.mv_registry.get_fresh()
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

    # Apply sampling unless role has full_results capability
    sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
    if sampling:
        compiled = apply_sampling(compiled, get_sample_size())

    # Cache check (after all SQL transformations are applied)
    rls_rules_for_key = rls.rules if rls.has_rules() else {}
    ck = cache_key(compiled.sql, compiled.params, role_id, rls_rules_for_key)
    cached = await check_cache(state.cache_store, ck)
    if cached is not None:
        headers = build_cache_headers(cached)
        return JSONResponse(
            content=json.loads(cached.data),
            headers=headers,
        )

    # Route decision (after MV rewrite may have changed source set)
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
    )
    log.info(
        "[QUERY %s] Route: %s | source=%s | reason: %s%s",
        compiled.root_field,
        decision.route.value,
        decision.source_id or "(trino)",
        decision.reason,
        " | MV optimized" if mv_used else "",
    )

    # Determine redirect config and effective format
    from provisa.executor.redirect import RedirectConfig, should_redirect, upload_and_presign
    from provisa.executor.trino_write import is_trino_native_format
    redirect_config = RedirectConfig.from_env()

    # Client can override the server threshold
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

    # --- CTAS path: Trino writes directly to S3 ---
    # For Trino-native formats (Parquet, ORC), use CTAS so data never passes
    # through Provisa. Force-redirect always takes this path; threshold-based
    # redirect must execute first to check row count (handled below).
    if (
        force_redirect
        and is_trino_native_format(effective_redirect_format)
        and state.trino_conn is not None
    ):
        try:
            # Always route through Trino for CTAS — recompile with catalog names
            compiled = compile_query(document, ctx, variables, use_catalog=True)[0]
            compiled = inject_rls(compiled, ctx, rls)
            compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
            compiled = rewrite_if_mv_match(compiled, fresh_mvs)
            if sampling:
                compiled = apply_sampling(compiled, get_sample_size())
            trino_sql = transpile_to_trino(compiled.sql)

            from provisa.executor.trino_write import (
                execute_ctas_redirect, presign_ctas_result,
                cleanup_result_table, schedule_s3_cleanup,
            )
            ctas_result = execute_ctas_redirect(
                state.trino_conn, trino_sql, effective_redirect_format,
            )
            url = await presign_ctas_result(ctas_result["s3_prefix"], redirect_config)

            # Drop table metadata immediately; schedule S3 data cleanup after TTL
            cleanup_result_table(state.trino_conn, ctas_result["table_name"])
            import asyncio
            asyncio.create_task(
                schedule_s3_cleanup(ctas_result["s3_prefix"], redirect_config),
            )

            content_type = {
                "parquet": "application/vnd.apache.parquet",
                "orc": "application/x-orc",
            }.get(effective_redirect_format, "application/octet-stream")

            return {
                "data": {compiled.root_field: None},
                "redirect": {
                    "redirect_url": url,
                    "row_count": ctas_result["row_count"],
                    "expires_in": redirect_config.ttl,
                    "content_type": content_type,
                },
            }
        except Exception as e:
            log.exception("CTAS redirect failed, falling back to standard execution")

    # --- Standard execution path ---
    try:
        if decision.route == Route.DIRECT and decision.source_id:
            if not state.source_pools.has(decision.source_id):
                raise HTTPException(
                    status_code=503,
                    detail=f"No connection pool for source {decision.source_id!r}",
                )
            target_sql = transpile(compiled.sql, decision.dialect or "postgres")
            result = await execute_direct(
                state.source_pools, decision.source_id, target_sql, compiled.params,
            )
        else:
            # Recompile with catalog-qualified names for Trino if not already rewritten
            if not fresh_mvs or compiled.sources == compile_query(document, ctx, variables)[0].sources:
                compiled = compile_query(
                    document, ctx, variables, use_catalog=True,
                )[0]
                compiled = inject_rls(compiled, ctx, rls)
                compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
                compiled = rewrite_if_mv_match(compiled, fresh_mvs)
                if sampling:
                    compiled = apply_sampling(compiled, get_sample_size())
            if state.trino_conn is None and state.flight_client is None:
                raise HTTPException(status_code=503, detail="Trino not connected")
            trino_sql = transpile_to_trino(compiled.sql)

            # Use Arrow Flight SQL when available
            if state.flight_client is not None:
                from provisa.executor.trino_flight import execute_trino_flight
                result = execute_trino_flight(
                    state.flight_client, trino_sql, compiled.params,
                )
            else:
                result = execute_trino(state.trino_conn, trino_sql, compiled.params)
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Query execution failed")
        raise HTTPException(status_code=500, detail=str(e))

    # --- Threshold-based redirect (after execution, check row count) ---
    if should_redirect(result, redirect_config, force=force_redirect):
        # For Trino-native formats, use CTAS even in threshold path
        if is_trino_native_format(effective_redirect_format) and state.trino_conn is not None:
            try:
                # Re-route through Trino via CTAS
                compiled_for_ctas = compile_query(document, ctx, variables, use_catalog=True)[0]
                compiled_for_ctas = inject_rls(compiled_for_ctas, ctx, rls)
                compiled_for_ctas = inject_masking(compiled_for_ctas, ctx, state.masking_rules, role_id)
                compiled_for_ctas = rewrite_if_mv_match(compiled_for_ctas, fresh_mvs)
                if sampling:
                    compiled_for_ctas = apply_sampling(compiled_for_ctas, get_sample_size())
                ctas_sql = transpile_to_trino(compiled_for_ctas.sql)

                from provisa.executor.trino_write import (
                    execute_ctas_redirect, presign_ctas_result,
                    cleanup_result_table, schedule_s3_cleanup,
                )
                ctas_result = execute_ctas_redirect(
                    state.trino_conn, ctas_sql, effective_redirect_format,
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

                return {
                    "data": {compiled.root_field: None},
                    "redirect": {
                        "redirect_url": url,
                        "row_count": ctas_result["row_count"],
                        "expires_in": redirect_config.ttl,
                        "content_type": content_type,
                    },
                }
            except Exception as e:
                log.exception("CTAS redirect failed, falling back to Provisa upload")

        # Non-native formats: Provisa serializes and uploads
        try:
            redirect_result = await upload_and_presign(
                result, redirect_config,
                output_format=effective_redirect_format,
                columns=compiled.columns,
            )
            return {"data": {compiled.root_field: None}, "redirect": redirect_result}
        except Exception as e:
            log.exception("Redirect upload failed, returning inline")

    response_data = _format_response(result.rows, compiled.columns, compiled.root_field, output_format)

    # Cache store (fire-and-forget for non-redirect JSON responses)
    if isinstance(response_data, dict):
        table_ids = {
            meta.table_id for meta in ctx.tables.values()
            if meta.field_name == compiled.root_field
        }
        await store_result(
            state.cache_store, ck, response_data,
            ttl=state.cache_default_ttl, table_ids=table_ids,
        )

    # Add cache MISS header for JSON responses
    if isinstance(response_data, dict):
        headers = build_cache_headers(None)
        return JSONResponse(content=response_data, headers=headers)

    return response_data


async def _handle_mutation(document, ctx, rls, state, variables, role_id):
    """Handle a GraphQL mutation operation."""
    try:
        mutations = compile_mutation(
            document, ctx, state.source_types, variables,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not mutations:
        raise HTTPException(status_code=400, detail="No mutation fields found")

    results = []
    for mutation in mutations:
        # Inject RLS into UPDATE/DELETE
        table_meta = ctx.tables.get(mutation.table_name)
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


@router.get("/proto/{role_id}")
async def proto_endpoint(role_id: str):
    """Return the .proto file content for a role as text/plain."""
    from provisa.api.app import state

    if role_id not in state.proto_files:
        raise HTTPException(
            status_code=404,
            detail=f"No proto file available for role {role_id!r}",
        )

    return Response(
        content=state.proto_files[role_id],
        media_type="text/plain",
    )
