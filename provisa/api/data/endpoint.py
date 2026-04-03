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


import re as _re

def _inject_probe_limit(sql: str, limit: int) -> str:
    """Inject or tighten a LIMIT clause for threshold probing.

    If the query already has a LIMIT, use the smaller of the two.
    """
    limit_match = _re.search(r"\bLIMIT\s+(\d+)", sql, _re.IGNORECASE)
    if limit_match:
        existing = int(limit_match.group(1))
        effective = min(existing, limit)
        return sql[:limit_match.start()] + f"LIMIT {effective}" + sql[limit_match.end():]
    return sql + f" LIMIT {limit}"


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


async def _prepare_compiled(compiled, ctx, rls, state, role_id, role, fresh_mvs):
    """Apply RLS, masking, MV rewrite, Kafka filters, and sampling to a compiled query."""
    if state.view_sql_map:
        from provisa.compiler.view_expand import expand_views
        compiled = expand_views(compiled, state.view_sql_map)

    compiled = inject_rls(compiled, ctx, rls)
    compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

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

    sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
    if sampling:
        compiled = apply_sampling(compiled, get_sample_size())

    return compiled, mv_used


async def _execute_one_field(
    compiled, ctx, rls, state, variables, role, role_id,
    document, fresh_mvs, output_format,
    *, force_redirect, redirect_config, effective_redirect_format, probe_limit,
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

    # Cache check
    rls_rules_for_key = rls.rules if rls.has_rules() else {}
    ck = cache_key(compiled.sql, compiled.params, role_id, rls_rules_for_key)
    cached = await check_cache(state.cache_store, ck)
    if cached is not None:
        cached_data = json.loads(cached.data)
        # Extract just this field's data from the cached response
        field_rows = cached_data.get("data", {}).get(root_field, [])
        return root_field, field_rows, None, ck, cached

    # Route decision
    has_json_extract = "->>" in compiled.sql
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract=has_json_extract,
    )
    log.info(
        "[QUERY %s] Route: %s | source=%s | reason: %s",
        root_field, decision.route.value,
        decision.source_id or "(trino)", decision.reason,
    )

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
            # Recompile with catalog for Trino CTAS
            catalog_compiled = _recompile_for_trino_single(
                compiled, document, ctx, rls, state, variables, role_id, role, fresh_mvs,
            )
            trino_sql = transpile_to_trino(catalog_compiled.sql)
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
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("CTAS redirect failed for %s, falling back", root_field)

    # --- Standard execution ---
    try:
        if decision.route == Route.DIRECT and decision.source_id:
            if not state.source_pools.has(decision.source_id):
                raise HTTPException(
                    status_code=503,
                    detail=f"No connection pool for source {decision.source_id!r}",
                )
            exec_sql = compiled.sql
            if probe_limit is not None:
                exec_sql = _inject_probe_limit(exec_sql, probe_limit)
            target_sql = transpile(exec_sql, decision.dialect or "postgres")
            result = await execute_direct(
                state.source_pools, decision.source_id, target_sql, compiled.params,
            )
        else:
            # Recompile with catalog-qualified names for Trino
            catalog_compiled = _recompile_for_trino_single(
                compiled, document, ctx, rls, state, variables, role_id, role, fresh_mvs,
            )
            if state.trino_conn is None:
                raise HTTPException(status_code=503, detail="Trino not connected")
            exec_sql = catalog_compiled.sql
            if probe_limit is not None:
                exec_sql = _inject_probe_limit(exec_sql, probe_limit)
            trino_sql = transpile_to_trino(exec_sql)
            result = execute_trino(state.trino_conn, trino_sql, catalog_compiled.params)
            compiled = catalog_compiled  # use catalog columns for serialization
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
                target_sql = transpile(compiled.sql, decision.dialect or "postgres")
                full_result = await execute_direct(
                    state.source_pools, decision.source_id, target_sql, compiled.params,
                )
            else:
                full_trino_sql = transpile_to_trino(compiled.sql)
                full_result = execute_trino(state.trino_conn, full_trino_sql, compiled.params)
            redirect_info = await upload_and_presign(
                full_result, redirect_config,
                output_format=effective_redirect_format,
                columns=compiled.columns,
            )
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
            return root_field, None, redirect_info, ck, None
        except Exception:
            log.exception("Redirect upload failed for %s, returning inline", root_field)

    # --- Inline result ---
    response_data = _format_response(result.rows, compiled.columns, root_field, output_format)

    # Extract rows from serialized response for merging
    if isinstance(response_data, dict):
        field_rows = response_data.get("data", {}).get(root_field, [])
    else:
        # Binary format (parquet/arrow/csv) — can't merge, return as-is
        field_rows = response_data

    # Cache store
    if isinstance(response_data, dict):
        table_ids = {
            meta.table_id for meta in ctx.tables.values()
            if meta.field_name == root_field
        }
        await store_result(
            state.cache_store, ck, response_data,
            ttl=state.cache_default_ttl, table_ids=table_ids,
        )

    return root_field, field_rows, None, ck, None


def _recompile_for_trino_single(compiled, document, ctx, rls, state, variables, role_id, role, fresh_mvs):
    """Recompile a single root field with catalog-qualified names for Trino."""
    catalog_queries = compile_query(document, ctx, variables, use_catalog=True)
    # Find the matching root field
    for cq in catalog_queries:
        if cq.root_field == compiled.root_field:
            catalog_compiled = cq
            break
    else:
        catalog_compiled = catalog_queries[0]

    if state.view_sql_map:
        from provisa.compiler.view_expand import expand_views
        catalog_compiled = expand_views(catalog_compiled, state.view_sql_map)
    catalog_compiled = inject_rls(catalog_compiled, ctx, rls)
    catalog_compiled = inject_masking(catalog_compiled, ctx, state.masking_rules, role_id)
    catalog_compiled = rewrite_if_mv_match(catalog_compiled, fresh_mvs)
    sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
    if sampling:
        catalog_compiled = apply_sampling(catalog_compiled, get_sample_size())
    return catalog_compiled


async def _handle_query(document, ctx, rls, state, variables, role, output_format="json", role_id="admin", *, force_redirect=False, redirect_threshold=None, redirect_format=None):
    """Handle a GraphQL query operation with content negotiation.

    Pipeline per root field: compile → RLS → masking → MV rewrite → sampling
      → cache check → route → transpile → execute → cache store → serialize.
    Multiple root fields are executed independently and merged.
    """
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
            prepared[0], ctx, rls, state, variables, role, role_id,
            document, fresh_mvs, output_format,
            force_redirect=force_redirect,
            redirect_config=redirect_config,
            effective_redirect_format=effective_redirect_format,
            probe_limit=probe_limit,
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
            compiled, ctx, rls, state, variables, role, role_id,
            document, fresh_mvs, "json",  # multi-field always uses JSON
            force_redirect=force_redirect,
            redirect_config=redirect_config,
            effective_redirect_format=effective_redirect_format,
            probe_limit=probe_limit,
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


class SinkRequest(BaseModel):
    topic: str
    trigger: str = "change_event"  # change_event, schedule, manual
    key_column: str | None = None


class SubmitRequest(BaseModel):
    query: str
    variables: dict | None = None
    role: str = "admin"
    sink: SinkRequest | None = None
    business_purpose: str | None = None
    use_cases: str | None = None
    data_sensitivity: str | None = None  # public, internal, confidential, restricted
    refresh_frequency: str | None = None  # real-time, hourly, daily, weekly, ad-hoc
    expected_row_count: str | None = None  # <1K, 1K-100K, 100K+
    owner_team: str | None = None
    expiry_date: str | None = None  # ISO date


def _extract_operation_name(query_text: str) -> str | None:
    """Extract the operation name from a GraphQL query string."""
    from graphql import parse as gql_parse
    from graphql.language.ast import OperationDefinitionNode
    try:
        doc = gql_parse(query_text)
        for defn in doc.definitions:
            if isinstance(defn, OperationDefinitionNode) and defn.name:
                return defn.name.value
    except Exception:
        pass
    return None


@router.post("/compile")
async def compile_endpoint(
    request: GraphQLRequest,
    x_provisa_role: str | None = Header(None),
):
    """Compile a GraphQL query and return the SQL that would execute.

    Shows the full compiled SQL with RLS, masking, and sampling applied.
    Does not execute the query.
    """
    from provisa.api.app import state

    role_id = x_provisa_role or request.role
    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    try:
        document = parse_query(schema, request.query, request.variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    compiled_queries = compile_query(document, ctx, request.variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()
    role = state.roles.get(role_id)
    results = []

    for compiled in compiled_queries:
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
        compiled = rewrite_if_mv_match(compiled, fresh_mvs)

        if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
            from provisa.kafka.window import inject_kafka_filters
            compiled = inject_kafka_filters(
                compiled, ctx, state.source_types, state.kafka_table_configs,
            )

        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        if sampling:
            compiled = apply_sampling(compiled, get_sample_size())

        has_json_extract = "->>" in compiled.sql
        decision = decide_route(
            sources=compiled.sources,
            source_types=state.source_types,
            source_dialects=state.source_dialects,
            has_json_extract=has_json_extract,
        )

        trino_sql = transpile_to_trino(compiled.sql) if decision.route == Route.TRINO else None
        direct_sql = None
        if decision.route == Route.DIRECT and decision.dialect:
            direct_sql = transpile(compiled.sql, decision.dialect)

        results.append({
            "sql": compiled.sql,
            "trino_sql": trino_sql,
            "direct_sql": direct_sql,
            "params": compiled.params,
            "route": decision.route.value,
            "route_reason": decision.reason,
            "sources": list(compiled.sources),
            "root_field": compiled.root_field,
        })

    # Single field: return flat for backward compatibility
    if len(results) == 1:
        return results[0]
    return {"queries": results}


@router.post("/submit")
async def submit_endpoint(
    request: SubmitRequest,
    x_provisa_role: str | None = Header(None),
):
    """Submit a named GraphQL query for steward approval.

    The query must have a named operation (e.g., 'query MyReport { ... }').
    """
    from provisa.api.app import state

    role_id = x_provisa_role or request.role
    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    # Require named query
    op_name = _extract_operation_name(request.query)
    if not op_name:
        raise HTTPException(
            status_code=400,
            detail="Query must have a named operation (e.g., 'query MyReport { ... }').",
        )

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    try:
        document = parse_query(schema, request.query, request.variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    compiled_queries = compile_query(document, ctx, request.variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    # Process all root fields for submission
    target_tables = []
    compiled_sqls = []
    for compiled in compiled_queries:
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
        compiled_sqls.append(compiled.sql)
        root_table = ctx.tables.get(compiled.root_field)
        if root_table:
            target_tables.append(root_table.table_id)
    compiled = compiled_queries[0]  # use first for backward-compat fields

    from provisa.registry.store import submit
    async with state.pg_pool.acquire() as conn:
        query_id = await submit(
            conn,
            query_text=request.query,
            compiled_sql=compiled.sql,
            target_tables=target_tables,
            developer_id=role_id,
        )

        # Save optional metadata and sink
        updates = []
        params = []
        idx = 1
        for field, value in [
            ("sink_topic", request.sink.topic if request.sink else None),
            ("sink_trigger", request.sink.trigger if request.sink else None),
            ("sink_key_column", request.sink.key_column if request.sink else None),
            ("business_purpose", request.business_purpose),
            ("use_cases", request.use_cases),
            ("data_sensitivity", request.data_sensitivity),
            ("refresh_frequency", request.refresh_frequency),
            ("expected_row_count", request.expected_row_count),
            ("owner_team", request.owner_team),
            ("expiry_date", request.expiry_date),
        ]:
            if value is not None:
                updates.append(f"{field} = ${idx}")
                params.append(value)
                idx += 1
        if updates:
            params.append(query_id)
            await conn.execute(
                f"UPDATE persisted_queries SET {', '.join(updates)} WHERE id = ${idx}",
                *params,
            )

    return {
        "query_id": query_id,
        "operation_name": op_name,
        "message": f"Query '{op_name}' submitted for approval (id={query_id}).",
    }


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
