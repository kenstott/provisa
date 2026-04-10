# Copyright (c) 2026 Kenneth Stott
# Canary: 4d5e6f7a-8b9c-0123-def0-123456789004
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Developer-facing data endpoints: compile, submit, proto, sql.

These endpoints are split from endpoint.py (which hit 1000+ lines) to keep
each module under the project's 1000-line limit.
"""

from __future__ import annotations

import logging

from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response
from graphql import GraphQLSyntaxError, parse as gql_parse_raw
from pydantic import BaseModel

from provisa.compiler.directives import extract_directives, extract_directives_from_sql_comments, merge_directives
from provisa.compiler.hints import graphql_comments_to_sql
from provisa.compiler.mask_inject import inject_masking
from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query, make_semantic_sql
from provisa.compiler.mutation_gen import inject_rls_into_mutation
from provisa.mv.rewriter import rewrite_if_mv_match
from provisa.security.rights import Capability, InsufficientRightsError, check_capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


class GraphQLRequest(BaseModel):
    query: str
    variables: dict | None = None
    role: str = "admin"


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
    data_sensitivity: str | None = None
    refresh_frequency: str | None = None
    expected_row_count: str | None = None
    owner_team: str | None = None
    expiry_date: str | None = None


class SQLRequest(BaseModel):
    sql: str
    role: str = "admin"


class EnforcementMetadata(BaseModel):
    """Governance enforcement details returned by the /data/compile endpoint (REQ-062)."""

    rls_filters_applied: list[str]
    columns_excluded: list[str]
    schema_scope: str
    masking_applied: list[str]
    ceiling_applied: Optional[int]
    route: str


def _build_enforcement_metadata(
    compiled,
    ctx,
    rls,
    masking_rules: dict,
    role_id: str,
    route_value: str,
) -> EnforcementMetadata:
    """Derive EnforcementMetadata from a compiled query and its governance inputs.

    Args:
        compiled:      CompiledQuery after RLS and masking injection.
        ctx:           CompilationContext for the role.
        rls:           RLSContext (rules dict keyed by table_id).
        masking_rules: Raw masking rules dict [(table_id, role_id)] → {col: (rule, dtype)}.
        role_id:       The requesting role.
        route_value:   Route string (e.g. "direct:postgres" or "trino").
    """
    # RLS filters — collect filter_expr for each table_id present in the compiled query
    rls_filters: list[str] = []
    root_table = ctx.tables.get(compiled.root_field)
    if root_table and root_table.table_id in rls.rules:
        rls_filters.append(rls.rules[root_table.table_id])

    for (type_name, _field_name), join_meta in ctx.joins.items():
        if root_table and type_name == root_table.type_name:
            if join_meta.target.table_id in rls.rules:
                rls_filters.append(rls.rules[join_meta.target.table_id])

    # Columns excluded — columns present in ctx but absent from compiled output
    compiled_column_names = {c.column for c in compiled.columns}
    excluded: list[str] = []
    if root_table:
        for col_name in (c.column for c in getattr(root_table, "columns", [])):
            if col_name not in compiled_column_names:
                excluded.append(f"{root_table.table_name}.{col_name}")

    # Masking applied — (table_id, role_id) entries matching this role
    masking_applied: list[str] = []
    for (table_id, r_id), col_map in masking_rules.items():
        if r_id != role_id:
            continue
        table_name = ""
        for meta in ctx.tables.values():
            if meta.table_id == table_id:
                table_name = meta.table_name
                break
        for col_name, (rule, _dtype) in col_map.items():
            label = f"{table_name}.{col_name} -> {rule.mask_type.value}" if table_name else col_name
            masking_applied.append(label)

    return EnforcementMetadata(
        rls_filters_applied=rls_filters,
        columns_excluded=excluded,
        schema_scope=f"role:{role_id}",
        masking_applied=masking_applied,
        ceiling_applied=None,
        route=route_value,
    )


def _extract_operation_name(query_text: str) -> str | None:
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
    raw_request: Request,
    request: GraphQLRequest,
    x_provisa_role: str | None = Header(None),
):
    """Compile a GraphQL query and return the SQL that would execute."""
    from provisa.api.app import state

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role or request.role
    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    _comment_directives = extract_directives_from_sql_comments(request.query)
    try:
        _ast_directives = extract_directives(gql_parse_raw(request.query))
    except Exception:
        _ast_directives = _comment_directives.__class__()
    directives = merge_directives(_comment_directives, _ast_directives)
    steward_hint = directives.steward_hint
    sql_comment_prefix = graphql_comments_to_sql(request.query)

    try:
        document = parse_query(schema, request.query, request.variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    effective_variables = coerce_variable_defaults(document, request.variables)
    compiled_queries = compile_query(document, ctx, effective_variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()
    results = []

    for compiled in compiled_queries:
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

        pre_mv_sources = set(compiled.sources)
        compiled = rewrite_if_mv_match(compiled, fresh_mvs)
        mv_applied = compiled.sources != pre_mv_sources

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
            steward_hint=steward_hint,
            has_json_extract=has_json_extract,
        )

        trino_sql = transpile_to_trino(compiled.sql) if decision.route == Route.TRINO else None
        direct_sql = None
        if decision.route == Route.DIRECT and decision.dialect:
            direct_sql = transpile(compiled.sql, decision.dialect)

        column_aliases = [
            {"field_name": c.field_name, "column": c.column}
            for c in compiled.columns
            if c.field_name != c.column
        ]

        route_str = decision.route.value
        if decision.route == Route.DIRECT and decision.dialect:
            route_str = f"direct:{decision.dialect}"

        enforcement = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=rls,
            masking_rules=state.masking_rules,
            role_id=role_id,
            route_value=route_str,
        )

        optimizations: list[str] = []
        warnings: list[str] = []
        if mv_applied:
            new_sources = compiled.sources - pre_mv_sources
            optimizations.append(
                f"Materialized view rewrite: sources → {', '.join(sorted(new_sources))}"
            )
        if directives.route == "DIRECT":
            if len(compiled.sources) > 1:
                warnings.append(
                    "route=direct ignored: query spans multiple sources and requires federation"
                )
            elif decision.route != Route.DIRECT:
                warnings.append(
                    "route=direct ignored: source has no direct driver"
                )
        session_props = directives.to_session_props()
        for k, v in session_props.items():
            optimizations.append(f"Federation hint: {k}={v} (via @provisa directive)")
        if sampling:
            optimizations.append("Sampling applied (role lacks FULL_RESULTS capability)")

        results.append({
            "sql": compiled.sql,
            "semantic_sql": sql_comment_prefix + make_semantic_sql(compiled.sql, ctx),
            "trino_sql": trino_sql,
            "direct_sql": direct_sql,
            "params": compiled.params,
            "route": decision.route.value,
            "route_reason": decision.reason,
            "sources": list(compiled.sources),
            "root_field": compiled.root_field,
            "canonical_field": compiled.canonical_field or compiled.root_field,
            "column_aliases": column_aliases,
            "enforcement": enforcement.model_dump(),
            "optimizations": optimizations,
            "warnings": warnings,
        })

    if len(results) == 1:
        return results[0]
    return {"queries": results}


@router.post("/submit")
async def submit_endpoint(
    raw_request: Request,
    request: SubmitRequest,
    x_provisa_role: str | None = Header(None),
):
    """Submit a named GraphQL query for steward approval."""
    from provisa.api.app import state

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role or request.role
    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    op_name = _extract_operation_name(request.query)
    if not op_name:
        raise HTTPException(
            status_code=400,
            detail="Query must have a named operation (e.g., 'query MyReport { ... }').",
        )

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    try:
        document = parse_query(schema, request.query, request.variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    effective_variables = coerce_variable_defaults(document, request.variables)
    compiled_queries = compile_query(document, ctx, effective_variables)
    if not compiled_queries:
        raise HTTPException(status_code=400, detail="No query fields found")

    target_tables = []
    compiled_sqls = []
    for compiled in compiled_queries:
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)
        compiled_sqls.append(compiled.sql)
        root_table = ctx.tables.get(compiled.root_field)
        if root_table:
            target_tables.append(root_table.table_id)
    compiled = compiled_queries[0]

    from provisa.registry.store import submit
    async with state.pg_pool.acquire() as conn:
        query_id = await submit(
            conn,
            query_text=request.query,
            compiled_sql=compiled.sql,
            target_tables=target_tables,
            developer_id=role_id,
        )

        updates = []
        params = []
        idx = 1
        for field_name, value in [
            ("sink_topic", request.sink.topic if request.sink else None),
            ("sink_trigger", request.sink.trigger if request.sink else None),
            ("sink_key_column", request.sink.key_column if request.sink else None),
            ("business_purpose", request.business_purpose),
            ("use_cases", request.use_cases),
            ("data_sensitivity", request.data_sensitivity),
            ("refresh_frequency", request.refresh_frequency),
            ("expected_row_count", request.expected_row_count),
            ("owner_team", request.owner_team),
            ("expiry_date", __import__("datetime").date.fromisoformat(request.expiry_date) if request.expiry_date else None),
        ]:
            if value is not None:
                updates.append(f"{field_name} = ${idx}")
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
    return Response(content=state.proto_files[role_id], media_type="text/plain")


@router.post("/sql")
async def sql_endpoint(
    raw_request: Request,
    request: SQLRequest,
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
):
    """Execute raw SQL through Stage 2 governance (REQ-264, REQ-266, REQ-267).

    Pipeline:
      1. Parse incoming SQL with SQLGlot.
      2. Construct GovernanceContext from the request role.
      3. Reject (HTTP 403) any table not in the role's schema scope.
      4. Apply Stage 2 governance: RLS, masking, visibility, ceiling.
      5. Route and execute the governed SQL.
    """
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.api.data.endpoint import _parse_accept, _format_response
    from provisa.compiler.stage2 import (
        apply_governance,
        build_governance_context,
        extract_sources,
    )
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role or request.role

    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    role = state.roles.get(role_id)
    if role:
        try:
            check_capability(role, Capability.QUERY_DEVELOPMENT)
        except InsufficientRightsError as e:
            raise HTTPException(status_code=403, detail=str(e))

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    # --- Step 1: Parse SQL via SQLGlot (REQ-266) ---
    try:
        parsed_tree = sqlglot.parse_one(request.sql, read="postgres")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"SQL parse error: {exc}")

    # --- Step 2: Build GovernanceContext for the role (REQ-266) ---
    gov_ctx = build_governance_context(
        role_id, rls, state.masking_rules, ctx,
        getattr(state, "tables", []),
    )

    # --- Step 3: Reject tables outside this role's schema scope (REQ-267) ---
    forbidden_tables: list[str] = []
    for tbl in parsed_tree.find_all(exp.Table):
        tbl_name = tbl.name
        tbl_db = tbl.db
        full_key = f"{tbl_db}.{tbl_name}" if tbl_db else tbl_name
        if (
            full_key not in gov_ctx.table_map
            and tbl_name not in gov_ctx.table_map
        ):
            forbidden_tables.append(full_key or tbl_name)

    if forbidden_tables:
        log.warning(
            "[SQL] role=%s forbidden tables referenced: %s",
            role_id,
            forbidden_tables,
        )
        raise HTTPException(
            status_code=403,
            detail=(
                f"Query references table(s) not accessible for role {role_id!r}: "
                + ", ".join(forbidden_tables)
            ),
        )

    # --- Step 4: Apply Stage 2 governance (RLS + masking + visibility + ceiling) ---
    governed_sql = apply_governance(request.sql, gov_ctx)
    sources = extract_sources(request.sql, gov_ctx, ctx)

    # When no tables are referenced (e.g. SELECT 1), fall back to the first
    # available relational pool rather than a hardcoded "pg" ID.
    _default_source = next(
        (sid for sid, t in state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
        next(iter(state.source_pools), "pg"),
    )

    decision = decide_route(
        sources=sources or {_default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract="->>" in governed_sql,
    )

    output_format = _parse_accept(accept)

    # --- Step 5: Execute the governed SQL ---
    if decision.route == Route.TRINO:
        sql_to_run = transpile_to_trino(governed_sql)
        result = await execute_trino(sql_to_run, [])
    else:
        dialect = decision.dialect or "postgres"
        sql_to_run = transpile(governed_sql, dialect)
        result = await execute_direct(
            state.source_pools, decision.source_id or _default_source, sql_to_run, [],
        )

    rows_as_dicts = [dict(zip(result.column_names, row)) for row in result.rows]
    if output_format == "json":
        return {"data": {"sql": rows_as_dicts}}
    from provisa.compiler.sql_gen import ColumnRef
    columns = [ColumnRef(field_name=c, column=c) for c in result.column_names]
    return _format_response(result.rows, columns, "sql", output_format)
