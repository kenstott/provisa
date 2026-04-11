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

from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import rewrite_semantic_to_physical
from provisa.security.rights import Capability, InsufficientRightsError, check_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


class SQLRequest(BaseModel):
    sql: str
    role: str = "admin"


def _detect_target(query: str) -> Literal["graphql", "sql", "cypher"]:
    """Detect query language from syntax."""
    import re
    stripped = query.strip()
    first = stripped.split()[0].lower() if stripped.split() else ""
    if first in ("query", "mutation", "subscription", "fragment") or stripped.startswith("{"):
        return "graphql"
    if first in ("match", "optional", "call") or re.search(r"\([\w]*:", stripped):
        return "cypher"
    return "sql"


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
    query_id: str | None = Query(None),
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

    if query_id:
        from provisa.api.flight.catalog import fetch_approved_queries_async
        queries = await fetch_approved_queries_async(state)
        matched = next((q for q in queries if q.stable_id == query_id), None)
        if matched is None:
            raise HTTPException(status_code=404, detail=f"Approved query not found: {query_id!r}")
        query_req = QueryRequest(query=matched.query_text or "", role=role_id)
        return await unified_query_endpoint(raw_request, query_req, x_provisa_role=x_provisa_role)

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

    # --- Step 1: Parse semantic SQL via SQLGlot (REQ-266) ---
    # Clients write semantic SQL (domain.field_name refs). Physical translation
    # happens after routing — governance runs on semantic refs.
    try:
        parsed_tree = sqlglot.parse_one(request.sql, read="postgres")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"SQL parse error: {exc}")

    # --- Step 2: Build GovernanceContext — table_map includes semantic refs ---
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

    # --- Step 4: Governance on semantic SQL ---
    governed_semantic = apply_governance(request.sql, gov_ctx)

    # --- Step 5: Routing decision (on governed semantic SQL) ---
    sources = extract_sources(governed_semantic, gov_ctx, ctx)

    _default_source = next(
        (sid for sid, t in state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
        next(iter(state.source_pools), "pg"),
    )

    decision = decide_route(
        sources=sources or {_default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract="->>" in governed_semantic,
    )

    # --- Step 6: Rewrite semantic → physical intermediate, then transpile ---
    governed_physical = rewrite_semantic_to_physical(governed_semantic, ctx)

    output_format = _parse_accept(accept)

    # --- Step 7: Execute ---
    if decision.route == Route.TRINO:
        sql_to_run = transpile_to_trino(governed_physical)
        result = await execute_trino(sql_to_run, [])
    else:
        dialect = decision.dialect or "postgres"
        sql_to_run = transpile(governed_physical, dialect)
        result = await execute_direct(
            state.source_pools, decision.source_id or _default_source, sql_to_run, [],
        )

    rows_as_dicts = [dict(zip(result.column_names, row)) for row in result.rows]
    if output_format == "json":
        return {"data": {"sql": rows_as_dicts}}
    from provisa.compiler.sql_gen import ColumnRef
    columns = [ColumnRef(field_name=c, column=c) for c in result.column_names]
    return _format_response(result.rows, columns, "sql", output_format)


class QueryRequest(BaseModel):
    query: str
    params: dict = {}
    variables: dict | None = None
    role: str = "admin"


@router.post("/query")
async def unified_query_endpoint(
    raw_request: Request,
    request: QueryRequest,
    x_provisa_role: str | None = Header(None),
    query_id: str | None = Query(None),
):
    """Execute a GraphQL, SQL, or Cypher query; auto-detected from syntax.

    Returns { columns, rows } for Cypher/SQL.
    Returns { data } for GraphQL (native format).
    """
    from provisa.api.app import state
    from fastapi.responses import JSONResponse as _JSONResponse

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role or request.role

    if query_id:
        from provisa.api.flight.catalog import fetch_approved_queries_async
        queries = await fetch_approved_queries_async(state)
        matched = next((q for q in queries if q.stable_id == query_id), None)
        if matched is None:
            from fastapi.responses import JSONResponse as _JSONResponse
            return _JSONResponse(status_code=404, content={"error": f"Approved query not found: {query_id!r}"})
        request = QueryRequest(query=matched.query_text or "", role=role_id)

    target = _detect_target(request.query)

    if target == "cypher":
        from provisa.api.rest.cypher_router import CypherRequest, cypher_query
        body = CypherRequest(query=request.query, params=request.params)
        return await cypher_query(body, raw_request)

    if target == "graphql":
        from provisa.api.data.endpoint import graphql_endpoint
        from provisa.api.data.endpoint import GraphQLRequest as GQLRequest
        gql_req = GQLRequest(query=request.query, variables=request.variables, role=role_id)
        return await graphql_endpoint(raw_request, gql_req)

    # SQL
    sql_req = SQLRequest(sql=request.query, role=role_id)
    return await sql_endpoint(raw_request, sql_req, x_provisa_role=x_provisa_role)
