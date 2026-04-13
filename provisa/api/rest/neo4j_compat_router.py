# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without expected written
# permission from the copyright holder.

"""Neo4j Browser compatibility layer.

Exposes the Neo4j HTTP Query API v2 surface so that Neo4j Browser
(connected via https://) can execute Cypher queries against Provisa.

Endpoints:
  GET  /                           — discovery (version + query endpoint URL)
  POST /db/{database}/query/v2    — Query API v2 (Neo4j 5.5+ format)

Neo4j Browser uses https:// mode when Bolt (port 7687) is unavailable,
falling back to the HTTP Query API.  No Bolt implementation required.
"""

from __future__ import annotations

import logging
from typing import Any

import trino.exceptions as _trino_exc
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)

router = APIRouter()


def _federation_error(exc: Exception) -> str:
    """Format execution errors without leaking the Trino backend name."""
    if isinstance(exc, _trino_exc.TrinoQueryError):
        parts = [f"type={exc.error_type}", f"name={exc.error_name}", f'message="{exc.message}"']
        if exc.query_id:
            parts.append(f"query_id={exc.query_id}")
        return "FederationUserError(" + ", ".join(parts) + ")"
    return str(exc)

_NEO4J_VERSION = "5.26.0"
_NEO4J_EDITION = "community"


# ── Discovery ─────────────────────────────────────────────────────────────────

@router.get("/")
async def neo4j_discovery(request: Request) -> JSONResponse:
    """Neo4j discovery endpoint — tells Browser where to send queries."""
    base = str(request.base_url).rstrip("/")
    return JSONResponse({
        "neo4j_version": _NEO4J_VERSION,
        "neo4j_edition": _NEO4J_EDITION,
        "transaction": f"{base}/db/{{databaseName}}/tx",
        "query": f"{base}/db/{{databaseName}}/query/v2",
    })


# ── Query API v2 ──────────────────────────────────────────────────────────────

class QueryV2Request(BaseModel):
    statement: str
    parameters: dict[str, Any] = {}


@router.post("/db/{database}/query/v2")
async def neo4j_query_v2(
    database: str,
    body: QueryV2Request,
    request: Request,
) -> JSONResponse:
    """Execute a Cypher query and return Neo4j Query API v2 format."""
    from provisa.api.app import state
    from provisa.cypher.parser import parse_cypher, CypherParseError
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.translator import cypher_to_sql, CypherCrossSourceError, CypherTranslateError
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.cypher.params import collect_param_names, bind_params, CypherParamError
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return _error_response("Schema not loaded", "ServiceUnavailable")

    try:
        ast = parse_cypher(body.statement)
    except CypherParseError as exc:
        return _error_response(str(exc), "SyntaxError")

    label_map = CypherLabelMap.from_schema(ctx)

    param_names = collect_param_names(body.statement)
    try:
        bind_params(param_names, body.parameters)
    except CypherParamError as exc:
        return _error_response(str(exc), "ParameterMissing")

    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, body.parameters)
    except (CypherCrossSourceError, CypherTranslateError) as exc:
        return _error_response(str(exc), "SyntaxError")

    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

    try:
        import sqlglot
        sql_str = sql_ast.sql(dialect="postgres")
    except Exception as exc:
        log.exception("Cypher SQL render failed")
        return _error_response(f"SQL generation failed: {exc}", "DatabaseError")

    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    gov_ctx = build_governance_context(
        role_id, rls, state.masking_rules, ctx, getattr(state, "tables", [])
    )
    semantic_sql = make_semantic_sql(sql_str, ctx)
    governed_sql = apply_governance(semantic_sql, gov_ctx)
    exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)

    try:
        trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    except Exception as exc:
        log.exception("Cypher SQL transpile failed")
        return _error_response(f"Transpile failed: {exc}", "DatabaseError")

    resolved_params = [body.parameters.get(name) for name in ordered_params]

    try:
        rows = await _execute(trino_sql, resolved_params, state)
    except Exception as exc:
        log.exception("Cypher execution failed: %s", trino_sql)
        return _error_response(f"Execution failed: {_federation_error(exc)}", "DatabaseError")

    try:
        assembled = assemble_rows(rows, graph_vars)
    except Exception as exc:
        return _error_response(f"Assembly failed: {exc}", "DatabaseError")

    columns = list(rows[0].keys()) if rows else []
    serializable_rows = [to_serializable(r) for r in assembled]

    return JSONResponse({
        "data": {
            "fields": columns,
            "values": [_to_query_v2_row(columns, r) for r in serializable_rows],
        },
        "bookmarks": [],
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_query_v2_row(columns: list[str], row: dict) -> list[Any]:
    """Convert an assembled row dict to a Query API v2 values array."""
    return [_to_query_v2_value(row.get(col)) for col in columns]


def _to_query_v2_value(value: Any) -> Any:
    """Convert a serialized Node/Edge/Path/scalar to Neo4j Query API v2 wire format."""
    if not isinstance(value, dict):
        return value

    # Node: has "id", "label", "properties"
    if "label" in value and "properties" in value and "type" not in value:
        return {
            "elementId": str(value.get("id", "")),
            "labels": [value["label"]] if value.get("label") else [],
            "properties": value.get("properties", {}),
        }

    # Edge: has "type", "startNode", "endNode"
    if "type" in value and "startNode" in value and "endNode" in value:
        start = value["startNode"]
        end = value["endNode"]
        return {
            "elementId": str(value.get("id", "")),
            "type": value["type"],
            "startNodeElementId": str(start.get("id", "") if isinstance(start, dict) else start),
            "endNodeElementId": str(end.get("id", "") if isinstance(end, dict) else end),
            "properties": value.get("properties", {}),
        }

    # Path: has "nodes", "edges"
    if "nodes" in value and "edges" in value:
        return {
            "nodes": [_to_query_v2_value(n) for n in value["nodes"]],
            "relationships": [_to_query_v2_value(e) for e in value["edges"]],
        }

    return value


def _error_response(message: str, code: str, status: int = 400) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"errors": [{"message": message, "code": f"Neo.ClientError.Statement.{code}"}]},
    )


def _resolve_role_id(request: Request, state: object) -> str:
    roles: dict = getattr(state, "roles", {})
    if roles:
        return next(iter(roles))
    return "default"


async def _execute(sql: str, params: list, state: object) -> list[dict]:
    import asyncio

    trino_conn = getattr(state, "trino_conn", None)
    if trino_conn is None:
        raise RuntimeError("Federation engine not connected")

    def _run() -> list[dict]:
        cursor = trino_conn.cursor()
        try:
            cursor.execute(sql, params or [])
            cols = [d[0] for d in (cursor.description or [])]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run)
