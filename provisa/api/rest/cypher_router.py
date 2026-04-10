# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""POST /query/cypher — Cypher query endpoint (Phase AU, REQ-345–353).

Five-stage pipeline:
  1. Cypher parser + translator → SQLGlot AST (physical refs)
  2. Graph type rewriter → CAST(ROW(...) AS JSON) for node/edge columns
  3. make_semantic_sql → semantic refs; apply_governance → RLS/masking/visibility
  4. rewrite_semantic_to_trino_physical → catalog-qualified refs
  5. Federation executor → flat rows → assembler → typed response
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)

router = APIRouter()

import re as _re
_PROC_RE = _re.compile(r"^\s*CALL\s+(db\.labels|db\.relationshipTypes|db\.propertyKeys)\s*\(\s*\)\s*$", _re.IGNORECASE)


def _detect_procedure(query: str) -> str | None:
    m = _PROC_RE.match(query.strip())
    return m.group(1).lower() if m else None


class CypherRequest(BaseModel):
    query: str
    params: dict[str, Any] = {}


@router.post("/query/cypher")
async def cypher_query(body: CypherRequest, request: Request) -> JSONResponse:
    """Execute a Cypher read query and return typed rows."""
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

    # Resolve role → use default role_id
    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    # Intercept Neo4j-compatible schema procedures before parse
    _proc = _detect_procedure(body.query)
    if _proc is not None:
        from provisa.cypher.label_map import CypherLabelMap
        label_map = CypherLabelMap.from_schema(ctx)
        if _proc == "db.labels":
            rows = [{"label": n.label} for n in sorted(label_map.nodes.values(), key=lambda x: x.label)]
            return JSONResponse(content={"columns": ["label"], "rows": rows})
        if _proc == "db.relationshiptypes":
            rows = [{"relationshipType": r.rel_type} for r in sorted(label_map.relationships.values(), key=lambda x: x.rel_type)]
            return JSONResponse(content={"columns": ["relationshipType"], "rows": rows})
        if _proc == "db.propertykeys":
            keys: set[str] = set()
            for nm in label_map.nodes.values():
                keys.update(nm.properties.keys())
            rows = [{"propertyKey": k} for k in sorted(keys)]
            return JSONResponse(content={"columns": ["propertyKey"], "rows": rows})

    # Stage 1: Parse
    try:
        ast = parse_cypher(body.query)
    except CypherParseError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Build label map
    label_map = CypherLabelMap.from_schema(ctx)

    # Validate and bind params
    param_names = collect_param_names(body.query)
    try:
        bind_params(param_names, body.params)
    except CypherParamError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Stage 1: Translate to SQLGlot (physical catalog.schema.table refs)
    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, body.params)
    except CypherCrossSourceError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except CypherTranslateError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Stage 2: Graph type rewriter
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

    # Render to SQL string (postgres dialect; make_semantic_sql handles catalog-qualified refs)
    try:
        import sqlglot
        sql_str = sql_ast.sql(dialect="postgres")
    except Exception as exc:
        log.exception("Cypher SQL render failed")
        return JSONResponse(status_code=500, content={"error": f"SQL generation failed: {exc}"})

    # Stage 3: Governance — semantic SQL → apply RLS/masking/visibility
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    gov_ctx = build_governance_context(
        role_id, rls, state.masking_rules, ctx, getattr(state, "tables", [])
    )
    semantic_sql = make_semantic_sql(sql_str, ctx)
    governed_sql = apply_governance(semantic_sql, gov_ctx)

    # Stage 4: Rewrite to Trino-physical (catalog.schema.table)
    exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)

    # Transpile to Trino dialect
    try:
        trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    except Exception as exc:
        log.exception("Cypher SQL transpile failed")
        return JSONResponse(status_code=500, content={"error": f"Transpile failed: {exc}"})

    # Resolve ordered parameter values
    resolved_params = [body.params.get(name) for name in ordered_params]

    # Stage 5: Execute via Trino/federation executor
    try:
        rows = await _execute(trino_sql, resolved_params, state)
    except Exception as exc:
        log.exception("Cypher execution failed: %s", trino_sql)
        return JSONResponse(status_code=500, content={"error": f"Execution failed: {exc}"})

    # Assemble
    try:
        assembled = assemble_rows(rows, graph_vars)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Assembly failed: {exc}"})

    columns = list(rows[0].keys()) if rows else []
    serializable_rows = [to_serializable(r) for r in assembled]

    return JSONResponse(content={"columns": columns, "rows": serializable_rows})


@router.get("/query/graph-schema")
async def graph_schema(request: Request) -> JSONResponse:
    """Return node labels and relationship types for the current role."""
    from provisa.api.app import state
    from provisa.cypher.label_map import CypherLabelMap

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = CypherLabelMap.from_schema(ctx)
    return JSONResponse(content={
        "node_labels": [
            {
                "label": n.label,
                "properties": list(n.properties.keys()),
            }
            for n in label_map.nodes.values()
        ],
        "relationship_types": [
            {
                "type": r.rel_type,
                "source": r.source_label,
                "target": r.target_label,
            }
            for r in label_map.relationships.values()
        ],
    })


def _resolve_role_id(request: Request, state: object) -> str:
    """Resolve the role_id from the request context."""
    # Use the first registered role as default
    roles: dict = getattr(state, "roles", {})
    if roles:
        return next(iter(roles))
    return "default"


async def _execute(sql: str, params: list, state: object) -> list[dict]:
    """Execute SQL against the federation engine and return rows as dicts."""
    from provisa.executor.drivers.postgresql import run_query_pg

    trino_conn = getattr(state, "trino_conn", None)
    if trino_conn is None:
        raise RuntimeError("Federation engine not connected")

    import asyncio

    def _run() -> list[dict]:
        cursor = trino_conn.cursor()
        try:
            cursor.execute(sql, params or [])
            cols = [d[0] for d in (cursor.description or [])]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run)
