# Copyright (c) 2026 Kenneth Stott
# Canary: 6eefbd91-a0db-4ac0-80da-896de4017488
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute a validated NL-generated query through the appropriate pipeline (Phase AV, REQ-357).

Cypher  → Phase AU pipeline (cypher_router._execute)
GraphQL → existing compiler pipeline
SQL     → Stage 2 governance + Trino
"""

from __future__ import annotations

import logging
from typing import Any, Literal

log = logging.getLogger(__name__)

NlTarget = Literal["cypher", "graphql", "sql"]


async def execute(query: str, target: NlTarget, role: str, app_state: Any) -> Any:
    """Execute a validated query and return raw result.

    Args:
        query: Validated query string (Cypher, GraphQL, or SQL).
        target: Query language.
        role: Role string for authorization/compilation context.
        app_state: AppState instance.

    Returns:
        Result dict with {"columns", "rows"} for Cypher/SQL or
        {"data"} for GraphQL.

    Raises:
        RuntimeError on execution failure.
    """
    if target == "cypher":
        return await _execute_cypher(query, role, app_state)
    if target == "graphql":
        return await _execute_graphql(query, role, app_state)
    if target == "sql":
        return await _execute_sql(query, role, app_state)
    raise ValueError(f"Unknown target: {target}")


async def _execute_cypher(query: str, role: str, app_state: Any) -> dict:
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.cypher.params import collect_param_names, bind_params
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    import sqlglot

    ctx = _get_ctx(app_state, role)
    ast = parse_cypher(query)
    label_map = CypherLabelMap.from_schema(ctx)
    param_names = collect_param_names(query)
    bind_params(param_names, {})
    sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    # Render to postgres SQL; make_semantic_sql handles catalog-qualified refs
    sql_str = sql_ast.sql(dialect="postgres")
    # Governance
    rls = getattr(app_state, "rls_contexts", {}).get(role, RLSContext.empty())
    gov_ctx = build_governance_context(
        role, rls, getattr(app_state, "masking_rules", {}), ctx,
        getattr(app_state, "tables", []),
    )
    governed_sql = apply_governance(make_semantic_sql(sql_str, ctx), gov_ctx)
    exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)
    trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    rows = await _run_trino(trino_sql, [], app_state)
    assembled = assemble_rows(rows, graph_vars)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": [to_serializable(r) for r in assembled]}


async def _execute_graphql(query: str, role: str, app_state: Any) -> dict:
    from graphql import graphql as gql_execute, parse as gql_parse

    schema = app_state.schemas.get(role)
    if schema is None:
        raise RuntimeError(f"No GraphQL schema for role: {role}")
    doc = gql_parse(query)
    result = await gql_execute(schema, source=doc)
    if result.errors:
        raise RuntimeError("; ".join(str(e) for e in result.errors))
    return {"data": result.data}


async def _execute_sql(query: str, role: str, app_state: Any) -> dict:
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    import sqlglot

    ctx = _get_ctx(app_state, role)
    rls = getattr(app_state, "rls_contexts", {}).get(role, RLSContext.empty())
    gov_ctx = build_governance_context(
        role, rls, getattr(app_state, "masking_rules", {}), ctx,
        getattr(app_state, "tables", []),
    )
    governed_sql = apply_governance(make_semantic_sql(query, ctx), gov_ctx)
    exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)
    trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
    rows = await _run_trino(trino_sql, [], app_state)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


def _get_ctx(app_state: Any, role: str) -> Any:
    ctx = app_state.contexts.get(role)
    if ctx is None:
        raise RuntimeError(f"Schema not loaded for role: {role}")
    return ctx


async def _run_trino(sql: str, params: list, app_state: Any) -> list[dict]:
    import asyncio
    trino_conn = getattr(app_state, "trino_conn", None)
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
