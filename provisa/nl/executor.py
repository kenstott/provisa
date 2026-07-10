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
SQL     → Stage 2 governance + the engine
"""

# Requirements: REQ-357, REQ-359

from __future__ import annotations

import logging
import re
from typing import Any, Literal

log = logging.getLogger(__name__)

NlTarget = Literal["cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"]


class FederationError(RuntimeError):
    """Wraps federation engine (the engine) errors with a clean message."""


def _federation_error(exc: Exception) -> FederationError:
    """Extract human-readable message from the engine exception repr."""
    raw = str(exc)
    m = re.search(r'message="([^"]*)"', raw)
    msg = m.group(1) if m else raw
    return FederationError(msg)


async def execute(
    query: str, target: NlTarget, role: str, app_state: Any
) -> Any:  # REQ-357, REQ-359
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
    dispatch = {
        "cypher": _execute_cypher,
        "graphql": _execute_graphql,
        "sql": _execute_sql,
    }
    fn = dispatch.get(target)
    if fn is None:
        raise ValueError(f"Unknown target: {target}")
    return await fn(query, role, app_state)


async def _execute_cypher(query: str, role: str, app_state: Any) -> dict:
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.cypher.params import collect_param_names, bind_params
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_rewrite import make_semantic_sql, rewrite_semantic_to_catalog_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    ctx = _get_ctx(app_state, role)
    ast = parse_cypher(query)
    label_map = CypherLabelMap.from_schema(ctx)
    param_names = collect_param_names(query)
    bind_params(param_names, {})
    sql_ast, _, graph_vars = cypher_to_sql(ast, label_map, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    # Render to postgres SQL; make_semantic_sql handles catalog-qualified refs
    sql_str = sql_ast.sql(dialect="postgres")
    # Governance. A role with no rules is legitimately empty; a missing
    # rls_contexts/masking_rules attribute is a wiring bug — fail loud.
    rls = app_state.rls_contexts.get(role, RLSContext.empty())
    gov_ctx = build_governance_context(
        role,
        rls,
        app_state.masking_rules,
        ctx,
        getattr(app_state, "tables", []),
        role=getattr(app_state, "roles", {}).get(role),
    )
    governed_sql = apply_governance(make_semantic_sql(sql_str, ctx), gov_ctx)
    exec_sql = rewrite_semantic_to_catalog_physical(governed_sql, ctx)
    physical_sql = app_state.federation_engine.transpile_physical(exec_sql)
    rows = await _run_engine(physical_sql, [], app_state)
    assembled = assemble_rows(rows, graph_vars)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": [to_serializable(r) for r in assembled]}


async def _execute_graphql(query: str, role: str, app_state: Any) -> dict:
    from graphql import GraphQLSchema
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.compiler.rls import RLSContext
    from provisa.executor.serialize import serialize_aggregate, serialize_rows

    schema = app_state.schemas.get(role)
    if not isinstance(schema, GraphQLSchema):
        raise RuntimeError(f"No GraphQL schema for role: {role}")
    # execute_engine guards its own connection — no direct engine-connection check.
    engine = app_state.federation_engine

    ctx = _get_ctx(app_state, role)
    rls = getattr(app_state, "rls_contexts", {}).get(role, RLSContext.empty())
    gov_ctx = build_governance_context(
        role,
        rls,
        getattr(app_state, "masking_rules", {}),
        ctx,
        getattr(app_state, "tables", []),
        role=getattr(app_state, "roles", {}).get(role),
    )

    document = parse_query(schema, query, {})
    compiled_queries = compile_query(document, ctx, {})
    if not compiled_queries:
        raise RuntimeError("No query fields found")

    merged: dict = {}
    for cq in compiled_queries:
        governed = apply_governance(cq.sql, gov_ctx)
        physical = rewrite_semantic_to_catalog_physical(governed, ctx)
        result = await engine.execute_engine(physical, cq.params)
        if cq.nodes_sql is not None:
            governed_nodes = apply_governance(cq.nodes_sql, gov_ctx)
            physical_nodes = rewrite_semantic_to_catalog_physical(governed_nodes, ctx)
            nodes_result = await engine.execute_engine(physical_nodes, cq.nodes_params)
            serialized = serialize_aggregate(
                result.rows,
                cq.columns,
                nodes_result.rows,
                cq.nodes_columns,
                cq.root_field,
                agg_alias=cq.agg_alias,
            )
        else:
            serialized = serialize_rows(
                result.rows, cq.columns, cq.root_field, result_limit=cq.result_limit
            )
        merged.update(serialized.get("data", {}))

    return {"data": merged}


async def _execute_sql(query: str, role: str, app_state: Any) -> dict:
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_rewrite import make_semantic_sql, rewrite_semantic_to_catalog_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    ctx = _get_ctx(app_state, role)
    rls = getattr(app_state, "rls_contexts", {}).get(role, RLSContext.empty())
    gov_ctx = build_governance_context(
        role,
        rls,
        getattr(app_state, "masking_rules", {}),
        ctx,
        getattr(app_state, "tables", []),
        role=getattr(app_state, "roles", {}).get(role),
    )
    governed_sql = apply_governance(make_semantic_sql(query, ctx), gov_ctx)
    exec_sql = rewrite_semantic_to_catalog_physical(governed_sql, ctx)
    physical_sql = app_state.federation_engine.transpile_physical(exec_sql)
    rows = await _run_engine(physical_sql, [], app_state)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


def _get_ctx(app_state: Any, role: str) -> Any:
    ctx = app_state.contexts.get(role)
    if ctx is None:
        raise RuntimeError(f"Schema not loaded for role: {role}")
    return ctx


async def _run_engine(sql: str, params: list, app_state: Any) -> list[dict]:
    # execute_engine guards its own connection/availability — no direct engine-connection check.
    try:
        result = await app_state.federation_engine.execute_engine(sql, params or [])
    except Exception as exc:
        raise _federation_error(exc) from exc
    return [dict(zip(result.column_names, row)) for row in result.rows]
