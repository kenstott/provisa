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
        "grpc": _execute_grpc,
        "jsonapi": _execute_jsonapi,
        "openapi": _execute_openapi,
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
    exec_sql = _expand_views(exec_sql, app_state)
    physical_sql = app_state.federation_engine.transpile_physical(exec_sql)
    rows = await _run_engine(physical_sql, [], app_state)
    assembled = assemble_rows(rows, graph_vars)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": [to_serializable(r) for r in assembled]}


async def _compile_and_execute_graphql(query: str, role: str, app_state: Any) -> list[tuple]:
    """Run ``query`` through parse_query/compile_query/governance/physical-rewrite/execute_engine
    and return the raw (compiled_query, result, nodes_result) tuples — the shared pipeline step
    both ``_execute_graphql`` (GraphQL-shaped merge) and the gRPC/JSON:API/OpenAPI aggregate
    branches (protocol-shaped envelopes, REQ-1359) build on, so there is exactly one place that
    compiles/governs/executes GraphQL text."""
    from graphql import GraphQLSchema
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.compiler.rls import RLSContext

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

    out: list[tuple] = []
    for cq in compiled_queries:
        governed = apply_governance(cq.sql, gov_ctx)
        physical = rewrite_semantic_to_catalog_physical(governed, ctx)
        physical = _expand_views(physical, app_state)
        result = await engine.execute_engine(physical, cq.params)
        nodes_result = None
        if cq.nodes_sql is not None:
            governed_nodes = apply_governance(cq.nodes_sql, gov_ctx)
            physical_nodes = rewrite_semantic_to_catalog_physical(governed_nodes, ctx)
            physical_nodes = _expand_views(physical_nodes, app_state)
            nodes_result = await engine.execute_engine(physical_nodes, cq.nodes_params)
        out.append((cq, result, nodes_result))
    return out


async def _run_single_compiled_graphql(query: str, role: str, app_state: Any) -> tuple:
    """The single-root-field case (gRPC/JSON:API/OpenAPI aggregate/group-by NL query text always
    targets exactly one root field) — returns (compiled_query, result, nodes_result)."""
    return (await _compile_and_execute_graphql(query, role, app_state))[0]


async def _execute_graphql(query: str, role: str, app_state: Any) -> dict:
    from provisa.executor.serialize import serialize_aggregate, serialize_rows

    compiled_results = await _compile_and_execute_graphql(query, role, app_state)

    merged: dict = {}
    for cq, result, nodes_result in compiled_results:
        if cq.nodes_sql is not None:
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
    exec_sql = _expand_views(exec_sql, app_state)
    physical_sql = app_state.federation_engine.transpile_physical(exec_sql)
    rows = await _run_engine(physical_sql, [], app_state)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


async def _execute_grpc(query: str, role: str, app_state: Any) -> dict:
    import json

    from provisa.grpc.query_ir import (
        grpc_table_to_semantic_sql,
        split_agg_columns,
        split_group_by_columns,
    )

    ctx = _get_ctx(app_state, role)

    # REQ-1359: Query{Type}Aggregate / Query{Type}GroupBy(by=[...]) — synthesized by
    # runner.py's _generate_grpc_query when the SQL branch resolved a GROUP BY/aggregate
    # question — are run through the same compile_query pipeline GraphQL uses, not a
    # second aggregation implementation. The result is then shaped into the same
    # (group_key, aggregate) / flat-aggregate-fields structure the real gRPC servicer's
    # {Type}AggregateResult/{Type}GroupByRow messages carry (provisa/grpc/server.py), not
    # GraphQL's raw {"data": ...} envelope — the NL preview must look like what the gRPC
    # client actually receives, same as the JSON:API/OpenAPI previews below.
    m = re.match(r"^Query(.+)GroupBy\(by=\[(.*?)\](?:, funcs=\[(.*)\])?\)$", query)
    if m is not None:
        type_name = m.group(1)
        by_columns = [c.strip() for c in m.group(2).split(",") if c.strip()]
        funcs = [c.strip() for c in m.group(3).split(",") if c.strip()] if m.group(3) else None
        graphql_text = _grpc_group_by_graphql_text(ctx, type_name, by_columns, funcs)
        if graphql_text is None:
            raise RuntimeError(f"No table matches gRPC type: {type_name}")
        cq, result, _ = await _run_single_compiled_graphql(graphql_text, role, app_state)
        group_key_cols, group_key_idx, agg_cols, agg_idx = split_group_by_columns(cq.columns)
        rows = []
        for row in result.rows:
            group_key = {c.field_name: row[i] for c, i in zip(group_key_cols, group_key_idx)}
            agg_row = tuple(row[i] for i in agg_idx)
            top, nested = split_agg_columns(agg_cols, agg_row)
            rows.append({"group_key": json.dumps(group_key, default=str), "aggregate": {**top, **nested}})
        return {"group_by": rows}

    m = re.match(r"^Query(.+)Aggregate(?:\(funcs=\[(.*)\]\))?$", query)
    if m is not None:
        type_name = m.group(1)
        funcs = [c.strip() for c in m.group(2).split(",") if c.strip()] if m.group(2) else None
        graphql_text = _grpc_aggregate_graphql_text(ctx, type_name, funcs)
        if graphql_text is None:
            raise RuntimeError(f"No table matches gRPC type: {type_name}")
        cq, result, _ = await _run_single_compiled_graphql(graphql_text, role, app_state)
        row = result.rows[0] if result.rows else ()
        top, nested = split_agg_columns(cq.columns, row)
        return {**top, **nested}

    type_name = query[len("Query") :] if query.startswith("Query") else query
    sql = grpc_table_to_semantic_sql(ctx, type_name, limit=20)
    if sql is None:
        raise RuntimeError(f"No table matches gRPC type: {type_name}")
    return await _execute_sql(sql, role, app_state)


def _grpc_aggregate_graphql_text(
    ctx: Any, type_name: str, funcs: list[str] | None = None
) -> str | None:
    from provisa.grpc.query_ir import grpc_table_to_aggregate_graphql_text

    return grpc_table_to_aggregate_graphql_text(ctx, type_name, funcs)


def _grpc_group_by_graphql_text(
    ctx: Any, type_name: str, by_columns: list[str], funcs: list[str] | None = None
) -> str | None:
    from provisa.grpc.query_ir import grpc_table_to_group_by_graphql_text

    return grpc_table_to_group_by_graphql_text(ctx, type_name, by_columns, funcs)


def _parse_aggregate_funcs(raw: str) -> list[str] | None:
    """``aggregate=true`` means every function; ``aggregate=count,sum`` (REQ-1361) restricts to
    that subset — mirrors JSON:API/REST's own ``_parse_aggregate_param`` convention."""
    return None if raw == "true" else [f.strip() for f in raw.split(",") if f.strip()]


async def _execute_jsonapi(query: str, role: str, app_state: Any) -> dict:
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)\?groupBy=([^&]+)&aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            m.group(3).split(","),
            role,
            app_state,
            "jsonapi",
            _parse_aggregate_funcs(m.group(4)),
        )
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)\?aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1), m.group(2), [], role, app_state, "jsonapi", _parse_aggregate_funcs(m.group(3))
        )
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)", query)
    if m is None:
        raise RuntimeError(f"Malformed jsonapi query: {query}")
    return await _execute_domain_table(m.group(1), m.group(2), role, app_state)


async def _execute_openapi(query: str, role: str, app_state: Any) -> dict:
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)\?groupBy=([^&]+)&aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            m.group(3).split(","),
            role,
            app_state,
            "openapi",
            _parse_aggregate_funcs(m.group(4)),
        )
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)\?aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1), m.group(2), [], role, app_state, "openapi", _parse_aggregate_funcs(m.group(3))
        )
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)", query)
    if m is None:
        raise RuntimeError(f"Malformed openapi query: {query}")
    return await _execute_domain_table(m.group(1), m.group(2), role, app_state)


async def _execute_domain_table(
    domain_id: str, table_name: str, role: str, app_state: Any
) -> dict:
    from provisa.compiler.sql_gen import _q
    from provisa.compiler.sql_rewrite import _semantic_table_ref

    ctx = _get_ctx(app_state, role)
    meta = next(
        (m for m in ctx.tables.values() if m.domain_id == domain_id and m.table_name == table_name),
        None,
    )
    if meta is None:
        raise RuntimeError(f"No table matches {domain_id}/{table_name}")
    cols = ", ".join(_q(c) for c, _t in ctx.aggregate_columns.get(meta.table_id, [])) or "*"
    sql = f"SELECT {cols} FROM {_semantic_table_ref(meta)} LIMIT 20"
    return await _execute_sql(sql, role, app_state)


async def _execute_domain_table_aggregate(
    domain_id: str,
    table_name: str,
    by_columns: list[str],
    role: str,
    app_state: Any,
    protocol: Literal["jsonapi", "openapi"],
    funcs: list[str] | None = None,
) -> dict:
    """REQ-1359: JSON:API/REST aggregate|groupBy params, routed through the same compile_query
    pipeline GraphQL/gRPC aggregate queries use (grpc_table_to_*_graphql_text), then shaped into
    each protocol's actual response envelope — JSON:API's ``{"data": null, "meta": {"aggregate":
    ...}}``/typed-resourceless-row shape (provisa/api/jsonapi/generator.py:588-615) or REST's flat
    ``{"data": ...}`` shape (provisa/api/rest/generator.py:479-497) — instead of returning
    GraphQL's raw ``{"data": {root_field: ...}}`` body unmodified."""
    from provisa.executor.serialize import serialize_aggregate, serialize_group_by

    ctx = _get_ctx(app_state, role)
    meta = next(
        (m for m in ctx.tables.values() if m.domain_id == domain_id and m.table_name == table_name),
        None,
    )
    if meta is None:
        raise RuntimeError(f"No table matches {domain_id}/{table_name}")
    if by_columns:
        graphql_text = _grpc_group_by_graphql_text(ctx, meta.type_name, by_columns, funcs)
    else:
        graphql_text = _grpc_aggregate_graphql_text(ctx, meta.type_name, funcs)
    if graphql_text is None:
        raise RuntimeError(f"No aggregate/group-by fields for {domain_id}/{table_name}")

    cq, result, _ = await _run_single_compiled_graphql(graphql_text, role, app_state)

    if by_columns:
        shaped = serialize_group_by(result.rows, cq.columns, None, None, cq.root_field)
        group_rows = shaped.get("data", {}).get(cq.root_field, [])
        if protocol == "jsonapi":
            return {
                "data": [
                    {"type": f"{table_name}GroupBy", "id": str(idx), "attributes": row}
                    for idx, row in enumerate(group_rows)
                ]
            }
        return {"data": group_rows}

    shaped = serialize_aggregate(
        result.rows, cq.columns, None, None, cq.root_field, agg_alias=cq.agg_alias
    )
    agg_payload = shaped.get("data", {}).get(cq.root_field, {}).get(cq.agg_alias, {})
    if protocol == "jsonapi":
        return {"data": None, "meta": {"aggregate": agg_payload}}
    return {"data": agg_payload}


def _expand_views(sql: str, app_state: Any) -> str:
    """Inline-expand __derived__ view refs before transpile/execute (REQ-135/REQ-1163).

    Mirrors the pgwire SQL path (provisa/pgwire/_pipeline.py) and the REST/GraphQL
    path (provisa/api/data/endpoint.py's _prepare_compiled) — without this, a
    __derived__-sourced table survives rewrite_semantic_to_catalog_physical as a
    literal "__derived__" catalog reference the engine has no such catalog for.
    """
    view_sql_map = getattr(app_state, "view_sql_map", None)
    if not view_sql_map:
        return sql
    from provisa.compiler.view_expand import expand_view_refs

    return expand_view_refs(sql, view_sql_map)


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
