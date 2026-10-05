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
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from provisa.executor.redirect import Delivery

log = logging.getLogger(__name__)

NlTarget = Literal["cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"]


class _Delivered(Exception):
    """A branch's statement was delivered to the results store (REQ-1194, REQ-1224): there are no
    rows to shape, and the handle is the branch's answer. Raised by the statement's runner and
    answered by :func:`execute`."""

    def __init__(self, handle: dict) -> None:
        super().__init__("delivered to the results store")
        self.handle = handle


async def execute(
    query: str, target: NlTarget, role: str, app_state: Any, *, deliver: "Delivery | None"
) -> Any:  # REQ-357, REQ-359
    """Execute a validated query and return raw result.

    Args:
        query: Validated query string (Cypher, GraphQL, or SQL).
        target: Query language.
        role: Role string for authorization/compilation context.
        app_state: AppState instance.
        deliver: the delivery the NL request forced (REQ-1194), or None. An NL result is buffered,
            so the operator's threshold also delivers one over it (REQ-1224).

    Returns:
        Result dict with {"columns", "rows"} for Cypher/SQL or
        {"data"} for GraphQL; ``{"redirect": handle}`` for a result delivered to the results
        store.

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
    try:
        return await fn(query, role, app_state, deliver)
    except _Delivered as delivered:
        # REQ-1194/REQ-1224: the branch's result went to the results store; its handle is the
        # branch's answer (there are no rows to shape).
        return {"redirect": delivered.handle}


async def _execute_cypher(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> dict:
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.cypher.params import collect_param_names, bind_params
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    ctx = _get_ctx(app_state, role)
    ast = parse_cypher(query)
    from provisa.security.rights import require_role

    label_map = CypherLabelMap.from_schema(
        ctx, domain_access=require_role(app_state.roles, role)["domain_access"]
    )
    param_names = collect_param_names(query)
    bind_params(param_names, {})
    sql_ast, _, graph_vars = cypher_to_sql(ast, label_map, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    # Render to postgres SQL; make_semantic_sql handles catalog-qualified refs
    sql_str = sql_ast.sql(dialect="postgres")
    semantic_sql = make_semantic_sql(sql_str, ctx)

    # Route through the ONE compiled pipeline (_govern_and_route_compiled → _execute_plan) —
    # same entrypoint the real Bolt/Cypher session uses (provisa/bolt/session.py) — so governance,
    # API-table hydration/materialization, and cache rewrites all apply exactly as they do there.
    plan = await _govern_and_route_compiled(
        semantic_sql,
        role,
        state=app_state,
        deliver=deliver,
        buffered=True,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=True,
    )
    result = await _execute_plan(plan, app_state)
    if result.redirect is not None:
        raise _Delivered(result.redirect)
    rows = [dict(zip(result.column_names, row)) for row in result.rows]
    assembled = assemble_rows(rows, graph_vars)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": [to_serializable(r) for r in assembled]}


async def _compile_and_execute_graphql(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> list[tuple]:
    """Compile ``query`` and run each root field through the ONE compiled pipeline
    (``_govern_and_route_compiled`` → ``_execute_plan``), returning the raw (compiled_query, result,
    nodes_result) tuples — the step both ``_execute_graphql`` (GraphQL-shaped merge) and the
    gRPC/JSON:API/OpenAPI aggregate branches (protocol-shaped envelopes, REQ-1359) build on.
    Governance, API-table hydration, the response cache and the terminal are the pipeline's, exactly
    as for REST and JSON:API reading the same compiled SQL."""
    from graphql import GraphQLSchema

    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

    schema = app_state.schemas.get(role)
    if not isinstance(schema, GraphQLSchema):
        raise RuntimeError(f"No GraphQL schema for role: {role}")
    ctx = _get_ctx(app_state, role)
    document = parse_query(schema, query, {}, ctx=ctx)
    compiled_queries = compile_query(document, ctx, {})
    if not compiled_queries:
        raise RuntimeError("No query fields found")

    out: list[tuple] = []
    for cq in compiled_queries:
        plan = await _govern_and_route_compiled(
            cq.sql,
            role,
            exec_params=cq.params or None,
            state=app_state,
            deliver=deliver,
            # An aggregate's answer is always small: only a row read is landed on the threshold.
            buffered=cq.nodes_sql is None,
            cache_hint=NO_CACHE_HINT,
            sdl_joins=True,
        )
        result = await _execute_plan(plan, app_state)
        if result.redirect is not None:
            raise _Delivered(result.redirect)
        nodes_result = None
        if cq.nodes_sql is not None:
            nodes_plan = await _govern_and_route_compiled(
                cq.nodes_sql,
                role,
                exec_params=cq.nodes_params or None,
                state=app_state,
                cache_hint=NO_CACHE_HINT,
                sdl_joins=True,
            )
            nodes_result = await _execute_plan(nodes_plan, app_state)
        out.append((cq, result, nodes_result))
    return out


async def _run_single_compiled_graphql(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> tuple:
    """The single-root-field case (gRPC/JSON:API/OpenAPI aggregate/group-by NL query text always
    targets exactly one root field) — returns (compiled_query, result, nodes_result)."""
    return (await _compile_and_execute_graphql(query, role, app_state, deliver))[0]


async def _execute_graphql(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> dict:
    from provisa.executor.serialize import serialize_aggregate, serialize_rows

    compiled_results = await _compile_and_execute_graphql(query, role, app_state, deliver)

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


async def _execute_sql(query: str, role: str, app_state: Any, deliver: "Delivery | None") -> dict:
    # Route through the ONE raw-SQL pipeline (execute_sql_batch → _govern_and_route → _execute_plan)
    # — the exact entrypoint /data/sql uses — so governance, API-table hydration/materialization,
    # and cache rewrites all apply exactly as they do for Explore/SQL running the same query text.
    from provisa.pgwire._pipeline import execute_sql_batch

    result = await execute_sql_batch(query, role, app_state, deliver=deliver, buffered=True)
    if result.redirect is not None:
        raise _Delivered(result.redirect)
    rows = [dict(zip(result.column_names, row)) for row in result.rows]
    columns = list(result.column_names)
    return {"columns": columns, "rows": rows}


async def _execute_grpc(query: str, role: str, app_state: Any, deliver: "Delivery | None") -> dict:
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
    m = re.match(
        r"^Query(.+)GroupBy\(by=\[(.*?)\](?:, funcs=\[(.*?)\])?"
        r"(?:, include_nodes=true, include=\[(.*?)\])?\)$",
        query,
    )
    if m is not None:
        type_name = m.group(1)
        by_columns = [c.strip() for c in m.group(2).split(",") if c.strip()]
        funcs = [c.strip() for c in m.group(3).split(",") if c.strip()] if m.group(3) else None
        include_nodes = m.group(4) is not None
        include = [c.strip() for c in m.group(4).split(",") if c.strip()] if m.group(4) else None
        graphql_text = _grpc_group_by_graphql_text(
            ctx, type_name, by_columns, funcs, include_nodes, include
        )
        if graphql_text is None:
            raise RuntimeError(f"No table matches gRPC type: {type_name}")
        cq, result, nodes_result = await _run_single_compiled_graphql(
            graphql_text, role, app_state, deliver
        )
        group_key_cols, group_key_idx, agg_cols, agg_idx = split_group_by_columns(cq.columns)
        nodes_by_key: dict = {}
        if include_nodes and nodes_result is not None and cq.nodes_columns is not None:
            join_key_indices = [
                i for i, c in enumerate(cq.nodes_columns) if c.nested_in == "__join_key__"
            ]
            output_cols = [(i, c) for i, c in enumerate(cq.nodes_columns) if c.nested_in is None]
            for nrow in nodes_result.rows:
                jkey = tuple(nrow[i] for i in join_key_indices)
                nodes_by_key.setdefault(jkey, []).append(
                    {c.field_name: nrow[i] for i, c in output_cols}
                )
        rows = []
        for row in result.rows:
            group_key = {c.column: row[i] for c, i in zip(group_key_cols, group_key_idx)}
            agg_row = tuple(row[i] for i in agg_idx)
            top, nested = split_agg_columns(agg_cols, agg_row)
            out_row = {
                "group_key": json.dumps(group_key, default=str),
                "aggregate": {**top, **nested},
            }
            if include_nodes:
                jkey = tuple(row[i] for i in group_key_idx)
                out_row["nodes"] = nodes_by_key.get(jkey, [])
            rows.append(out_row)
        return {"group_by": rows}

    m = re.match(r"^Query(.+)Aggregate(?:\(funcs=\[(.*)\]\))?$", query)
    if m is not None:
        type_name = m.group(1)
        funcs = [c.strip() for c in m.group(2).split(",") if c.strip()] if m.group(2) else None
        graphql_text = _grpc_aggregate_graphql_text(ctx, type_name, funcs)
        if graphql_text is None:
            raise RuntimeError(f"No table matches gRPC type: {type_name}")
        cq, result, _ = await _run_single_compiled_graphql(graphql_text, role, app_state, deliver)
        row = result.rows[0] if result.rows else ()
        top, nested = split_agg_columns(cq.columns, row)
        return {**top, **nested}

    type_name = query[len("Query") :] if query.startswith("Query") else query
    semantic = grpc_table_to_semantic_sql(ctx, type_name, limit=20)
    if semantic is None:
        raise RuntimeError(f"No table matches gRPC type: {type_name}")
    sql, _no_filter_params = semantic  # no filter is passed, so nothing is bound
    return await _execute_sql(sql, role, app_state, deliver)


def _grpc_aggregate_graphql_text(
    ctx: Any, type_name: str, funcs: list[str] | None = None
) -> str | None:
    from provisa.grpc.query_ir import grpc_table_to_aggregate_graphql_text

    return grpc_table_to_aggregate_graphql_text(ctx, type_name, funcs)


def _grpc_group_by_graphql_text(
    ctx: Any,
    type_name: str,
    by_columns: list[str],
    funcs: list[str] | None = None,
    include_nodes: bool = False,
    include: list[str] | None = None,
) -> str | None:
    from provisa.grpc.query_ir import grpc_table_to_group_by_graphql_text

    return grpc_table_to_group_by_graphql_text(
        ctx, type_name, by_columns, funcs, include_nodes, include
    )


def _parse_aggregate_funcs(raw: str) -> list[str] | None:
    """``aggregate=true`` means every function; ``aggregate=count,sum`` (REQ-1361) restricts to
    that subset — mirrors JSON:API/REST's own ``_parse_aggregate_param`` convention."""
    return None if raw == "true" else [f.strip() for f in raw.split(",") if f.strip()]


def _has_include_nodes(query: str) -> bool:
    return re.search(r"[?&]includeNodes=[^&]+(&|$)", query) is not None


def _include_relations(query: str) -> list[str]:
    """What the URL's ``nodes`` projection selects (REQ-1408).

    The two surfaces express it differently: JSON:API carries relationships in its own
    ``?include=rel1,rel2`` (its ``includeNodes`` is a true/1 flag), while REST has no ``include``
    and puts the whole projection into ``?includeNodes=`` as dot-paths (``id,user.email``). Both
    forms are what ``grpc_table_to_group_by_graphql_text``'s ``include`` already accepts
    (query_ir._include_node_fields), so the list passes through unchanged.
    """
    m = re.search(r"[?&]include=([^&]+)(&|$)", query)
    if m is None:
        m = re.search(r"[?&]includeNodes=([^&]+)(&|$)", query)
    if m is None or m.group(1) in ("true", "1"):
        return []
    return [p.strip() for p in m.group(1).split(",") if p.strip()]


async def _execute_jsonapi(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> dict:
    # REQ-1361: _generate_jsonapi_query appends `&includeNodes=true` to every group-by URL, so
    # the groupBy+aggregate match must tolerate (and act on) that trailing param instead of
    # anchoring `$` right after `aggregate=...` — an anchored match falls through to the plain
    # `/data/jsonapi/{domain}/{table}` case below and silently drops the aggregation.
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)\?groupBy=([^&]+)&aggregate=([^&]+)", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            m.group(3).split(","),
            role,
            app_state,
            "jsonapi",
            _parse_aggregate_funcs(m.group(4)),
            _has_include_nodes(query),
            _include_relations(query),
            deliver=deliver,
        )
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)\?aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            [],
            role,
            app_state,
            "jsonapi",
            _parse_aggregate_funcs(m.group(3)),
            deliver=deliver,
        )
    m = re.match(r"^/data/jsonapi/([^/]+)/([^/?]+)", query)
    if m is None:
        raise RuntimeError(f"Malformed jsonapi query: {query}")
    return await _execute_domain_table(m.group(1), m.group(2), role, app_state, deliver)


async def _execute_openapi(
    query: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> dict:
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)\?groupBy=([^&]+)&aggregate=([^&]+)", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            m.group(3).split(","),
            role,
            app_state,
            "openapi",
            _parse_aggregate_funcs(m.group(4)),
            _has_include_nodes(query),
            _include_relations(query),
            deliver=deliver,
        )
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)\?aggregate=([^&]+)$", query)
    if m is not None:
        return await _execute_domain_table_aggregate(
            m.group(1),
            m.group(2),
            [],
            role,
            app_state,
            "openapi",
            _parse_aggregate_funcs(m.group(3)),
            deliver=deliver,
        )
    m = re.match(r"^GET /data/rest/([^/]+)/([^/?]+)", query)
    if m is None:
        raise RuntimeError(f"Malformed openapi query: {query}")
    return await _execute_domain_table(m.group(1), m.group(2), role, app_state, deliver)


async def _execute_domain_table(
    domain_id: str, table_name: str, role: str, app_state: Any, deliver: "Delivery | None"
) -> dict:
    from provisa.compiler.sql_gen import _q
    from provisa.compiler.sql_rewrite import semantic_ref

    ctx = _get_ctx(app_state, role)
    meta = next(
        (m for m in ctx.tables.values() if m.domain_id == domain_id and m.table_name == table_name),
        None,
    )
    if meta is None:
        raise RuntimeError(f"No table matches {domain_id}/{table_name}")
    cols = ", ".join(_q(c) for c, _t in ctx.aggregate_columns.get(meta.table_id, [])) or "*"
    sql = f"SELECT {cols} FROM {semantic_ref(meta)} LIMIT 20"
    return await _execute_sql(sql, role, app_state, deliver)


def _physical_include_path(ctx: Any, meta: Any, entry: str) -> str:
    """One ``?include=`` entry translated from the schema's naming to the physical one.

    ``exposed_to_physical`` carries an entry only where the two names differ, so an unrenamed
    column maps to itself. A bare relationship name (no dot) and a base-table scalar both pass
    through — neither addresses a related column.
    """
    rel, _, column = entry.partition(".")
    if not column:
        return entry
    join_meta = ctx.joins.get((meta.type_name, rel))
    if join_meta is None:
        return entry
    return f"{rel}.{ctx.exposed_to_physical.get((join_meta.target.table_id, column), column)}"


async def _execute_domain_table_aggregate(
    domain_id: str,
    table_name: str,
    by_columns: list[str],
    role: str,
    app_state: Any,
    protocol: Literal["jsonapi", "openapi"],
    funcs: list[str] | None = None,
    include_nodes: bool = False,
    include: list[str] | None = None,
    *,
    deliver: "Delivery | None",
) -> dict:
    """REQ-1359: JSON:API/REST aggregate|groupBy params, routed through the same compile_query
    pipeline GraphQL/gRPC aggregate queries use (grpc_table_to_*_graphql_text), then shaped into
    each protocol's actual response envelope — JSON:API's ``{"data": null, "meta": {"aggregate":
    ...}}``/typed-resourceless-row shape (provisa/api/jsonapi/generator.py:588-615) or REST's flat
    ``{"data": ...}`` shape (provisa/api/rest/generator.py:479-497) — instead of returning
    GraphQL's raw ``{"data": {root_field: ...}}`` body unmodified.

    ``include_nodes`` (REQ-1361/REQ-1401) mirrors ``?includeNodes=true`` — the per-group
    row-level detail JSON:API/REST's real group-by endpoints attach via a ``nodes { ... }``
    sub-selection."""
    from provisa.executor.serialize import serialize_aggregate, serialize_group_by

    ctx = _get_ctx(app_state, role)
    meta = next(
        (m for m in ctx.tables.values() if m.domain_id == domain_id and m.table_name == table_name),
        None,
    )
    if meta is None:
        raise RuntimeError(f"No table matches {domain_id}/{table_name}")
    if by_columns:
        if protocol == "jsonapi" and include:
            # JSON:API's ?include= names related columns as the schema exposes them
            # (jsonapi/generator.py::_build_group_by_node_selection validates against the
            # relationship's GraphQL fields), while the group-by GraphQL synthesis takes the
            # physical names gRPC and REST use. Translate, or every renamed column silently
            # drops out of the nodes selection here.
            include = [_physical_include_path(ctx, meta, entry) for entry in include]
        graphql_text = _grpc_group_by_graphql_text(
            ctx, meta.type_name, by_columns, funcs, include_nodes, include
        )
    else:
        graphql_text = _grpc_aggregate_graphql_text(ctx, meta.type_name, funcs)
    if graphql_text is None:
        raise RuntimeError(f"No aggregate/group-by fields for {domain_id}/{table_name}")

    cq, result, nodes_result = (
        await _compile_and_execute_graphql(graphql_text, role, app_state, deliver)
    )[0]

    if by_columns:
        shaped = serialize_group_by(
            result.rows,
            cq.columns,
            nodes_result.rows if nodes_result is not None else None,
            cq.nodes_columns if nodes_result is not None else None,
            cq.root_field,
        )
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


def _get_ctx(app_state: Any, role: str) -> Any:
    ctx = app_state.contexts.get(role)
    if ctx is None:
        raise RuntimeError(f"Schema not loaded for role: {role}")
    return ctx
