# Copyright (c) 2026 Kenneth Stott
# Canary: e325769e-5371-4146-9573-0cc70b0e6917
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Orchestrates three parallel generation loops and writes results to job store (Phase AV, REQ-358).

Pipeline:
  1. Fetch role-scoped SDL
  2. Launch three generation_loop coroutines via asyncio.gather
  3. For each valid query: execute via executor
  4. Write branch results to job store as each completes
  5. Mark job complete when all three finish
"""

# Requirements: REQ-355, REQ-357, REQ-358, REQ-359

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, NamedTuple

from provisa.api.jsonapi.naming import physical_rel_name
from provisa.grpc.proto_gen import _to_proto_type_name
from provisa.nl.job import BranchResult, InMemoryJobStore, NlTarget, RedisJobStore
from provisa.nl.loop import (
    LLMClient,
    generation_loop,
    make_graphql_compiler,
)

if TYPE_CHECKING:
    from provisa.api.app import AppState
    from provisa.cypher.label_map import CypherLabelMap

JobStore = InMemoryJobStore | RedisJobStore

log = logging.getLogger(__name__)

_TARGETS: list[NlTarget] = ["cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"]


_AGG_EXPR_TYPES: dict[Any, str] = {}


def _agg_expr_types() -> dict[Any, str]:
    """Lazily built map of sqlglot AST node type -> AGG_FUNCS name (REQ-1361), so the funcs a
    Query{Type}Aggregate/GroupBy request restricts to can be derived from whichever functions
    the SQL branch's LLM-generated SQL actually called, instead of always requesting every
    function the schema exposes for the table."""
    if not _AGG_EXPR_TYPES:
        from sqlglot import exp

        _AGG_EXPR_TYPES.update(
            {
                exp.Count: "count",
                exp.Sum: "sum",
                exp.Avg: "avg",
                exp.Max: "max",
                exp.Min: "min",
                exp.Stddev: "stddev",
                exp.Variance: "variance",
            }
        )
    return _AGG_EXPR_TYPES


def _join_fk_column(tree: Any, target_ref: str, dim_ref: str) -> str | None:
    """Physical column on ``target_ref`` that a JOIN equates to ``dim_ref`` (e.g.
    ``inquiries.user_id`` for ``... JOIN users ON users.id = inquiries.user_id``), so a GROUP BY
    on a joined dimension column (e.g. ``users.name``) can be resolved back to the aggregate
    table's own FK column — the shape a single-table gRPC/JSON:API/OpenAPI group-by can actually
    express (REQ-1359)."""
    from sqlglot import exp

    for join in tree.args.get("joins") or []:
        on = join.args.get("on")
        if on is None:
            continue
        for eq in on.find_all(exp.EQ):
            left, right = eq.left, eq.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            if left.table == target_ref and right.table == dim_ref:
                return left.name
            if right.table == target_ref and left.table == dim_ref:
                return right.name
    return None


class AggregationPlan(NamedTuple):
    """What the gRPC/JSON:API/OpenAPI branches must express (REQ-1359/REQ-1405/REQ-1408).

    ``node_scalars`` are the base-table scalar columns a ``nodes`` sub-selection projects; it is
    populated only by the GraphQL-driven rewrite (the SQL-driven resolver has no nodes concept)
    and drives REST's ``?includeNodes=`` dot-path projection.

    ``dim_paths`` names related columns physically after a relationship field named as the schema
    exposes it (``pet.breed_name``) — the split gRPC's include= takes. ``dim_paths_api`` names both
    segments physically (``pet.breed_name``, ``breedGroup.breed_name`` → ``breed_group.breed_name``)
    for JSON:API, whose params are physical end to end (REQ-1417).
    """

    meta: Any
    group_cols: list[str]
    is_aggregate_only: bool
    funcs: list[str]
    dim_paths: list[str]
    node_scalars: list[str] = []
    dim_paths_api: list[str] = []


def _aggregation_plan_from_graphql(ctx: Any, graphql_query: str) -> AggregationPlan | None:
    """Rewrite the strict chain's GraphQL query into the protocol plan (REQ-1408).

    Strict mode has already produced a schema-validated GraphQL query, and JSON:API/REST/gRPC
    expose variants of that same governed shape, so the rewrite is mechanical: root field → table,
    the ``by:`` argument → group-by columns, the ``aggregate`` sub-selection → funcs, the ``nodes``
    sub-selection → includeNodes scalars and relationship paths. Re-deriving the plan by parsing
    the compiled SQL (``_resolve_aggregation_plan``) cannot work on this path — the merged
    group-by + nodes SQL has a subquery in its outer FROM, so no table is found and every protocol
    surface degrades to a plain row fetch of an unrelated table.

    Returns None when the query has no root field this role's schema maps to a table.
    """
    from graphql import FieldNode, OperationDefinitionNode, parse

    document = parse(graphql_query)
    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        for selection in definition.selection_set.selections:
            if not isinstance(selection, FieldNode):
                continue
            meta = ctx.tables.get(selection.name.value)
            if meta is None:
                continue
            return _plan_for_root_field(ctx, meta, selection)
    return None


def _plan_for_root_field(ctx: Any, meta: Any, field_node: Any) -> AggregationPlan:
    """Build the protocol plan for one validated GraphQL root field (REQ-1408)."""
    from graphql import EnumValueNode, FieldNode, ListValueNode, StringValueNode

    from provisa.grpc.query_ir import AGG_FUNCS

    name = field_node.name.value
    is_aggregate_only = name.endswith("_aggregate") or name.endswith("Aggregate")
    is_group_by = name.endswith("_group_by") or name.endswith("GroupBy")

    def _physical(table_id: int, gql_name: str) -> str:
        # Same idiom as the compiler's own resolution (provisa/compiler/aggregates.py): the map
        # holds an entry only when the exposed and physical names differ.
        return ctx.exposed_to_physical.get((table_id, gql_name), gql_name)

    group_cols: list[str] = []
    if is_group_by:
        for arg in field_node.arguments or []:
            if arg.name.value != "by":
                continue
            values = arg.value.values if isinstance(arg.value, ListValueNode) else [arg.value]
            for value in values:
                if isinstance(value, (EnumValueNode, StringValueNode)):
                    group_cols.append(_physical(meta.table_id, value.value))

    funcs: list[str] = []
    node_scalars: list[str] = []
    dim_paths: list[str] = []
    dim_paths_api: list[str] = []
    for sub in (field_node.selection_set.selections if field_node.selection_set else []):
        if not isinstance(sub, FieldNode):
            continue
        if sub.name.value == "aggregate" and sub.selection_set:
            requested = {
                s.name.value for s in sub.selection_set.selections if isinstance(s, FieldNode)
            }
            funcs = [fn for fn in AGG_FUNCS if fn in requested]
        elif sub.name.value == "nodes" and sub.selection_set:
            for node_sel in sub.selection_set.selections:
                if not isinstance(node_sel, FieldNode):
                    continue
                if node_sel.selection_set is None:
                    node_scalars.append(_physical(meta.table_id, node_sel.name.value))
                    continue
                # A sub-selection under nodes is a relationship field; the query is
                # schema-validated, so ctx.joins always carries its JoinMeta.
                rel = node_sel.name.value
                target = ctx.joins[(meta.type_name, rel)].target
                for rel_sel in node_sel.selection_set.selections:
                    if isinstance(rel_sel, FieldNode):
                        phys_col = _physical(target.table_id, rel_sel.name.value)
                        dim_paths.append(f"{rel}.{phys_col}")
                        dim_paths_api.append(f"{physical_rel_name(rel)}.{phys_col}")

    return AggregationPlan(
        meta, group_cols, is_aggregate_only, funcs, dim_paths, node_scalars, dim_paths_api
    )


def _resolve_aggregation_plan(
    ctx: Any, app_state: Any, semantic_sql: str
) -> AggregationPlan | None:
    """Parse the SQL branch's compiled semantic SQL to find the table and any GROUP BY the
    model settled on (REQ-1359), so gRPC/JSON:API/OpenAPI target the identical table and
    aggregation SQL/GraphQL/Cypher already resolved instead of independently guessing from the
    raw table-selection set, which can include unrelated star-schema tables (e.g. a `dim_pet`
    dimension view sorting alphabetically ahead of the `pets` table the question was about).

    Returns (TableMeta, group_by_columns, is_aggregate_only, funcs, dim_paths), or None when the
    SQL can't be parsed, its table isn't found, the GROUP BY isn't plain columns, or the table
    doesn't have the corresponding enable_aggregates/enable_group_by flag on. ``funcs``
    (REQ-1361) is the AGG_FUNCS-ordered subset of aggregate functions the SQL actually calls, so
    gRPC/JSON:API/OpenAPI can restrict their synthesized requests the same way the SQL branch
    already restricted itself — empty when no known aggregate function was found (e.g. a plain
    GROUP BY with no aggregate column), meaning "every function". ``dim_paths`` (REQ-1405) is the
    ``includeNodes`` dot-paths (e.g. ``["user.name", "user.email"]``) for any joined dimension
    table's SELECT-projected columns the SQL branch pulled in (e.g. "by user with user
    details") — empty when the SQL projects no joined-dimension columns.
    """
    import sqlglot
    from sqlglot import exp

    from provisa.compiler.naming import domain_to_sql_name, rel_field_name
    from provisa.grpc.query_ir import AGG_FUNCS

    try:
        tree = sqlglot.parse_one(semantic_sql, dialect="postgres")
    except Exception:
        return None

    # The aggregate's OWN table (the one COUNT/SUM/etc. actually reads from) — not necessarily
    # the FROM table. "count of inquiries by user" naturally joins users in for a readable name
    # (`SELECT users.name, COUNT(inquiries.id) ... FROM users JOIN inquiries ...`), but the
    # single-table group-by gRPC/JSON:API/OpenAPI (and GraphQL/Cypher) expose lives on inquiries.
    from_expr = tree.find(exp.From)
    from_table = from_expr.this if from_expr is not None else None
    joined_tables = [j.this for j in (tree.args.get("joins") or []) if isinstance(j.this, exp.Table)]
    all_tables = ([from_table] if isinstance(from_table, exp.Table) else []) + joined_tables
    if not all_tables:
        return None

    agg_node = next(
        tree.find_all(exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min, exp.Stddev, exp.Variance), None
    )
    agg_col = agg_node.find(exp.Column) if agg_node is not None else None
    tables_by_ref = {t.alias_or_name: t for t in all_tables}
    table_expr = (tables_by_ref.get(agg_col.table) if agg_col is not None and agg_col.table else None) or (
        from_table if isinstance(from_table, exp.Table) else all_tables[0]
    )
    table_name = table_expr.name
    table_ref = table_expr.alias_or_name
    schema_name = table_expr.db or ""

    meta = next(
        (
            m
            for m in ctx.tables.values()
            if m.table_name == table_name
            and (not schema_name or domain_to_sql_name(m.domain_id) == schema_name)
        ),
        None,
    )
    if meta is None:
        return None

    raw = next(
        (
            t
            for t in getattr(app_state, "tables", [])
            if t.get("table_name") == meta.table_name and t.get("domain_id") == meta.domain_id
        ),
        None,
    )
    if raw is None:
        return None

    group_exprs = tree.args.get("group")
    group_cols: list[str] = []
    dim_refs: set[str] = set()
    if group_exprs is not None:
        for e in group_exprs.expressions:
            col = e if isinstance(e, exp.Column) else e.find(exp.Column)
            if col is None:
                return None  # non-column GROUP BY (expression/ordinal) — fall back to plain fetch
            if not col.table or col.table in (table_name, table_ref):
                name = col.name
            else:
                # GROUP BY column belongs to a joined dimension table (e.g. users.name while
                # aggregating inquiries) — resolve it to the aggregate table's own FK column via
                # the JOIN condition instead of bailing to a plain fetch.
                fk_col = _join_fk_column(tree, table_ref, col.table)
                if fk_col is None:
                    return None
                name = fk_col
                dim_refs.add(col.table)
            if name not in group_cols:
                group_cols.append(name)

    # REQ-1405: SELECT-projected columns from a joined dimension table (e.g. "by user with user
    # details" pulling in users.name/users.email alongside COUNT(inquiries.id)) become
    # includeNodes dot-paths for gRPC/JSON:API/OpenAPI, the same relationship detail
    # SQL/GraphQL already resolved — a single-table group-by has no other way to express them.
    dim_paths: list[str] = []
    dim_paths_api: list[str] = []
    select_exprs = tree.args.get("expressions") or []
    dim_cols_by_ref: dict[str, list[str]] = {}
    for e in select_exprs:
        # A detail item may be a bare joined column (`users.name`) or, per the array_agg/json_agg
        # convention taught in prompt.py's SQL instructions, an aggregate wrapping several joined
        # columns (`json_agg(json_build_object('id', pets.id, 'name', pets.name, ...))`) — walk
        # the whole subtree so every joined column surfaces, not just the first one found.
        cols = [e] if isinstance(e, exp.Column) else list(e.find_all(exp.Column))
        for col in cols:
            if not col.table or col.table in (table_name, table_ref):
                continue
            if col.name not in dim_cols_by_ref.setdefault(col.table, []):
                dim_cols_by_ref[col.table].append(col.name)
            dim_refs.add(col.table)
    if dim_refs:
        joined_by_ref = {t.alias_or_name: t for t in all_tables if t.alias_or_name != table_ref}
        for dim_ref in dim_refs:
            dim_table = joined_by_ref.get(dim_ref)
            dim_cols = dim_cols_by_ref.get(dim_ref)
            if dim_table is None or not dim_cols:
                continue
            dim_meta = next(
                (m for m in ctx.tables.values() if m.table_name == dim_table.name),
                None,
            )
            if dim_meta is None:
                continue
            rel_field = rel_field_name(dim_meta.field_name, "many-to-one")
            for dim_col in dim_cols:
                dim_paths.append(f"{rel_field}.{dim_col}")
                dim_paths_api.append(f"{physical_rel_name(rel_field)}.{dim_col}")

    is_aggregate_only = not group_cols and (
        tree.find(exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min, exp.Stddev, exp.Variance)
        is not None
    )

    if not group_cols and not is_aggregate_only:
        return None  # plain SELECT — no aggregation intent, use existing raw-row behavior
    if group_cols and not raw.get("enable_group_by"):
        return None
    if is_aggregate_only and not raw.get("enable_aggregates"):
        return None

    found = {_agg_expr_types()[type(n)] for n in tree.find_all(*_agg_expr_types()) if type(n) in _agg_expr_types()}
    funcs = [fn for fn in AGG_FUNCS if fn in found]

    return AggregationPlan(
        meta, group_cols, is_aggregate_only, funcs, dim_paths, [], dim_paths_api
    )


def _generate_grpc_query(
    plan: AggregationPlan | None,
    selected_type_names: set[str],
    user_nodes: dict,
) -> tuple[str | None, str | None]:
    if plan is not None:
        # REQ-1359/REQ-1361: {Type}AggregateRequest.funcs and {Type}GroupByRequest.funcs
        # (provisa/grpc/proto_gen.py) restrict to a caller-chosen subset of aggregate functions,
        # mirroring jsonapi/openapi's aggregate=... param.
        proto_type = _to_proto_type_name(plan.meta.type_name)
        funcs_arg = f"funcs=[{', '.join(plan.funcs)}]" if plan.funcs else ""
        if plan.is_aggregate_only:
            if funcs_arg:
                return f"Query{proto_type}Aggregate({funcs_arg})", None
            return f"Query{proto_type}Aggregate", None
        if not plan.group_cols:
            return f"Query{proto_type}", None
        args = [f"by=[{', '.join(plan.group_cols)}]"]
        if funcs_arg:
            args.append(funcs_arg)
        if plan.dim_paths or plan.node_scalars:
            args.append("include_nodes=true")
        # REQ-1408: GroupByRequest.include takes the nodes projection as base scalars and
        # "rel.col" dot-paths (query_ir._include_node_fields), the same list REST carries in
        # ?includeNodes= — so the gRPC call mirrors the GraphQL nodes sub-selection column for
        # column instead of naming relations only.
        include_paths = [*plan.node_scalars, *plan.dim_paths]
        if include_paths:
            args.append(f"include=[{', '.join(include_paths)}]")
        return f"Query{proto_type}GroupBy({', '.join(args)})", None
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None:
        return None, "NOT_APPLICABLE"
    return f"Query{_to_proto_type_name(nm.type_name)}", None


def _generate_jsonapi_query(
    plan: AggregationPlan | None,
    selected_type_names: set[str],
    user_nodes: dict,
) -> tuple[str | None, str | None]:
    if plan is not None:
        base = f"/data/jsonapi/{plan.meta.domain_id}/{plan.meta.table_name}"
        aggregate_param = ",".join(plan.funcs) if plan.funcs else "true"
        if plan.is_aggregate_only:
            return f"{base}?aggregate={aggregate_param}", None
        if not plan.group_cols:
            return f"{base}?page[size]=20", None
        # REQ-1408: JSON:API's group-by handler honours includeNodes only as true/1 and takes the
        # relationship projection through its own ?include= list, whose entries are relationship
        # names or "rel.col" dot-paths (generator.py::_build_group_by_node_selection). Naming the
        # relationship alone pulls every column of the related table, so the dot-paths carry the
        # GraphQL nodes sub-selection column for column. Base scalars are implicit there.
        # Both segments are physical, matching ?groupBy= — REQ-1417 made the whole JSON:API
        # surface physical, params and emitted keys alike.
        query = f"{base}?groupBy={','.join(plan.group_cols)}&aggregate={aggregate_param}&includeNodes=true"
        if plan.dim_paths_api:
            query += f"&include={','.join(plan.dim_paths_api)}"
        return query, None
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None or nm.domain_id is None:
        return None, "NOT_APPLICABLE"
    return f"/data/jsonapi/{nm.domain_id}/{nm.table_name}?page[size]=20", None


def _generate_openapi_query(
    plan: AggregationPlan | None,
    selected_type_names: set[str],
    user_nodes: dict,
) -> tuple[str | None, str | None]:
    if plan is not None:
        base = f"GET /data/rest/{plan.meta.domain_id}/{plan.meta.table_name}"
        aggregate_param = ",".join(plan.funcs) if plan.funcs else "true"
        if plan.is_aggregate_only:
            return f"{base}?aggregate={aggregate_param}", None
        if not plan.group_cols:
            return base, None
        # REQ-1408: REST has no ?include= — its group-by handler (provisa/api/rest/generator.py)
        # reads the whole nodes projection off ?includeNodes= as a dot-path list, so scalars and
        # relation columns go into the one param.
        paths = [*plan.node_scalars, *plan.dim_paths]
        include_nodes = ",".join(paths) if paths else "true"
        return (
            f"{base}?groupBy={','.join(plan.group_cols)}&aggregate={aggregate_param}"
            f"&includeNodes={include_nodes}",
            None,
        )
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None or nm.domain_id is None:
        return None, "NOT_APPLICABLE"
    return f"GET /data/rest/{nm.domain_id}/{nm.table_name}", None


async def _resolve_llm_org_ctx(app_state: AppState) -> tuple[dict | None, dict[str, str]]:
    """Org-resolved LLM config + per-vendor API keys for the org bound to this request.

    REQ-1349 (config override) + REQ-1395/REQ-1398 (org's own vendor keys). ``app_state`` with
    no bound tenant (startup/discovery paths) resolves to (None, {}) — callers fall back to the
    deployment config / no keys, same as before this existed.
    """
    tenant_db = getattr(app_state, "tenant_db", None)
    if tenant_db is None:
        return None, {}
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config

    config = await resolve_org_config(tenant_db)
    api_keys = await read_org_api_keys(tenant_db)
    return config, api_keys


async def _generate_sql_from_nl(
    nl_query: str,
    role: str,
    app_state: AppState,
    pre_selected_types: "set[str] | None" = None,
) -> tuple[str | None, str | None]:
    """Generate semantic SQL via the proven endpoint_dev pipeline.

    Returns (sql, None) on success or (None, error_message) on failure.
    """
    from provisa.api.data.endpoint_dev import (
        _collect_nl_user_tables,
        _run_table_selection,
        _build_multihop_lines,
        _build_relevant_type_names,
        _build_schema_block,
        _run_sql_generation_loop,
    )
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.stage2 import build_governance_context
    from provisa.compiler.rls import RLSContext

    ctx = getattr(app_state, "contexts", {}).get(role)
    if ctx is None:
        return None, f"No schema context for role: {role}"

    # Missing rls_contexts attribute is a wiring bug; a role with no rules is legitimately empty.
    rls = app_state.rls_contexts.get(role, RLSContext.empty())
    role_obj = getattr(app_state, "roles", {}).get(role)
    gov_ctx = build_governance_context(
        role,
        rls,
        getattr(app_state, "masking_rules", {}),
        ctx,
        getattr(app_state, "tables", []),
        role=role_obj,
    )
    raw_tables = getattr(app_state, "tables", [])

    all_tables, user_nodes, table_name_to_type, lm = _collect_nl_user_tables(ctx)
    llm_config, llm_api_keys = await _resolve_llm_org_ctx(app_state)

    def _sql_domain(domain_id: str | None) -> str:
        return domain_to_sql_name(domain_id) if domain_id else "default"

    if pre_selected_types is not None:
        selected_types = pre_selected_types
    else:
        selected_types = await _run_table_selection(
            user_nodes,
            nl_query,
            _sql_domain,
            table_name_to_type,
            config=llm_config,
            api_keys=llm_api_keys,
        )
    multihop_lines = _build_multihop_lines(selected_types, lm, _sql_domain)
    relevant_type_names = _build_relevant_type_names(selected_types, lm)
    schema_block, table_columns = _build_schema_block(
        all_tables, relevant_type_names, ctx, _sql_domain, multihop_lines
    )

    last_sql, _, last_error = await _run_sql_generation_loop(
        nl_query,
        schema_block,
        ctx,
        gov_ctx,
        role_obj,
        raw_tables,
        table_columns,
        config=llm_config,
        api_keys=llm_api_keys,
    )

    if last_error == "NOT_APPLICABLE":
        return None, "NOT_APPLICABLE"
    if last_error:
        return last_sql or None, last_error

    # Queries are always expressed in semantic SQL with {schema}.{table} using the
    # logical domain schema (e.g. pet_store.users), never the physical schema (default).
    # The model may emit bare or physical-schema refs; normalize to physical first to
    # qualify every table, then lift to the semantic domain form for display/execution.
    # _execute_sql re-runs make_semantic_sql, so semantic input round-trips correctly.
    from provisa.compiler.sql_rewrite import make_semantic_sql, rewrite_semantic_to_physical

    semantic_sql = make_semantic_sql(rewrite_semantic_to_physical(last_sql, ctx), ctx)
    return semantic_sql, None


def _build_cypher_label_map(
    ctx: object,  # object-ok: circular-import boundary — CompilationContext lives in provisa.compiler
    role: str,
    app_state: AppState,
) -> "CypherLabelMap":
    from provisa.cypher.label_map import CypherLabelMap

    role_obj = getattr(app_state, "roles", {}).get(role) or {}
    schema_build_cache = getattr(app_state, "schema_build_cache", {})
    return CypherLabelMap.from_schema(
        ctx,
        domain_access=role_obj.get("domain_access"),
        all_tables=schema_build_cache.get("tables"),
        all_relationships=schema_build_cache.get("relationships"),
        all_column_types=schema_build_cache.get("column_types"),
        source_catalogs=getattr(app_state, "source_catalogs", None),
    )


def best_effort_cypher_for_sql(
    sql: str,
    ctx: object,  # object-ok: circular-import boundary — CompilationContext lives in provisa.compiler
    role: str,
    app_state: AppState,
) -> str | None:
    """Best-effort semantic-SQL -> Cypher translation for the direct-SQL (non-strict) path.

    Reuses the same translator as the strict (GraphQL-backed) path. There is no compiled-query
    list here with per-query result_limit/params, so the whole SQL string is passed through once.
    Returns None (not an error) when the SQL has no Cypher-representable pattern — the SQL result
    is still valid and useful on its own.
    """
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher

    label_map = _build_cypher_label_map(ctx, role, app_state)
    return semantic_sql_to_cypher(sql, label_map, ctx)


async def _generate_graphql_sql_from_nl(
    nl_query: str,
    role: str,
    app_state: AppState,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Generate GraphQL, SQL, and Cypher via the GraphQL-backed pipeline ("strict" mode).

    The NL question is first compiled to a schema-validated GraphQL query, then to SQL via
    the same compiler the GraphQL Explorer uses — join reachability and aggregate/group-by
    shape come from the approved schema relationships, not prompt discipline, closing the
    class of hallucinated-join/column bugs the direct-SQL path can only catch after the fact.
    The Cypher form reuses ``semantic_sql_to_cypher`` (the same translator behind the GraphQL
    Explorer's Cypher tab) since the compiled SQL is already ARRAY_AGG-shaped (flat=False),
    which is what that translator requires as input.

    Returns (graphql, sql, cypher, None) on success or (None, None, None, error_message) on
    failure. ``cypher`` is None when the query structure has no Cypher-representable pattern
    (not itself an error).
    """
    from graphql import GraphQLSchema
    from provisa.api.admin.dev_queries import compile_query as _compile_query_governed
    from provisa.llm.client import ProvisaLLMClient

    schema = getattr(app_state, "schemas", {}).get(role)
    if not isinstance(schema, GraphQLSchema):
        return None, None, None, f"No GraphQL schema for role: {role}"

    schema_sdl = _get_schema_sdl(app_state, role)
    compiler = make_graphql_compiler(schema)
    llm_config, llm_api_keys = await _resolve_llm_org_ctx(app_state)
    llm = ProvisaLLMClient("sql_generation", config=llm_config, api_keys=llm_api_keys)

    valid_query, error = await generation_loop(
        nl_query, "graphql", schema_sdl, compiler, llm  # type: ignore[arg-type]
    )
    if error or valid_query is None:
        return None, None, None, error or "GraphQL generation failed"

    try:
        results = await _compile_query_governed(role, valid_query, {})
    except ValueError as exc:
        return valid_query, None, None, str(exc)
    if not results:
        return valid_query, None, None, "No query fields found"

    sql = "\n".join(r["semantic_sql"] for r in results)
    cypher_parts = [r["compiled_cypher"] for r in results if r.get("compiled_cypher")]
    cypher = "\n".join(cypher_parts) if cypher_parts else None

    return valid_query, sql, cypher, None


async def run_nl_job(  # REQ-355, REQ-357, REQ-358, REQ-359
    job_id: str,
    nl_query: str,
    role: str,
    app_state: AppState,
    job_store: JobStore,
    llm: LLMClient,
    strict: bool = False,
) -> None:
    """Background coroutine: runs all six generation branches, writes results.

    ``strict`` (REQ-1400) selects the generation order for the graphql/sql/cypher branches:
    strict compiles NL -> GraphQL first (schema-validated), then GraphQL -> SQL, then SQL ->
    Cypher, so join reachability and aggregate shape come from the approved schema relationships
    rather than prompt discipline. Non-strict keeps NL -> SQL and NL -> GraphQL independent
    (direct LLM generation each), and derives Cypher from the SQL branch's result (best-effort).
    grpc/jsonapi/openapi are unaffected by ``strict`` — both modes drive them off the SQL
    branch's compiled semantic SQL.
    """
    await job_store.set_state(job_id, "running")

    schema_sdl = _get_schema_sdl(app_state, role)
    graphql_schema = getattr(app_state, "schemas", {}).get(role)

    # Semantic entity matching: surface exact schema names for the LLM prompt.
    from provisa.nl.schema_matcher import get_matcher, make_embed_fn
    from provisa.nl.prompt import format_entities

    embed_fn = make_embed_fn(app_state)
    # REQ-1319: metrics visible to this role join the matcher vocabulary; REQ-1320: table
    # modeling roles ground the star shape in the prompt and metric resolution.
    _metrics = [
        m
        for m in (getattr(app_state, "metrics", {}) or {}).values()
        if "*" in m.visible_to or role in m.visible_to
    ]
    _table_roles = {
        t["table_name"]: t["modeling_role"]
        for t in getattr(app_state, "tables", [])
        if t.get("modeling_role")
    }
    matcher = await get_matcher(
        role, schema_sdl, embed_fn, metrics=_metrics, table_roles=_table_roles
    )
    query_emb: list[float] | None = None
    if embed_fn is not None:
        try:
            import asyncio as _aio

            query_emb = await _aio.get_event_loop().run_in_executor(
                None, lambda: embed_fn([nl_query])[0]
            )
        # complexity-gate: allow-ble=3 reason="[file ceiling 3] Best-effort query embedding via a pluggable embed_fn (local/remote model) with an unbounded failure taxonomy — a failure only drops entity hints (query_emb stays None) and is logged; NL processing must continue without it."
        except Exception as exc:
            log.debug("Query embedding failed: %s", exc)
    relevant_entities = format_entities(matcher.top_k(query_emb), table_roles=_table_roles)

    # A role may have no GraphQL schema. Non-strict mode never validates the GraphQL branch
    # with the Cypher compiler (that masks the absence); instead the graphql compiler is
    # omitted so the graphql branch fails independently as a per-branch error (see
    # _run_branch), leaving the other branches unaffected. Strict mode's graphql/sql/cypher
    # branches run through the shared chain below instead of this compiler map.
    compilers: dict[str, Any] = {}
    if not strict and graphql_schema is not None:
        compilers["graphql"] = make_graphql_compiler(graphql_schema)

    ctx = getattr(app_state, "contexts", {}).get(role)

    # Shared table selection for SQL and protocol-based branches (one LLM call). A failure here
    # (e.g. a missing/invalid per-org LLM API key, REQ-1395) must not crash the whole job — it is
    # captured and surfaced as the error for each branch that depends on it, the same way
    # _run_branch converts a branch-local exception into that branch's error string.
    shared_selected_types: set[str] = set()
    shared_user_nodes: dict = {}
    table_selection_error: str | None = None
    if ctx is not None:
        from provisa.api.data.endpoint_dev import _collect_nl_user_tables, _run_table_selection
        from provisa.compiler.naming import domain_to_sql_name

        _, _u_nodes, _tbl_to_type, _ = _collect_nl_user_tables(ctx)

        def _sql_dom(d: str | None) -> str:
            return domain_to_sql_name(d) if d else "default"

        _shared_llm_config, _shared_llm_api_keys = await _resolve_llm_org_ctx(app_state)
        try:
            shared_selected_types = await _run_table_selection(
                _u_nodes,
                nl_query,
                _sql_dom,
                _tbl_to_type,
                config=_shared_llm_config,
                api_keys=_shared_llm_api_keys,
            )
        # complexity-gate: allow-ble=3 reason="[file ceiling 3] Shared table-selection LLM call has
        # an unbounded failure taxonomy (auth, rate limit, network); a failure must become each
        # dependent branch's error string, not abort the whole NL job."
        except Exception as exc:
            log.warning("NL shared table selection failed: %s", exc)
            table_selection_error = str(exc)
        shared_user_nodes = _u_nodes

    _QUERY_TARGETS = {"cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"}

    # Strict mode (REQ-1400): graphql/sql/cypher come from a single NL -> GraphQL -> SQL ->
    # Cypher chain, run once and shared by all three branches, instead of being generated
    # independently. asyncio.Task caches its result, so awaiting it from three branches only
    # runs the chain once.
    strict_chain_task: "asyncio.Task[tuple[str | None, str | None, str | None, str | None]] | None" = None
    if strict:

        async def _run_strict_chain() -> tuple[str | None, str | None, str | None, str | None]:
            try:
                return await _generate_graphql_sql_from_nl(nl_query, role, app_state)
            # complexity-gate: allow-ble=3 reason="[file ceiling 3] The strict chain runs an
            # LLM + compiler pipeline with an unbounded failure surface; a failure must become
            # the shared error surfaced to the graphql/sql/cypher branches, not crash the job."
            except Exception as exc:
                log.warning("NL strict chain failed: %s", exc)
                return None, None, None, str(exc)

        strict_chain_task = asyncio.create_task(_run_strict_chain())

    async def _run_branch(target: NlTarget) -> tuple[NlTarget, str | None, str | None]:
        # Each branch is independent: a failure in one (e.g. SQL generation
        # raising) must not abort the asyncio.as_completed loop and discard the
        # other branches' results. Convert any exception into a branch error.
        try:
            if strict and target in ("graphql", "sql", "cypher") and strict_chain_task is not None:
                graphql_query, chain_sql, chain_cypher, chain_error = await strict_chain_task
                if target == "graphql":
                    return target, graphql_query, chain_error if graphql_query is None else None
                if target == "sql":
                    return target, chain_sql, chain_error if chain_sql is None else None
                return target, chain_cypher, chain_error if chain_sql is None else None
            if target == "sql":
                # REQ-1319: a metric-shaped question resolves to the governed semantic
                # form (metric + matched dimension columns) instead of asking the model
                # to invent a raw aggregation.
                metric_sql = matcher.resolve_metric(nl_query)
                if metric_sql is not None:
                    return target, metric_sql, None
                if table_selection_error is not None:
                    return target, None, table_selection_error
                valid_query, error = await _generate_sql_from_nl(
                    nl_query, role, app_state, pre_selected_types=shared_selected_types
                )
                return target, valid_query, error
            if target == "cypher":
                # Non-strict: derive Cypher from the SQL branch's result (best-effort,
                # sql-to-cypher) instead of generating it independently from NL.
                if table_selection_error is not None:
                    return target, None, table_selection_error
                _, sql_query, sql_error = await sql_task
                if sql_query is None or sql_error is not None:
                    return target, None, sql_error
                if ctx is None:
                    return target, None, f"No schema context for role: {role}"
                cypher_query = best_effort_cypher_for_sql(sql_query, ctx, role, app_state)
                return target, cypher_query, None
            if target in ("grpc", "jsonapi", "openapi"):
                # REQ-1359: wait for the SQL branch's compiled semantic SQL so these three
                # protocol surfaces target the identical table/GROUP BY it resolved to,
                # instead of independently guessing from the raw table-selection set.
                if table_selection_error is not None:
                    return target, None, table_selection_error
                plan = None
                # REQ-1408: these three surfaces expose variants of the GraphQL shape, so the plan
                # is a mechanical rewrite of the GraphQL branch's own query — strict or not. The
                # SQL-derived plan cannot express it: a query whose GraphQL carries a nodes
                # sub-selection compiles to SQL with a subquery in its outer FROM, so no table
                # resolves and every surface degrades to a plain row fetch of another table.
                _, graphql_query, graphql_error = await graphql_task
                if graphql_query is not None and graphql_error is None and ctx is not None:
                    plan = _aggregation_plan_from_graphql(ctx, graphql_query)
                if plan is None:
                    _, sql_query, sql_error = await sql_task
                    if sql_query is not None and sql_error is None and ctx is not None:
                        plan = _resolve_aggregation_plan(ctx, app_state, sql_query)
                gen_fn = {
                    "grpc": _generate_grpc_query,
                    "jsonapi": _generate_jsonapi_query,
                    "openapi": _generate_openapi_query,
                }[target]
                q, e = gen_fn(plan, shared_selected_types, shared_user_nodes)
                return target, q, e
            compiler = compilers.get(target)  # type: ignore[arg-type]
            if compiler is None:
                return target, None, f"No GraphQL schema for role: {role}"
            valid_query, error = await generation_loop(
                nl_query,
                target,
                schema_sdl,
                compiler,
                llm,
                relevant_entities=relevant_entities,  # type: ignore[arg-type]
            )
            return target, valid_query, error
        # complexity-gate: allow-ble=3 reason="[file ceiling 3] Per-branch NL compilation boundary: each target (graphql/cypher/…) runs an LLM + compiler pipeline with an unbounded failure surface; a failing branch must be captured as that branch's error string and returned so sibling branches still complete — never abort the whole NL run."
        except Exception as exc:
            log.warning("NL branch %s failed: %s", target, exc)
            return target, None, str(exc)

    # The sql and graphql branches must be scheduled before grpc/jsonapi/openapi (and,
    # non-strict, cypher) so those can await the compiled semantic SQL and the GraphQL query
    # they rewrite (see _run_branch) without a forward reference.
    sql_task = asyncio.create_task(_run_branch("sql"))
    graphql_task = asyncio.create_task(_run_branch("graphql"))
    branch_tasks = [sql_task, graphql_task] + [
        asyncio.create_task(_run_branch(t)) for t in _TARGETS if t not in ("sql", "graphql")
    ]

    from provisa.nl.executor import execute as _execute

    for coro in asyncio.as_completed(branch_tasks):
        target, valid_query, error = await coro
        result = None
        if valid_query is not None and error is None and target in _QUERY_TARGETS:
            try:
                result = await _execute(valid_query, target, role, app_state)  # type: ignore[arg-type]
            # complexity-gate: allow-ble=3 reason="[file ceiling 3] Per-branch NL execution boundary: running a generated query against the pluggable engine has an unbounded failure surface; a failure is captured as this branch's error (valid_query kept so the UI shows the query alongside it) and must not abort the other branches' execution."
            except Exception as exc:
                error = str(exc)
                # Keep valid_query so the UI can show the generated query alongside the error
        await job_store.update_branch(
            job_id, target, BranchResult(query=valid_query, result=result, error=error)
        )
        log.debug("Branch %s complete: valid=%s", target, valid_query is not None)

    await job_store.set_state(job_id, "complete")


def _get_schema_sdl(app_state: AppState, role: str) -> str:
    """Return role-scoped GraphQL SDL string, or empty string if unavailable."""
    schema = getattr(app_state, "schemas", {}).get(role)
    if schema is None:
        return ""
    from graphql import print_schema

    return print_schema(schema)
