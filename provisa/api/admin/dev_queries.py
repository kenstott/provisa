# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1a2b4c-5d6e-7f8a-9b0c-1d2e3f4a5b6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Business logic for compile and submit developer operations (admin GQL mutations)."""

# Requirements: REQ-001, REQ-002, REQ-007, REQ-009, REQ-040, REQ-041, REQ-066, REQ-067, REQ-262, REQ-263, REQ-345, REQ-347, REQ-478, REQ-554

from __future__ import annotations

from provisa.api.role_surface import role_surface

import logging
from dataclasses import dataclass
from typing import Any


log = logging.getLogger(__name__)


@dataclass
class EnforcementMetadata:
    rls_filters_applied: list[str]
    columns_excluded: list[str]
    schema_scope: str
    masking_applied: list[str]
    ceiling_applied: int | None
    route: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _merge_nodes_sql(group_by_sql: str, nodes_sql: str, nodes_columns: list) -> str | None:
    """Join group-by rows against nodes_sql, pre-aggregated per group into json_agg(nodes).

    nodes_sql already resolves relationship fields to correlated JSON subqueries
    (aliased to their field name); inlining nodes_columns as bare columns on the
    group-by table breaks for those fields, so nodes must be aggregated from
    nodes_sql itself rather than reconstructed from column names.
    """
    node_cols = [c for c in nodes_columns if c.nested_in is None]
    join_key_cols = [c for c in nodes_columns if c.nested_in == "__join_key__"]
    if not node_cols or not join_key_cols:
        return None
    # SQL-standard json_object(KEY k VALUE v) — the same spelling sql_selection.py emits — so
    # sqlglot transpiles it per engine. Postgres json_build_object reaches DuckDB unrewritten.
    kv = ", ".join(f"KEY '{c.field_name}' VALUE n.\"{c.column}\"" for c in node_cols)
    key_select = ", ".join(f'n."{c.column}"' for c in join_key_cols)
    join_cond = " AND ".join(f'g."{c.column}" = nodes_agg."{c.column}"' for c in join_key_cols)
    nodes_agg_sql = (
        f"SELECT {key_select}, json_agg(json_object({kv})) AS nodes\n"
        f"FROM (\n  {nodes_sql}\n) n\n"
        f"GROUP BY {key_select}"
    )
    return (
        f"SELECT g.*, nodes_agg.nodes\n"
        f"FROM (\n  {group_by_sql}\n) g\n"
        f"JOIN (\n  {nodes_agg_sql}\n) nodes_agg ON {join_cond}"
    )


def _merge_nodes_sql_denormalized(
    group_by_sql: str, nodes_sql: str, nodes_columns: list
) -> str | None:
    """Return a JOIN query that denormalizes group-by rows with their matching nodes."""
    node_cols = [c for c in nodes_columns if c.nested_in is None]
    join_key_cols = [c for c in nodes_columns if c.nested_in == "__join_key__"]
    if not node_cols or not join_key_cols:
        return None
    node_selects = ", ".join(f'n."{c.column}"' for c in node_cols)
    join_cond = " AND ".join(f'n."{c.column}" = g."{c.column}"' for c in join_key_cols)
    return (
        f"SELECT g.*, {node_selects}\n"
        f"FROM (\n  {group_by_sql}\n) g\n"
        f"JOIN (\n  {nodes_sql}\n) n ON {join_cond}"
    )


def _build_enforcement_metadata(  # REQ-040, REQ-041, REQ-263
    compiled, ctx, rls, masking_rules: dict, role_id: str, route_value: str
) -> EnforcementMetadata:
    rls_filters: list[str] = []
    root_table = ctx.tables.get(compiled.root_field)
    if root_table and root_table.table_id in rls.rules:
        rls_filters.append(rls.rules[root_table.table_id])
    for (type_name, _), join_meta in ctx.joins.items():
        if root_table and type_name == root_table.type_name:
            if join_meta.target.table_id in rls.rules:
                rls_filters.append(rls.rules[join_meta.target.table_id])

    compiled_column_names = {c.column for c in compiled.columns}
    excluded: list[str] = []
    if root_table:
        for col_name in (c.column for c in getattr(root_table, "columns", [])):
            if col_name not in compiled_column_names:
                excluded.append(f"{root_table.table_name}.{col_name}")

    masking_applied: list[str] = []
    for (table_id, r_id), col_map in masking_rules.items():
        if r_id != role_id:
            continue
        table_name = ""
        for meta in ctx.tables.values():
            if meta.table_id == table_id:
                table_name = meta.table_name
                break
        for col_name, (rule, _) in col_map.items():
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _parse_directives(query: str) -> tuple[Any, str]:
    """Return (directives, sql_comment_prefix) for a GraphQL query string."""
    from provisa.compiler.directives import (
        extract_directives,
        extract_directives_from_sql_comments,
        merge_directives,
    )
    from provisa.compiler.hints import graphql_comments_to_sql
    from graphql import parse as gql_parse_raw

    _comment_directives = extract_directives_from_sql_comments(query)
    try:
        _ast_directives = extract_directives(gql_parse_raw(query))
    except Exception:
        _ast_directives = _comment_directives.__class__()
    directives = merge_directives(_comment_directives, _ast_directives)
    sql_comment_prefix = graphql_comments_to_sql(query)
    return directives, sql_comment_prefix


async def _apply_pipeline_transforms(  # REQ-040, REQ-041, REQ-134, REQ-198, REQ-262, REQ-263
    compiled, ctx, role_id: str, fresh_mvs, state
) -> tuple[Any, bool]:
    """Govern, then the MV rewrite and Kafka filters. Returns (compiled, mv_applied).

    Governed by the compiled pipeline's own stage (pgwire._pipeline ``_govern_compiled``), the one
    the statement runs through: its views expanded, its validation, then row filters, masks
    wherever a column is referenced (a join on a masked key included, REQ-740), hidden columns and
    the row cap -- so the SQL shown is the SQL that runs, refused where the run would be."""
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical
    from provisa.core.request_context import current_session_vars
    from provisa.mv.view_reinject import reinject
    from provisa.pgwire._pipeline import _govern_compiled

    governed = await _govern_compiled(
        compiled.sql, role_id, state, current_session_vars.get(), sdl_joins=True
    )
    compiled.sql = governed.governed_sql

    # The view the pipeline's optimize stage would read in place of the governed statement
    # (pgwire._pipeline._view_answering): shown as the view's catalog among the sources.
    hit = reinject(lower_to_catalog_physical(compiled.sql, ctx), fresh_mvs, ctx)
    mv_applied = hit is not None
    if hit is not None:
        compiled.sources = {hit.mv.target_catalog}

    if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
        from provisa.kafka.window import inject_kafka_filters

        compiled = inject_kafka_filters(
            compiled, ctx, state.source_types, state.kafka_table_configs
        )

    return compiled, mv_applied


def _decide_transpile(  # REQ-066, REQ-067, REQ-068, REQ-152, REQ-229, REQ-030
    compiled, state, steward_hint: str | None, operator_floor: dict[str, str]
) -> tuple[Any, str | None, str | None, str]:
    """Return (decision, engine_sql, direct_sql, route_str)."""
    from provisa.fakes.read_sql import reads_fakes
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile

    has_json_extract = "->>" in compiled.sql
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=steward_hint,
        statement=compiled.sql,
        has_json_extract=has_json_extract,
        source_dsns=getattr(state, "source_dsns", None),
        operator_floor=operator_floor,
        reads_fakes=reads_fakes(compiled.sql),  # REQ-1494
    )

    engine_sql = (
        state.federation_engine.transpile_physical(compiled.sql)
        if decision.route == Route.ENGINE
        else None
    )
    direct_sql = (
        transpile(compiled.sql, decision.dialect)
        if decision.route == Route.DIRECT and decision.dialect
        else None
    )

    route_str = decision.route.value
    if decision.route == Route.DIRECT and decision.dialect:
        route_str = f"direct:{decision.dialect}"

    return decision, engine_sql, direct_sql, route_str


def _build_optimizations_and_warnings(
    compiled,
    pre_mv_sources: set,
    mv_applied: bool,
    directives,
    decision,
    sampling: bool,
) -> tuple[list[str], list[str]]:
    """Return (optimizations, warnings) lists."""
    from provisa.transpiler.router import Route

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
            warnings.append("route=direct ignored: source has no direct driver")
    for k, v in directives.to_session_props().items():
        optimizations.append(f"Federation hint: {k}={v} (via @provisa directive)")
    if sampling:
        optimizations.append("Sampling applied (role lacks FULL_RESULTS capability)")
    return optimizations, warnings


def _compile_cypher_for_result(  # REQ-345, REQ-347, REQ-349, REQ-350, REQ-351, REQ-2135
    ctx,
    state,
    role,
    document,
    effective_variables: dict | None,
    flat_cypher: bool,
    node_only_cypher: bool,
    root_index: int,
) -> tuple[str | None, str | None]:
    """Return (compiled_cypher, cypher_error) for the query's root field at ``root_index``: the
    read lowered to the graph IR and printed, each root field its own statement. The pane's
    options are what it projects: its nodes and relationships alone (node_only), its fields under
    the graph's names (flat), or its answer as GraphQL shapes it. A read the front end does not
    lower says what, by name."""
    from provisa.cypher.printer import CypherPrintRefused, print_cypher
    from provisa.gql.label_map import CypherLabelMap
    from provisa.graphql_front.lower import (
        FIELDS,
        GRAPHQL,
        NODES,
        GraphQLLoweringRefused,
        lower_root,
    )

    # A missing role would silently grant unrestricted domain_access — require it.
    if role is None:
        raise ValueError("role is required to compile Cypher (domain_access unknown)")

    _cache = getattr(state, "schema_build_cache", {})
    label_map = CypherLabelMap.from_schema(
        ctx,
        domain_access=role["domain_access"],
        all_tables=_cache.get("tables"),
        all_relationships=_cache.get("relationships"),
        all_column_types=_cache.get("column_types"),
        source_catalogs=getattr(state, "source_catalogs", None),
    )
    projection = NODES if node_only_cypher else FIELDS if flat_cypher else GRAPHQL
    try:
        root = lower_root(document, root_index, ctx, label_map, effective_variables, projection)
        return print_cypher(root.statement), None
    except (GraphQLLoweringRefused, CypherPrintRefused) as e:
        return None, f"Not expressible as Cypher yet: {e}"


async def compile_query(  # REQ-001, REQ-002, REQ-007, REQ-009, REQ-038, REQ-039, REQ-262, REQ-263, REQ-266
    role_id: str,
    query: str,
    variables: dict | None,
    flat_sql: bool = False,
    flat_cypher: bool = False,
    node_only_cypher: bool = False,
) -> list[dict[str, Any]]:
    """Compile a GraphQL query → SQL. Returns list of compile result dicts."""
    from provisa.api.app import state
    from provisa.compiler.params import embed_params_comment
    from provisa.compiler.parser import (
        GraphQLValidationError,
        coerce_variable_defaults,
        parse_query,
    )
    from provisa.compiler.sql_gen import compile_query as _compile_query
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.security.rights import has_capability, Capability
    from graphql import GraphQLSyntaxError

    surface = role_surface(state, role_id)  # one snapshot: the parts are of one build
    if surface is None:
        raise ValueError(f"No schema for role {role_id!r}")

    schema = surface.schema
    ctx = surface.context
    rls = surface.rls
    role = state.roles.get(role_id)

    directives, sql_comment_prefix = _parse_directives(query)
    steward_hint = directives.steward_hint

    try:
        document = parse_query(schema, query, variables, ctx=ctx)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise ValueError(str(e))

    effective_variables = coerce_variable_defaults(document, variables)
    compiled_queries = _compile_query(document, ctx, effective_variables, flat=flat_sql)
    if not compiled_queries:
        raise ValueError("No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()
    results = []
    from provisa.federation.registry_view import operator_floor

    for root_index, _compiled_orig in enumerate(compiled_queries):
        pre_mv_sources = set(_compiled_orig.sources)
        compiled, mv_applied = await _apply_pipeline_transforms(
            _compiled_orig, ctx, role_id, fresh_mvs, state
        )

        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        decision, engine_sql, direct_sql, route_str = _decide_transpile(
            compiled, state, steward_hint, operator_floor(state, compiled.table_ids)
        )

        enforcement = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=rls,
            masking_rules=state.masking_rules,
            role_id=role_id,
            route_value=route_str,
        )

        optimizations, warnings = _build_optimizations_and_warnings(
            compiled, pre_mv_sources, mv_applied, directives, decision, sampling
        )

        raw_semantic_sql = make_semantic_sql(
            embed_params_comment(compiled.sql, compiled.params), ctx
        )
        semantic_sql_str = sql_comment_prefix + raw_semantic_sql

        nodes_semantic_sql: str | None = None
        if compiled.nodes_sql is not None:
            nodes_semantic_sql = make_semantic_sql(
                embed_params_comment(compiled.nodes_sql, compiled.nodes_params), ctx
            )

        compiled_cypher, cypher_error = _compile_cypher_for_result(
            ctx,
            state,
            role,
            document,
            effective_variables,
            flat_cypher,
            node_only_cypher,
            root_index,
        )

        has_nodes = nodes_semantic_sql is not None and bool(compiled.nodes_columns)

        if has_nodes:
            assert nodes_semantic_sql is not None
            assert compiled.nodes_columns is not None
            if not flat_sql:
                merged_sql = _merge_nodes_sql(
                    raw_semantic_sql, nodes_semantic_sql, compiled.nodes_columns
                )
            else:
                merged_sql = _merge_nodes_sql_denormalized(
                    raw_semantic_sql, nodes_semantic_sql, compiled.nodes_columns
                )
            if merged_sql:
                semantic_sql_str = sql_comment_prefix + merged_sql
            nodes_semantic_sql = None

        results.append(
            {
                "sql": compiled.sql,
                "semantic_sql": semantic_sql_str,
                "nodes_semantic_sql": nodes_semantic_sql,
                "engine_sql": engine_sql,
                "direct_sql": direct_sql,
                "route": decision.route.value,
                "route_reason": decision.reason,
                "sources": list(compiled.sources),
                "root_field": compiled.root_field,
                "canonical_field": compiled.canonical_field or compiled.root_field,
                "column_aliases": [
                    {"field_name": c.field_name, "column": c.column}
                    for c in compiled.columns
                    if c.field_name != c.column
                ],
                "enforcement": enforcement,
                "optimizations": optimizations,
                "warnings": warnings,
                "has_nodes": has_nodes,
                "compiled_cypher": compiled_cypher,
                "cypher_error": cypher_error,
            }
        )

    # One result per root field, in the query's order, each with its own Cypher statement or the
    # reason it has none: a root that can't be Cypher leaves the others theirs.
    return results
