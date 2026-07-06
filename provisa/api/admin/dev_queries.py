# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1a2b4c-5d6e-7f8a-9b0c-1d2e3f4a5b6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Business logic for compile and submit developer operations (admin GQL mutations)."""

# Requirements: REQ-001, REQ-002, REQ-007, REQ-009, REQ-040, REQ-041, REQ-066, REQ-067, REQ-262, REQ-263, REQ-345, REQ-347, REQ-478, REQ-554

from __future__ import annotations

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


def _merge_nodes_sql(group_by_sql: str, nodes_columns: list) -> str | None:
    """Inject json_agg(json_build_object(...)) AS nodes into the GROUP BY SELECT clause."""
    node_cols = [(c.field_name, c.column) for c in nodes_columns if c.nested_in is None]
    if not node_cols:
        return None
    kv = ", ".join(f"'{fname}', \"{col}\"" for fname, col in node_cols)
    inject = f", json_agg(json_build_object({kv})) AS nodes"
    from_idx = group_by_sql.upper().find(" FROM ")
    if from_idx == -1:
        return None
    return group_by_sql[:from_idx] + inject + group_by_sql[from_idx:]


def _merge_nodes_cypher(group_by_cypher: str, nodes_columns: list) -> str | None:
    """Append collect({...}) AS nodes to the RETURN clause of a group-by Cypher query."""
    import re

    node_cols = [c for c in nodes_columns if c.nested_in is None]
    if not node_cols:
        return None
    match_line = next((ln for ln in group_by_cypher.splitlines() if "MATCH" in ln.upper()), "")
    m = re.search(r"\((\w+):", match_line)
    var = m.group(1) if m else "a"
    entries = ", ".join(f"{c.field_name}: {var}.{c.field_name}" for c in node_cols)
    collect_expr = f"collect({{{entries}}}) AS nodes"
    lines = group_by_cypher.strip().splitlines()
    merged = "\n".join(
        line.rstrip() + ", " + collect_expr if line.strip().upper().startswith("RETURN") else line
        for line in lines
    )
    return merged if merged != group_by_cypher else None


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


def _merge_nodes_cypher_denormalized(group_by_cypher: str, nodes_columns: list) -> str | None:
    """Return a WITH/UNWIND Cypher that denormalizes group-by rows with their matching nodes."""
    import re

    node_cols = [c for c in nodes_columns if c.nested_in is None]
    if not node_cols:
        return None

    lines = group_by_cypher.strip().splitlines()
    ret_idx = next(
        (i for i, ln in enumerate(lines) if ln.strip().upper().startswith("RETURN")), None
    )
    if ret_idx is None:
        return None

    match_line = next((ln for ln in lines if "MATCH" in ln.upper()), "")
    m = re.search(r"\((\w+):", match_line)
    var = m.group(1) if m else "a"

    ret_body = lines[ret_idx].strip()[len("RETURN") :].strip()
    agg_items = [item.strip() for item in ret_body.split(",")]

    def _alias(expr: str) -> str:
        if " AS " in expr.upper():
            return expr.split(" AS ")[-1].strip()
        if "." in expr:
            return expr.rsplit(".", 1)[-1]
        if "COUNT(*)" in expr.upper():
            return "count"
        return "agg"

    collect_entries = ", ".join(f"{c.field_name}: {var}.{c.field_name}" for c in node_cols)
    with_parts = [
        item if " AS " in item.upper() else f"{item} AS {_alias(item)}" for item in agg_items
    ] + [f"collect({{{collect_entries}}}) AS _nodes"]

    final_ret = [_alias(item) for item in agg_items] + [
        f"node.{c.field_name} AS {c.field_name}" for c in node_cols
    ]

    return "\n".join(
        [
            *lines[:ret_idx],
            "WITH " + ", ".join(with_parts),
            "UNWIND _nodes AS node",
            "RETURN " + ", ".join(final_ret),
            *lines[ret_idx + 1 :],
        ]
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


def _apply_pipeline_transforms(  # REQ-040, REQ-041, REQ-134, REQ-198, REQ-262, REQ-263, REQ-554
    compiled, ctx, rls, role_id: str, role, fresh_mvs, state
) -> tuple[Any, bool]:
    """Apply RLS, masking, MV rewrite, Kafka filters, and the row cap. Returns (compiled, mv_applied)."""
    from provisa.compiler.mask_inject import inject_masking
    from provisa.compiler.rls import inject_rls
    from provisa.compiler.stage2 import apply_row_cap, resolve_row_cap
    from provisa.mv.rewriter import rewrite_if_mv_match

    compiled = inject_rls(compiled, ctx, rls)
    compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

    pre_mv_sources = set(compiled.sources)
    compiled = rewrite_if_mv_match(compiled, fresh_mvs)
    mv_applied = compiled.sources != pre_mv_sources

    if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
        from provisa.kafka.window import inject_kafka_filters

        compiled = inject_kafka_filters(
            compiled, ctx, state.source_types, state.kafka_table_configs
        )

    compiled.sql = apply_row_cap(compiled.sql, resolve_row_cap(role))

    return compiled, mv_applied


def _decide_transpile(  # REQ-066, REQ-067, REQ-068, REQ-152, REQ-229
    compiled, state, steward_hint: str | None
) -> tuple[Any, str | None, str | None, str]:
    """Return (decision, engine_sql, direct_sql, route_str)."""
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile

    has_json_extract = "->>" in compiled.sql
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=steward_hint,
        has_json_extract=has_json_extract,
        source_dsns=getattr(state, "source_dsns", None),
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


def _compile_cypher_for_result(  # REQ-345, REQ-347, REQ-349, REQ-350, REQ-351
    compiled,
    ctx,
    state,
    role,
    document,
    effective_variables: dict | None,
    raw_semantic_sql: str,
    flat_sql: bool,
    flat_cypher: bool,
    node_only_cypher: bool,
) -> tuple[str | None, str | None]:
    """Return (compiled_cypher, cypher_error)."""
    from provisa.compiler.params import embed_params_comment
    from provisa.compiler.sql_gen import compile_query as _compile_query, make_semantic_sql
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher

    try:
        _cache = getattr(state, "schema_build_cache", {})
        _label_map = CypherLabelMap.from_schema(
            ctx,
            domain_access=role.get("domain_access") if role else None,
            all_tables=_cache.get("tables"),
            all_relationships=_cache.get("relationships"),
            all_column_types=_cache.get("column_types"),
            source_catalogs=getattr(state, "source_catalogs", None),
        )
        # Cypher translator requires ARRAY_AGG (flat=False) SQL as input — it maps ARRAY_AGG→collect().
        # flat_sql only controls the SQL tab display; Cypher aggregation is controlled by flat_cypher.
        if flat_sql:
            _cypher_compiled = _compile_query(document, ctx, effective_variables, flat=False)[0]
            _cypher_sql = make_semantic_sql(
                embed_params_comment(_cypher_compiled.sql, _cypher_compiled.params), ctx
            )
        else:
            _cypher_compiled = compiled
            _cypher_sql = raw_semantic_sql
        compiled_cypher = semantic_sql_to_cypher(
            _cypher_sql,
            _label_map,
            ctx,
            override_limit=_cypher_compiled.result_limit,
            params=_cypher_compiled.params,
            flat=flat_cypher,
            node_only=node_only_cypher,
        )
        if compiled_cypher is None:
            return None, "Query structure cannot be represented as a Cypher pattern"
        return compiled_cypher, None
    except Exception as e:
        return None, str(e)


def _combine_cypher_results(results: list[dict[str, Any]]) -> None:
    """Merge multi-part Cypher queries in-place on results."""
    cypher_parts = [r["compiled_cypher"] for r in results if r.get("compiled_cypher")]
    if len(cypher_parts) > 1:
        try:
            from provisa.cypher.sql_to_cypher import combine_cypher_queries

            combined = combine_cypher_queries(cypher_parts)
            for r in results:
                r["compiled_cypher"] = combined
        except Exception:
            pass


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
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import compile_query as _compile_query, make_semantic_sql
    from provisa.security.rights import has_capability, Capability
    from graphql import GraphQLSyntaxError

    if role_id not in state.schemas:
        raise ValueError(f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    directives, sql_comment_prefix = _parse_directives(query)
    steward_hint = directives.steward_hint

    try:
        document = parse_query(schema, query, variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise ValueError(str(e))

    effective_variables = coerce_variable_defaults(document, variables)
    compiled_queries = _compile_query(document, ctx, effective_variables, flat=flat_sql)
    if not compiled_queries:
        raise ValueError("No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()
    results = []

    for _compiled_orig in compiled_queries:
        pre_mv_sources = set(_compiled_orig.sources)
        compiled, mv_applied = _apply_pipeline_transforms(
            _compiled_orig, ctx, rls, role_id, role, fresh_mvs, state
        )

        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        decision, engine_sql, direct_sql, route_str = _decide_transpile(
            compiled, state, steward_hint
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
            compiled,
            ctx,
            state,
            role,
            document,
            effective_variables,
            raw_semantic_sql,
            flat_sql,
            flat_cypher,
            node_only_cypher,
        )

        has_nodes = nodes_semantic_sql is not None and bool(compiled.nodes_columns)

        if has_nodes:
            assert nodes_semantic_sql is not None
            assert compiled.nodes_columns is not None
            if not flat_sql:
                merged_sql = _merge_nodes_sql(raw_semantic_sql, compiled.nodes_columns)
            else:
                merged_sql = _merge_nodes_sql_denormalized(
                    raw_semantic_sql, nodes_semantic_sql, compiled.nodes_columns
                )
            if merged_sql:
                semantic_sql_str = sql_comment_prefix + merged_sql
            nodes_semantic_sql = None

        nodes_compiled_cypher: str | None = None
        if has_nodes and compiled.nodes_columns and compiled_cypher and not node_only_cypher:
            assert compiled.nodes_columns is not None
            if not flat_cypher:
                merged_cypher = _merge_nodes_cypher(compiled_cypher, compiled.nodes_columns)
            else:
                merged_cypher = _merge_nodes_cypher_denormalized(
                    compiled_cypher, compiled.nodes_columns
                )
            if merged_cypher:
                compiled_cypher = merged_cypher

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
                "nodes_compiled_cypher": nodes_compiled_cypher,
                "cypher_error": cypher_error,
            }
        )

    _combine_cypher_results(results)
    return results
