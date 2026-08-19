# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1a2b4c-5d6e-7f8a-9b0c-1d2e3f4a5b6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Business logic for compile and submit developer operations (admin GQL mutations)."""

# Requirements: REQ-001, REQ-002, REQ-007, REQ-009, REQ-040, REQ-041, REQ-066, REQ-067, REQ-262, REQ-263, REQ-345, REQ-347, REQ-478, REQ-554

from __future__ import annotations

import logging
import re
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


def _split_top_level_commas(text: str) -> list[str]:
    """Split on commas not nested inside {}, [], or () (e.g. map-literal RETURN items)."""
    parts = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch in "{[(":
            depth += 1
        elif ch in "}])":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


def _unwrap_collect(expr: str) -> str:
    """Strip an outer collect(...) so the value is per-row, not an aggregate.

    A relationship field compiles to `collect({...}) AS user` on its own, but here it becomes an
    entry of an outer `collect({...}) AS nodes` — and Cypher rejects a nested aggregate. The
    per-row map literal is what the outer collect must aggregate.
    """
    m = re.match(r"^collect\s*\((.*)\)$", expr.strip(), flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return expr
    inner = m.group(1)
    # Guard against `collect(x) + collect(y)`, where the regex's greedy tail spans two calls.
    if _split_top_level_commas(inner) != [inner.strip()] or inner.count("(") != inner.count(")"):
        return expr
    return inner.strip()


def _return_line_to_entries(return_line: str) -> list[tuple[str, str]]:
    """Convert 'RETURN a.x, collect({..}) AS user' into [("x", "a.x"), ("user", "{..}")].

    The compiled nodes fragment aliases only the relationship items; scalar columns come back as
    bare `a.col` property accesses, whose alias is the property name.
    """
    body = re.sub(r"^\s*RETURN\s+", "", return_line.strip(), flags=re.IGNORECASE)
    entries = []
    for item in _split_top_level_commas(body):
        m = re.search(r"\sAS\s+(\w+)\s*$", item, flags=re.IGNORECASE)
        if m:
            entries.append((m.group(1), _unwrap_collect(item[: m.start()].strip())))
            continue
        prop = re.match(r"^\w+\.(\w+)$", item.strip())
        if prop:
            entries.append((prop.group(1), item.strip()))
    return entries


def _optional_matches_to_splice(group_by_cypher: str, nodes_cypher: str) -> list[str]:
    """OPTIONAL MATCH lines from nodes_cypher not already present in group_by_cypher."""
    existing = {ln.strip() for ln in group_by_cypher.strip().splitlines()}
    return [
        ln
        for ln in nodes_cypher.strip().splitlines()
        if ln.strip().upper().startswith("OPTIONAL MATCH") and ln.strip() not in existing
    ]


def _merge_nodes_cypher(group_by_cypher: str, nodes_cypher: str) -> str | None:
    """Append collect({...}) AS nodes to the RETURN clause of a group-by Cypher query.

    nodes_cypher is compiled from nodes_semantic_sql via semantic_sql_to_cypher, so it
    already resolves relationship fields to proper OPTIONAL MATCH traversals and map-literal
    RETURN items (aliased to their field name); reconstructing entries from ColumnRefs alone
    breaks for those fields, since a bare "{var}.{field_name}" access is invalid Cypher for a
    relationship property.
    """
    lines = group_by_cypher.strip().splitlines()
    match_idx = next(
        (i for i, ln in enumerate(lines) if ln.strip().upper().startswith("MATCH")), None
    )
    if match_idx is None:
        return None

    nodes_return_line = next(
        (ln for ln in nodes_cypher.strip().splitlines() if ln.strip().upper().startswith("RETURN")),
        None,
    )
    if nodes_return_line is None:
        return None
    entries = _return_line_to_entries(nodes_return_line)
    if not entries:
        return None

    insert = _optional_matches_to_splice(group_by_cypher, nodes_cypher)
    collect_expr = f"collect({{{', '.join(f'{a}: {e}' for a, e in entries)}}}) AS nodes"

    merged_lines = lines[: match_idx + 1] + insert + lines[match_idx + 1 :]
    merged = "\n".join(
        ln.rstrip() + ", " + collect_expr if ln.strip().upper().startswith("RETURN") else ln
        for ln in merged_lines
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


def _merge_nodes_cypher_denormalized(group_by_cypher: str, nodes_cypher: str) -> str | None:
    """Return a WITH/UNWIND Cypher that denormalizes group-by rows with their matching nodes.

    See _merge_nodes_cypher: entries come from the compiled nodes_cypher fragment, not raw
    ColumnRefs, so relationship fields resolve to their OPTIONAL MATCH-derived map literals.
    """
    lines = group_by_cypher.strip().splitlines()
    match_idx = next(
        (i for i, ln in enumerate(lines) if ln.strip().upper().startswith("MATCH")), None
    )
    ret_idx = next(
        (i for i, ln in enumerate(lines) if ln.strip().upper().startswith("RETURN")), None
    )
    if match_idx is None or ret_idx is None:
        return None

    nodes_return_line = next(
        (ln for ln in nodes_cypher.strip().splitlines() if ln.strip().upper().startswith("RETURN")),
        None,
    )
    if nodes_return_line is None:
        return None
    entries = _return_line_to_entries(nodes_return_line)
    if not entries:
        return None

    insert = _optional_matches_to_splice(group_by_cypher, nodes_cypher)

    ret_body = lines[ret_idx].strip()[len("RETURN") :].strip()
    agg_items = _split_top_level_commas(ret_body)

    def _alias(expr: str) -> str:
        if " AS " in expr.upper():
            return expr.split(" AS ")[-1].strip()
        if "." in expr:
            return expr.rsplit(".", 1)[-1]
        if "COUNT(*)" in expr.upper():
            return "count"
        return "agg"

    collect_entries = ", ".join(f"{a}: {e}" for a, e in entries)
    with_parts = [
        item if " AS " in item.upper() else f"{item} AS {_alias(item)}" for item in agg_items
    ] + [f"collect({{{collect_entries}}}) AS _nodes"]

    final_ret = [_alias(item) for item in agg_items] + [f"node.{a} AS {a}" for a, _ in entries]

    return "\n".join(
        [
            *lines[: match_idx + 1],
            *insert,
            *lines[match_idx + 1 : ret_idx],
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
) -> tuple[str | None, str | None, str | None]:
    """Return (compiled_cypher, cypher_error, nodes_compiled_cypher)."""
    from provisa.compiler.params import embed_params_comment
    from provisa.compiler.sql_gen import compile_query as _compile_query
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher

    # A missing role would silently grant unrestricted domain_access — require it.
    if role is None:
        raise ValueError("role is required to compile Cypher (domain_access unknown)")

    try:
        _cache = getattr(state, "schema_build_cache", {})
        _label_map = CypherLabelMap.from_schema(
            ctx,
            domain_access=role.get("domain_access"),
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
            return None, "Query structure cannot be represented as a Cypher pattern", None

        nodes_compiled_cypher = None
        if _cypher_compiled.nodes_sql is not None and not node_only_cypher:
            _cypher_nodes_sql = make_semantic_sql(
                embed_params_comment(_cypher_compiled.nodes_sql, _cypher_compiled.nodes_params), ctx
            )
            nodes_compiled_cypher = semantic_sql_to_cypher(
                _cypher_nodes_sql,
                _label_map,
                ctx,
                params=_cypher_compiled.nodes_params,
                flat=flat_cypher,
            )

        return compiled_cypher, None, nodes_compiled_cypher
    except Exception as e:
        return None, str(e), None


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
    from provisa.compiler.sql_gen import compile_query as _compile_query
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.security.rights import has_capability, Capability
    from graphql import GraphQLSyntaxError

    if role_id not in state.schemas:
        raise ValueError(f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    # Invariant: contexts and rls_contexts are written together per-role; if a
    # schema/context exists for role_id, its RLSContext must too. Fail loud.
    rls = state.rls_contexts[role_id]
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

        compiled_cypher, cypher_error, nodes_compiled_cypher = _compile_cypher_for_result(
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

        if has_nodes and nodes_compiled_cypher and compiled_cypher and not node_only_cypher:
            if not flat_cypher:
                merged_cypher = _merge_nodes_cypher(compiled_cypher, nodes_compiled_cypher)
            else:
                merged_cypher = _merge_nodes_cypher_denormalized(
                    compiled_cypher, nodes_compiled_cypher
                )
            if merged_cypher:
                compiled_cypher = merged_cypher
            nodes_compiled_cypher = None

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
