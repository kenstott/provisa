# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1a2b4c-5d6e-7f8a-9b0c-1d2e3f4a5b6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Business logic for compile and submit developer operations (admin GQL mutations)."""

from __future__ import annotations

import json
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
# Internal helpers (ported from endpoint_dev.py)
# ---------------------------------------------------------------------------

def _extract_operation_name(query_text: str) -> str | None:
    from graphql import parse as gql_parse
    from graphql.language.ast import OperationDefinitionNode
    try:
        doc = gql_parse(query_text)
        for defn in doc.definitions:
            if isinstance(defn, OperationDefinitionNode) and defn.name:
                return defn.name.value
    except Exception:
        pass
    return None


def _extract_cypher_name(query: str) -> str:
    import re
    m = re.search(r"\([\w]*:(\w+)", query)
    return m.group(1) if m else "CypherQuery"


def _extract_sql_name(query: str) -> str:
    import re
    m = re.search(r'FROM\s+"?(\w+)"?\."?(\w+)"?', query, re.IGNORECASE)
    if m:
        return m.group(2)
    m = re.search(r'FROM\s+"?(\w+)"?', query, re.IGNORECASE)
    return m.group(1) if m else "SqlQuery"


def _detect_target(query: str) -> str:
    import re
    stripped = query.strip()
    first = stripped.split()[0].lower() if stripped.split() else ""
    if first in ("query", "mutation", "subscription", "fragment") or stripped.startswith("{"):
        return "graphql"
    if first in ("match", "optional", "call") or re.search(r"\([\w]*:", stripped):
        return "cypher"
    return "sql"


def _build_enforcement_metadata(compiled, ctx, rls, masking_rules: dict, role_id: str, route_value: str) -> EnforcementMetadata:
    rls_filters: list[str] = []
    root_table = ctx.tables.get(compiled.root_field)
    if root_table and root_table.table_id in rls.rules:
        rls_filters.append(rls.rules[root_table.table_id])
    for (type_name, _field_name), join_meta in ctx.joins.items():
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
        for col_name, (rule, _dtype) in col_map.items():
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


async def _compile_graphql(query: str, variables: dict | None, role_id: str, ctx, rls, state) -> tuple[str, str, list[int], str | None]:
    from graphql import GraphQLSyntaxError
    from provisa.compiler.directives import extract_directives, extract_directives_from_sql_comments, merge_directives
    from provisa.compiler.hints import graphql_comments_to_sql
    from provisa.compiler.mask_inject import inject_masking
    from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
    from provisa.compiler.rls import inject_rls
    from provisa.compiler.sql_gen import compile_query, make_semantic_sql
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher, combine_cypher_queries

    op_name = _extract_operation_name(query)
    if not op_name:
        raise ValueError("GraphQL query must have a named operation (e.g., 'query MyReport { ... }').")
    schema = state.schemas[role_id]
    try:
        document = parse_query(schema, query, variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise ValueError(str(e))

    effective_variables = coerce_variable_defaults(document, variables)
    compiled_queries = compile_query(document, ctx, effective_variables)
    if not compiled_queries:
        raise ValueError("No query fields found")

    masking_rules = getattr(state, "masking_rules", {})
    target_tables: list[int] = []
    sql_parts: list[str] = []

    for cq in compiled_queries:
        governed = inject_masking(inject_rls(cq, ctx, rls), ctx, masking_rules, role_id)
        root_table = ctx.tables.get(governed.root_field)
        if root_table:
            target_tables.append(root_table.table_id)
        sql_parts.append(make_semantic_sql(governed.sql, ctx))

    compiled_sql = sql_parts[0] if len(sql_parts) == 1 else json.dumps(sql_parts)

    compiled_cypher: str | None = None
    try:
        label_map = CypherLabelMap.from_schema(ctx)
        cypher_parts = [c for s in sql_parts if (c := semantic_sql_to_cypher(s, label_map, ctx))]
        if cypher_parts:
            compiled_cypher = combine_cypher_queries(cypher_parts)
    except Exception:
        pass

    return op_name, compiled_sql, target_tables, compiled_cypher


def _compile_cypher_submit(query: str, role_id: str, ctx) -> tuple[str, str, list[int], str | None]:
    from provisa.cypher.parser import parse_cypher, CypherParseError
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.translator import cypher_to_sql, CypherTranslateError
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.cypher.params import collect_param_names, bind_params
    from provisa.compiler.sql_gen import make_semantic_sql

    try:
        ast = parse_cypher(query)
    except CypherParseError as e:
        raise ValueError(str(e))

    label_map = CypherLabelMap.from_schema(ctx)

    if ast.call_subqueries:
        from provisa.cypher.translator import cypher_calls_to_sql_list
        try:
            call_results = cypher_calls_to_sql_list(ast, label_map, {})
        except CypherTranslateError as e:
            raise ValueError(str(e))
        sql_parts: list[str] = []
        for sql_ast_part, _ordered_params, graph_vars in call_results:
            sql_ast_part = apply_graph_rewrites(sql_ast_part, graph_vars, label_map)
            sql_parts.append(make_semantic_sql(sql_ast_part.sql(dialect="postgres"), ctx))
        compiled_sql = sql_parts[0] if len(sql_parts) == 1 else json.dumps(sql_parts)
        op_name = _extract_cypher_name(query)
        target_tables = [nm.table_id for label, nm in label_map.nodes.items() if label in query]
        return op_name, compiled_sql, target_tables, query

    collect_param_names(query)
    bind_params(collect_param_names(query), {})

    try:
        sql_ast, _ordered_params, graph_vars = cypher_to_sql(ast, label_map, {})
    except CypherTranslateError as e:
        raise ValueError(str(e))

    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    compiled_sql = make_semantic_sql(sql_ast.sql(dialect="postgres"), ctx)
    op_name = _extract_cypher_name(query)
    target_tables = [nm.table_id for label, nm in label_map.nodes.items() if label in query]
    return op_name, compiled_sql, target_tables, query


def _compile_sql_submit(query: str, ctx) -> tuple[str, str, list[int], str | None]:
    import sqlglot
    import sqlglot.expressions as exp

    op_name = _extract_sql_name(query)
    target_tables: list[int] = []
    try:
        tree = sqlglot.parse_one(query, read="postgres")
        for tbl in tree.find_all(exp.Table):
            key = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
            for meta in ctx.tables.values():
                domain_key = f"{meta.domain_id}.{meta.field_name}"
                if key in (domain_key, meta.field_name, meta.table_name):
                    target_tables.append(meta.table_id)
                    break
    except Exception:
        pass
    return op_name, query, target_tables, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def compile_query(role_id: str, query: str, variables: dict | None) -> list[dict[str, Any]]:
    """Compile a GraphQL query → SQL. Returns list of compile result dicts."""
    from provisa.api.app import state
    from provisa.compiler.directives import extract_directives, extract_directives_from_sql_comments, merge_directives
    from provisa.compiler.hints import graphql_comments_to_sql
    from provisa.compiler.mask_inject import inject_masking
    from provisa.compiler.parser import GraphQLValidationError, coerce_variable_defaults, parse_query
    from provisa.compiler.rls import RLSContext, inject_rls
    from provisa.compiler.sampling import apply_sampling, get_sample_size
    from provisa.compiler.sql_gen import compile_query as _compile_query, make_semantic_sql
    from provisa.mv.rewriter import rewrite_if_mv_match
    from provisa.security.rights import has_capability, Capability
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino
    from graphql import GraphQLSyntaxError, parse as gql_parse_raw

    if role_id not in state.schemas:
        raise ValueError(f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    _comment_directives = extract_directives_from_sql_comments(query)
    try:
        _ast_directives = extract_directives(gql_parse_raw(query))
    except Exception:
        _ast_directives = _comment_directives.__class__()
    directives = merge_directives(_comment_directives, _ast_directives)
    steward_hint = directives.steward_hint
    sql_comment_prefix = graphql_comments_to_sql(query)

    try:
        document = parse_query(schema, query, variables)
    except (GraphQLValidationError, GraphQLSyntaxError) as e:
        raise ValueError(str(e))

    effective_variables = coerce_variable_defaults(document, variables)
    compiled_queries = _compile_query(document, ctx, effective_variables)
    if not compiled_queries:
        raise ValueError("No query fields found")

    fresh_mvs = state.mv_registry.get_fresh()
    results = []

    for compiled in compiled_queries:
        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

        pre_mv_sources = set(compiled.sources)
        compiled = rewrite_if_mv_match(compiled, fresh_mvs)
        mv_applied = compiled.sources != pre_mv_sources

        if hasattr(state, "kafka_table_configs") and state.kafka_table_configs:
            from provisa.kafka.window import inject_kafka_filters
            compiled = inject_kafka_filters(compiled, ctx, state.source_types, state.kafka_table_configs)

        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        if sampling:
            compiled = apply_sampling(compiled, get_sample_size())

        has_json_extract = "->>" in compiled.sql
        decision = decide_route(
            sources=compiled.sources,
            source_types=state.source_types,
            source_dialects=state.source_dialects,
            steward_hint=steward_hint,
            has_json_extract=has_json_extract,
        )

        trino_sql = transpile_to_trino(compiled.sql) if decision.route == Route.TRINO else None
        direct_sql = transpile(compiled.sql, decision.dialect) if decision.route == Route.DIRECT and decision.dialect else None

        route_str = decision.route.value
        if decision.route == Route.DIRECT and decision.dialect:
            route_str = f"direct:{decision.dialect}"

        enforcement = _build_enforcement_metadata(
            compiled=compiled, ctx=ctx, rls=rls,
            masking_rules=state.masking_rules, role_id=role_id, route_value=route_str,
        )

        optimizations: list[str] = []
        warnings: list[str] = []
        if mv_applied:
            new_sources = compiled.sources - pre_mv_sources
            optimizations.append(f"Materialized view rewrite: sources → {', '.join(sorted(new_sources))}")
        if directives.route == "DIRECT":
            if len(compiled.sources) > 1:
                warnings.append("route=direct ignored: query spans multiple sources and requires federation")
            elif decision.route != Route.DIRECT:
                warnings.append("route=direct ignored: source has no direct driver")
        for k, v in directives.to_session_props().items():
            optimizations.append(f"Federation hint: {k}={v} (via @provisa directive)")
        if sampling:
            optimizations.append("Sampling applied (role lacks FULL_RESULTS capability)")

        raw_semantic_sql = make_semantic_sql(compiled.sql, ctx)
        semantic_sql_str = sql_comment_prefix + raw_semantic_sql

        compiled_cypher = None
        try:
            from provisa.cypher.label_map import CypherLabelMap
            from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher
            _label_map = CypherLabelMap.from_schema(ctx)
            compiled_cypher = semantic_sql_to_cypher(raw_semantic_sql, _label_map, ctx)
        except Exception:
            pass

        results.append({
            "sql": compiled.sql,
            "semantic_sql": semantic_sql_str,
            "trino_sql": trino_sql,
            "direct_sql": direct_sql,
            "route": decision.route.value,
            "route_reason": decision.reason,
            "sources": list(compiled.sources),
            "root_field": compiled.root_field,
            "canonical_field": compiled.canonical_field or compiled.root_field,
            "column_aliases": [
                {"field_name": c.field_name, "column": c.column}
                for c in compiled.columns if c.field_name != c.column
            ],
            "enforcement": enforcement,
            "optimizations": optimizations,
            "warnings": warnings,
            "compiled_cypher": compiled_cypher,
        })

    cypher_parts = [r["compiled_cypher"] for r in results if r.get("compiled_cypher")]
    if len(cypher_parts) > 1:
        try:
            from provisa.cypher.sql_to_cypher import combine_cypher_queries
            combined = combine_cypher_queries(cypher_parts)
            for r in results:
                r["compiled_cypher"] = combined
        except Exception:
            pass

    return results


async def submit_query(
    role_id: str,
    query: str,
    variables: dict | None = None,
    compiled_cypher: str | None = None,
    sink_topic: str | None = None,
    sink_trigger: str = "change_event",
    sink_key_column: str | None = None,
    schedule_cron: str | None = None,
    schedule_output_type: str | None = None,
    schedule_output_format: str | None = None,
    schedule_destination: str | None = None,
    business_purpose: str | None = None,
    use_cases: str | None = None,
    data_sensitivity: str | None = None,
    refresh_frequency: str | None = None,
    expected_row_count: str | None = None,
    owner_team: str | None = None,
    expiry_date: str | None = None,
) -> tuple[int, str, str]:
    """Compile and submit a GQL/SQL/Cypher query for steward approval.

    Returns (query_id, operation_name, message).
    """
    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.registry.store import submit

    if role_id not in state.schemas:
        raise ValueError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    target = _detect_target(query)

    if target == "graphql":
        op_name, compiled_sql, target_tables, _cypher = await _compile_graphql(query, variables, role_id, ctx, rls, state)
        compiled_cypher = compiled_cypher or _cypher
    elif target == "cypher":
        op_name, compiled_sql, target_tables, _cypher = _compile_cypher_submit(query, role_id, ctx)
        compiled_cypher = compiled_cypher or _cypher
    else:
        op_name, compiled_sql, target_tables, _ = _compile_sql_submit(query, ctx)

    async with state.pg_pool.acquire() as conn:
        query_id = await submit(
            conn,
            query_text=query,
            compiled_sql=compiled_sql,
            target_tables=target_tables,
            developer_id=role_id,
            compiled_cypher=compiled_cypher or None,
        )

        updates = []
        params = []
        idx = 1
        for field_name, value in [
            ("sink_topic", sink_topic),
            ("sink_trigger", sink_trigger if sink_topic else None),
            ("sink_key_column", sink_key_column),
            ("business_purpose", business_purpose),
            ("use_cases", use_cases),
            ("data_sensitivity", data_sensitivity),
            ("refresh_frequency", refresh_frequency),
            ("expected_row_count", expected_row_count),
            ("owner_team", owner_team),
            ("expiry_date", __import__("datetime").date.fromisoformat(expiry_date) if expiry_date else None),
            ("schedule_cron", schedule_cron),
            ("schedule_output_type", schedule_output_type),
            ("schedule_output_format", schedule_output_format),
            ("schedule_destination", schedule_destination),
        ]:
            if value is not None:
                updates.append(f"{field_name} = ${idx}")
                params.append(value)
                idx += 1
        if updates:
            params.append(query_id)
            await conn.execute(
                f"UPDATE persisted_queries SET {', '.join(updates)} WHERE id = ${idx}",
                *params,
            )

    return query_id, op_name, f"Query '{op_name}' submitted for approval (id={query_id})."
