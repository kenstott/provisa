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
from typing import TYPE_CHECKING

from provisa.nl.job import BranchResult, InMemoryJobStore, NlTarget, RedisJobStore
from provisa.nl.loop import (
    LLMClient,
    generation_loop,
    make_cypher_compiler,
    make_graphql_compiler,
)

if TYPE_CHECKING:
    from provisa.api.app import AppState
    from provisa.cypher.label_map import CypherLabelMap

JobStore = InMemoryJobStore | RedisJobStore

log = logging.getLogger(__name__)

_TARGETS: list[NlTarget] = ["cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"]


def _generate_grpc_query(
    selected_type_names: set[str], user_nodes: dict
) -> tuple[str | None, str | None]:
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None:
        return None, "NOT_APPLICABLE"
    return f"Query{nm.table_label}", None


def _generate_jsonapi_query(
    selected_type_names: set[str], user_nodes: dict
) -> tuple[str | None, str | None]:
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None or nm.domain_id is None:
        return None, "NOT_APPLICABLE"
    return f"/data/jsonapi/{nm.domain_id}/{nm.table_name}?page[size]=20", None


def _generate_openapi_query(
    selected_type_names: set[str], user_nodes: dict
) -> tuple[str | None, str | None]:
    if not selected_type_names:
        return None, "NOT_APPLICABLE"
    type_name = next(iter(sorted(selected_type_names)))
    nm = user_nodes.get(type_name)
    if nm is None or nm.domain_id is None:
        return None, "NOT_APPLICABLE"
    return f"GET /data/rest/{nm.domain_id}/{nm.table_name}", None


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

    def _sql_domain(domain_id: str | None) -> str:
        return domain_to_sql_name(domain_id) if domain_id else "default"

    if pre_selected_types is not None:
        selected_types = pre_selected_types
    else:
        selected_types = await _run_table_selection(
            user_nodes, nl_query, _sql_domain, table_name_to_type
        )
    multihop_lines = _build_multihop_lines(selected_types, lm, _sql_domain)
    relevant_type_names = _build_relevant_type_names(selected_types, lm)
    schema_block = _build_schema_block(
        all_tables, relevant_type_names, ctx, _sql_domain, multihop_lines
    )

    last_sql, _, last_error = await _run_sql_generation_loop(
        nl_query, schema_block, ctx, gov_ctx, role_obj, raw_tables
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


async def run_nl_job(  # REQ-355, REQ-357, REQ-358, REQ-359
    job_id: str, nl_query: str, role: str, app_state: AppState, job_store: JobStore, llm: LLMClient
) -> None:
    """Background coroutine: runs all six generation branches, writes results."""
    await job_store.set_state(job_id, "running")

    schema_sdl = _get_schema_sdl(app_state, role)
    graphql_schema = getattr(app_state, "schemas", {}).get(role)

    # Semantic entity matching: surface exact schema names for the LLM prompt.
    from provisa.nl.schema_matcher import get_matcher, make_embed_fn
    from provisa.nl.prompt import format_entities

    embed_fn = make_embed_fn(app_state)
    matcher = await get_matcher(role, schema_sdl, embed_fn)
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
    relevant_entities = format_entities(matcher.top_k(query_emb))

    # A role may have no GraphQL schema. Never validate the GraphQL branch with the
    # Cypher compiler (that masks the absence); instead omit the graphql compiler so
    # the graphql branch fails independently as a per-branch error (see _run_branch),
    # leaving the other branches unaffected.
    compilers = {"cypher": make_cypher_compiler()}
    if graphql_schema is not None:
        compilers["graphql"] = make_graphql_compiler(graphql_schema)

    ctx = getattr(app_state, "contexts", {}).get(role)
    cypher_schema_block = ""
    if ctx is not None:
        from provisa.cypher.label_map import CypherLabelMap

        lm = CypherLabelMap.from_schema(ctx)
        cypher_schema_block = _format_cypher_schema(lm)

    # Shared table selection for SQL and protocol-based branches (one LLM call).
    shared_selected_types: set[str] = set()
    shared_user_nodes: dict = {}
    if ctx is not None:
        from provisa.api.data.endpoint_dev import _collect_nl_user_tables, _run_table_selection
        from provisa.compiler.naming import domain_to_sql_name

        _, _u_nodes, _tbl_to_type, _ = _collect_nl_user_tables(ctx)

        def _sql_dom(d: str | None) -> str:
            return domain_to_sql_name(d) if d else "default"

        shared_selected_types = await _run_table_selection(
            _u_nodes, nl_query, _sql_dom, _tbl_to_type
        )
        shared_user_nodes = _u_nodes

    _QUERY_TARGETS = {"cypher", "graphql", "sql"}

    async def _run_branch(target: NlTarget) -> tuple[NlTarget, str | None, str | None]:
        # Each branch is independent: a failure in one (e.g. SQL generation
        # raising) must not abort the asyncio.as_completed loop and discard the
        # other branches' results. Convert any exception into a branch error.
        try:
            if target == "sql":
                valid_query, error = await _generate_sql_from_nl(
                    nl_query, role, app_state, pre_selected_types=shared_selected_types
                )
                return target, valid_query, error
            if target == "grpc":
                q, e = _generate_grpc_query(shared_selected_types, shared_user_nodes)
                return target, q, e
            if target == "jsonapi":
                q, e = _generate_jsonapi_query(shared_selected_types, shared_user_nodes)
                return target, q, e
            if target == "openapi":
                q, e = _generate_openapi_query(shared_selected_types, shared_user_nodes)
                return target, q, e
            compiler = compilers.get(target)  # type: ignore[arg-type]
            if compiler is None:
                return target, None, f"No GraphQL schema for role: {role}"
            entities = cypher_schema_block if target == "cypher" else relevant_entities
            valid_query, error = await generation_loop(
                nl_query,
                target,
                schema_sdl,
                compiler,
                llm,
                relevant_entities=entities,  # type: ignore[arg-type]
            )
            return target, valid_query, error
        # complexity-gate: allow-ble=3 reason="[file ceiling 3] Per-branch NL compilation boundary: each target (graphql/cypher/…) runs an LLM + compiler pipeline with an unbounded failure surface; a failing branch must be captured as that branch's error string and returned so sibling branches still complete — never abort the whole NL run."
        except Exception as exc:
            log.warning("NL branch %s failed: %s", target, exc)
            return target, None, str(exc)

    branch_tasks = [asyncio.create_task(_run_branch(t)) for t in _TARGETS]

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


def _format_cypher_schema(lm: "CypherLabelMap") -> str:  # type: ignore[name-defined]
    """Serialize CypherLabelMap to an authoritative label/relationship reference block."""

    lm_typed: CypherLabelMap = lm
    lines = [
        "GRAPH SCHEMA (use these exact node labels and relationship types — do not invent others):"
    ]
    lines.append("Node labels:")
    seen_labels: set[str] = set()
    for nm in lm_typed.nodes.values():
        label = lm_typed.display_label(nm)
        if label not in seen_labels:
            seen_labels.add(label)
            props = ", ".join(nm.properties.keys())
            lines.append(f"  ({label})  properties: {props}" if props else f"  ({label})")
    lines.append("Relationship types:")
    seen_rels: set[str] = set()
    for rel_list in lm_typed.aliases.values():
        for rel in rel_list:
            key = f"{rel.rel_type}::{rel.source_label}→{rel.target_label}"
            if key not in seen_rels:
                seen_rels.add(key)
                src_nm = lm_typed.nodes.get(rel.source_label)
                tgt_nm = lm_typed.nodes.get(rel.target_label)
                src = lm_typed.display_label(src_nm) if src_nm else rel.source_label
                tgt = lm_typed.display_label(tgt_nm) if tgt_nm else rel.target_label
                lines.append(f"  ({src})-[:{rel.rel_type}]->({tgt})")
    return "\n".join(lines)


def _get_schema_sdl(app_state: AppState, role: str) -> str:
    """Return role-scoped GraphQL SDL string, or empty string if unavailable."""
    schema = getattr(app_state, "schemas", {}).get(role)
    if schema is None:
        return ""
    from graphql import print_schema

    return print_schema(schema)
