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

JobStore = InMemoryJobStore | RedisJobStore

log = logging.getLogger(__name__)

_TARGETS: list[NlTarget] = ["cypher", "graphql", "sql"]


async def _generate_sql_from_nl(
    nl_query: str, role: str, app_state: AppState
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

    rls = getattr(app_state, "rls_contexts", {}).get(role, RLSContext.empty())
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

    # Normalize domain-qualified refs (e.g. pet_store.pets) to physical schema.table
    # so _execute_sql's make_semantic_sql → rewrite_semantic_to_trino_physical pipeline works.
    from provisa.compiler.sql_gen import rewrite_semantic_to_physical

    physical_sql = rewrite_semantic_to_physical(last_sql, ctx)
    return physical_sql, None


async def run_nl_job(  # REQ-355, REQ-357, REQ-358, REQ-359
    job_id: str, nl_query: str, role: str, app_state: AppState, job_store: JobStore, llm: LLMClient
) -> None:
    """Background coroutine: runs all three generation branches, writes results."""
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
        except Exception as exc:
            log.debug("Query embedding failed: %s", exc)
    relevant_entities = format_entities(matcher.top_k(query_emb))

    compilers = {
        "cypher": make_cypher_compiler(),
        "graphql": make_graphql_compiler(graphql_schema)
        if graphql_schema
        else make_cypher_compiler(),
    }

    ctx = getattr(app_state, "contexts", {}).get(role)
    cypher_schema_block = ""
    if ctx is not None:
        from provisa.cypher.label_map import CypherLabelMap

        lm = CypherLabelMap.from_schema(ctx)
        cypher_schema_block = _format_cypher_schema(lm)

    async def _run_branch(target: NlTarget) -> tuple[NlTarget, str | None, str | None]:
        if target == "sql":
            valid_query, error = await _generate_sql_from_nl(nl_query, role, app_state)
            return target, valid_query, error
        compiler = compilers[target]
        entities = cypher_schema_block if target == "cypher" else relevant_entities
        valid_query, error = await generation_loop(
            nl_query, target, schema_sdl, compiler, llm, relevant_entities=entities
        )
        return target, valid_query, error

    branch_tasks = [asyncio.create_task(_run_branch(t)) for t in _TARGETS]

    from provisa.nl.executor import execute as _execute

    for coro in asyncio.as_completed(branch_tasks):
        target, valid_query, error = await coro
        result = None
        if valid_query is not None and error is None:
            try:
                result = await _execute(valid_query, target, role, app_state)
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
    from provisa.cypher.label_map import CypherLabelMap

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
    try:
        from graphql import print_schema

        return print_schema(schema)
    except Exception:
        return ""
