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

from __future__ import annotations

import asyncio
import logging
from typing import Any

from provisa.nl.job import BranchResult, NlJob, NlTarget
from provisa.nl.loop import (
    LLMClient,
    generation_loop,
    make_cypher_compiler,
    make_graphql_compiler,
    make_sql_compiler,
)

log = logging.getLogger(__name__)

_TARGETS: list[NlTarget] = ["cypher", "graphql", "sql"]


async def run_nl_job(job_id: str, nl_query: str, role: str, app_state: Any, job_store: Any, llm: LLMClient) -> None:
    """Background coroutine: runs all three generation branches, writes results."""
    await job_store.set_state(job_id, "running")

    schema_sdl = _get_schema_sdl(app_state, role)
    graphql_schema = getattr(app_state, "schemas", {}).get(role)

    compilers = {
        "cypher": make_cypher_compiler(),
        "graphql": make_graphql_compiler(graphql_schema) if graphql_schema else make_cypher_compiler(),
        "sql": make_sql_compiler(),
    }

    async def _run_branch(target: NlTarget) -> tuple[NlTarget, str | None, str | None]:
        compiler = compilers[target]
        valid_query, error = await generation_loop(nl_query, target, schema_sdl, compiler, llm)
        return target, valid_query, error

    branch_tasks = [asyncio.create_task(_run_branch(t)) for t in _TARGETS]

    from provisa.nl.executor import execute as _execute

    for coro in asyncio.as_completed(branch_tasks):
        target, valid_query, error = await coro
        result = None
        if valid_query is not None:
            try:
                result = await _execute(valid_query, target, role, app_state)
            except Exception as exc:
                error = str(exc)
                valid_query = None
        await job_store.update_branch(job_id, target, BranchResult(query=valid_query, result=result, error=error))
        log.debug("Branch %s complete: valid=%s", target, valid_query is not None)

    await job_store.set_state(job_id, "complete")


def _get_schema_sdl(app_state: Any, role: str) -> str:
    """Return role-scoped GraphQL SDL string, or empty string if unavailable."""
    schema = getattr(app_state, "schemas", {}).get(role)
    if schema is None:
        return ""
    try:
        from graphql import print_schema
        return print_schema(schema)
    except Exception:
        return ""
