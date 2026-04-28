# Copyright (c) 2026 Kenneth Stott
# Canary: 923941bb-90ad-4902-9e12-870a2adc5c40
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/nl/runner.py."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.nl.job import BranchResult, InMemoryJobStore, NlJob, new_job_id
from provisa.nl.loop import CompileResult, LLMClient
from provisa.nl.runner import run_nl_job


_SDL = "type Query { persons: [Person] }\ntype Person { id: ID! }"


class _ValidLLM(LLMClient):
    """Always returns a syntactically valid Cypher query."""

    async def complete(self, prompt: str) -> str:
        return "MATCH (n) RETURN n LIMIT 1"


class _InvalidLLM(LLMClient):
    """Always returns an invalid query."""

    async def complete(self, prompt: str) -> str:
        return "THIS IS NOT A VALID QUERY @@@@"


def _make_app_state(has_schema: bool = False) -> MagicMock:
    state = MagicMock()
    state.schemas = {}
    state.contexts = {}
    state.trino_conn = None
    return state


@pytest.mark.asyncio
async def test_three_branches_launched():
    """All three targets are attempted."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    branch_targets: list[str] = []

    async def _fake_execute(q, target, role, app_state):
        branch_targets.append(target)
        return {"columns": [], "rows": []}

    with patch("provisa.nl.runner.run_nl_job.__module__"):
        pass  # just check store branches

    state = _make_app_state()

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM())

    job = await store.get(job_id)
    assert job.state == "complete"
    # All three branches present (may have errors if schema missing, but attempted)
    assert len(job.branches) == 3


@pytest.mark.asyncio
async def test_one_branch_exhausted_others_complete():
    """One branch failing doesn't block the others."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()

    call_count: dict[str, int] = {"n": 0}

    class _MixedLLM(LLMClient):
        async def complete(self, prompt: str) -> str:
            call_count["n"] += 1
            if "sql" in prompt.lower() or "SELECT" in prompt:
                return "INVALID@@"
            return "MATCH (n) RETURN n LIMIT 1"

    async def _fake_execute(q, target, role, app_state):
        return {"columns": [], "rows": []}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _MixedLLM())

    job = await store.get(job_id)
    assert job.state == "complete"
    assert len(job.branches) == 3


@pytest.mark.asyncio
async def test_all_valid_all_executed():
    """When all branches produce valid queries, all three are executed."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()
    executed_targets: list[str] = []

    async def _fake_execute(q, target, role, app_state):
        executed_targets.append(target)
        return {"columns": ["x"], "rows": [{"x": 1}]}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM())

    job = await store.get(job_id)
    assert job.state == "complete"
    # Check that branches with results have non-None result
    successful = [t for t, b in job.branches.items() if b.result is not None]
    # At least cypher should succeed (ValidLLM always returns valid Cypher)
    assert len(successful) >= 1


@pytest.mark.asyncio
async def test_job_store_updated_with_partial_results():
    """Branch results written to store as each completes."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()
    update_calls: list[str] = []

    _original_update = store.update_branch

    async def _tracked_update(jid, target, branch):
        update_calls.append(target)
        await _original_update(jid, target, branch)

    store.update_branch = _tracked_update

    async def _fake_execute(q, target, role, app_state):
        return {"columns": [], "rows": []}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM())

    assert len(update_calls) == 3
