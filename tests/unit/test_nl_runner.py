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

import json
from unittest.mock import MagicMock, patch

import pytest

from provisa.nl.job import InMemoryJobStore, NlJob, RedisJobStore, make_job_store, new_job_id
from provisa.nl.loop import LLMClient
from provisa.nl.runner import run_nl_job
from tests.role_state import with_surfaces


_SDL = "type Query { persons: [Person] }\ntype Person { id: ID! }"


def test_make_job_store_embedded_uses_fakeredis(monkeypatch):
    """Regression: PROVISA_REDIS_EMBEDDED must force the embedded fakeredis store even when
    REDIS_URL points at a Redis server — the native/desktop tier has no Redis running, so dialing
    it (the old behavior) 500'd every NL submit."""
    monkeypatch.setenv("PROVISA_REDIS_EMBEDDED", "1")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    store = make_job_store()
    assert isinstance(store, RedisJobStore)
    # The client must be embedded fakeredis, not a real-server connection to localhost:6379.
    # make_redis returns a SharedRedis (REQ-1882: one shared client per worker); the underlying
    # client it resolves to is the one that must be fakeredis.
    assert type(store._redis.client()).__module__.startswith("fakeredis")


async def test_embedded_job_store_roundtrips_without_a_server(monkeypatch):
    """The embedded store must put/get without any Redis server listening."""
    monkeypatch.setenv("PROVISA_REDIS_EMBEDDED", "1")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    store = make_job_store()
    job = NlJob(job_id=new_job_id(), nl_query="q", role="admin")
    await store.put(job)
    got = await store.get(job.job_id)
    assert got is not None and got.nl_query == "q"


class _ValidLLM(LLMClient):
    """Always returns a syntactically valid Cypher query."""

    async def complete(self, prompt: str) -> str:
        return "MATCH (n) RETURN n LIMIT 1"


class _InvalidLLM(LLMClient):
    """Always returns an invalid query."""

    async def complete(self, prompt: str) -> str:
        return "THIS IS NOT A VALID QUERY @@@@"


def _make_app_state(has_schema: bool = False) -> MagicMock:
    state = with_surfaces(MagicMock())
    state.schemas = {}
    state.contexts = {}
    state.engine_conn = None
    # No vector model configured for these orchestration tests → make_embed_fn
    # returns None cleanly (a real misconfig now propagates rather than being swallowed).
    state.config.vector_models = []
    # REQ-603: a role that may bypass the relationship guard, so non-strict runs as asked.
    state.roles = {
        "default": {
            "id": "default",
            "capabilities": [],
            "domain_access": ["*"],
            "relationship_guard": False,
        }
    }
    state.security_high = False
    return state


@pytest.mark.asyncio
async def test_three_branches_launched():
    """All six targets are attempted."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    branch_targets: list[str] = []

    async def _fake_execute(q, target, role, app_state, *, deliver):
        branch_targets.append(target)
        return {"columns": [], "rows": []}

    with patch("provisa.nl.runner.run_nl_job.__module__"):
        pass  # just check store branches

    state = _make_app_state()

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM(), deliver=None)

    job = await store.get(job_id)
    assert job.state == "complete"
    # All six branches present (may have errors if schema missing, but attempted)
    assert len(job.branches) == 6


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

    async def _fake_execute(q, target, role, app_state, *, deliver):
        return {"columns": [], "rows": []}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _MixedLLM(), deliver=None)

    job = await store.get(job_id)
    assert job.state == "complete"
    assert len(job.branches) == 6


@pytest.mark.asyncio
async def test_all_valid_all_executed():
    """When branches produce valid queries, they are executed and results recorded."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()
    executed_targets: list[str] = []

    async def _fake_execute(q, target, role, app_state, *, deliver):
        executed_targets.append(target)
        return {"columns": ["x"], "rows": [{"x": 1}]}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM(), deliver=None)

    job = await store.get(job_id)
    assert job.state == "complete"
    assert len(job.branches) == 6


@pytest.mark.asyncio
async def test_non_strict_cypher_is_generated_on_its_own():
    """Non-strict (REQ-603): Cypher is the model's own best effort from the question, not derived
    from the SQL branch -- the SQL failing (no schema context for the role) leaves it standing."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()

    async def _fake_execute(q, target, role, app_state, *, deliver):
        return {"columns": ["x"], "rows": [{"x": 1}]}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(
            job_id, "test", "default", state, store, _ValidLLM(), strict=False, deliver=None
        )

    job = await store.get(job_id)
    assert job.state == "complete"
    assert job.branches["sql"].query is None
    assert job.branches["cypher"].query == "MATCH (n) RETURN n LIMIT 1"
    assert job.branches["cypher"].result == {"columns": ["x"], "rows": [{"x": 1}]}


@pytest.mark.asyncio
async def test_strict_mode_shares_one_chain_across_graphql_sql_cypher():
    """Strict mode (REQ-1400): graphql/sql/cypher all come from one NL -> GraphQL -> SQL ->
    Cypher chain, so they share the same failure (no GraphQL schema for the role) rather than
    generating independently."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))

    state = _make_app_state()

    async def _fake_execute(q, target, role, app_state, *, deliver):
        return {"columns": ["x"], "rows": [{"x": 1}]}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(
            job_id, "test", "default", state, store, _ValidLLM(), strict=True, deliver=None
        )

    job = await store.get(job_id)
    assert job is not None
    assert job.state == "complete"
    graphql_branch = job.branches["graphql"]
    sql_branch = job.branches["sql"]
    cypher_branch = job.branches["cypher"]
    assert graphql_branch.query is None
    assert sql_branch.query is None
    assert cypher_branch.query is None
    assert graphql_branch.error == sql_branch.error == cypher_branch.error


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

    async def _fake_execute(q, target, role, app_state, *, deliver):
        return {"columns": [], "rows": []}

    with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
        await run_nl_job(job_id, "test", "default", state, store, _ValidLLM(), deliver=None)

    assert len(update_calls) == 6


# REQ-603: no panel shows a query with an error.

_UNBOUND = "MATCH (a:Pets) RETURN a.breedName, COUNT(id) AS pet_count LIMIT 100"
_FIXED = "MATCH (a:Pets) RETURN a.breedName, COUNT(a.id) AS pet_count LIMIT 100"
# The refusal the Cypher front end gives it (cypher.undefined_variable), as the retry is given it.
_REFUSAL = "RETURN reads id, which nothing in the statement binds"


@pytest.mark.asyncio
async def test_a_refused_cypher_is_fixed_on_retry_and_never_shown():
    """The case from the NL page: the model's first Cypher is refused when run; the retry is given
    the refusal and fixes it; the panel shows only the fixed statement and its rows."""
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="pets per breed", role="default"))
    prompts: list[str] = []

    class _Model(LLMClient):
        async def complete(self, prompt: str) -> str:
            prompts.append(prompt)
            return _FIXED if _REFUSAL in prompt else _UNBOUND

    async def _execute(q, target, role, app_state, *, deliver):
        assert q != _UNBOUND, "the refused statement was run"
        return {"columns": ["breedName", "pet_count"], "rows": [["pug", 2]]}

    with patch("provisa.nl.executor.execute", side_effect=_execute):
        await run_nl_job(
            job_id,
            "pets per breed",
            "default",
            _make_app_state(),
            store,
            _Model(),
            strict=False,
            deliver=None,
        )

    cypher = (await store.get(job_id)).branches["cypher"]
    assert any(_REFUSAL in p for p in prompts), "the retry was not given the refusal"
    assert cypher.query == _FIXED and cypher.error is None
    assert cypher.result == {"columns": ["breedName", "pet_count"], "rows": [["pug", 2]]}
    shown = json.dumps((await store.get(job_id)).to_dict())
    assert _UNBOUND not in shown, "the refused statement reached the response"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "generated, reason",
    [
        (_UNBOUND, _REFUSAL),  # refused by the Cypher front end
        (_FIXED, "the join is not an approved relationship"),  # refused when run (V002)
    ],
    ids=["front-end", "governance"],
)
async def test_a_query_that_never_runs_is_not_shown_only_its_reason_is(generated, reason):
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="pets per breed", role="default"))

    class _Model(LLMClient):
        async def complete(self, prompt: str) -> str:
            return generated

    async def _execute(q, target, role, app_state, *, deliver):
        raise ValueError("[V002] the join is not an approved relationship")

    with patch("provisa.nl.executor.execute", side_effect=_execute):
        await run_nl_job(
            job_id,
            "pets per breed",
            "default",
            _make_app_state(),
            store,
            _Model(),
            strict=False,
            deliver=None,
        )

    job = await store.get(job_id)
    cypher = job.branches["cypher"]
    assert cypher.query is None and cypher.result is None
    assert cypher.error.startswith(f"Not available: {reason}"), cypher.error
    assert generated not in json.dumps(job.to_dict()), "the failing statement reached the response"
    for branch in job.branches.values():
        assert branch.query is None or branch.error is None, "a query was shown with an error"


@pytest.mark.asyncio
async def test_a_role_that_may_not_bypass_the_guard_runs_strict_when_it_asks_non_strict():
    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="test", role="default"))
    state = _make_app_state()
    state.roles["default"]["relationship_guard"] = True
    chains: list[str] = []

    async def _chain(nl_query, role, app_state, check=None):
        chains.append(role)
        return None, None, None, "no GraphQL schema"

    with patch("provisa.nl.runner._generate_graphql_sql_from_nl", side_effect=_chain):
        await run_nl_job(
            job_id, "test", "default", state, store, _ValidLLM(), strict=False, deliver=None
        )

    assert chains == ["default"], "a non-strict request from a guarded role did not run strict"


def test_a_best_effort_retry_falls_back_only_when_the_language_cannot_express_it():
    from provisa.nl.prompt import BEST_EFFORT_RETRY, build_prompt

    best = build_prompt("q", "cypher", "", "unknown label Pet", best_effort=True)
    exact = build_prompt("q", "cypher", "", "unknown label Pet", best_effort=False)
    assert "Correct the query to fix this error." in best and BEST_EFFORT_RETRY in best
    assert "Correct the query to fix this error." in exact and BEST_EFFORT_RETRY not in exact
