# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-354 / REQ-355 / REQ-357 / REQ-358 — Natural Language Query Service (Phase AV)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.nl.job import (
    InMemoryJobStore,
    NlJob,
    new_job_id,
)
from provisa.nl.loop import CompileResult, LLMClient, generation_loop
from provisa.nl.runner import run_nl_job


scenarios("../features/REQ-354.feature")
scenarios("../features/REQ-355.feature")
scenarios("../features/REQ-357.feature")
scenarios("../features/REQ-358.feature")


_SDL = "type Query { persons: [Person] }\ntype Person { id: ID! name: String }"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


class _ValidCypherLLM(LLMClient):
    """Deterministic LLM that produces a syntactically valid Cypher query."""

    def __init__(self) -> None:
        self.call_count = 0

    async def complete(self, prompt: str) -> str:
        self.call_count += 1
        return "MATCH (n) RETURN n LIMIT 1"


class _FakeAppState:
    """Minimal AppState stand-in for NL job execution."""

    def __init__(self) -> None:
        from graphql import build_schema as _build_schema

        self.schemas = {"default": _build_schema(_SDL)}
        self.contexts = {}
        self.rls_contexts = {}
        self.masking_rules = {}
        self.engine_conn = None


@pytest.fixture
def job_store() -> InMemoryJobStore:
    return InMemoryJobStore()


# ---------------------------------------------------------------------------
# Given (REQ-354)
# ---------------------------------------------------------------------------


@given("a non-technical user submitting a natural language question to POST /query/nl")
def submit_nl_question(shared_data: dict) -> None:
    nl_query = "List all persons in plain English"
    role = "default"
    shared_data["nl_query"] = nl_query
    shared_data["role"] = role
    shared_data["app_state"] = _FakeAppState()
    shared_data["llm"] = _ValidCypherLLM()
    assert nl_query and not nl_query.strip().upper().startswith(("SELECT", "MATCH", "QUERY"))


# ---------------------------------------------------------------------------
# When (REQ-354)
# ---------------------------------------------------------------------------


@when("the service receives it")
def service_receives(shared_data: dict, job_store: InMemoryJobStore) -> None:
    async def _body() -> None:
        job_id = new_job_id()
        job = NlJob(
            job_id=job_id,
            nl_query=shared_data["nl_query"],
            role=shared_data["role"],
        )
        await job_store.put(job)
        shared_data["job_id"] = job_id
        shared_data["store"] = job_store

        persisted = await job_store.get(job_id)
        assert persisted is not None
        assert persisted.state in ("pending", "running")
        shared_data["state_at_submit"] = persisted.state

    asyncio.run(_body())


# ---------------------------------------------------------------------------
# Then (REQ-354)
# ---------------------------------------------------------------------------


@then("it returns a job_id immediately and the result is available via polling or SSE")
def job_id_and_pollable_result(shared_data: dict) -> None:
    async def _body() -> None:
        job_id = shared_data["job_id"]
        store: InMemoryJobStore = shared_data["store"]

        assert isinstance(job_id, str)
        assert len(job_id) > 0
        assert shared_data["state_at_submit"] in ("pending", "running")

        executed: list[str] = []

        async def _fake_execute(query, target, role, app_state):
            executed.append(target)
            return {"columns": ["n"], "rows": [{"n": 1}]}

        with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
            await run_nl_job(
                job_id,
                shared_data["nl_query"],
                shared_data["role"],
                shared_data["app_state"],
                store,
                shared_data["llm"],
            )

        polled = await store.get(job_id)
        assert polled is not None
        assert polled.job_id == job_id
        assert polled.state in ("complete", "failed")

        assert len(polled.branches) == 6
        assert set(polled.branches.keys()) == {
            "cypher",
            "graphql",
            "sql",
            "grpc",
            "jsonapi",
            "openapi",
        }

        successful = [
            t for t, b in polled.branches.items() if b.result is not None and b.error is None
        ]
        assert successful, "expected at least one successful branch result"

        assert shared_data["llm"].call_count >= 1

        payload = polled.to_dict()
        assert payload["job_id"] == job_id
        assert payload["state"] in ("complete", "failed")
        assert "branches" in payload
        rebuilt = NlJob.from_dict(payload)
        assert rebuilt.job_id == job_id
        assert set(rebuilt.branches.keys()) == {
            "cypher",
            "graphql",
            "sql",
            "grpc",
            "jsonapi",
            "openapi",
        }

    asyncio.run(_body())


# ---------------------------------------------------------------------------
# REQ-355 — three independent parallel generation loops
# ---------------------------------------------------------------------------


class _SingleQueryLLM(LLMClient):
    """LLM that always returns one fixed candidate query and counts calls."""

    def __init__(self, candidate: str) -> None:
        self.candidate = candidate
        self.call_count = 0
        self.prompts: list[str] = []

    async def complete(self, prompt: str) -> str:
        self.call_count += 1
        self.prompts.append(prompt)
        return self.candidate


def _make_refining_compiler(target: str, counter: list[int]):
    """Compiler that rejects the first candidate then accepts on retry."""

    def _compile(query: str) -> CompileResult:
        counter[0] += 1
        if counter[0] >= 2:
            return CompileResult(valid=True)
        return CompileResult(valid=False, error=f"{target} compiler: parse error near token")

    return _compile


@given("an NL query submitted to the service")
def nl_query_for_loops(shared_data: dict) -> None:
    nl_query = "Show me every person and the data they own"
    shared_data["nl_query"] = nl_query
    shared_data["sdl"] = _SDL
    stripped = nl_query.strip().upper()
    assert nl_query and not stripped.startswith(("SELECT", "MATCH", "{", "QUERY"))


@when("the three generation loops run")
def three_generation_loops_run(shared_data: dict) -> None:
    async def _body() -> None:
        nl_query = shared_data["nl_query"]
        sdl = shared_data["sdl"]

        candidates = {
            "cypher": "MATCH (n:Person) RETURN n LIMIT 10",
            "graphql": "{ persons { id name } }",
            "sql": "SELECT id, name FROM persons LIMIT 10",
        }

        counters = {t: [0] for t in candidates}
        llms = {t: _SingleQueryLLM(q) for t, q in candidates.items()}

        async def _run_loop(target: str):
            return await generation_loop(
                nl_query,
                target,
                sdl,
                _make_refining_compiler(target, counters[target]),
                llms[target],
                max_iterations=5,
            )

        results = await asyncio.gather(
            *(_run_loop(t) for t in candidates),
            return_exceptions=True,
        )

        shared_data["loop_results"] = dict(zip(candidates.keys(), results))
        shared_data["loop_counters"] = counters
        shared_data["loop_llms"] = llms

    asyncio.run(_body())


@then(
    "each independently generates and validates a Cypher, GraphQL, and SQL candidate with compiler-driven refinement"
)
def each_loop_generates_and_validates(shared_data: dict) -> None:
    results: dict = shared_data["loop_results"]
    counters: dict = shared_data["loop_counters"]
    llms: dict = shared_data["loop_llms"]

    assert set(results.keys()) == {"cypher", "graphql", "sql"}

    for target, outcome in results.items():
        assert not isinstance(outcome, Exception), f"{target} loop raised: {outcome!r}"
        query, error = outcome

        assert query is not None, f"{target} produced no valid query"
        assert error is None, f"{target} reported error: {error}"

        assert counters[target][0] >= 2, f"{target} did not refine via compiler feedback"

        assert llms[target].call_count == counters[target][0]

        assert any(
            "compiler" in p.lower() or "parse error" in p.lower() for p in llms[target].prompts[1:]
        ), f"{target} did not feed the compiler error back to the LLM"

    assert counters["cypher"][0] >= 1
    assert counters["graphql"][0] >= 1
    assert counters["sql"][0] >= 1

    valid_queries = [q for (q, e) in results.values() if q is not None and e is None]
    assert valid_queries, "expected at least one valid candidate across the loops"


# ---------------------------------------------------------------------------
# REQ-357 — all three query forms executed in parallel and returned
# ---------------------------------------------------------------------------


class _AllValidLLM(LLMClient):
    """LLM that returns a syntactically valid query for whichever target is prompted."""

    def __init__(self) -> None:
        self.call_count = 0

    async def complete(self, prompt: str) -> str:
        self.call_count += 1
        low = prompt.lower()
        if "cypher" in low:
            return "MATCH (n) RETURN n LIMIT 1"
        if "sql select" in low:
            return "SELECT id, name FROM persons LIMIT 10"
        return "query PersonsQuery { persons { id name } }"


@given("all three generation loops complete")
def all_three_loops_complete(shared_data: dict, job_store: InMemoryJobStore) -> None:
    async def _body() -> None:
        nl_query = "List persons and the data they own"
        role = "default"
        job_id = new_job_id()
        await job_store.put(NlJob(job_id=job_id, nl_query=nl_query, role=role))

        shared_data["nl_query"] = nl_query
        shared_data["role"] = role
        shared_data["job_id"] = job_id
        shared_data["store"] = job_store
        shared_data["app_state"] = _FakeAppState()
        shared_data["llm"] = _AllValidLLM()

        pre = await job_store.get(job_id)
        assert pre is not None
        assert pre.state in ("pending", "running")

    asyncio.run(_body())


@when("results are returned")
def results_are_returned(shared_data: dict) -> None:
    async def _body() -> None:
        store: InMemoryJobStore = shared_data["store"]
        job_id = shared_data["job_id"]

        executed: list[str] = []

        async def _fake_execute(query, target, role, app_state):
            executed.append(target)
            if target == "graphql":
                return {"data": {"persons": [{"id": "1", "name": "Alice"}]}}
            return {"columns": ["id", "name"], "rows": [{"id": 1, "name": "Alice"}]}

        async def _fake_sql(nl_query, role, app_state, pre_selected_types=None):
            return ("SELECT id, name FROM persons LIMIT 10", None)

        with patch("provisa.nl.runner._generate_sql_from_nl", side_effect=_fake_sql):
            with patch("provisa.nl.executor.execute", side_effect=_fake_execute):
                await run_nl_job(
                    job_id,
                    shared_data["nl_query"],
                    shared_data["role"],
                    shared_data["app_state"],
                    store,
                    shared_data["llm"],
                )

        job = await store.get(job_id)
        assert job is not None
        shared_data["job"] = job
        shared_data["executed_targets"] = executed

    asyncio.run(_body())


@then("the response includes cypher, graphql, and sql branches each with query text and result")
def response_includes_all_three_branches(shared_data: dict) -> None:
    job: NlJob = shared_data["job"]

    assert job.state in ("complete", "failed")
    # All six generation branches are attempted (REQ-799..804 added grpc,
    # jsonapi, openapi); only the three query targets execute via Trino.
    assert set(job.branches.keys()) == {
        "cypher",
        "graphql",
        "sql",
        "grpc",
        "jsonapi",
        "openapi",
    }

    assert set(shared_data["executed_targets"]) == {"cypher", "graphql", "sql"}

    successful = 0
    for target in ("cypher", "graphql", "sql"):
        branch = job.branches[target]
        if branch.query is not None:
            assert branch.error is None, f"{target} has both query and error"
            assert branch.result is not None, f"{target} valid query has no result"
            successful += 1
        else:
            assert branch.result is None, f"{target} null query but has result"
            assert branch.error, f"{target} exhausted branch missing error message"

    assert successful == 3, "expected all three branches to return query + result"

    payload = job.to_dict()
    branches = payload["branches"]
    assert set(branches.keys()) == {"cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"}
    for target in ("cypher", "graphql", "sql"):
        b = branches[target]
        assert "query" in b and "result" in b and "error" in b
        assert b["query"] is not None
        assert b["result"] is not None

    assert "data" in branches["graphql"]["result"]
    assert "rows" in branches["cypher"]["result"]
    assert "rows" in branches["sql"]["result"]

    rebuilt = NlJob.from_dict(payload)
    assert set(rebuilt.branches.keys()) == {
        "cypher",
        "graphql",
        "sql",
        "grpc",
        "jsonapi",
        "openapi",
    }


# ---------------------------------------------------------------------------
# REQ-358 — differentiators: three-target, role-scoped, compiler-validated
# ---------------------------------------------------------------------------


class _CapturePromptLLM(LLMClient):
    """LLM that records every prompt it receives and returns a fixed candidate."""

    def __init__(self, candidate: str) -> None:
        self.candidate = candidate
        self.call_count = 0
        self.prompts: list[str] = []

    async def complete(self, prompt: str) -> str:
        self.call_count += 1
        self.prompts.append(prompt)
        return self.candidate


@given("the NL query service")
def the_nl_query_service(shared_data: dict) -> None:
    from provisa.nl.executor import execute as _execute_fn

    shared_data["execute_fn"] = _execute_fn
    shared_data["targets"] = ("cypher", "graphql", "sql")
    shared_data["nl_query"] = "show every person and what they own"
    shared_data["role"] = "analyst"

    shared_data["forbidden_field"] = "ssn"
    shared_data["role_scoped_sdl"] = (
        "type Query { persons: [Person] }\ntype Person { id: ID! name: String }"
    )

    shared_data["full_sdl"] = (
        "type Query { persons: [Person] }\ntype Person { id: ID! name: String ssn: String }"
    )

    assert callable(shared_data["execute_fn"])

    assert shared_data["forbidden_field"] not in shared_data["role_scoped_sdl"]
    assert shared_data["forbidden_field"] in shared_data["full_sdl"]


@when("compared to commodity text-to-SQL tools")
def compared_to_commodity_tools(shared_data: dict) -> None:
    async def _body() -> None:
        nl_query = shared_data["nl_query"]
        role_sdl = shared_data["role_scoped_sdl"]

        candidates = {
            "cypher": "MATCH (p:Person) RETURN p.id, p.name LIMIT 10",
            "graphql": "{ persons { id name } }",
            "sql": "SELECT id, name FROM persons LIMIT 10",
        }

        counters = {t: [0] for t in candidates}
        llms = {t: _CapturePromptLLM(q) for t, q in candidates.items()}

        async def _run_loop(target: str):
            return await generation_loop(
                nl_query,
                target,
                role_sdl,
                _make_refining_compiler(target, counters[target]),
                llms[target],
                max_iterations=5,
            )

        results = await asyncio.gather(
            *(_run_loop(t) for t in candidates),
            return_exceptions=True,
        )

        shared_data["diff_results"] = dict(zip(candidates.keys(), results))
        shared_data["diff_counters"] = counters
        shared_data["diff_llms"] = llms

    asyncio.run(_body())


@then("it provides three-target output, role-scoped prompts, and compiler-driven refinement")
def provides_differentiators(shared_data: dict) -> None:
    results: dict = shared_data["diff_results"]
    counters: dict = shared_data["diff_counters"]
    llms: dict = shared_data["diff_llms"]
    forbidden = shared_data["forbidden_field"]
    role_scoped_sdl = shared_data["role_scoped_sdl"]
    full_sdl = shared_data["full_sdl"]

    assert set(results.keys()) == {"cypher", "graphql", "sql"}, (
        "all three target languages must be generated — Cypher output is the "
        "key differentiator absent from commodity text-to-SQL tools"
    )

    for target, outcome in results.items():
        assert not isinstance(outcome, Exception), f"{target} loop raised unexpectedly: {outcome!r}"
        query, error = outcome

        assert query is not None, (
            f"{target}: expected a validated query but got None (three-target generation failed)"
        )
        assert error is None, (
            f"{target}: expected no error after compiler refinement but got: {error}"
        )

        if target == "cypher":
            assert "MATCH" in query.upper(), (
                "Cypher output must use MATCH syntax — this is the differentiator "
                "absent from commodity text-to-SQL tools"
            )

        for prompt in llms[target].prompts:
            assert "Person" in prompt, (
                f"{target}: prompt did not include schema context "
                "(role-scoped SDL must be injected into LLM prompt)"
            )
            assert forbidden not in prompt, (
                f"{target}: forbidden field '{forbidden}' leaked into LLM prompt — "
                "role-scoped schema context is broken"
            )
            assert "ssn" not in prompt, (
                f"{target}: full unscoped schema was sent to LLM instead of role-scoped SDL"
            )

        assert counters[target][0] >= 2, (
            f"{target}: compiler was only called {counters[target][0]} time(s) — "
            "compiler-driven refinement loop did not execute (expected ≥ 2 iterations)"
        )

        assert llms[target].call_count == counters[target][0], (
            f"{target}: LLM call count ({llms[target].call_count}) does not match "
            f"compiler iteration count ({counters[target][0]})"
        )

        if len(llms[target].prompts) >= 2:
            retry_prompts = llms[target].prompts[1:]
            assert any(
                "compiler" in p.lower() or "parse error" in p.lower() for p in retry_prompts
            ), (
                f"{target}: compiler error was not fed back to the LLM on retry — "
                "compiler-driven refinement requires error propagation to the prompt"
            )

    valid_by_target = {t: (q is not None and e is None) for t, (q, e) in results.items()}
    assert all(valid_by_target.values()), (
        f"Not all three targets produced valid output: {valid_by_target}"
    )

    all_prompts = [p for llm in llms.values() for p in llm.prompts]
    assert all_prompts, "No prompts were captured — LLMs were never called"
    for prompt in all_prompts:
        assert forbidden not in prompt, (
            f"Role-scoping failure: '{forbidden}' found in prompt: {prompt[:120]!r}"
        )

    for target in ("cypher", "graphql", "sql"):
        assert counters[target][0] >= 2, (
            f"Compiler-driven refinement not demonstrated for {target}: "
            f"only {counters[target][0]} compiler call(s)"
        )

    assert forbidden not in role_scoped_sdl
    assert forbidden in full_sdl
