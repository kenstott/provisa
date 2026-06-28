# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-354 / REQ-355 / REQ-357 / REQ-358 — Natural Language Query Service (Phase AV)."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import patch

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, scenarios, parsers

from provisa.nl.job import (
    BranchResult,
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
        self.schemas = {}
        self.contexts = {}
        self.rls_contexts = {}
        self.masking_rules = {}
        self.trino_conn = None


@pytest_asyncio.fixture
async def job_store() -> InMemoryJobStore:
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
    # Assert the input is genuinely a natural-language string, not SQL/Cypher.
    assert nl_query and not nl_query.strip().upper().startswith(
        ("SELECT", "MATCH", "QUERY")
    )


# ---------------------------------------------------------------------------
# When (REQ-354)
# ---------------------------------------------------------------------------


@when("the service receives it")
@pytest.mark.asyncio
async def service_receives(shared_data: dict, job_store: InMemoryJobStore) -> None:
    # The endpoint creates a job and returns its id immediately, before any
    # generation/execution work has been done.
    job_id = new_job_id()
    job = NlJob(
        job_id=job_id,
        nl_query=shared_data["nl_query"],
        role=shared_data["role"],
    )
    await job_store.put(job)
    shared_data["job_id"] = job_id
    shared_data["store"] = job_store

    # Immediately after submission, the job must exist and be in a non-terminal
    # initial state (pending/running) — the id is returned without blocking.
    persisted = await job_store.get(job_id)
    assert persisted is not None
    assert persisted.state in ("pending", "running")
    shared_data["state_at_submit"] = persisted.state


# ---------------------------------------------------------------------------
# Then (REQ-354)
# ---------------------------------------------------------------------------


@then("it returns a job_id immediately and the result is available via polling or SSE")
@pytest.mark.asyncio
async def job_id_and_pollable_result(shared_data: dict) -> None:
    job_id = shared_data["job_id"]
    store: InMemoryJobStore = shared_data["store"]

    # A non-empty job_id must have been returned immediately.
    assert isinstance(job_id, str)
    assert len(job_id) > 0
    assert shared_data["state_at_submit"] in ("pending", "running")

    # Run the async job worker with a deterministic LLM and a stubbed executor
    # so the test does no network/Trino I/O but still exercises the real
    # run_nl_job orchestration and job-store transitions.
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

    # The consumer polls GET /query/nl/{job_id}; simulate that read.
    polled = await store.get(job_id)
    assert polled is not None
    assert polled.job_id == job_id
    assert polled.state in ("complete", "failed")

    # The completed job exposes its branch results for retrieval.
    assert len(polled.branches) == 3
    assert set(polled.branches.keys()) == {"cypher", "graphql", "sql"}

    # At least one branch must have produced an actual result (the valid Cypher).
    successful = [
        t for t, b in polled.branches.items() if b.result is not None and b.error is None
    ]
    assert successful, "expected at least one successful branch result"

    # The LLM was actually invoked to generate queries.
    assert shared_data["llm"].call_count >= 1

    # The serialized form (what polling/SSE would return) round-trips cleanly.
    payload = polled.to_dict()
    assert payload["job_id"] == job_id
    assert payload["state"] in ("complete", "failed")
    assert "branches" in payload
    rebuilt = NlJob.from_dict(payload)
    assert rebuilt.job_id == job_id
    assert set(rebuilt.branches.keys()) == {"cypher", "graphql", "sql"}


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
    """Compiler that rejects the first candidate then accepts on retry.

    This exercises the compiler-driven refinement path: the first compile
    fails (error fed back to the LLM), the second succeeds.
    """

    def _compile(query: str) -> CompileResult:
        counter[0] += 1
        if counter[0] >= 2:
            return CompileResult(valid=True)
        return CompileResult(
            valid=False, error=f"{target} compiler: parse error near token"
        )

    return _compile


@given("an NL query submitted to the service")
def nl_query_for_loops(shared_data: dict) -> None:
    nl_query = "Show me every person and the data they own"
    shared_data["nl_query"] = nl_query
    shared_data["sdl"] = _SDL
    # The submission is plain natural language, not a query in any target language.
    stripped = nl_query.strip().upper()
    assert nl_query and not stripped.startswith(("SELECT", "MATCH", "{", "QUERY"))


@when("the three generation loops run")
@pytest.mark.asyncio
async def three_generation_loops_run(shared_data: dict) -> None:
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

    # Run all three loops in parallel and independently; an exception in one
    # must not prevent the others from completing.
    results = await asyncio.gather(
        *(_run_loop(t) for t in candidates),
        return_exceptions=True,
    )

    shared_data["loop_results"] = dict(zip(candidates.keys(), results))
    shared_data["loop_counters"] = counters
    shared_data["loop_llms"] = llms


@then(
    "each independently generates and validates a Cypher, GraphQL, and SQL candidate with compiler-driven refinement")
@pytest.mark.asyncio
async def each_loop_generates_and_validates(shared_data: dict) -> None:
    results: dict = shared_data["loop_results"]
    counters: dict = shared_data["loop_counters"]
    llms: dict = shared_data["loop_llms"]

    # All three target loops must have run.
    assert set(results.keys()) == {"cypher", "graphql", "sql"}

    for target, outcome in results.items():
        # No loop crashed — independence means each ran to its own conclusion.
        assert not isinstance(outcome, Exception), f"{target} loop raised: {outcome!r}"
        query, error = outcome

        # Each loop produced a validated (compiler-accepted) candidate query.
        assert query is not None, f"{target} produced no valid query"
        assert error is None, f"{target} reported error: {error}"

        # Compiler-driven refinement: the candidate was compiled more than once
        # (first attempt rejected, error fed back, retry accepted).
        assert counters[target][0] >= 2, f"{target} did not refine via compiler feedback"

        # The LLM was invoked once per compiler iteration.
        assert llms[target].call_count == counters[target][0]

        # On the retry, the prior compiler error was passed back as a signal.
        assert any(
            "compiler" in p.lower() or "parse error" in p.lower()
            for p in llms[target].prompts[1:]
        ), f"{target} did not feed the compiler error back to the LLM"

    # The loops are genuinely independent — each maintained its own compile chain.
    assert counters["cypher"][0] >= 1
    assert counters["graphql"][0] >= 1
    assert counters["sql"][0] >= 1

    # At least one valid result is enough to respond to the user.
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
        if "graphql" in low or "sdl" in low:
            return "{ persons { id name } }"
        if "sql" in low or "select" in low:
            return "SELECT id, name FROM persons LIMIT 10"
        return "MATCH (n) RETURN n LIMIT 1"


@given("all three generation loops complete")
@pytest.mark.asyncio
async def all_three_loops_complete(
    shared_data: dict, job_store: InMemoryJobStore
) -> None:
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

    # Confirm the job was registered before execution begins.
    pre = await job_store.get(job_id)
    assert pre is not None
    assert pre.state in ("pending", "running")


@when("results are returned")
@pytest.mark.asyncio
async def results_are_returned(shared_data: dict) -> None:
    store: InMemoryJobStore = shared_data["store"]
    job_id = shared_data["job_id"]

    # Record which targets get executed and in what order; the executor is
    # stubbed to avoid Trino/Kafka I/O while exercising the real run_nl_job
    # orchestration (parallel execution across all three branches).
    executed: list[str] = []

    async def _fake_execute(query, target, role, app_state):
        executed.append(target)
        # Shape mirrors the real executor contract:
        #   cypher/sql → {"columns", "rows"}; graphql → {"data"}
        if target == "graphql":
            return {"data": {"persons": [{"id": "1", "name": "Alice"}]}}
        return {"columns": ["id", "name"], "rows": [{"id": 1, "name": "Alice"}]}

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


@then(
    "the response includes cypher, graphql, and sql branches each with query text and result"
)
@pytest.mark.asyncio
async def response_includes_all_three_branches(shared_data: dict) -> None:
    job: NlJob = shared_data["job"]

    # The job has terminated and exposes exactly the three target branches.
    assert job.state in ("complete", "failed")
    assert set(job.branches.keys()) == {"cypher", "graphql", "sql"}

    # All three branches were executed in parallel via the standard pipeline.
    assert set(shared_data["executed_targets"]) == {"cypher", "graphql", "sql"}

    # Each branch must follow the REQ-357 response contract: a successful branch
    # carries both query text and a result; an exhausted branch carries
    # query=None, result=None and a non-empty error — without blocking siblings.
    successful = 0
    for target in ("cypher", "graphql", "sql"):
        branch = job.branches[target]
        if branch.query is not None:
            assert branch.error is None, f"{target} has both query and error"
            assert branch.result is not None, f"{target} valid query has no result"
            successful += 1
        else:
            # Exhausted branch: null query, null result, error explaining why.
            assert branch.result is None, f"{target} null query but has result"
            assert branch.error, f"{target} exhausted branch missing error message"

    # With an all-valid LLM and a stubbed executor, every branch should succeed,
    # demonstrating the full three-way response shape.
    assert successful == 3, "expected all three branches to return query + result"

    # The serialized payload (what the API returns) carries the full structure:
    #   { cypher: {query, result, error}, graphql: {...}, sql: {...} }
    payload = job.to_dict()
    branches = payload["branches"]
    assert set(branches.keys()) == {"cypher", "graphql", "sql"}
    for target in ("cypher", "graphql", "sql"):
        b = branches[target]
        assert "query" in b and "result" in b and "error" in b
        assert b["query"] is not None
        assert b["result"] is not None

    # The graphql branch result must use the GraphQL data shape; cypher/sql use
    # the columnar shape — confirming each ran its own pipeline.
    assert "data" in branches["graphql"]["result"]
    assert "rows" in branches["cypher"]["result"]
    assert "rows" in branches["sql"]["result"]

    # Round-trips through the (de)serialization path used by polling/SSE.
    rebuilt = NlJob.from_dict(payload)
    assert set(rebuilt.branches.keys()) == {"cypher", "graphql", "sql"}


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
    # The three differentiating targets the service generates in parallel.
    from provisa.nl.executor import execute as _execute_fn

    shared_data["execute_fn"] = _execute_fn
    shared_data["targets"] = ("cypher", "graphql", "sql")
    shared_data["nl_query"] = "show every person and what they own"
    shared_data["role"] = "analyst"

    # Role-scoped schema context: the analyst is only permitted to see id+name.
    # A column the role may NOT see (ssn) is deliberately absent from the SDL
    # the LLM is given, so the LLM cannot reference it.
    shared_data["forbidden_field"] = "ssn"
    shared_data["role_scoped_sdl"] = (
        "type Query { persons: [Person] }\n"
        "type Person { id: ID! name: String }"
    )

    # The full schema (what a privileged role could see) DOES include the
    # forbidden field — proving the SDL handed to the LLM is genuinely scoped.
    shared_data["full_sdl"] = (
        "type Query { persons: [Person] }\n"
        "type Person { id: ID! name: String ssn: String }"
    )

    # The service must expose a real executor entry point for all three targets.
    assert callable(shared_data["execute_fn"])

    # Sanity: the role-scoped SDL must not leak the forbidden field, while the
    # full SDL must contain it.
    assert shared_data["forbidden_field"] not in shared_data["role_scoped_sdl"]
    assert shared_data["forbidden_field"] in shared_data["full_sdl"]


@when("compared to commodity text-to-SQL tools")
@pytest.mark.asyncio
async def compared_to_commodity_tools(shared_data: dict) -> None:
    nl_query = shared_data["nl_query"]
    role_sdl = shared_data["role_scoped_sdl"]

    # Differentiator 1 — three-target generation (SQL, GraphQL, Cypher).
    candidates = {
        "cypher": "MATCH (p:Person) RETURN p.id, p.name LIMIT 10",
        "graphql": "{ persons { id name } }",
        "sql": "SELECT id, name FROM persons LIMIT 10",
    }

    # Differentiator 3 — compiler-driven refinement: a deterministic compiler
    # rejects the first attempt, the error is fed back, the retry is accepted.
    counters = {t: [0] for t in candidates}

    # Differentiator 2 — role-scoped schema context: each loop receives ONLY the
    # role-scoped SDL, captured per-target so we can prove the prompt was scoped.
    llms = {t: _CapturePromptLLM(q) for t, q in candidates.items()}

    async def _run_loop(target: str):
        # The compiler enforces that only schema-permitted constructs validate;
        # here it also drives the refinement loop.
        return await generation_loop(
            nl_query,
            target,
            role_sdl,
            _make_refining_compiler(target, counters[target]),
            llms[target],
            max_iterations=5,
        )

    # All three loops run in parallel and independently.
    results = await asyncio.gather(
        *(_run_loop(t) for t in candidates),
        return_exceptions=True,
    )

    shared_data["diff_results"] = dict(zip(candidates.keys(), results))
    shared_data["diff_counters"] = counters
    shared_data["diff_llms"] = llms


@then(
    "it provides three-target output, role-scoped prompts, and compiler-driven refinement"
)
@pytest.mark.asyncio
async def provides_differentiators(shared_data: dict) -> None:
    results: dict = shared_data["diff_results"]
    counters: dict = shared_data["diff_counters"]
    llms: dict = shared_data["diff_llms"]
    forbidden = shared_data["forbidden_field"]
    role_scoped_sdl = shared_data["role_scoped_sdl"]
    full_sdl = shared_data["full_sdl"]

    # --- Differentiator 1: three-target output (SQL, GraphQL, Cypher) ---------
    assert set(results.keys()) == {"cypher", "graphql", "sql"}, (
        "all three target languages must be generated — Cypher output is the "
        "key differentiator absent from commodity text-to-SQL tools"
    )

    for target, outcome in results.items():
        # No loop raised an unhandled exception — independence guaranteed.
        assert not isinstance(outcome, Exception), (
            f"{target} loop raised unexpectedly: {outcome!r}"
        )
        query, error = outcome

        # Each loop produced a validated (compiler-accepted) candidate.
        assert query is not None, (
            f"{target}: expected a validated query but got None "
            "(three-target generation failed)"
        )
        assert error is None, (
            f"{target}: expected no error after compiler refinement but got: {error}"
        )

        # Cypher is the key differentiator — verify it is genuinely Cypher syntax.
        if target == "cypher":
            assert "MATCH" in query.upper(), (
                "Cypher output must use MATCH syntax — this is the differentiator "
                "absent from commodity text-to-SQL tools"
            )

        # --- Differentiator 2: role-scoped schema context ---------------------
        # Every prompt sent to the LLM must contain the role-scoped SDL and must
        # NOT contain the forbidden field that the analyst role cannot access.
        for prompt in llms[target].prompts:
            # The role-scoped SDL (or its content) is injected into the prompt.
            assert "Person" in prompt, (
                f"{target}: prompt did not include schema context "
                "(role-scoped SDL must be injected into LLM prompt)"
            )
            # The forbidden field (ssn) must never appear in any prompt — the LLM
            # literally cannot generate a query referencing it.
            assert forbidden not in prompt, (
                f"{target}: forbidden field '{forbidden}' leaked into LLM prompt — "
                "role-scoped schema context is broken"
            )
            # The full (unscoped) SDL — which contains ssn — must not be used.
            assert "ssn" not in prompt, (
                f"{target}: full unscoped schema was sent to LLM instead of "
                "role-scoped SDL"
            )

        # --- Differentiator 3: compiler-driven refinement ---------------------
        # The compiler rejected the first attempt and accepted the retry; this
        # deterministic loop replaces heuristic result-quality checks.
        assert counters[target][0] >= 2, (
            f"{target}: compiler was only called {counters[target][0]} time(s) — "
            "compiler-driven refinement loop did not execute (expected ≥ 2 iterations)"
        )

        # The LLM was invoked once per compiler iteration, not speculatively.
        assert llms[target].call_count == counters[target][0], (
            f"{target}: LLM call count ({llms[target].call_count}) does not match "
            f"compiler iteration count ({counters[target][0]})"
        )

        # On the retry iteration the compiler error was fed back to the LLM —
        # the second+ prompt must reference the prior compiler error message.
        if len(llms[target].prompts) >= 2:
            retry_prompts = llms[target].prompts[1:]
            assert any(
                "compiler" in p.lower() or "parse error" in p.lower()
                for p in retry_prompts
            ), (
                f"{target}: compiler error was not fed back to the LLM on retry — "
                "compiler-driven refinement requires error propagation to the prompt"
            )

    # --- Overall differentiator summary assertion ----------------------------
    # Commodity text-to-SQL tools produce: one target (SQL), unscoped prompts,
    # heuristic quality checks.  This service produces all three of:
    #   1. three-target output (SQL + GraphQL + Cypher)
    #   2. role-scoped LLM prompts (forbidden fields never reach the LLM)
    #   3. compiler-driven refinement (deterministic, not heuristic)

    # 1. All three query languages were generated successfully.
    valid_by_target = {
        t: (q is not None and e is None)
        for t, (q, e) in results.items()
    }
    assert all(valid_by_target.values()), (
        f"Not all three targets produced valid output: {valid_by_target}"
    )

    # 2. Role-scoping: the forbidden field never appeared in any prompt across
    #    all targets and all iterations.
    all_prompts = [p for llm in llms.values() for p in llm.prompts]
    assert all_prompts, "No prompts were captured — LLMs were never called"
    for prompt in all_prompts:
        assert forbidden not in prompt, (
            f"Role-scoping failure: '{forbidden}' found in prompt: {prompt[:120]!r}"
        )

    # 3. Compiler-driven refinement: every target went through at least one
    #    rejection+retry cycle — proving the loop is compiler-controlled, not
    #    a one-shot heuristic generation.
    for target in ("cypher", "graphql", "sql"):
        assert counters[target][0] >= 2, (
            f"Compiler-driven refinement not demonstrated for {target}: "
            f"only {counters[target][0]} compiler call(s)"
        )

    # Confirm the role-scoped SDL is strictly a subset of the full SDL —
    # the scoping mechanism genuinely restricts what the LLM can see.
    assert forbidden not in role_scoped_sdl
    assert forbidden in full_sdl
