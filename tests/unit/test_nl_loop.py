# Copyright (c) 2026 Kenneth Stott
# Canary: 46922297-2782-4403-a8f5-bb5b39fae709
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/nl/loop.py."""

import pytest

from provisa.nl.loop import CompileResult, LLMClient, generation_loop


_SDL = "type Query { persons: [Person] }\ntype Person { id: ID! name: String }"


class _FixedLLM(LLMClient):
    """Returns a fixed sequence of responses."""

    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self._index = 0
        self.call_count = 0

    async def complete(self, prompt: str) -> str:
        self.call_count += 1
        resp = self._responses[self._index % len(self._responses)]
        self._index += 1
        return resp


def _always_valid(query: str) -> CompileResult:
    return CompileResult(valid=True)


def _always_invalid(query: str) -> CompileResult:
    return CompileResult(valid=False, error="Syntax error")


def _valid_on_second(call_count: list) -> callable:
    def _compile(query: str) -> CompileResult:
        call_count[0] += 1
        if call_count[0] >= 2:
            return CompileResult(valid=True)
        return CompileResult(valid=False, error="First attempt invalid")
    return _compile


@pytest.mark.asyncio
async def test_valid_on_first_attempt_returns_immediately():
    llm = _FixedLLM(["MATCH (n) RETURN n"])
    query, error = await generation_loop("find nodes", "cypher", _SDL, _always_valid, llm, max_iterations=5)
    assert query == "MATCH (n) RETURN n"
    assert error is None
    assert llm.call_count == 1


@pytest.mark.asyncio
async def test_invalid_then_valid_retries():
    llm = _FixedLLM(["bad query", "MATCH (n) RETURN n"])
    call_count = [0]
    compiler = _valid_on_second(call_count)
    query, error = await generation_loop("find nodes", "cypher", _SDL, compiler, llm, max_iterations=5)
    assert query == "MATCH (n) RETURN n"
    assert error is None
    assert llm.call_count == 2


@pytest.mark.asyncio
async def test_exhausts_max_iterations():
    llm = _FixedLLM(["bad"] * 10)
    query, error = await generation_loop("find nodes", "cypher", _SDL, _always_invalid, llm, max_iterations=3)
    assert query is None
    assert error is not None
    assert llm.call_count == 3


@pytest.mark.asyncio
async def test_compiler_called_once_per_iteration():
    call_count = [0]

    def _compiler(query: str) -> CompileResult:
        call_count[0] += 1
        return CompileResult(valid=False, error="err")

    llm = _FixedLLM(["q"] * 5)
    await generation_loop("q", "sql", _SDL, _compiler, llm, max_iterations=4)
    assert call_count[0] == 4


@pytest.mark.asyncio
async def test_prior_error_passed_to_next_iteration():
    """Ensure the prompt on retry mentions the prior error."""
    prompts_seen: list[str] = []

    class _CaptureLLM(LLMClient):
        async def complete(self, prompt: str) -> str:
            prompts_seen.append(prompt)
            return "bad"

    compiler_calls = [0]

    def _compiler(query: str) -> CompileResult:
        compiler_calls[0] += 1
        return CompileResult(valid=False, error="specific error msg")

    await generation_loop("q", "sql", _SDL, _compiler, _CaptureLLM(), max_iterations=2)
    # Second prompt should include the error from the first iteration
    assert len(prompts_seen) == 2
    assert "specific error msg" in prompts_seen[1]
