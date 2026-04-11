# Copyright (c) 2026 Kenneth Stott
# Canary: 2993be7f-f2a1-4130-bce7-19634ebffdd4
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Single generation loop: LLM → compile-validate → refine (Phase AV, REQ-356).

Each iteration:
  1. build_prompt → send to LLM
  2. validate via compiler(query) → CompileResult
  3. If valid: return (query, None)
  4. If invalid: pass error to next iteration
  5. After max_iterations: return (None, last_error)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Literal

from provisa.nl.prompt import build_prompt, NlTarget

log = logging.getLogger(__name__)

MAX_ITERATIONS = 5


@dataclass
class CompileResult:
    """Result of compiler validation for a generated query."""

    valid: bool
    error: str | None = None


class LLMClient:
    """Minimal LLM client interface.

    Concrete implementations wrap Anthropic SDK or any chat-completion API.
    """

    async def complete(self, prompt: str) -> str:
        raise NotImplementedError


async def generation_loop(
    nl_query: str,
    target: NlTarget,
    schema_sdl: str,
    compiler: Callable[[str], CompileResult],
    llm: LLMClient,
    max_iterations: int = MAX_ITERATIONS,
) -> tuple[str | None, str | None]:
    """Run a single generate→validate loop for one target language.

    Args:
        nl_query: User's natural-language question.
        target: "cypher" | "graphql" | "sql".
        schema_sdl: Role-scoped GraphQL SDL string.
        compiler: Callable that validates a query string → CompileResult.
        llm: LLM client used to generate query text.
        max_iterations: Maximum number of generate-validate cycles.

    Returns:
        (valid_query, None) on success, or (None, last_error) on exhaustion.
    """
    prior_error: str | None = None

    for iteration in range(max_iterations):
        prompt = build_prompt(nl_query, target, schema_sdl, prior_error)
        try:
            generated = await llm.complete(prompt)
        except Exception as exc:
            log.warning("LLM call failed on iteration %d for %s: %s", iteration, target, exc)
            prior_error = f"LLM error: {exc}"
            continue

        generated = generated.strip()
        result = compiler(generated)

        if result.valid:
            return generated, None

        prior_error = result.error
        log.debug("Iteration %d/%d failed for %s: %s", iteration + 1, max_iterations, target, prior_error)

    return None, prior_error


# ---------------------------------------------------------------------------
# Compiler validators (thin wrappers over existing pipeline)
# ---------------------------------------------------------------------------

def make_cypher_compiler() -> Callable[[str], CompileResult]:
    """Return a compiler callable that validates Cypher syntax."""
    from provisa.cypher.parser import parse_cypher, CypherParseError

    def _compile(query: str) -> CompileResult:
        try:
            parse_cypher(query)
            return CompileResult(valid=True)
        except CypherParseError as exc:
            return CompileResult(valid=False, error=str(exc))

    return _compile


def make_graphql_compiler(schema: Any) -> Callable[[str], CompileResult]:
    """Return a compiler callable that validates a GraphQL query against schema."""
    from graphql import parse as gql_parse, validate as gql_validate

    def _compile(query: str) -> CompileResult:
        try:
            doc = gql_parse(query)
        except Exception as exc:
            return CompileResult(valid=False, error=f"Parse error: {exc}")
        errors = gql_validate(schema, doc)
        if errors:
            return CompileResult(valid=False, error="; ".join(str(e) for e in errors))
        return CompileResult(valid=True)

    return _compile


def make_sql_compiler() -> Callable[[str], CompileResult]:
    """Return a compiler callable that validates SQL syntax via sqlglot."""
    import sqlglot

    def _compile(query: str) -> CompileResult:
        try:
            parsed = sqlglot.parse(query, dialect="trino")
            if not parsed or parsed[0] is None:
                return CompileResult(valid=False, error="Empty or unparseable SQL")
            return CompileResult(valid=True)
        except Exception as exc:
            return CompileResult(valid=False, error=str(exc))

    return _compile
