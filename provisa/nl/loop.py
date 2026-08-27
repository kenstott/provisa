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

# Requirements: REQ-355, REQ-356

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from provisa.nl.prompt import NlTarget, build_prompt

if TYPE_CHECKING:
    from graphql import GraphQLSchema

log = logging.getLogger(__name__)

MAX_ITERATIONS = 5


@dataclass
class CompileResult:
    """Result of compiler validation for a generated query."""

    valid: bool
    error: str | None = None


class LLMClient:  # REQ-464
    """Minimal LLM client interface.

    Concrete implementations wrap Anthropic SDK or any chat-completion API.
    """

    async def complete(self, prompt: str) -> str:
        raise NotImplementedError


async def generation_loop(  # REQ-355, REQ-356
    nl_query: str,
    target: NlTarget,
    schema_sdl: str,
    compiler: Callable[[str], CompileResult | Awaitable[CompileResult]],
    llm: LLMClient,
    max_iterations: int = MAX_ITERATIONS,
    relevant_entities: str = "",
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
        prompt = build_prompt(nl_query, target, schema_sdl, prior_error, relevant_entities)
        try:
            generated = await llm.complete(prompt)
        except Exception as exc:
            log.warning("LLM call failed on iteration %d for %s: %s", iteration, target, exc)
            prior_error = f"LLM error: {exc}"
            continue

        generated = generated.strip()
        # No target's prompt permits NOT_APPLICABLE (REQ-355/356) — treat a stray one as an
        # ordinary invalid generation and retry with corrective feedback, same as any compile
        # failure, instead of bailing out on the LLM's first non-compliant response.
        if generated == "NOT_APPLICABLE":
            prior_error = (
                "NOT_APPLICABLE is not a valid response — this language can express the "
                "question. Generate the actual query."
            )
            continue
        # Strip leading "cypher" keyword some LLMs emit for Cypher queries
        if target == "cypher" and generated.lower().startswith("cypher "):
            generated = generated[7:].lstrip()
        # A compiler may be async when validation runs the real downstream compile (the strict
        # chain compiles the GraphQL to SQL and parses it), so the loop refines against the same
        # SQL the panel will show rather than against a syntax check the SQL never sees.
        compiled = compiler(generated)
        result = await compiled if inspect.isawaitable(compiled) else compiled

        if result.valid:
            return generated, None

        prior_error = result.error
        log.debug(
            "Iteration %d/%d failed for %s: %s", iteration + 1, max_iterations, target, prior_error
        )

    return None, prior_error


# ---------------------------------------------------------------------------
# Compiler validators (thin wrappers over existing pipeline)
# ---------------------------------------------------------------------------


def make_cypher_compiler() -> Callable[[str], CompileResult]:  # REQ-464
    """Return a compiler callable that validates Cypher syntax."""
    from provisa.cypher.parser import CypherParseError, parse_cypher

    def _compile(query: str) -> CompileResult:
        try:
            parse_cypher(query)
            return CompileResult(valid=True)
        except CypherParseError as exc:
            return CompileResult(valid=False, error=str(exc))

    return _compile


def _check_single_root_field(doc: object) -> CompileResult:  # object-ok: graphql DocumentNode
    """One question is one query: reject a document with more than one root field.

    Everything downstream of the generated GraphQL is single-query shaped — the strict chain
    renders one semantic SQL statement, and the gRPC/JSON:API/OpenAPI branches synthesize one
    request. A document with two root fields compiles to two SQL statements, which the SQL
    branch then shows as one unparseable statement while the protocol branches silently answer
    only the first field. Caught here so the generation loop feeds it back and the model
    rewrites the question as a single field (a join or a group_by), instead of the split
    surfacing as a downstream parse error.
    """
    from graphql import FieldNode, OperationDefinitionNode

    operations = [d for d in doc.definitions if isinstance(d, OperationDefinitionNode)]  # type: ignore[attr-defined]
    if len(operations) != 1:
        return CompileResult(
            valid=False,
            error=(
                f"The document defines {len(operations)} operations. Return exactly one query "
                "operation answering the whole question."
            ),
        )
    roots = operations[0].selection_set.selections
    if len(roots) != 1:
        names = ", ".join(s.name.value for s in roots if isinstance(s, FieldNode))
        return CompileResult(
            valid=False,
            error=(
                f"The query selects {len(roots)} root fields ({names}). Return exactly one root "
                "field: join the tables through a nested relationship field, or use the type's "
                "_group_by/_aggregate field, rather than listing each table separately."
            ),
        )
    return CompileResult(valid=True)


def make_graphql_compiler(schema: GraphQLSchema) -> Callable[[str], CompileResult]:  # REQ-464
    """Return a compiler callable that validates a GraphQL query against schema."""
    from graphql import parse as gql_parse
    from graphql import validate as gql_validate

    def _compile(query: str) -> CompileResult:
        try:
            doc = gql_parse(query)
        except Exception as exc:
            return CompileResult(valid=False, error=f"Parse error: {exc}")
        errors = gql_validate(schema, doc)
        if errors:
            return CompileResult(valid=False, error="; ".join(str(e) for e in errors))
        return _check_single_root_field(doc)

    return _compile


def make_sql_compiler() -> Callable[[str], CompileResult]:  # REQ-464
    """Return a compiler callable that validates SQL syntax and GROUP BY semantics via sqlglot."""
    import sqlglot

    from provisa.nl.sql_group_by import check_distinct_json_agg, check_group_by_semantics

    def _compile(query: str) -> CompileResult:
        try:
            parsed = sqlglot.parse(query, dialect="postgres")
            if not parsed or parsed[0] is None:
                return CompileResult(valid=False, error="Empty or unparseable SQL")
        except Exception as exc:
            return CompileResult(valid=False, error=str(exc))

        for stmt in parsed:
            error = check_group_by_semantics(stmt) or check_distinct_json_agg(stmt)
            if error:
                return CompileResult(valid=False, error=error)
        return CompileResult(valid=True)

    return _compile
