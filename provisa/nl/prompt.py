# Copyright (c) 2026 Kenneth Stott
# Canary: 7a06ff66-0973-4905-99ec-758d32701f9d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LLM prompt builder for NL → query generation (Phase AV, REQ-355).

Each target language gets a distinct system prompt with:
  - Role-scoped GraphQL SDL as schema context
  - Target-specific instructions (dialect, patterns)
  - Prior compiler error for self-correction on retry
"""

from __future__ import annotations

from typing import Literal

NlTarget = Literal["cypher", "graphql", "sql"]

_TARGET_INSTRUCTIONS: dict[NlTarget, str] = {
    "cypher": (
        "Generate a read-only Cypher query (no CREATE, MERGE, SET, DELETE, DETACH, REMOVE).\n"
        "Use MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/SKIP/LIMIT only.\n"
        "Variable-length paths must have an explicit upper bound (e.g. [*..5]).\n"
        "Node labels and relationship types come from the schema context below.\n"
        "Return only the Cypher query — no explanation, no markdown fences."
    ),
    "graphql": (
        "Generate a GraphQL query (read-only, no mutations or subscriptions).\n"
        "Use only fields and types present in the schema SDL below.\n"
        "Return only the GraphQL query — no explanation, no markdown fences."
    ),
    "sql": (
        "Generate a read-only SQL SELECT statement using semantic table references.\n"
        "Table references must use the form \"domain\".\"field_name\" exactly as shown in the schema SDL.\n"
        "Use only tables and columns present in the schema SDL below.\n"
        "Do not use vendor-specific syntax; write standard SQL (postgres dialect).\n"
        "Return only the SQL statement — no explanation, no markdown fences."
    ),
}


def build_prompt(
    nl_query: str,
    target: NlTarget,
    schema_sdl: str,
    prior_error: str | None = None,
) -> str:
    """Build the full LLM prompt for a single generation iteration.

    Args:
        nl_query: The user's natural-language question.
        target: One of "cypher", "graphql", "sql".
        schema_sdl: Role-scoped GraphQL SDL string (schema context).
        prior_error: Compiler error from the previous iteration, or None.

    Returns:
        A single string prompt ready to send to the LLM.
    """
    instructions = _TARGET_INSTRUCTIONS[target]

    parts = [
        f"INSTRUCTIONS:\n{instructions}",
        f"\nSCHEMA CONTEXT (GraphQL SDL, role-scoped):\n{schema_sdl}",
        f"\nQUESTION:\n{nl_query}",
    ]

    if prior_error:
        parts.append(
            f"\nPREVIOUS ATTEMPT FAILED WITH ERROR:\n{prior_error}\n"
            "Correct the query to fix this error."
        )

    return "\n".join(parts)
