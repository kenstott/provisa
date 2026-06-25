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

# Requirements: REQ-355, REQ-356, REQ-358

from __future__ import annotations

from typing import Literal

NlTarget = Literal["cypher", "graphql", "sql"]

_NOT_APPLICABLE_RULE = (
    "If the question cannot be naturally expressed as a {lang} query "
    "(e.g. it requires features absent from the language), "
    "respond with exactly the token: NOT_APPLICABLE"
)

_TARGET_INSTRUCTIONS: dict[NlTarget, str] = {
    "cypher": (
        "Generate a read-only Cypher query (no CREATE, MERGE, SET, DELETE, DETACH, REMOVE).\n"
        "Use MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/SKIP/LIMIT only.\n"
        "Variable-length paths must have an explicit upper bound (e.g. [*..5]).\n"
        "Use ONLY the node labels and relationship types listed in the GRAPH SCHEMA block below — "
        "do not invent or guess labels.\n"
        "For aggregations like COUNT, use WITH + RETURN.\n"
        "Cypher can express everything SQL can and more — always generate a Cypher query. "
        "Never respond NOT_APPLICABLE.\n"
        "Return only the Cypher query — no explanation, no markdown fences."
    ),
    "graphql": (
        "Generate a GraphQL query (read-only, no mutations or subscriptions).\n"
        "Use only fields and types present in the schema SDL below.\n"
        "Always wrap the query with a named operation: query SomeCamelCaseName { ... } "
        "where the name is a concise CamelCase slug of the question (e.g. UsersWithInquiryCount).\n"
        "GraphQL cannot perform GROUP BY or aggregations (COUNT, SUM, AVG, etc.) — "
        "it can only return fields defined in the schema. "
        "When the question asks for an aggregation or grouping, return the closest meaningful query instead: "
        "fetch the raw rows that would be grouped (e.g. for 'count inquiries per user', "
        "return inquiries with their nested user fields so the client can count). "
        "Never respond NOT_APPLICABLE — always return the best approximation.\n"
        "Return only the GraphQL query — no explanation, no markdown fences."
    ),
    "sql": (
        "Generate a read-only SQL SELECT statement.\n"
        "Use the GraphQL type names from the schema SDL exactly as the SQL table names "
        "(e.g. if the SDL defines 'type ps_users', the SQL table name is ps_users).\n"
        "Do NOT prefix table names with any schema or catalog (no 'public.', no catalog qualifiers).\n"
        "Use only tables and columns present in the schema SDL below.\n"
        "Do not use vendor-specific syntax; write standard SQL (postgres dialect).\n"
        + _NOT_APPLICABLE_RULE.format(lang="SQL")
        + "\n"
        "Return only the SQL statement or NOT_APPLICABLE — no explanation, no markdown fences."
    ),
}


def format_entities(entities: list) -> str:
    """Render a list of SchemaEntity into a compact exact-name reference block."""
    if not entities:
        return ""
    tables: dict[str, list[str]] = {}
    for e in entities:
        if e.kind == "table":
            tables.setdefault(e.exact_name, [])
        elif e.kind == "field" and e.parent:
            tables.setdefault(e.parent, []).append(e.exact_name)
    lines = ["EXACT SCHEMA NAMES (use these verbatim — do not guess or alter case):"]
    for table, fields in tables.items():
        field_list = ", ".join(fields) if fields else "(no fields matched)"
        lines.append(f"  table: {table}  fields: {field_list}")
    return "\n".join(lines)


def build_prompt(  # REQ-355, REQ-356
    nl_query: str,
    target: NlTarget,
    schema_sdl: str,
    prior_error: str | None = None,
    relevant_entities: str = "",
) -> str:
    """Build the full LLM prompt for a single generation iteration."""
    instructions = _TARGET_INSTRUCTIONS[target]

    parts = [f"INSTRUCTIONS:\n{instructions}"]

    if relevant_entities:
        parts.append(f"\n{relevant_entities}")

    parts.append(f"\nSCHEMA CONTEXT (GraphQL SDL, role-scoped):\n{schema_sdl}")
    parts.append(f"\nQUESTION:\n{nl_query}")

    if prior_error:
        parts.append(
            f"\nPREVIOUS ATTEMPT FAILED WITH ERROR:\n{prior_error}\n"
            "Correct the query to fix this error."
        )

    return "\n".join(parts)
