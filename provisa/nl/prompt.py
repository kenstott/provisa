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

from provisa.nl.sql_group_by import GROUP_BY_GUIDANCE

NlTarget = Literal["cypher", "graphql", "sql", "grpc", "jsonapi", "openapi"]

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
        "The query must select exactly ONE root field — one question is one query. When the "
        "question spans two tables, reach the second through a nested relationship field on the "
        "first, or group/aggregate over the relationship; never list two tables side by side as "
        "separate root fields.\n"
        "Always wrap the query with a named operation: query SomeCamelCaseName { ... } "
        "where the name is a concise CamelCase slug of the question (e.g. UsersWithInquiryCount).\n"
        "GROUP BY and aggregations (COUNT, SUM, AVG, MIN, MAX) ARE supported, but not via bare "
        "SQL-style syntax — every aggregatable type exposes two extra root query fields "
        "alongside its plain list field (look for them in the schema SDL below, spelled either "
        "<type>_aggregate/<type>_group_by or <Type>Aggregate/<Type>GroupBy depending on the "
        "schema's naming convention):\n"
        "  - <type>_aggregate(where: ...): { aggregate { count sum { ... } avg { ... } min { ... } "
        "max { ... } } nodes { ... } } — use this for a single aggregate over the whole set "
        "(the question has no 'by <dimension>' / 'per <dimension>' phrase).\n"
        "  - <type>_group_by(by: [...], where: ..., having: ..., order_by: ..., limit: ...): "
        "[{ groupKey aggregate { count sum { ... } avg { ... } min { ... } max { ... } } "
        "nodes { ... } }] — use this whenever the question groups/aggregates 'by' or 'per' a "
        "dimension. `by` takes the grouping column(s) as an enum list. Each returned row's "
        "`groupKey` is the JSON group key; `aggregate` holds the measure(s); `nodes` is the full "
        "list of underlying rows in that group — request nested relationship fields under `nodes` "
        "(e.g. nodes { pet { id name } }) to get 'details' alongside the aggregate, instead of "
        "trying to flatten or dedupe anything yourself. There is no separate DISTINCT concept — "
        "`nodes` already lists every row belonging to the group.\n"
        "Example — question: 'count of inquiries by user, with pet details':\n"
        "  query InquiriesCountByUser {\n"
        "    inquiries_group_by(by: [userId]) {\n"
        "      groupKey\n"
        "      aggregate { count }\n"
        "      nodes { id pet { id name species } }\n"
        "    }\n"
        "  }\n"
        "If neither an _aggregate nor a _group_by field exists for the relevant type in the "
        "schema below, fall back to the plain list field with the raw rows the client would need "
        "to aggregate itself. Never respond NOT_APPLICABLE — always return the best approximation.\n"
        "Return only the GraphQL query — no explanation, no markdown fences."
    ),
    "sql": (
        "Generate a read-only SQL SELECT statement.\n"
        "Use the GraphQL type names from the schema SDL exactly as the SQL table names "
        "(e.g. if the SDL defines 'type ps_users', the SQL table name is ps_users).\n"
        "Do NOT prefix table names with any schema or catalog (no 'public.', no catalog qualifiers).\n"
        "Use only tables and columns present in the schema SDL below.\n"
        "Do not use vendor-specific syntax; write standard SQL (postgres dialect).\n"
        "SQL can express everything GraphQL and Cypher can and more (GROUP BY, aggregates, joins, "
        "window functions) — always generate a SQL query. Never respond NOT_APPLICABLE.\n"
        f"{GROUP_BY_GUIDANCE}\n"
        "Return only the SQL statement — no explanation, no markdown fences."
    ),
}


def format_entities(entities: list, table_roles: dict[str, str] | None = None) -> str:  # REQ-464
    """Render a list of SchemaEntity into a compact exact-name reference block.

    ``table_roles`` (table name → "fact" | "dimension") tags each table with its
    star-schema role, and metric entities render as a METRICS section with the
    semantic addressing form, so the model sees the star shape (REQ-1319, REQ-1320).
    """
    if not entities:
        return ""
    roles = table_roles or {}
    tables: dict[str, list[str]] = {}
    metrics: list = []
    for e in entities:
        if e.kind == "table":
            tables.setdefault(e.exact_name, [])
        elif e.kind == "field" and e.parent:
            tables.setdefault(e.parent, []).append(e.exact_name)
        elif e.kind == "metric":
            metrics.append(e)
    lines = ["EXACT SCHEMA NAMES (use these verbatim — do not guess or alter case):"]
    for table, fields in tables.items():
        field_list = ", ".join(fields) if fields else "(no fields matched)"
        role_tag = f" [{roles[table]}]" if table in roles else ""
        lines.append(f"  table: {table}{role_tag}  fields: {field_list}")
    if metrics:
        lines.append(
            "METRICS (governed aggregates — query as: "
            "SELECT <dimensions>, value FROM metrics.<name> GROUP BY <dimensions>):"
        )
        for m in metrics:
            desc = f" — {m.description}" if m.description else ""
            lines.append(f"  metric: {m.exact_name}{desc}")
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
