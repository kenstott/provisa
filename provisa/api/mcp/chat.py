# Copyright (c) 2026 Kenneth Stott
# Canary: 794c8bae-86ac-4f4f-bba9-8d6b12bacb96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LLM chat agent over the MCP tools (REQ-1008).

A Claude model drives the same governed MCP tools an external agent would use —
list_schemas / list_tables / describe_table / run_sql / explain_sql /
search_catalog — via a manual tool-use loop (Anthropic Messages API). Every tool
runs under the caller's fixed role (never chosen by the model), so the agent is
bound by the same domain access + governance as any other client.

The loop yields discrete events (assistant text, tool_use, tool_result, done,
error) that the endpoint streams to the browser as SSE.
"""

# complexity-gate: allow-ble=1 reason="the agent loop reports any tool-execution failure back to the
# model as an is_error tool_result (so it can recover) instead of aborting the chat — a broad catch is
# the correct behaviour for an arbitrary tool call, and the error text is surfaced, never swallowed"

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from typing import Any

from provisa.api.mcp import tools as mcp_tools

# Tool schemas mirror the MCP tools, minus `role` — the role is pinned by the endpoint from the
# caller's identity and injected at execution, so the model can never select or escalate it.
_TOOLS: list[Any] = [
    {
        "name": "list_schemas",
        "description": "List catalog schemas with description and table count.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_tables",
        "description": "List tables in a schema with description and column count.",
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}},
            "required": ["schema"],
        },
    },
    {
        "name": "describe_table",
        "description": "Describe a table: columns (name, type, description) and foreign keys.",
        "input_schema": {
            "type": "object",
            "properties": {"schema": {"type": "string"}, "table": {"type": "string"}},
            "required": ["schema", "table"],
        },
    },
    {
        "name": "search_catalog",
        "description": (
            "Semantically search the catalog for datasets matching a natural-language query. "
            "Returns the best-matching table branches. Use when the table list is too large to scan."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "k": {"type": "integer", "description": "max results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "run_sql",
        "description": "Execute SQL through the governed pipeline; returns row-capped JSON rows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "limit": {"type": "integer"},
                "offset": {"type": "integer"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "explain_sql",
        "description": "Validate and govern a query without executing it; confirms it plans cleanly for your role.",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
        },
    },
]

_SYSTEM = (
    "You are Provisa's data assistant. You answer questions about a governed data catalog and run "
    "SQL on the user's behalf, using ONLY the provided tools. Every tool call runs under the user's "
    "role and its domain access + row/column governance — you cannot see or change the role. "
    "Prefer search_catalog to find relevant datasets, describe_table to confirm structure, and "
    "explain_sql before run_sql on non-trivial queries. Never invent table, column, or schema names — "
    "discover them with the tools. If a tool returns an error or empty result, say so plainly. Keep "
    "answers concise and lead with the answer."
)


def _resolve_model() -> str:
    """The chat model: env override → ai_models.mcp_chat config → Claude Opus 4.8 default."""
    env = os.environ.get("PROVISA_MCP_CHAT_MODEL")
    if env and env.strip():
        return env.strip()
    from provisa.api.admin._config_io import read_config

    cfg = (read_config().get("ai_models", {}) or {}).get("mcp_chat")
    if isinstance(cfg, str) and cfg.strip():
        return cfg.strip()
    if isinstance(cfg, dict) and cfg.get("model"):
        return str(cfg["model"])
    return "claude-opus-4-8"


async def _execute_tool(state: Any, role: str, name: str, args: dict) -> Any:
    """Dispatch one tool call to the governed MCP tool, with the role pinned by the endpoint."""
    if name == "list_schemas":
        return await mcp_tools.list_schemas(state, role)
    if name == "list_tables":
        return await mcp_tools.list_tables(state, role, args["schema"])
    if name == "describe_table":
        return await mcp_tools.describe_table(state, role, args["schema"], args["table"])
    if name == "search_catalog":
        return await mcp_tools.search_catalog(state, role, args["query"], k=int(args.get("k", 5)))
    if name == "run_sql":
        return await mcp_tools.run_sql(
            state,
            role,
            args["sql"],
            limit=args.get("limit"),
            offset=int(args.get("offset", 0)),
        )
    if name == "explain_sql":
        return await mcp_tools.explain_sql(state, role, args["sql"])
    raise ValueError(f"unknown tool {name!r}")


async def run_chat(
    state: Any,
    role: str,
    messages: list[dict],
    *,
    max_iterations: int = 8,
) -> AsyncIterator[dict]:
    """Drive the Claude tool-use loop, yielding UI events until the model stops calling tools.

    ``messages`` is the prior conversation ({role, content} with string content). The role is
    validated up front — an invalid role fails the whole chat rather than silently degrading.
    """
    mcp_tools.require_role(role, state)  # raises ValueError / PermissionError

    from anthropic import AsyncAnthropic

    client = AsyncAnthropic()
    model = _resolve_model()
    convo: list[Any] = list(messages)

    for _ in range(max_iterations):
        resp = await client.messages.create(
            model=model,
            max_tokens=8192,
            system=_SYSTEM,
            tools=_TOOLS,
            thinking={"type": "adaptive"},
            messages=convo,
        )
        for block in resp.content:
            if block.type == "text" and block.text:
                yield {"type": "text", "text": block.text}

        if resp.stop_reason != "tool_use":
            break

        # Append the assistant turn VERBATIM (thinking + tool_use blocks preserved for replay).
        convo.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for block in resp.content:
            if block.type != "tool_use":
                continue
            yield {"type": "tool_use", "name": block.name, "input": block.input}
            try:
                result = await _execute_tool(state, role, block.name, dict(block.input))
                content = json.dumps(result, default=str)
                is_error = False
            except Exception as exc:  # noqa: BLE001 - reported back to the model, see pragma
                content = f"Error: {exc}"
                is_error = True
            yield {"type": "tool_result", "name": block.name, "is_error": is_error}
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                    "is_error": is_error,
                }
            )
        convo.append({"role": "user", "content": tool_results})

    yield {"type": "done"}
