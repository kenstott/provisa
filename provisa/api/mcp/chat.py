# Copyright (c) 2026 Kenneth Stott
# Canary: 794c8bae-86ac-4f4f-bba9-8d6b12bacb96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LLM chat agent over the MCP tools (REQ-1008), plus UI-actuation tools (REQ-1795).

A Claude model drives the same governed MCP tools an external agent would use —
list_schemas / list_tables / describe_table / run_sql / explain_sql /
search_catalog — via a manual tool-use loop (Anthropic Messages API). Every tool
runs under the caller's fixed role (never chosen by the model), so the agent is
bound by the same domain access + governance as any other client.

The loop yields discrete events (assistant text, tool_use, tool_result, done,
error) that the endpoint streams to the browser as SSE.

CLIENT TOOLS (REQ-1795): a second tool category — `navigate` and admin-mutation
dispatch — that only make sense in the browser (router, Apollo client, confirm
dialogs). The server never executes these. When the model calls one, the loop
stops mid-turn and yields `awaiting_client_tools` carrying the verbatim assistant
content plus any ALREADY-computed server tool_results from the same turn. The
frontend executes the pending client tool(s) itself, appends the assistant
message and a matching tool_result message to its own history, and POSTs the
whole history back to /admin/mcp/chat to resume — the endpoint is already
stateless per-request (the caller resends full history each time), so resuming
needs no new server-side session state."""

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
    {
        "name": "propose_source",
        "description": (
            "Propose a newly-discovered data source for a human to review and register "
            "(REQ-1792). Normally queues a pending creation request a rights-holder must execute "
            "or reject on the admin UI's Requests page — NEVER creates a live source directly. "
            "EXCEPTION (REQ-1799): if the caller's own role already holds source_registration, "
            "this instead returns status 'confirm_required' without queuing anything — see the "
            "system prompt for what to do then. Use this when asked to find/add/connect a new "
            "data source: propose it here rather than just describing it in prose."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "object",
                    "description": (
                        "SourceInput-shaped: at minimum {id, type}, plus whatever of "
                        "host/port/database/username/password/path/description/allowed_domains "
                        "is known."
                    ),
                },
                "reason": {
                    "type": "string",
                    "description": "Why this source is being proposed.",
                },
            },
            "required": ["source", "reason"],
        },
    },
    {
        "name": "propose_table",
        "description": (
            "Propose registering a table from an already-registered source, for a human to "
            "approve (REQ-1792). Normally queues a pending creation request the same way "
            "propose_source does. EXCEPTION (REQ-1799): if the caller's own role already holds "
            "table_registration, this instead returns status 'confirm_required' without queuing "
            "anything — see the system prompt for what to do then."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "object",
                    "description": (
                        "TableInput-shaped: at minimum {source_id, domain_id, schema_name, "
                        "table_name, columns} where each column is at least "
                        "{name, visible_to}."
                    ),
                },
                "reason": {
                    "type": "string",
                    "description": "Why this table is being proposed.",
                },
            },
            "required": ["table", "reason"],
        },
    },
    {
        "name": "create_source_now",
        "description": (
            "Create a live Source immediately, bypassing the Requests-page review queue "
            "(REQ-1799). ONLY call this after propose_source returned 'confirm_required' AND the "
            "user has explicitly confirmed in THIS conversation that they want it created now — "
            "never call it speculatively or without that confirmation. Takes the exact same "
            "source/reason propose_source was called with."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "object",
                    "description": "Same shape as propose_source's `source`.",
                },
                "reason": {
                    "type": "string",
                    "description": "Same reason passed to propose_source.",
                },
            },
            "required": ["source", "reason"],
        },
    },
    {
        "name": "register_table_now",
        "description": (
            "Register a live Table immediately, bypassing the Requests-page review queue "
            "(REQ-1799). ONLY call this after propose_table returned 'confirm_required' AND the "
            "user has explicitly confirmed in THIS conversation that they want it registered now. "
            "Takes the exact same table/reason propose_table was called with."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "object",
                    "description": "Same shape as propose_table's `table`.",
                },
                "reason": {"type": "string", "description": "Same reason passed to propose_table."},
            },
            "required": ["table", "reason"],
        },
    },
    {
        "name": "search_govdata_subjects",
        "description": (
            "Check GovData (askamerica) FIRST for a topic-matching source, before web_search or "
            "propose_source (REQ-1798) — GovData and Kaggle are this org's subscription "
            "sources. Matches a free-text topic (e.g. 'inflation', 'unemployment', 'crime "
            "rates') against GovData's real schema/table catalog and reports whether each match's "
            "subject is already subscribed for this org."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "search_kaggle_datasets",
        "description": (
            "Check Kaggle SECOND (after search_govdata_subjects, REQ-1798) for a topic-matching "
            "public dataset, before web_search or propose_source. Reads the user's Kaggle API "
            "token from a FIXED secret name — 'kaggle_api_token' — never a raw token argument and "
            "never a name you choose or ask the user to choose. If this tool errors saying that "
            "secret doesn't exist, tell the user to create one on the Secrets page (/admin/secrets) "
            "named EXACTLY 'kaggle_api_token' with their Kaggle API token as its value, then retry."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
]

# REQ-1795: executed in the BROWSER, never on the server — see the module docstring. Kept as a
# short, explicit, hand-maintained set for this first slice of the UI-actuation plumbing; a later
# pass generalizes admin-mutation dispatch to the full admin_schema via introspection instead of
# one hardcoded tool per mutation.
_CLIENT_TOOLS: list[Any] = [
    {
        "name": "navigate",
        "description": "Navigate the browser to a route within the Provisa app (e.g. '/sources').",
        "input_schema": {
            "type": "object",
            "properties": {"route": {"type": "string"}},
            "required": ["route"],
        },
    },
    {
        "name": "refresh_mv",
        "description": (
            "Trigger a manual refresh of a materialized view. Shows the user a confirmation "
            "dialog before running — if they decline, the tool result says so."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"mv_id": {"type": "string"}},
            "required": ["mv_id"],
        },
    },
    {
        "name": "present_choice",
        "description": (
            "Ask the user to pick from a small set of options via a real UI widget — a multiple "
            "choice list, a checklist, or a yes/no — instead of asking them to type a free-text "
            "reply. Use this whenever you're offering the user a decision among a handful of "
            "concrete options (e.g. 'which source?', 'which of these tables?', 'proceed?') rather "
            "than describing the options in prose and waiting for them to type one back. "
            "mode='single' renders radio buttons and returns the one option string picked; "
            "mode='multi' renders checkboxes and returns an array of the options picked (zero or "
            "more); mode='yes_no' renders Yes/No buttons and returns a boolean, no 'options' "
            "needed. The tool result names exactly which option(s) — or true/false — were picked."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "What you're asking."},
                "mode": {"type": "string", "enum": ["single", "multi", "yes_no"]},
                "options": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Required for 'single'/'multi'; omit for 'yes_no'.",
                },
            },
            "required": ["question", "mode"],
        },
    },
]
_CLIENT_TOOL_NAMES = frozenset(t["name"] for t in _CLIENT_TOOLS)

# REQ-1796: Anthropic's own HOSTED tools — executed entirely on Anthropic's servers within the
# same model turn, never by us or the browser. A third tool category alongside _TOOLS (executed
# here) and _CLIENT_TOOLS (executed by the browser): the SDK returns their results as ordinary
# content blocks (server_tool_use / web_search_tool_result / web_fetch_tool_result) in the SAME
# response, so `resp.stop_reason` stays "end_turn" and the existing loop needs no new branch to
# execute them — only to surface a UI badge so the user can see a search/fetch happened.
# max_uses bounds cost/latency per call, matching run_sql's row cap elsewhere in this module.
_WEB_TOOLS: list[Any] = [
    {"type": "web_search_20260209", "name": "web_search", "max_uses": 5},
    {
        "type": "web_fetch_20260309",
        "name": "web_fetch",
        "max_uses": 5,
        "max_content_tokens": 50_000,
    },
]
_SERVER_HOSTED_TOOL_NAMES = frozenset(t["name"] for t in _WEB_TOOLS)

_SYSTEM = (
    'You are Polly, Provisa\'s assistant — the name is a nod to "poly" (many): many sources, many '
    "engines, one governed catalog. You answer questions about a governed data catalog, run SQL on "
    "the user's behalf, and can navigate the app or trigger admin actions, using ONLY the provided "
    "tools. Every tool call runs under the user's role and its domain access + row/column "
    "governance — you cannot see or change the role. Prefer search_catalog to find relevant "
    "datasets, describe_table to confirm structure, and explain_sql before run_sql on non-trivial "
    "queries. Never invent table, column, or schema names — discover them with the tools. If a "
    "tool returns an error or empty result, say so plainly. Keep answers concise and lead with the "
    "answer. When you're offering the user a decision among a handful of concrete options — which "
    "source, which table, proceed or not, pick some of these — call present_choice rather than "
    "listing options in prose and waiting for a typed reply; it's a real UI widget (multiple "
    "choice, checklist, or yes/no) and reads better than typing a number or a name back.\n\n"
    "If asked to find, add, connect, or register a new data source, or a new table from an "
    "existing source: first check search_catalog/list_schemas to avoid proposing a duplicate. "
    "Then, for a topical data request (e.g. 'find me inflation data'), check this org's "
    "subscription sources BEFORE the open web (REQ-1798): ALWAYS call BOTH "
    "search_govdata_subjects AND search_kaggle_datasets — never stop after the first one finds a "
    "match and ask the user whether to check the other; check both unconditionally, in the same "
    "turn, then present whichever option(s) you found (there may be one, both, or neither). "
    "search_kaggle_datasets reads the user's Kaggle API token from a secret named exactly "
    "'kaggle_api_token'. Never ask the user to paste a token into chat or to choose a secret name; "
    "if the tool errors because that secret doesn't exist, just tell them to create one on the "
    "Secrets page (/admin/secrets) named 'kaggle_api_token' with their Kaggle API token as the "
    "value, then retry. Only once GovData and Kaggle have both been checked and neither has a "
    "match, fall back to web_search/web_fetch to find real connection "
    "details (API base URL, auth scheme, official docs) for a brand-new external source. If more "
    "than one candidate was found (multiple GovData subjects, multiple Kaggle datasets, or a mix "
    "of both), you do not yet know which one the user wants — ask via present_choice (mode="
    "'single') listing each candidate, and WAIT for their answer. Only after they pick one, call "
    "propose_source or propose_table for that ONE chosen candidate with your best-effort "
    "connection details and a reason. If exactly one candidate was found, present_choice (mode="
    "'yes_no') with a genuinely NEUTRAL question — 'Would you like to create the <name> "
    "source/table?' — never 'request' (that word itself already implies the queued/pending "
    "outcome, which you don't know yet — most callers testing this hold the capability and will "
    "get 'confirm_required', where nothing is ever queued, making 'request' simply wrong) and "
    "never 'now'/'directly' either (that implies the opposite, equally unknown, outcome). Never "
    "call propose_source or propose_table for a candidate the user has not yet confirmed in this "
    "conversation — asking 'which one?' in plain text and then proposing one anyway without an "
    "actual answer is wrong. "
    "For a GovData match, the 'source' is the subject/schema itself (say what subscribing would "
    "require if the org isn't subscribed yet); for a Kaggle match, propose type='files' with "
    'federation_hints_json set to \'{"kaggle_owner": "<owner>", "kaggle_ref": "<ref>"}\' '
    "(split the dataset's ref on its '/') and any placeholder for path — the server downloads the "
    "real files and overwrites path itself when the source is actually created (REQ-1819), so "
    "there is no separate staging/download step to call here or anywhere else. ALWAYS use the "
    "tool rather than only describing the source in prose. If none of these tools can find "
    "something (an API key, "
    "a customer-specific hostname), say what's still needed rather than inventing it.\n\n"
    "propose_source/propose_table normally queue a pending request a human approves on the "
    "Requests page and never create anything directly. REQ-1799 EXCEPTION: if the result's status "
    "is 'confirm_required' instead of 'pending', nothing was queued — the caller's own role "
    "already holds the needed capability, so a review queue is irrelevant here and must never be "
    "mentioned. In that case, ask via present_choice (mode='yes_no') a plain, simple question — "
    "'Create the <name> source now?' / 'Register the <name> table now?' — nothing about queuing, "
    "review, or 'instead of'; wait for their answer. Only if they answer yes, call "
    "create_source_now/register_table_now with the exact same source/table and reason. If they "
    "answer no, do nothing further — do not queue it as a fallback and do not create it. Never "
    "call create_source_now or register_table_now without that explicit confirmation in the same "
    "conversation."
)


def _system_prompt(current_route: str | None) -> str:
    """_SYSTEM plus the frontend's current route, when it sent one (REQ-1800).

    There is no tool for "what page is the user on" — the browser already knows its own route on
    every turn, so the frontend just sends it (ChatPanel.tsx) rather than the model spending a
    tool round-trip to ask for something the caller could hand over for free. Kept OUT of _SYSTEM
    itself since it changes per turn, not per deployment."""
    if not current_route:
        return _SYSTEM
    return f"{_SYSTEM}\n\nThe user is currently viewing the app at route: {current_route!r}."


async def _effective_config(state: Any) -> dict:
    """The acting org's resolved config (REQ-1349) for vendor/model/endpoint lookups.

    Delegates to mcp_tools.effective_config when an org context (state.tenant_db) is bound, so an
    org's ai_models.mcp_chat/ai_endpoints choice made through the AI Models UI is honored on the
    very next chat turn, no restart. With no tenant_db bound, reads the bare deployment file
    (read_config()) — the same fallback this module's functions always used before REQ-1349 wiring,
    kept distinct from mcp_tools.effective_config's own fallback (state.config) because chat.py's
    tests configure the deployment file via PROVISA_CONFIG, not a fake state.config object."""
    tenant_db = getattr(state, "tenant_db", None)
    if tenant_db is None:
        from provisa.api.admin._config_io import read_config

        return read_config()
    return await mcp_tools.effective_config(state)


async def _resolve_vendor(state: Any) -> str:
    """The vendor configured for ai_models.mcp_chat, defaulting to 'anthropic'.

    REQ-1349: reads the ACTING ORG's config (_effective_config — deployment config with that
    org's org_settings overrides layered on, request-time resolved), not the static
    deployment-only config — an org that picks a vendor through the AI Models UI writes an
    org_settings row, and this must see it on the very next chat turn, no restart.

    REQ-1797: run_chat() branches on this — 'anthropic' uses Anthropic's own Messages API
    (including the hosted web_search/web_fetch tools, REQ-1796); anything else goes through
    aisuite's OpenAI-normalized tool-calling (openai/ollama/google/...), with no hosted web tools
    (each vendor's own equivalent, where one exists, is differently shaped — see _run_chat_aisuite)."""
    cfg = await _effective_config(state)
    op_cfg = (cfg.get("ai_models", {}) or {}).get("mcp_chat")
    vendor = op_cfg.get("vendor") if isinstance(op_cfg, dict) else None
    return vendor or "anthropic"


async def _llm_configured(state: Any) -> tuple[bool, str]:
    """Whether a usable credential (and, for a non-Anthropic vendor, an explicit model) exists
    for mcp_chat's configured vendor (REQ-1349, REQ-1794, REQ-1797).

    Mirrors ProvisaLLMClient's resolution (REQ-1790 custom endpoints, else the vendor's own
    default env var), but against the acting org's resolved config (mcp_tools.effective_config)
    rather than the deployment file alone — see _resolve_vendor. No fallback model guess for a
    non-Anthropic vendor (CLAUDE.md): the admin must name one explicitly in AI Models settings."""
    import os

    cfg = await _effective_config(state)
    vendor = await _resolve_vendor(state)

    endpoints = {
        ep["id"]: ep for ep in (cfg.get("ai_endpoints", []) or []) if ep.get("enabled", True)
    }
    endpoint = endpoints.get(vendor)
    if endpoint is not None:
        key_env = endpoint.get("api_key_env")
        if key_env and not os.environ.get(key_env):
            return False, f"the {vendor!r} endpoint needs {key_env} set"
    elif vendor == "ollama":
        pass  # REQ-1797: no credential — a local server, reachable at OLLAMA_API_URL or the default
    elif vendor == "google":
        # REQ-1797: aisuite's Google provider is Vertex AI, not the simpler Gemini API-key auth —
        # it needs a GCP project + service-account credentials, not one key.
        missing = [
            v
            for v in ("GOOGLE_PROJECT_ID", "GOOGLE_REGION", "GOOGLE_APPLICATION_CREDENTIALS")
            if not os.environ.get(v)
        ]
        if missing:
            return False, f"google (Vertex AI) needs {', '.join(missing)} set"
    else:
        env_var = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}.get(vendor)
        if env_var and not os.environ.get(env_var):
            return False, f"{env_var} is not set"

    if vendor != "anthropic" and not await _resolve_model(state):
        return False, f"no model is set for vendor {vendor!r}"
    return True, ""


async def _resolve_model(state: Any) -> str | None:
    """The chat model: env override → the acting org's resolved ai_models.mcp_chat config
    (REQ-1349, via mcp_tools.effective_config — see _resolve_vendor) → Claude Opus 4.8 IF the
    vendor is (implicitly or explicitly) anthropic, else None (REQ-1797: no fallback guess for a
    vendor we don't know a safe default model for — the admin names one explicitly)."""
    env = os.environ.get("PROVISA_MCP_CHAT_MODEL")
    if env and env.strip():
        return env.strip()
    cfg = (await _effective_config(state)).get("ai_models", {}) or {}
    op_cfg = cfg.get("mcp_chat")
    if isinstance(op_cfg, str) and op_cfg.strip():
        return op_cfg.strip()
    if isinstance(op_cfg, dict) and op_cfg.get("model"):
        return str(op_cfg["model"])
    return "claude-opus-4-8" if await _resolve_vendor(state) == "anthropic" else None


async def _execute_tool(
    state: Any, role: str, name: str, args: dict, *, request: Any = None
) -> Any:
    """Dispatch one tool call to the governed MCP tool, with the role pinned by the endpoint.

    `request` is the real, AuthMiddleware-verified HTTP request behind this chat turn (REQ-1799)
    — passed through to the handful of tools (propose_source/propose_table's confirm_required
    check, create_source_now, register_table_now) that need the caller's verified identity to
    safely bypass the REQ-434 review queue. Every other tool ignores it."""
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
    if name == "propose_source":
        return await mcp_tools.propose_source(
            state, role, args["source"], args["reason"], request=request
        )
    if name == "propose_table":
        return await mcp_tools.propose_table(
            state, role, args["table"], args["reason"], request=request
        )
    if name == "create_source_now":
        if request is None:
            raise ValueError("create_source_now requires a verified request context")
        return await mcp_tools.create_source_now(
            state, role, args["source"], args["reason"], request=request
        )
    if name == "register_table_now":
        if request is None:
            raise ValueError("register_table_now requires a verified request context")
        return await mcp_tools.register_table_now(
            state, role, args["table"], args["reason"], request=request
        )
    if name == "search_govdata_subjects":
        return await mcp_tools.search_govdata_subjects(state, role, args["query"])
    if name == "search_kaggle_datasets":
        return await mcp_tools.search_kaggle_datasets(state, role, args["query"])
    raise ValueError(f"unknown tool {name!r}")


def _to_openai_tool(t: dict) -> dict:
    """Our Anthropic-shaped tool def (name/description/input_schema) -> the OpenAI/aisuite
    function-tool shape every non-Anthropic aisuite provider expects (REQ-1797)."""
    return {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    }


def _wire_convo_to_openai_messages(convo: list[dict]) -> list[dict]:
    """The WIRE conversation format (Anthropic-shaped content blocks — what every vendor's SSE
    events and the frontend's resume POST already use, REQ-1795) -> aisuite/OpenAI chat messages
    (REQ-1797). Keeping the wire format vendor-neutral like this means useMcpChat.ts needs no
    vendor awareness at all: it always stores/replays wire blocks; only the SERVER, per vendor,
    translates them to what that vendor's API actually needs."""
    out: list[dict] = []
    for msg in convo:
        role, content = msg["role"], msg["content"]
        if isinstance(content, str):
            out.append({"role": role, "content": content})
            continue
        if role == "assistant":
            text = "\n".join(b["text"] for b in content if b.get("type") == "text")
            tool_calls = [
                {
                    "id": b["id"],
                    "type": "function",
                    "function": {"name": b["name"], "arguments": json.dumps(b.get("input", {}))},
                }
                for b in content
                if b.get("type") == "tool_use"
            ]
            entry: dict[str, Any] = {"role": "assistant", "content": text or None}
            if tool_calls:
                entry["tool_calls"] = tool_calls
            out.append(entry)
        else:
            # A user-role wire turn carrying tool_result blocks from a prior paused turn — each
            # becomes its own role="tool" message (OpenAI's convention; Anthropic bundles them
            # into one user-role message instead, which is what `content` still looks like here).
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    out.append(
                        {
                            "role": "tool",
                            "tool_call_id": b["tool_use_id"],
                            "content": b.get("content", ""),
                        }
                    )
                else:
                    out.append({"role": role, "content": content})
                    break
    return out


def _openai_message_to_wire_blocks(message: Any) -> list[dict]:
    """An aisuite/OpenAI-shaped response message -> the shared wire content-block format
    (REQ-1797) — so the SSE events this vendor path emits are indistinguishable, on the wire,
    from Anthropic's."""
    blocks: list[dict] = []
    text = getattr(message, "content", None)
    if text:
        blocks.append({"type": "text", "text": text})
    for tc in getattr(message, "tool_calls", None) or []:
        try:
            args = json.loads(tc.function.arguments or "{}")
        except (ValueError, TypeError):
            args = {}
        blocks.append({"type": "tool_use", "id": tc.id, "name": tc.function.name, "input": args})
    return blocks


def _split_tool_calls(calls: list[dict]) -> tuple[list[dict], list[dict]]:
    """Normalized {id, name, input} tool calls -> (client_calls, server_calls)."""
    client_calls = [c for c in calls if c["name"] in _CLIENT_TOOL_NAMES]
    server_calls = [c for c in calls if c["name"] not in _CLIENT_TOOL_NAMES]
    return client_calls, server_calls


async def _run_chat_anthropic(
    state: Any,
    role: str,
    convo: list[Any],
    *,
    max_iterations: int,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """The original REQ-1008/1795/1796 loop: Anthropic's own Messages API, including the
    Anthropic-only hosted web_search/web_fetch tools. Unchanged behavior from before REQ-1797."""
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic()
    model = await _resolve_model(state)
    assert model is not None  # _llm_configured() guarantees this for vendor == "anthropic"
    system = _system_prompt(current_route)

    for _ in range(max_iterations):
        resp = await client.messages.create(
            model=model,
            max_tokens=8192,
            system=system,
            tools=_TOOLS + _CLIENT_TOOLS + _WEB_TOOLS,
            thinking={"type": "adaptive"},
            messages=convo,
        )
        for block in resp.content:
            if block.type == "text" and block.text:
                yield {"type": "text", "text": block.text}
            # REQ-1796: Anthropic already executed these (see _WEB_TOOLS' note) — no dispatch
            # needed, just a UI badge so the user can see a search/fetch happened.
            elif block.type == "server_tool_use" and block.name in _SERVER_HOSTED_TOOL_NAMES:
                yield {"type": "tool_use", "name": block.name, "input": block.input}
            elif block.type == "web_search_tool_result":
                content_type = getattr(getattr(block, "content", None), "type", "")
                yield {
                    "type": "tool_result",
                    "name": "web_search",
                    "is_error": content_type == "web_search_tool_result_error",
                }
            elif block.type == "web_fetch_tool_result":
                content_type = getattr(getattr(block, "content", None), "type", "")
                yield {
                    "type": "tool_result",
                    "name": "web_fetch",
                    "is_error": content_type == "web_fetch_tool_result_error",
                }

        if resp.stop_reason != "tool_use":
            break

        tool_use_blocks = [b for b in resp.content if b.type == "tool_use"]
        client_blocks = [b for b in tool_use_blocks if b.name in _CLIENT_TOOL_NAMES]
        server_blocks = [b for b in tool_use_blocks if b.name not in _CLIENT_TOOL_NAMES]

        tool_results = []
        for block in server_blocks:
            yield {"type": "tool_use", "name": block.name, "input": block.input}
            try:
                result = await _execute_tool(
                    state, role, block.name, dict(block.input), request=request
                )
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

        if client_blocks:
            # REQ-1795: stop here — the browser must execute these, then resume by POSTing the
            # full history again with the assistant content below plus a tool_result for every
            # block.id in `pending` (including the server ones already computed above).
            for block in client_blocks:
                yield {"type": "tool_use", "name": block.name, "input": block.input, "client": True}
            yield {
                "type": "awaiting_client_tools",
                "assistant_content": [b.model_dump() for b in resp.content],
                "server_tool_results": tool_results,
                "pending": [{"id": b.id, "name": b.name, "input": b.input} for b in client_blocks],
            }
            yield {"type": "done"}
            return

        # No client tools this turn — append and continue exactly as before.
        convo.append({"role": "assistant", "content": resp.content})
        convo.append({"role": "user", "content": tool_results})

    yield {"type": "done"}


async def _resolve_aisuite_routing(state: Any, vendor: str, model: str) -> tuple[str, dict]:
    """(model_id, provider_configs) for aisuite, given the resolved vendor/model (REQ-1808).

    ``vendor`` is EITHER one of aisuite's own named providers (openai, ollama, google, ...) — in
    which case aisuite already knows how to reach it and needs no override — OR the `id` of a
    configured custom AI endpoint (REQ-1790: an OpenAI- or Anthropic-wire-protocol server aisuite
    has no name for, e.g. a local Ollama, LiteLLM, or OpenRouter gateway). aisuite's own model
    string can only ever name a provider IT recognizes (``Client.create`` looks `provider_key` up
    in its fixed registry and rejects anything else), so a custom endpoint is reached by
    overriding the underlying "openai"/"anthropic" provider's `base_url` (whichever the
    endpoint's `style` says it speaks) via `provider_configs`, and the model string names THAT
    provider, not the endpoint's own id.

    EVERY field taken from config here — base_url as much as the API key — is resolved through
    ``${secret:NAME}``/``${user:NAME}`` when its value uses that grammar (provisa.core.secrets),
    not just the key: an operator may just as reasonably want a private gateway's base_url held
    in the vault (it can itself carry embedded credentials, e.g. a signed URL or a ``user:pass@``
    authority) as they want the API key held there. The same indirection provisa.core.org_secrets
    already gives named vendor keys, generalized to every config field on a custom endpoint.
    """
    cfg = await _effective_config(state)
    endpoints = {ep["id"]: ep for ep in (cfg.get("ai_endpoints") or [])}
    endpoint = endpoints.get(vendor)
    if endpoint is None:
        return f"{vendor}:{model}", {}

    style = endpoint["style"]  # "openai" | "anthropic"
    api_key = await _resolve_api_key_field(endpoint.get("api_key_env"))
    resolved_base_url = await _resolve_config_field(endpoint["base_url"])
    provider_config: dict[str, Any] = {"base_url": resolved_base_url}
    if api_key:
        provider_config["api_key"] = api_key
    elif style == "openai":
        # aisuite's OpenAI provider requires a non-empty api_key even when the server behind
        # base_url doesn't check one at all (e.g. Ollama's OpenAI-compatible endpoint) — a
        # placeholder here is not a credential, just satisfying that constructor guard.
        provider_config["api_key"] = "not-required"
    return f"{style}:{model}", {style: provider_config}


async def _resolve_config_field(value: str) -> str:
    """``value`` as-is, unless it contains a ``${env:NAME}``/``${secret:NAME}``/``${user:NAME}``
    reference (provisa.core.secrets' full reference grammar), in which case that's resolved
    first — so ANY custom-endpoint field can be a literal, an env-var indirection, or a vault
    reference, the caller's choice, not a fixed convention per field.

    Only ``${secret:...}``/``${user:...}`` need the org vault bound (see
    secrets_store.bound_to_request_org) — ``${env:...}``/``${scope:...}`` resolve against the
    process environment alone, so requiring org context for THOSE too would needlessly fail a
    deployment with no admin_db reachable from this call for a field that never touches the vault.
    """
    if "${" not in value:
        return value
    from provisa.core.secrets import resolve_secrets

    if "${secret:" in value or "${user:" in value:
        from provisa.core.secrets_store import bound_to_request_org

        async with bound_to_request_org():
            return resolve_secrets(value)
    return resolve_secrets(value)


async def _resolve_api_key_field(api_key_env: str | None) -> str | None:
    """The endpoint's API key, from its ``api_key_env`` field (REQ-1790, REQ-1808).

    Despite the field's name, it is no longer ONLY "the name of an env var": a value containing
    ``${...}`` is resolved directly as the reference it is (``${secret:NAME}``, ``${env:NAME}``,
    ``${user:NAME}``) — the key itself, not a variable naming where to find it. A plain string
    with no ``${`` keeps the field's original, narrower meaning: literally an env var name, whose
    OWN value is then resolved the same way (so an env var can itself hold a ``${secret:...}``
    reference one layer down, e.g. for a credential injected by the deployment environment that
    should still resolve against the org's vault)."""
    if not api_key_env:
        return None
    if "${" in api_key_env:
        return await _resolve_config_field(api_key_env)
    raw = os.environ.get(api_key_env)
    return await _resolve_config_field(raw) if raw else None


async def _run_chat_aisuite(
    state: Any,
    role: str,
    convo: list[Any],
    vendor: str,
    *,
    max_iterations: int,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """REQ-1797: the non-Anthropic path — openai/ollama/google (or any other aisuite-supported
    vendor), via aisuite's OpenAI-normalized tool-calling. No hosted web_search/web_fetch: those
    are Anthropic-proprietary (see _WEB_TOOLS); this path gets none, since each vendor's own
    equivalent (OpenAI's Responses-API web tool, Gemini's Search grounding, Ollama's none at all)
    is differently shaped and not unified by aisuite.

    Speaks the SAME wire format as the Anthropic path (Anthropic-shaped content blocks) for every
    SSE event and for `convo` itself — see _wire_convo_to_openai_messages/
    _openai_message_to_wire_blocks — so useMcpChat.ts on the frontend needs no vendor awareness.

    Calls the aisuite PROVIDER directly (``ProviderFactory.create_provider`` + its own
    ``chat_completions_create``), not ``aisuite.Client().chat.completions.create()`` — verified
    live (REQ-1809) that aisuite 0.1.14's own wrapper POPS ``tools`` out of kwargs and only ever
    forwards it back to the provider when ``max_turns`` is also given, which instead routes
    through aisuite's OWN automatic tool-execution loop (`Client._tool_runner`) — one that
    requires real Python callables, not the JSON tool schemas this loop's own governed dispatch
    needs. Passing `tools` with no `max_turns`, the documented-looking way, silently drops it: the
    model never saw a single tool definition and could never have called one, on ANY non-Anthropic
    vendor, since REQ-1797 first shipped. Calling the provider directly is exactly what
    ``Client.create()`` itself does in the max_turns-less case — except this actually keeps
    `tools` in the call."""
    import asyncio

    from aisuite.provider import ProviderFactory

    model = await _resolve_model(state)
    assert model is not None  # _llm_configured() guarantees this
    model_id, provider_configs = await _resolve_aisuite_routing(state, vendor, model)
    provider_key, model_name = model_id.split(":", 1)
    provider = ProviderFactory.create_provider(provider_key, provider_configs.get(provider_key, {}))
    openai_tools = [_to_openai_tool(t) for t in _TOOLS + _CLIENT_TOOLS]
    system_message = {"role": "system", "content": _system_prompt(current_route)}

    for _ in range(max_iterations):
        oa_messages = [system_message, *_wire_convo_to_openai_messages(convo)]
        # aisuite providers are synchronous (no async client) — off the event loop, same reasoning
        # as every other sync-SDK call in this codebase (e.g. provisa/llm/client.py).
        resp = await asyncio.to_thread(
            provider.chat_completions_create, model_name, oa_messages, tools=openai_tools
        )
        message = resp.choices[0].message
        if message.content:
            yield {"type": "text", "text": message.content}

        if resp.choices[0].finish_reason != "tool_calls" or not getattr(
            message, "tool_calls", None
        ):
            break

        wire_blocks = _openai_message_to_wire_blocks(message)
        calls = [b for b in wire_blocks if b["type"] == "tool_use"]
        client_calls, server_calls = _split_tool_calls(calls)

        tool_results = []
        for call in server_calls:
            yield {"type": "tool_use", "name": call["name"], "input": call["input"]}
            try:
                result = await _execute_tool(
                    state, role, call["name"], call["input"], request=request
                )
                content = json.dumps(result, default=str)
                is_error = False
            except Exception as exc:  # noqa: BLE001 - reported back to the model, see pragma
                content = f"Error: {exc}"
                is_error = True
            yield {"type": "tool_result", "name": call["name"], "is_error": is_error}
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": call["id"],
                    "content": content,
                    "is_error": is_error,
                }
            )

        if client_calls:
            for call in client_calls:
                yield {
                    "type": "tool_use",
                    "name": call["name"],
                    "input": call["input"],
                    "client": True,
                }
            yield {
                "type": "awaiting_client_tools",
                "assistant_content": wire_blocks,
                "server_tool_results": tool_results,
                "pending": client_calls,
            }
            yield {"type": "done"}
            return

        convo.append({"role": "assistant", "content": wire_blocks})
        convo.append({"role": "user", "content": tool_results})

    yield {"type": "done"}


async def run_chat(
    state: Any,
    role: str,
    messages: list[dict],
    *,
    max_iterations: int = 8,
    request: Any = None,
    current_route: str | None = None,
) -> AsyncIterator[dict]:
    """Drive the model's tool-use loop, yielding UI events until it stops calling tools.

    ``messages`` is the prior conversation ({role, content} with string content, or the richer
    wire-block shape when resuming after a client-tool pause — REQ-1795). The role is validated
    up front — an invalid role fails the whole chat rather than silently degrading. Dispatches to
    Anthropic's own API (REQ-1008/1796) or aisuite's multi-vendor path (REQ-1797) depending on
    ai_models.mcp_chat's configured vendor; both speak the identical wire format, so the caller
    (and the frontend) never need to know which one is active.

    ``request`` is the real, AuthMiddleware-verified HTTP request behind this call (REQ-1799) —
    threaded through to create_source_now/register_table_now and propose_source/propose_table's
    confirm_required check, which need the caller's verified identity to safely bypass the
    REQ-434 review queue. Optional so existing/test callers that don't need that path keep working.

    ``current_route`` is the frontend's own current URL (REQ-1800), sent with every turn — this
    answers "what page am I on" without a tool round-trip, since the browser already knows its
    own route for free; see _system_prompt.
    """
    mcp_tools.require_role(role, state)  # raises ValueError / PermissionError

    configured, reason = await _llm_configured(state)
    if not configured:
        yield {
            "type": "error",
            "error": (
                "Polly isn't configured yet"
                + (f" ({reason})" if reason else "")
                + ". Set a vendor, model, and API key in Settings, then try again."
            ),
            "action": {"label": "Open AI Models settings", "route": "/admin/ai-models"},
        }
        yield {"type": "done"}
        return

    vendor = await _resolve_vendor(state)
    convo: list[Any] = list(messages)
    impl = (
        _run_chat_anthropic(
            state,
            role,
            convo,
            max_iterations=max_iterations,
            request=request,
            current_route=current_route,
        )
        if vendor == "anthropic"
        else _run_chat_aisuite(
            state,
            role,
            convo,
            vendor,
            max_iterations=max_iterations,
            request=request,
            current_route=current_route,
        )
    )
    async for event in impl:
        yield event
