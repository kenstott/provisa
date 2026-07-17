# Copyright (c) 2026 Kenneth Stott
# Canary: f9e238be-fe85-424f-88a4-6aa0652ee30d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Read-only MCP server status for the admin UI (REQ-1008).

Reflects the SAME runtime/env configuration ``start_mcp_server`` reads
(PROVISA_MCP_PORT / PROVISA_MCP_ROLE / PROVISA_MCP_MAX_ROWS). No new persisted
config is invented and no fallback is applied: an unset port means the server is
disabled, an unset stdio role is reported as null (the transport fails loud on
use, exactly as ``_pinned_stdio_role`` does).
"""

# complexity-gate: allow-ble=1 reason="the SSE chat stream converts any LLM/transport failure into a
# terminal error event so the browser sees a clean message instead of a broken stream — the error
# text is surfaced to the client, never swallowed"

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()

# The tools the MCP server exposes (server.build_mcp_server). Static: the tool
# surface is fixed in code, not configuration.
_TOOLS = [
    {
        "name": "list_schemas",
        "description": "List catalog schemas with description and table count.",
    },
    {
        "name": "list_tables",
        "description": "List tables in a schema with description and column count.",
    },
    {
        "name": "describe_table",
        "description": "Describe a table: columns (name, type, description) and foreign keys.",
    },
    {
        "name": "run_sql",
        "description": "Execute SQL through the governed pipeline; returns row-capped JSON rows.",
    },
    {
        "name": "explain_sql",
        "description": "Validate and govern a query without executing it; confirms it plans cleanly for your role.",
    },
    {
        "name": "search_catalog",
        "description": "Semantically search the catalog for datasets matching a natural-language query.",
    },
]


def _resolve_mcp_url(request: Request | None, port: int) -> str | None:
    """The Streamable HTTP URL a client (e.g. Claude Desktop) connects to (REQ-1102).

    Server-side we cannot authoritatively know our externally-reachable domain (reverse proxy,
    k8s ingress, NAT), so this is best-effort and the UI renders it EDITABLE — never a silently
    wrong URL. Resolution order:
      1. PROVISA_MCP_EXTERNAL_URL — an explicit operator override for proxied deployments.
      2. The host the browser reached the UI on (X-Forwarded-Host / Host, port stripped) — correct
         for the native/desktop tier (localhost) and a sane default elsewhere. The MCP server runs
         on its OWN port, so only the hostname is reused; the port is always PROVISA_MCP_PORT.
    """
    if not port:
        return None
    override = os.environ.get("PROVISA_MCP_EXTERNAL_URL")
    if override and override.strip():
        return override.strip()
    proto = "http"
    hostname = "localhost"
    if request is not None:
        proto = request.headers.get("x-forwarded-proto") or request.url.scheme or "http"
        raw_host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        # Strip any UI port from the host header — MCP has its own port. IPv6 hosts are bracketed.
        if raw_host:
            hostname = raw_host.rsplit(":", 1)[0] if ":" in raw_host and not raw_host.endswith(
                "]"
            ) else raw_host
    return f"{proto}://{hostname}:{port}/mcp"


def mcp_status(request: Request | None = None) -> dict:
    """The MCP server's effective config, read from the same env the start hook uses."""
    port_raw = os.environ.get("PROVISA_MCP_PORT", "0")
    port = int(port_raw) if port_raw.strip() else 0
    enabled = bool(port)

    role_raw = os.environ.get("PROVISA_MCP_ROLE")
    role = role_raw.strip() if role_raw and role_raw.strip() else None

    max_rows_raw = os.environ.get("PROVISA_MCP_MAX_ROWS")
    max_rows = int(max_rows_raw) if max_rows_raw and max_rows_raw.strip() else 1000

    return {
        "enabled": enabled,
        "port": port if enabled else None,
        # Streamable HTTP is the only transport start_mcp_server binds a port for.
        "transport": "streamable-http" if enabled else None,
        # The connect URL (editable in the UI) — None when disabled. REQ-1102.
        "url": _resolve_mcp_url(request, port),
        # The host-accessible interpreter that runs the bundled mcp-proxy stdio bridge for Claude
        # Desktop (REQ-1104). Set ONLY by the native launcher (PROVISA_MCP_BRIDGE_COMMAND = the
        # ~/.provisa/runtime python that ships mcp-proxy). Deliberately NOT sys.executable: on the
        # container tier that would be the in-VM python, which host Claude Desktop cannot launch.
        # None -> the panel shows the "supply your own bridge" note instead of a broken config.
        "bridge_command": os.environ.get("PROVISA_MCP_BRIDGE_COMMAND") or None,
        "bridge_args": ["-m", "mcp_proxy", "--transport", "streamablehttp"],
        "stdio_role": role,
        "max_rows": max_rows,
        "tools": _TOOLS,
        "enable_env_var": "PROVISA_MCP_PORT",
        "role_env_var": "PROVISA_MCP_ROLE",
    }


@router.get("/admin/mcp-server")
async def get_mcp_server(request: Request):  # REQ-1008
    """Effective MCP server status (enabled, port, transport, connect URL, bound role, tools)."""
    return mcp_status(request)


@router.post("/admin/mcp/search-catalog")
async def mcp_search_catalog(request: Request):  # REQ-1008
    """Browser-callable wrapper over the MCP ``search_catalog`` tool.

    The MCP transport speaks the MCP protocol; the UI Explore page needs plain
    HTTP, so this exposes the same governed tool. Role comes from the
    ``x-provisa-role`` header (or the body), and results are filtered to that
    role's accessible domains exactly as the MCP tool does.
    """
    from provisa.api.app import state
    from provisa.api.mcp import tools

    body = await request.json()
    role = request.headers.get("x-provisa-role") or body.get("role") or ""
    query = body.get("query", "")
    k = int(body.get("k", 5))
    try:
        results = await tools.search_catalog(state, role, query, k=k)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"results": results}


@router.post("/admin/mcp/chat")
async def mcp_chat(request: Request):  # REQ-1008
    """Stream an LLM chat that drives the MCP tools, governed by the caller's role.

    Server-Sent Events: each line is ``data: {json}`` with a ``type`` of text / tool_use /
    tool_result / done / error. Role comes from the ``x-provisa-role`` header (or body).
    """
    import json as _json

    from fastapi.responses import StreamingResponse

    from provisa.api.app import state
    from provisa.api.mcp.chat import run_chat

    body = await request.json()
    role = request.headers.get("x-provisa-role") or body.get("role") or ""
    messages = body.get("messages") or []

    async def _events():
        try:
            async for event in run_chat(state, role, messages):
                yield f"data: {_json.dumps(event)}\n\n"
        except (PermissionError, ValueError) as exc:
            yield f"data: {_json.dumps({'type': 'error', 'error': str(exc)})}\n\n"
        except Exception as exc:  # noqa: BLE001 - surface any LLM/transport failure to the client
            yield f"data: {_json.dumps({'type': 'error', 'error': str(exc)})}\n\n"

    return StreamingResponse(_events(), media_type="text/event-stream")
