# Copyright (c) 2026 Kenneth Stott
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

from __future__ import annotations

import os

from fastapi import APIRouter

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
        "description": "Return the governed execution plan (route + physical SQL) without executing.",
    },
]


def mcp_status() -> dict:
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
        "stdio_role": role,
        "max_rows": max_rows,
        "tools": _TOOLS,
        "enable_env_var": "PROVISA_MCP_PORT",
        "role_env_var": "PROVISA_MCP_ROLE",
    }


@router.get("/admin/mcp-server")
async def get_mcp_server():  # REQ-1008
    """Effective MCP server status (enabled, port, transport, bound role, tools)."""
    return mcp_status()
