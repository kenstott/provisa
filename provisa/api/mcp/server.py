# Copyright (c) 2026 Kenneth Stott
# Canary: df1c3a33-397b-4407-b573-de06d17e5662
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FastMCP protocol adapter for Provisa (REQ-1008, phase 1).

Binds the pure tool functions in tools.py to the shared FastAPI ``AppState`` and
exposes them over MCP. Two transports:
  - local stdio: role pinned via ``PROVISA_MCP_ROLE`` (development).
  - remote Streamable HTTP: OAuth bearer token -> provisa role via the same
    OIDC path pgwire uses (build_auth_provider + resolve_role).

Role rule (CLAUDE.md): a role is REQUIRED on every call and the server NEVER
defaults to admin. When no role can be resolved the call fails loud.

search_catalog(nl_text) is the semantic "explore" surface (search.py): a
DuckDB VSS index over schema/table/column chunks, resolved up to authoritative
table branches and filtered to the caller's accessible domains.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from provisa.api.mcp import tools

log = logging.getLogger(__name__)


def resolve_token_role(token: str, state: Any) -> str:
    """Map a remote OAuth/OIDC bearer token to a provisa role.

    Reuses the exact provider + claim->role mapping pgwire uses
    (provisa.pgwire.server._authenticate_oidc). No admin default: if auth is not
    configured or the token yields no role, this raises — the MCP call then
    fails rather than silently escalating.
    """
    import asyncio

    from provisa.auth.role_mapping import resolve_role
    from provisa.auth.wiring import build_auth_provider

    auth_config = getattr(state, "auth_config", None)
    if not auth_config:
        raise PermissionError("MCP OAuth requested but no auth config is loaded")

    provider = build_auth_provider(auth_config)
    identity = asyncio.run(provider.validate_token(token))
    default_role = auth_config.get("default_role")
    if not default_role:
        # No admin fallback: a token that matches no mapping rule and has no
        # configured default_role is rejected rather than escalated.
        raise PermissionError("token matched no role and no default_role is configured")
    return resolve_role(identity, auth_config.get("role_mapping", []), default_role)


def _pinned_stdio_role() -> str:
    """The role for local stdio calls. Must be explicitly configured via
    PROVISA_MCP_ROLE — there is no admin default."""
    role = os.environ.get("PROVISA_MCP_ROLE")
    if not role or not role.strip():
        raise ValueError(
            "PROVISA_MCP_ROLE must be set to a provisa role for the local stdio MCP transport"
        )
    return role.strip()


def build_mcp_server(state: Any):
    """Build a FastMCP server whose tools are bound to ``state``.

    Each tool takes an explicit ``role`` (required). For local stdio a client
    may omit it and the pinned PROVISA_MCP_ROLE is used; a remote HTTP caller's
    role is derived from its bearer token by the transport layer and passed in.
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP(
        "provisa",
        instructions=(
            "Provisa governed catalog + SQL. Drill down with list_schemas -> "
            "list_tables -> describe_table, then run_sql / explain_sql. Every "
            "call is governed by the caller's role."
        ),
    )

    def _role(role: str | None) -> str:
        if role and str(role).strip():
            return str(role).strip()
        # stdio: fall back to the explicitly-pinned dev role (never admin).
        return _pinned_stdio_role()

    @mcp.tool()
    async def list_schemas(role: str | None = None) -> list[dict]:
        """List catalog schemas with description and table count."""
        return await tools.list_schemas(state, _role(role))

    @mcp.tool()
    async def list_tables(schema: str, role: str | None = None) -> list[dict]:
        """List tables in a schema with description and column count."""
        return await tools.list_tables(state, _role(role), schema)

    @mcp.tool()
    async def describe_table(schema: str, table: str, role: str | None = None) -> dict:
        """Describe a table: columns (name, type, description) and foreign keys."""
        return await tools.describe_table(state, _role(role), schema, table)

    @mcp.tool()
    async def list_commands(role: str | None = None) -> list[dict]:
        """List registered commands the role may invoke: name, domain, kind, arguments (REQ-1150)."""
        return tools.list_commands(state, _role(role))

    @mcp.tool()
    async def run_sql(
        sql: str,
        role: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict:
        """Execute SQL through the governed pipeline; returns row-capped JSON rows."""
        return await tools.run_sql(state, _role(role), sql, limit=limit, offset=offset)

    @mcp.tool()
    async def explain_sql(sql: str, role: str | None = None) -> dict:
        """Validate and govern a query without executing it; confirms it plans cleanly for the role."""
        return await tools.explain_sql(state, _role(role), sql)

    @mcp.tool()
    async def search_catalog(query: str, role: str | None = None, k: int = 5) -> list[dict]:
        """Semantically search the catalog for datasets matching a natural-language query.

        Returns the top table branches (columns + foreign keys + schema breadcrumb) whose
        schema/table/column detail best matches, scoped to the caller's accessible domains.
        Use this when the flat table list is too large to scan by hand.
        """
        return await tools.search_catalog(state, _role(role), query, k=k)

    return mcp


def start_mcp_server(state: Any, log_: logging.Logger | None = None) -> Any | None:
    """Start the MCP Streamable HTTP transport in a background thread.

    Opt-in via ``PROVISA_MCP_PORT`` (mirrors the bolt/pgwire optional-server
    pattern in app_startup). Returns the FastMCP instance, or None when disabled.
    Isolated here so app startup wiring stays a one-line call.
    """
    _log = log_ or log
    port_raw = os.environ.get("PROVISA_MCP_PORT", "0")
    port = int(port_raw)
    if not port:
        return None

    import threading

    import uvicorn

    # Bind host. Default 0.0.0.0 preserves the prior behavior for an explicitly-opted-in server
    # deployment (the design mandates this default — the server tier expects the MCP port reachable
    # off-box; documented per REQ-1101). The native/desktop tier turns MCP on by default and sets
    # PROVISA_MCP_HOST=127.0.0.1, so its always-on server is loopback-only (same-machine Claude
    # Desktop connector, no LAN exposure) — the safe posture for a default-on data gateway.
    host = os.environ.get("PROVISA_MCP_HOST", "0.0.0.0") or "0.0.0.0"  # nosec B104

    # Optional TLS (REQ-1106): the native tier sets PROVISA_MCP_TLS=1 so Claude Desktop's "Add custom
    # connector" (which only accepts an https:// URL) can hit https://localhost:<port>/mcp directly -
    # no stdio bridge. Best-effort with a DESIGN-MANDATED fallback: if a per-machine cert can't be
    # created, serve plain HTTP (the mcp-proxy bridge still works over http). The ACTIVE scheme is
    # published to the process env so the status endpoint tells the UI which connect path to show.
    ssl_kwargs: dict[str, str] = {}
    scheme = "http"
    if os.environ.get("PROVISA_MCP_TLS", "").strip().lower() in ("1", "true", "yes"):
        from provisa.api.mcp.tls import ensure_cert, trust_cert

        pair = ensure_cert()
        if pair:
            ssl_kwargs = {"ssl_certfile": pair[0], "ssl_keyfile": pair[1]}
            scheme = "https"
            trust_cert(pair[0])  # best-effort OS user-store trust; failure just leaves it untrusted
    os.environ["PROVISA_MCP_ACTIVE_SCHEME"] = scheme

    mcp = build_mcp_server(state)
    mcp.settings.host = host
    mcp.settings.port = port
    # MCP's DNS-rebinding protection (on by default) only accepts Host headers of localhost/127.0.0.1,
    # so a non-loopback bind (0.0.0.0 for a WSL-hosted backend, or a LAN/deployment) is rejected with
    # 421 Misdirected Request when a client connects via the machine's real IP/hostname (e.g. the WSL
    # VM IP, which is dynamic and unpredictable). That check is a BROWSER-origin defense; MCP clients
    # here are stdio bridges (mcp-proxy), not browsers, and a non-loopback bind is an explicit opt-in
    # to off-box access whose real gate is the network binding + role — not the Host header. Disable
    # it only for non-loopback binds; loopback keeps the strict default (REQ-1106).
    if host not in ("127.0.0.1", "localhost", "::1"):
        from mcp.server.transport_security import TransportSecuritySettings

        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False
        )
    app = mcp.streamable_http_app()

    def _serve() -> None:
        uvicorn.run(app, host=host, port=port, log_level="warning", **ssl_kwargs)  # nosec B104

    threading.Thread(target=_serve, daemon=True).start()
    _log.info("MCP Streamable %s server listening on %s:%d", scheme.upper(), host, port)
    return mcp
