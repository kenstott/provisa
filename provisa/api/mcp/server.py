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
from contextvars import ContextVar
from typing import Any

from provisa.api.mcp import tools
from provisa.api.org_resolve import OrgResolutionError
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security.rights import can_act_cross_org, capabilities_for_claims

log = logging.getLogger(__name__)


# Per-request role resolved from the remote HTTP bearer token (REQ-1105). Set by the transport
# middleware for the duration of one Streamable-HTTP request so the bound tools pick it up without
# the caller passing an explicit ``role`` argument (a remote MCP client sends a token, not a role).
_request_role: ContextVar[str | None] = ContextVar("provisa_mcp_request_role", default=None)

# REQ-1857: the validated identity + resolved org for a remote HTTP call, set alongside
# ``_request_role`` — needed only by the handful of tools that carry their own capability check
# (glossary/data-product/metric writes) via ``require_capability_request``/``require_active_org_id``,
# which read ``request.state.identity``/``request.state.active_org_id``. MCP resolves identity/org
# through its own bearer-token path (below), never the HTTP app's AuthMiddleware/_OrgRoutingMiddleware,
# so those admin-gate functions need a request-shaped shim built from these instead of a real Request.
_request_identity: ContextVar[Any | None] = ContextVar("provisa_mcp_request_identity", default=None)
_request_org_id: ContextVar[str | None] = ContextVar("provisa_mcp_request_org_id", default=None)


async def _validate_mcp_token(token: str, state: Any):
    """Validate a remote MCP client's credential and return its identity (REQ-1105, REQ-1263).

    MCP carries exactly one credential presentation — an HTTP bearer token — so the bearer
    validator is selected by name rather than calling ``validate_token``, whose meaning differs
    per provider. The platform pool is passed through so a personal access token resolves here
    the same way it does on every other surface.
    """
    from provisa.auth.models import validator_for_scheme
    from provisa.auth.throttle import throttled
    from provisa.auth.wiring import build_auth_provider

    auth_config = getattr(state, "auth_config", None)
    if not auth_config:
        raise PermissionError("MCP OAuth requested but no auth config is loaded")

    provider = build_auth_provider(auth_config, admin_pool=getattr(state, "admin_db", None))
    validator = validator_for_scheme(provider, "bearer")
    if validator is None:
        raise PermissionError(
            f"auth provider {provider.provider_name!r} accepts no bearer credential, "
            "so it cannot authenticate an MCP client"
        )
    # REQ-1393: MCP names no principal, so the throttle keys on the credential digest.
    return await throttled(validator, token, principal=None)


async def _resolve_token_role_async(token: str, state: Any) -> str:
    """Async core of :func:`resolve_token_role` — safe to await inside a running event loop.

    Reuses the exact provider + claim->role mapping pgwire uses. No admin default: if auth is not
    configured or the token yields no role, this raises so the MCP call fails rather than escalating.
    """
    return _role_for_identity(await _validate_mcp_token(token, state), state)


def _role_for_identity(identity: Any, state: Any) -> str:
    """The provisa role a validated MCP identity acts as (REQ-1105)."""
    from provisa.auth.role_mapping import resolve_role

    auth_config = state.auth_config
    default_role = auth_config.get("default_role")
    if not default_role:
        # No admin fallback: a token that matches no mapping rule and has no
        # configured default_role is rejected rather than escalated.
        raise PermissionError("token matched no role and no default_role is configured")
    return resolve_role(identity, auth_config.get("role_mapping", []), default_role)


async def _resolve_token_org_async(token: str, state: Any) -> str | None:
    """Resolve the org a remote MCP session binds from its bearer token (REQ-1266).

    Returns None for single-org deployments (leave ``current_org`` unset → default runtime).
    Under multitenancy, validates the token and maps its subject to an org via the shared
    membership rule (:func:`resolve_session_org`); raises on an unresolvable principal.
    """
    if not getattr(state, "multitenancy", False):
        return None
    return await _org_for_identity(await _validate_mcp_token(token, state), state)


async def _org_for_identity(identity: Any, state: Any) -> str | None:
    """The org a validated MCP identity binds, or None on a single-org deployment (REQ-1266)."""
    if not getattr(state, "multitenancy", False):
        return None
    from provisa.api.org_resolve import resolve_session_org

    # REQ-1337: resolve the claims to RIGHTS and test cross_org — never the role name.
    caps = capabilities_for_claims(
        getattr(identity, "roles", []) or [], getattr(state, "roles", {})
    )
    return await resolve_session_org(
        state,
        user_id=getattr(identity, "user_id", None),
        can_act_any_org=can_act_cross_org(caps),
        requested_org=getattr(identity, "active_org_id", None),
    )


def resolve_token_role(token: str, state: Any) -> str:
    """Map a remote OAuth/OIDC bearer token to a provisa role (sync wrapper, REQ-1105).

    For a synchronous caller only — the Streamable-HTTP transport awaits
    :func:`_resolve_token_role_async` directly. Raises rather than defaulting to admin.
    """
    import asyncio

    return asyncio.run(_resolve_token_role_async(token, state))


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
    from mcp.server.fastmcp import Context, FastMCP

    # ``from __future__ import annotations`` stringizes the ``ctx: Context`` annotation on the
    # run_sql tool; FastMCP evaluates tool annotations against the module globals, so expose Context
    # there (the import is deferred to keep ``mcp`` optional at module import time).
    globals()["Context"] = Context

    async def _emit_mcp_nag(ctx: Context) -> None:
        """Emit the REQ-1137 license nag once per session as an MCP notifications/message (an
        out-of-band log notification — never part of a tool result). Best-effort; never fails a call."""
        try:
            from provisa.licensing import emit as _lic_emit

            key = f"mcp:{getattr(ctx, 'client_id', None) or id(getattr(ctx, 'session', ctx))}"
            msg = _lic_emit.nag_for_connection(key)
            if msg:
                await ctx.info(msg.replace("\n", " "))
        except Exception:
            log.debug("MCP license nag emission skipped", exc_info=True)

    mcp = FastMCP(
        "provisa",
        instructions=(
            "Provisa governed catalog + SQL. Drill down with list_schemas -> "
            "list_tables -> describe_table, then run_sql / explain_sql. Look up "
            "business vocabulary with search_terms. Every call is governed by "
            "the caller's role."
        ),
    )

    def _role(role: str | None) -> str:
        if role and str(role).strip():
            return str(role).strip()
        # Remote HTTP: the transport middleware resolved the bearer token to a role for this
        # request (REQ-1105); prefer it over any ambient stdio role.
        req_role = _request_role.get()
        if req_role and req_role.strip():
            return req_role.strip()
        # stdio: fall back to the explicitly-pinned dev role (never admin).
        return _pinned_stdio_role()

    def _capability_request(resolved_role: str) -> Any:  # REQ-1857
        """A minimal request-shaped shim for the handful of tools that carry their own
        capability check (glossary/data-product/metric writes) via
        ``require_capability_request``/``require_active_org_id`` — see the ContextVars' own
        docstring for why a real Request isn't available here."""
        from types import SimpleNamespace

        identity = _request_identity.get()
        org_id = _request_org_id.get()
        if identity is None:
            # stdio: no bearer token, so no real identity — capabilities resolve purely from
            # the pinned role's own capability list (capabilities_for_claims keys on role id,
            # not on anything else the identity carries), so a synthetic identity naming just
            # that one role resolves correctly.
            identity = SimpleNamespace(user_id="mcp-stdio", roles=[resolved_role])
            org_id = org_id or getattr(state, "org_id", None)
        return SimpleNamespace(state=SimpleNamespace(identity=identity, active_org_id=org_id))

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
        """List registered commands the role may invoke: name, domain, kind, arguments (REQ-1156)."""
        return tools.list_commands(state, _role(role))

    @mcp.tool()
    async def run_sql(
        sql: str,
        ctx: Context,
        role: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict:
        """Execute SQL through the governed pipeline; returns row-capped JSON rows."""
        result = await tools.run_sql(state, _role(role), sql, limit=limit, offset=offset)
        await _emit_mcp_nag(ctx)  # REQ-1137: out-of-band license nag, once per session
        return result

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

    @mcp.tool()
    async def search_terms(query: str, role: str | None = None, limit: int = 25) -> list[dict]:
        """Look up business-glossary terms by name or definition (REQ-1387).

        Each hit carries the term's definition, its physical refs (every table/column
        that means this concept), typed relationships to other terms, and the experts
        who can answer questions about it. Use it to ground vocabulary before
        composing SQL — e.g. find every physical field that means "order date".
        """
        return await tools.search_terms(state, _role(role), query, limit=limit)

    @mcp.tool()
    async def list_metrics(role: str | None = None) -> list[dict]:
        """List governed metric definitions — agents select meanings by name instead of
        composing aggregation SQL (REQ-1319).

        Each entry carries name, description, ai_context (definition text written for AI
        consumers), datatype, and from_fact. Follow up with query_metric to evaluate one
        at a chosen grain.
        """
        return tools.list_metrics(state, _role(role))

    @mcp.tool()
    async def query_metric(
        metric: str,
        dimensions: list[str] | None = None,
        filters: str | None = None,
        role: str | None = None,
    ) -> list[dict]:
        """Query a governed metric grouped by caller-chosen dimensions (REQ-1319).

        Selects the metric's governed meaning by name from the reserved ``metrics``
        schema instead of composing aggregation SQL. Empty dimensions returns a single
        grand-total row. ``filters`` is a raw boolean SQL condition; the governed
        pipeline validates it.
        """
        return await tools.query_metric(
            state, _role(role), metric, dimensions or [], filters=filters
        )

    @mcp.tool()
    async def propose_source(source: dict, reason: str, role: str | None = None) -> dict:
        """Propose a newly-discovered data source for a human to review and register (REQ-1792).

        Never creates a live Source — this always queues a pending creation request (the same
        REQ-434 queue a low-privilege GraphQL caller falls back to) for a rights-holder to execute
        or reject on the admin UI's Requests page, regardless of what capability this MCP session's
        own credential carries. `source` is a SourceInput-shaped dict: at minimum {"id", "type"},
        plus whatever of host/port/database/username/password/path/description/allowed_domains the
        discovery turned up. `reason` should say what led to this proposal.
        """
        return await tools.propose_source(state, _role(role), source, reason)

    @mcp.tool()
    async def propose_table(table: dict, reason: str, role: str | None = None) -> dict:
        """Propose registering a table from an already-registered source (REQ-1792).

        Never registers the table itself — queues a pending creation request the same way
        propose_source does; a rights-holder must execute or reject it on the Requests page.
        `table` is a TableInput-shaped dict: at minimum {"source_id", "domain_id", "schema_name",
        "table_name", "columns"} where each column is at least {"name", "visible_to"}. `reason`
        should say what led to this proposal.
        """
        return await tools.propose_table(state, _role(role), table, reason)

    @mcp.tool()
    async def graphql_field_names(schema: str, table: str, role: str | None = None) -> dict:
        """The REAL GraphQL field names (and gRPC/JSON:API/OpenAPI deep-link identifiers) for a
        table and its columns — NOT a guessed transform of describe_table's SQL-plane names.
        ALWAYS call this before writing a GraphQL/gRPC/JSON:API/OpenAPI query for a table."""
        return await tools.graphql_field_names(state, _role(role), schema, table)

    @mcp.tool()
    async def cypher_field_names(schema: str, table: str, role: str | None = None) -> dict:
        """The REAL Cypher node label, id property, and column property names for a table — NOT
        a guessed transform. ALWAYS call this before writing a Cypher query for a table."""
        return await tools.cypher_field_names(state, _role(role), schema, table)

    @mcp.tool()
    async def generate_explore_queries(question: str, role: str | None = None) -> dict:
        """Generate ready-to-run queries for all six query surfaces (sql, graphql, cypher, grpc,
        jsonapi, openapi) from one natural-language question — the same pipeline the NL Explore
        page runs. Use this for anything beyond a flat single-table browse: aggregation, GROUP
        BY, a business-term filter, or a question spanning more than one table."""
        return await tools.generate_explore_queries(state, _role(role), question)

    @mcp.tool()
    async def list_native_tables(
        source_id: str, schema_name: str = "public", role: str | None = None
    ) -> list[dict]:
        """List a source's own native tables directly from the source, bypassing the governed
        catalog — for a source with nothing registered on it yet, which the governed catalog
        tools can never see."""
        return await tools.list_native_tables(state, _role(role), source_id, schema_name)

    @mcp.tool()
    async def describe_native_table(
        source_id: str, schema_name: str, table_name: str, role: str | None = None
    ) -> list[dict]:
        """Describe one of a source's own native tables (columns, types) directly from the
        source — use before propose_table/register_table_now on an unregistered source."""
        return await tools.describe_native_table(
            state, _role(role), source_id, schema_name, table_name
        )

    @mcp.tool()
    async def list_glossary_terms(
        q: str | None = None, include_deprecated: bool = True, role: str | None = None
    ) -> list[dict]:
        """The org's glossary terms, optionally filtered by a search string."""
        resolved = _role(role)
        return await tools.list_glossary_terms(
            state, resolved, _capability_request(resolved), q, include_deprecated
        )

    @mcp.tool()
    async def create_glossary_term(
        name: str,
        definition: str | None = None,
        domains: list[str] | None = None,
        role: str | None = None,
    ) -> dict:
        """Create a new abstract glossary term (a definition with no physical column yet)."""
        resolved = _role(role)
        return await tools.create_glossary_term(
            state, resolved, _capability_request(resolved), name, definition, domains
        )

    @mcp.tool()
    async def update_glossary_term(
        term_id: int,
        name: str | None = None,
        definition: str | None = None,
        export_excluded: bool | None = None,
        retired: bool | None = None,
        role: str | None = None,
    ) -> dict:
        """Update an existing glossary term's fields."""
        resolved = _role(role)
        return await tools.update_glossary_term(
            state,
            resolved,
            _capability_request(resolved),
            term_id,
            name=name,
            definition=definition,
            export_excluded=export_excluded,
            retired=retired,
        )

    @mcp.tool()
    async def delete_glossary_term(term_id: int, role: str | None = None) -> dict:
        """Delete a glossary term. Irreversible."""
        resolved = _role(role)
        return await tools.delete_glossary_term(
            state, resolved, _capability_request(resolved), term_id
        )

    @mcp.tool()
    async def add_glossary_term_edge(
        term_id: int, to_term_id: int, rel_type: str, role: str | None = None
    ) -> dict:
        """Add a relationship edge between two glossary terms."""
        resolved = _role(role)
        return await tools.add_glossary_term_edge(
            state, resolved, _capability_request(resolved), term_id, to_term_id, rel_type
        )

    @mcp.tool()
    async def remove_glossary_term_edge(
        term_id: int, to_term_id: int, rel_type: str, role: str | None = None
    ) -> dict:
        """Remove a relationship edge between two glossary terms."""
        resolved = _role(role)
        return await tools.remove_glossary_term_edge(
            state, resolved, _capability_request(resolved), term_id, to_term_id, rel_type
        )

    @mcp.tool()
    async def list_data_products(role: str | None = None) -> list[dict]:
        """The org's data products (id, domain, name, purpose, owner/team role, status, etc.)."""
        resolved = _role(role)
        return await tools.list_data_products(state, resolved, _capability_request(resolved))

    @mcp.tool()
    async def create_data_product(
        id: str,
        domain_id: str,
        name: str,
        owner_role: str | None = None,
        team_role: str | None = None,
        purpose: str = "",
        limitations: str = "",
        usage: str = "",
        version: str | None = None,
        status: str | None = None,
        sla: str | None = None,
        support: str | None = None,
        role: str | None = None,
    ) -> dict:
        """Create or replace a data product by id — an id that already exists is overwritten in
        full. `domain_id` must be a real, existing domain."""
        resolved = _role(role)
        return await tools.create_data_product(
            state,
            resolved,
            _capability_request(resolved),
            id,
            domain_id,
            name,
            owner_role=owner_role,
            team_role=team_role,
            purpose=purpose,
            limitations=limitations,
            usage=usage,
            version=version,
            status=status,
            sla=sla,
            support=support,
        )

    @mcp.tool()
    async def delete_data_product(id: str, role: str | None = None) -> dict:
        """Delete a data product by id. Irreversible."""
        resolved = _role(role)
        return await tools.delete_data_product(state, resolved, _capability_request(resolved), id)

    @mcp.tool()
    async def upsert_metric(
        name: str,
        expression: str,
        datatype: str | None = None,
        description: str | None = None,
        ai_context: str | None = None,
        visible_to: list[str] | None = None,
        role: str | None = None,
    ) -> dict:
        """Create or replace a governed metric by name — a name that already exists is
        overwritten in full. `expression` is an aggregate ANSI-SQL expression over semantic
        table.column references; must parse under sqlglot and contain at least one aggregate
        function."""
        resolved = _role(role)
        return await tools.upsert_metric(
            state,
            resolved,
            _capability_request(resolved),
            name,
            expression,
            datatype=datatype,
            description=description,
            ai_context=ai_context,
            visible_to=visible_to,
        )

    @mcp.tool()
    async def delete_metric(name: str, role: str | None = None) -> dict:
        """Delete a governed metric by name. Irreversible."""
        resolved = _role(role)
        return await tools.delete_metric(state, resolved, _capability_request(resolved), name)

    # Optional: only registered when a Jev credential is configured, so an agent never sees
    # a tool it cannot use — no fallback, the tool simply does not exist without the key.
    if os.environ.get("TYPESAFEAI_API_KEY", "").strip():

        @mcp.tool()
        async def jev_evaluate(
            questions: list[dict],
            jev_state: Any = None,
            role: str | None = None,
        ) -> dict:
            """Evaluate typed decision questions via TypeSafe's Jev API — fast, cheap,
            calibrated machine-native decisions (not text generation).

            Each question in ``questions`` is ``{"id", "type", "instructions", "criteria"}``:
              - noul: yes/no; criteria = {"true": "...", "false": "..."}; returns P(yes)
              - choice: pick one of <=255 options; criteria = {option: description}
              - score: rate on a 2-10 level rubric; criteria = [level description, ...]
            All questions are evaluated in parallel against the same ``jev_state``
            (any JSON value — the context to judge). Each answer carries a probability
            distribution and a confidence score; route low-confidence answers to a human
            or further reasoning rather than trusting them blind.
            """
            return await tools.jev_evaluate(state, _role(role), jev_state, questions)

    return mcp


def _wrap_role_auth(app: Any, state: Any, *, require_token: bool) -> Any:
    """Wrap the Streamable-HTTP ASGI app so a request's bearer token sets the per-request role (REQ-1105).

    A pure-ASGI middleware (not BaseHTTPMiddleware) so the ``_request_role`` ContextVar it sets is
    visible to the downstream tool coroutine — the tools read it via ``_role``. A present bearer is
    resolved through the same OIDC path pgwire uses; a token that fails to resolve is 401 (fail
    closed, never a silent role). When ``require_token`` (a non-loopback bind), a request with no
    bearer is 401 — MCP is not exposed off the loopback without per-caller auth.
    """
    import json as _json

    async def _send_401(send: Any, detail: str) -> None:
        body = _json.dumps({"error": "unauthorized", "detail": detail}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"www-authenticate", b"Bearer"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def _middleware(scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization")
        token = None
        if auth and auth.lower().startswith(b"bearer "):
            token = auth[len(b"bearer ") :].decode("utf-8").strip()
        if not token:
            if require_token:
                await _send_401(send, "bearer token required")
                return
            await app(scope, receive, send)  # loopback stdio-style: pinned role applies
            return
        try:
            identity = await _validate_mcp_token(token, state)
            role = _role_for_identity(identity, state)
            org_id = await _org_for_identity(identity, state)
        except (PermissionError, ValueError) as exc:
            # Token present but rejected (bad/expired token, or no mapped role) — fail closed.
            await _send_401(send, str(exc))
            return
        except OrgResolutionError as exc:
            # Authenticated but not resolvable to one permitted org — fail closed, no default.
            await _send_401(send, str(exc))
            return
        reset = _request_role.set(role)
        # REQ-1857: see the ContextVar's own docstring — needed by the capability-gated tools.
        identity_reset = _request_identity.set(identity)
        org_id_reset = _request_org_id.set(org_id)
        # REQ-1266: bind the org's data-plane runtime for the request so the tools' state.X reads
        # route to it. None (single-org / default) leaves current_org unset → default runtime.
        org_token = None
        if org_id is not None:
            from provisa.api.app import ensure_org_runtime

            await ensure_org_runtime(org_id)
            org_token = set_current_org(org_id)
        # REQ-074/REQ-1386: attribute the tools' governed statements to the token's principal. The
        # MCP transport runs on its own event loop in-process, so a plain scope binds it for the
        # request; the pipeline's terminals write the audit row.
        from provisa.audit.context import audit_identity_scope

        try:
            with audit_identity_scope(identity.user_id, "mcp"):
                await app(scope, receive, send)
        finally:
            _request_role.reset(reset)
            _request_identity.reset(identity_reset)
            _request_org_id.reset(org_id_reset)
            if org_token is not None:
                reset_current_org(org_token)

    return _middleware


def start_mcp_server(state: Any, log_: logging.Logger | None = None) -> Any | None:
    """Start the MCP Streamable HTTP transport in a background thread.

    Opt-in via ``PROVISA_MCP_PORT`` (mirrors the bolt/pgwire optional-server
    pattern in app_startup). Returns the FastMCP instance, or None when disabled.
    Isolated here so app startup wiring stays a one-line call.
    """
    from provisa.security.high_security import mcp_start_allowed

    _log = log_ or log
    port_raw = os.environ.get("PROVISA_MCP_PORT", "0")
    port = int(port_raw)
    if not port:
        return None
    if not mcp_start_allowed(state, port):
        # REQ-693: high-security mode never starts the MCP server — a tool call hands query
        # results to a model as text, and nothing on that path can decrypt client-side.
        _log.warning("MCP server not started: security.mode=high (REQ-693)")
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
    # Any, not str: the values are cert/key paths, but the dict is splatted into ``uvicorn.run``,
    # whose keyword parameters span int/bool/float -- a str-valued mapping cannot satisfy that
    # signature no matter which keys it actually carries.
    ssl_kwargs: dict[str, Any] = {}
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
    # REQ-1105: map each remote request's bearer token to a role before it reaches a tool. A
    # non-loopback bind exposes MCP off-box, so a bearer is REQUIRED there (no anonymous off-loopback
    # access); a loopback bind keeps the pinned-role stdio posture but still honors a bearer if sent.
    require_token = host not in ("127.0.0.1", "localhost", "::1")
    app = _wrap_role_auth(app, state, require_token=require_token)

    def _serve() -> None:
        uvicorn.run(app, host=host, port=port, log_level="warning", **ssl_kwargs)  # nosec B104

    threading.Thread(target=_serve, daemon=True).start()
    _log.info("MCP Streamable %s server listening on %s:%d", scheme.upper(), host, port)
    return mcp
