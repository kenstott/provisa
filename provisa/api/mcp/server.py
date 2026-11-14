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
exposes them over MCP. Every call runs as a principal, its entitlements (REQ-2219) narrowing the
role it acts as (REQ-1105). Two transports:
  - local stdio: roles pinned via ``PROVISA_MCP_ROLE`` and the principal they act for named via
    ``PROVISA_MCP_PRINCIPAL`` (``local_principal``); a call names a role within that pin, or omits
    it and acts as the pin. With no principal named, every call is refused.
  - remote Streamable HTTP: OAuth bearer token -> the token's principal and a provisa role via the
    same OIDC path pgwire uses (build_auth_provider + resolve_role).

Role rule (CLAUDE.md): a role is REQUIRED on every call and the server NEVER
defaults to admin. When no role can be resolved the call fails loud.

search_catalog(nl_text) is the semantic "explore" surface (search.py): a
DuckDB VSS index over schema/table/column chunks, resolved up to authoritative
table branches and filtered to the caller's accessible domains.
"""

from __future__ import annotations

import functools
import logging
import time
import os
from contextvars import ContextVar
from typing import Any

from provisa.api.mcp import tools
from provisa.api.mcp.column_state import DESCRIBE_TABLE
from provisa.api.org_resolve import OrgResolutionError
from provisa.core import request_deadline
from provisa.core.request_context import require_current_org, reset_current_org, set_current_org
from provisa.core.request_thread import run_on_request_thread
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import in_request_span as _in_request_span
from provisa.otel_compat import request_span as _request_span
from provisa.security.rights import can_act_cross_org, capabilities_for_claims

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


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
# REQ-1105: the principal a local call (stdio, or the loopback with no bearer) acts for, the one
# the operator named (``local_principal``), resolved by the tool wrapper for each call.
_local_identity: ContextVar[Any | None] = ContextVar("provisa_mcp_local_identity", default=None)
# REQ-2076: the MCP session (``Mcp-Session-Id``) a tool call belongs to, and the sessions ended
# because a call in them named a role the caller does not hold, each with when it ended. A request
# in an ended session is answered 404, which tells an MCP client its session is over.
_request_session: ContextVar[str | None] = ContextVar("provisa_mcp_request_session", default=None)
_ended_sessions: dict[str, float] = {}
# How long a tombstone is kept, and how many: the session manager's idle timeout and its session
# cap, set when the server is built (build_mcp_server). A request in an ended session is answered
# here and never reaches the manager, which reaps the session after its idle timeout; past it the
# tombstone guards nothing, and the manager never keeps more sessions than its cap. None, as the
# manager reads it: no idle timeout (a session is never reaped) or no cap.
_session_bounds: tuple[float | None, int | None] | None = None


def _end_session(session: str) -> None:
    """End ``session`` (a security event): its requests are answered 404 from now on."""
    assert _session_bounds is not None, "an MCP session ended before the server was built"
    _, most = _session_bounds
    now = time.monotonic()
    _drop_expired(now)
    while most is not None and len(_ended_sessions) >= most:
        _ended_sessions.pop(next(iter(_ended_sessions)))
    _ended_sessions.pop(session, None)
    _ended_sessions[session] = now


def _session_ended(session: str) -> bool:
    """Whether ``session`` was ended, within its tombstone's lifetime."""
    if session not in _ended_sessions:
        return False
    _drop_expired(time.monotonic())
    return session in _ended_sessions


def _drop_expired(now: float) -> None:
    # Oldest first: a tombstone is (re)inserted when its session ends.
    assert _session_bounds is not None
    lasts, _ = _session_bounds
    while lasts is not None and _ended_sessions:
        session, ended = next(iter(_ended_sessions.items()))
        if now - ended < lasts:
            return
        _ended_sessions.pop(session)


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


def _with_warnings(result: Any, found: list) -> Any:
    """``result`` with the call's warnings (REQ-1350) under ``warnings`` — a dict result carries
    them beside its own keys, any other is wrapped as ``{"result", "warnings"}``."""
    if not found:
        return result
    entries = [w.as_dict() for w in found]
    if isinstance(result, dict):
        return {**result, "warnings": entries}
    return {"result": result, "warnings": entries}


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


async def _resolve_token_org_async(token: str, state: Any) -> str:
    """Resolve the org a remote MCP session binds from its bearer token (REQ-1266).

    A single-org deployment binds its one org. Under multitenancy, validates the token and maps its subject to an org via the shared
    membership rule (:func:`resolve_session_org`); raises on an unresolvable principal.
    """
    if not getattr(state, "multitenancy", False):
        return state.org_id
    return await _org_for_identity(await _validate_mcp_token(token, state), state)


# REQ-1235: how an MCP client names the org on the HTTP transport — the same two ways an HTTP
# client does. Quoted in the refusal so a client is told what to send.
_MCP_NAMES_AN_ORG = (
    "send the X-Org-Provisa header (or connect to the org's own hostname, <org>.<domain>)"
)


def requested_org_from_scope(scope: dict) -> str | None:  # REQ-1235
    """The org an MCP HTTP request names: the Host subdomain, else ``X-Org-Provisa``."""
    from provisa.security.sni import org_from_host

    headers = dict(scope.get("headers") or [])
    host = headers.get(b"host", b"").decode("latin-1")
    named = org_from_host(host)
    if named is not None:
        return named
    raw = headers.get(b"x-org-provisa")
    return raw.decode("latin-1") if raw else None


async def _org_for_identity(identity: Any, state: Any, requested_org: str | None = None) -> str:
    """The org a validated MCP identity binds; a single-org deployment's one org (REQ-1266)."""
    if not getattr(state, "multitenancy", False):
        return state.org_id
    from provisa.api.org_resolve import resolve_session_org

    # REQ-1337: resolve the claims to RIGHTS and test cross_org — never the role name.
    caps = capabilities_for_claims(getattr(identity, "roles", []) or [], state.platform_roles)
    return await resolve_session_org(
        state,
        user_id=getattr(identity, "user_id", None),
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested_org,
        credential_org=getattr(identity, "active_org_id", None),  # REQ-1235
        named_by=_MCP_NAMES_AN_ORG,
    )


def resolve_token_role(token: str, state: Any) -> str:
    """Map a remote OAuth/OIDC bearer token to a provisa role (sync wrapper, REQ-1105).

    For a synchronous caller only — the Streamable-HTTP transport awaits
    :func:`_resolve_token_role_async` directly. Raises rather than defaulting to admin.
    """
    import asyncio

    return asyncio.run(_resolve_token_role_async(token, state))


def named_role(named: str, state: Any) -> str:  # REQ-273, REQ-1105, REQ-1620
    """The role a tool call acts as from the role, or comma-separated set of roles, it NAMES — or
    PermissionError when the caller does not hold one. A set acts as its meta-role.

    Remote HTTP: naming a role in a tool call is the same act as naming one in the role header,
    and follows the same rule — the token's principal must hold it. The role the token itself
    maps to always qualifies. stdio (and a loopback HTTP call with no bearer) carries no token:
    the operator pinned the roles the process may act as (PROVISA_MCP_ROLE, a role or a set), and
    a local client names only roles within that pin; any other is refused by name. Within it the
    set is resolved as every unsecured transport resolves one (``unsecured_principal``, REQ-1952):
    a named derived role qualifies the set's meta-role, and its attribute is bound for the call.
    The call acts for the principal the operator named (``local_principal``), bound as every
    transport binds its caller, so its entitlements narrow the role (REQ-1105, REQ-2219).
    """
    from provisa.security.meta_role import refuse_named_meta_role, resolve_requested_role

    identity = _request_identity.get()
    if identity is not None:
        from provisa.security.rights import role_ids_from_claims

        held = role_ids_from_claims(getattr(identity, "roles", None) or ())
        mapped = _request_role.get()
        if mapped is None:
            # The transport sets the identity and the role it maps to together.
            raise RuntimeError("a remote MCP call carries an identity but no role")
        held.add(mapped)
        from provisa.auth.derived_roles import principal_of

        # REQ-2057: a derived role the principal holds is named like any role.
        return resolve_requested_role(state, held, named, lambda: principal_of(state, identity)[1])
    held = {r.strip() for r in named.split(",") if r.strip()}
    refuse_named_meta_role(sorted(held))
    pinned = {r.strip() for r in _pinned_stdio_role().split(",") if r.strip()}
    beyond = sorted(held - pinned)
    if beyond:
        from provisa.security.meta_role import RoleNotHeld

        refused = RoleNotHeld(beyond[0], pinned)
        # A local agent's own operator reads this: it says what the pin is.
        refused.args = (
            f"role {beyond[0]!r} is not among the roles this MCP server was given "
            f"(mcp.role / PROVISA_MCP_ROLE: {', '.join(sorted(pinned))})",
        )
        raise refused
    from provisa.auth.derived_roles import (
        principal_of,
        principal_session_vars,
        unsecured_principal,
    )
    from provisa.core.request_context import current_acting_role, set_session_vars
    from provisa.entitlements.runtime import bind_entitlements

    principal = _local_principal()
    # REQ-1952: unsecured, a declared derived role named beside a data role is that set's
    # qualifier and binds its attribute, as the HTTP middleware, Flight and gRPC resolve it.
    # Bound in this tool call's own context (its request thread, a copy of the call's).
    role, session_vars = unsecured_principal(state, named)
    # REQ-1105: the principal's own attributes and entitlement lists, as every transport binds
    # its caller's (principal_scope); what the pin names is the operator's word over them.
    attributes, _held = principal_of(state, principal)
    set_session_vars(
        principal_session_vars(principal.user_id, {**attributes, **session_vars}, role)
    )
    bind_entitlements(principal)
    # REQ-2188: who a write of the model is held to, on the one transport no middleware binds it.
    current_acting_role.set(role)
    return role


def switchable_roles(state: Any) -> set[str]:  # REQ-2082
    """The roles a tool call may name -- what :func:`named_role` checks a named role against: over
    remote HTTP the token's held roles, the role it maps to and the derived roles its principal
    holds; over stdio, the roles the operator pinned."""
    identity = _request_identity.get()
    if identity is None:
        return {r.strip() for r in _pinned_stdio_role().split(",") if r.strip()}
    from provisa.auth.derived_roles import principal_of
    from provisa.security.rights import role_ids_from_claims

    held = role_ids_from_claims(getattr(identity, "roles", None) or ())
    mapped = _request_role.get()
    if mapped is None:
        raise RuntimeError("a remote MCP call carries an identity but no role")
    return {*held, mapped, *principal_of(state, identity)[1]}


def _pinned_stdio_role() -> str:
    """The role for local stdio calls. Must be explicitly configured via
    PROVISA_MCP_ROLE — there is no admin default."""
    from provisa.core import settings_registry  # REQ-1913: the operator setting `mcp.role`

    role = settings_registry.value("mcp.role")
    if role is None:
        raise ValueError(
            "mcp.role (PROVISA_MCP_ROLE) must be set to a provisa role for the local stdio MCP "
            "transport"
        )
    return role.strip()


def _local_principal() -> Any:
    """The principal a local call acts for, as the tool wrapper resolved it. Refused, naming the
    setting, when the operator named none (REQ-1105)."""
    from provisa.api.mcp import local_principal

    identity = _local_identity.get()
    if identity is None:
        local_principal.configured_principal()  # names what is missing
        raise RuntimeError("a local MCP call reached its role with no principal resolved")
    return identity


def _call_identity() -> Any:
    """The identity a tool call runs as: a remote caller's token, or the principal a local call
    acts for (REQ-1105)."""
    identity = _request_identity.get()
    return identity if identity is not None else _local_principal()


def _call_principal(state: Any) -> tuple[str, list[str]]:
    """The graph-session principal of a tool call (REQ-2176): the identity it runs as, a remote
    caller's or the principal a local one acts for, as on every client."""
    from provisa.api import graph_session

    return graph_session.principal(state, _call_identity())


def _call_actor() -> str | None:
    """The member a tool call was made by: the identity it runs as (REQ-1524)."""
    user_id = getattr(_request_identity.get() or _local_identity.get(), "user_id", None)
    return None if user_id in (None, "anonymous") else user_id


async def _within_request(make_coro: Any, label: str) -> Any:
    """One MCP tool call as a request (REQ-1905): its work — governance, execution, shaping the
    result — runs under MCP's own request deadline, on the call's request thread. Once that
    deadline has passed the call is answered with the timeout, naming the transport and the
    setting, whatever it had produced.

    REQ-1524: one tool call is one model change, as one HTTP request is
    (``provisa.api.model_change_middleware``): what a write tool changes in the model is committed
    once, when the call ends. A call that writes nothing commits nothing."""
    from provisa.core import model_change

    with request_deadline.request("mcp") as deadline:
        async with model_change.scope(label, actor=_call_actor):
            result = await make_coro()
        deadline.check()
        return result


def _redacted_error(exc: BaseException) -> RuntimeError | None:
    """The error to send in place of ``exc`` when redaction changes its text (REQ-2087); None
    when nothing in it matched, and ``exc`` goes out as it is. Told in the model's names too
    (REQ-2152)."""
    from provisa.api.engine_error_names import client_text

    said = client_text(str(exc))
    return None if said == str(exc) else RuntimeError(said)


def build_mcp_server(state: Any):
    """Build a FastMCP server whose tools are bound to ``state``.

    Each tool takes an explicit ``role`` (required). For local stdio a client
    names a role within the pinned PROVISA_MCP_ROLE set, or omits it and the pin
    is used; a remote HTTP caller's
    role is derived from its bearer token by the transport layer and passed in.
    """
    from mcp.server.fastmcp import Image, Context, FastMCP

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
            "the caller's role. A tool's optional role argument names the role to act as "
            "(one you hold: see describe_role); omitted, your session's role."
        ),
    )
    global _session_bounds
    _session_bounds = (mcp.settings.session_idle_timeout, mcp.settings.max_sessions)

    from provisa.security.meta_role import RoleNotHeld

    async def _probed(refused: RoleNotHeld, tool: str) -> PermissionError:
        """REQ-2076: a role named that the caller does not hold. The event is recorded, the MCP
        session ends, and the refusal is returned for the call to raise: a remote caller is told
        nothing of the role, a local one (no principal: the pinned posture) what its pin is."""
        from provisa.security import role_probe

        identity = _request_identity.get()
        transport = "mcp" if identity is not None else "stdio"
        probe = role_probe.probe_of(
            identity,
            refused,
            org_id=_request_org_id.get() or (None if identity is not None else state.org_id),
            transport=transport,
            endpoint=tool,
        )
        # On the call's own request thread, where its stores are bound (REQ-1882).
        await run_on_request_thread(lambda: role_probe.record(probe))
        session = _request_session.get()
        if session:
            _end_session(session)
        return PermissionError(role_probe.NOT_PERMITTED if identity is not None else str(refused))

    async def _as_principal(make_coro: Any) -> Any:
        """Run the call ``make_coro`` makes as the call's principal (REQ-1105). A remote call's was bound by the
        transport from its token; a local call acts for the principal the operator named, found
        in the call's org, and is refused by name when there is none."""
        if _request_identity.get() is not None:
            return await make_coro()
        from provisa.api.mcp import local_principal
        from provisa.audit.context import audit_identity_scope

        from provisa.core import settings_registry

        # The roles the identity holds are the operator's pin; a tool that acts as a role
        # requires the pin itself (_pinned_stdio_role), and one that acts as none does not.
        pin = settings_registry.value("mcp.role") or ""
        pinned = sorted({r.strip() for r in pin.split(",") if r.strip()})
        identity = await local_principal.resolve(state, require_current_org(), pinned)
        reset = _local_identity.set(identity)
        try:
            with audit_identity_scope(identity.user_id, "mcp"):
                return await make_coro()
        finally:
            _local_identity.reset(reset)

    def _tool(fn):
        """Register ``fn`` as an MCP tool whose body runs on its own request thread (REQ-1882).

        The MCP transport only receives and dispatches; the tool — catalog reads, governance,
        execution, audit — runs on a request thread's own loop (a
        ``provisa.core.connection_loop.ConnectionLoop``), in a copy of this call's context
        (the bearer-resolved role/identity/org ContextVars travel with it)."""

        @functools.wraps(fn)
        async def _on_request_thread(*args: Any, **kwargs: Any) -> Any:
            # Async on the MCP loop by necessity: the MCP SDK dispatches tools as coroutines on
            # its own loop, which must keep serving other calls while this one runs.
            from provisa.core.statement_warnings import collecting

            with _request_span(_tracer, f"mcp.{fn.__name__}", transport="mcp"):  # REQ-1910
                # REQ-1350: what a tool's answers say about themselves goes in its result.
                with collecting() as found:
                    try:
                        result = await run_on_request_thread(
                            lambda: _within_request(
                                lambda: _as_principal(lambda: fn(*args, **kwargs)),
                                f"MCP {fn.__name__}",
                            )
                        )
                    except RoleNotHeld as refused:
                        raise await _probed(refused, fn.__name__) from refused
                    except Exception as exc:  # noqa: BLE001  # allow-blind-except: FastMCP sends any tool error's text to the client; REQ-2087 redacts that text, and an unchanged one is re-raised as it is
                        said = _redacted_error(exc)
                        if said is None:
                            raise
                        raise said from None
                return _with_warnings(result, found)

        return mcp.tool()(_on_request_thread)

    def _role(role: str | None) -> str:
        if role and str(role).strip():
            return named_role(str(role).strip(), state)
        # Remote HTTP: the transport middleware resolved the bearer token to a role for this
        # request (REQ-1105); prefer it over any ambient stdio role.
        req_role = _request_role.get()
        if req_role and req_role.strip():
            return req_role.strip()
        # stdio: fall back to the explicitly-pinned dev role (never admin), which may be a set.
        return named_role(_pinned_stdio_role(), state)

    def _proposer(resolved_role: str) -> str:
        """The user a proposal made through this server is recorded as made by: the remote
        caller's validated identity, or over stdio the principal the call acts for."""
        return _capability_request(resolved_role).state.identity.user_id

    def _capability_request(resolved_role: str) -> Any:  # REQ-1857
        """A minimal request-shaped shim for the handful of tools that carry their own
        capability check (glossary/data-product/metric writes) via
        ``require_capability_request``/``require_active_org_id`` — see the ContextVars' own
        docstring for why a real Request isn't available here."""
        from types import SimpleNamespace

        identity = _request_identity.get()
        org_id = _request_org_id.get()
        if identity is None:
            # stdio: no bearer token. The call acts for the principal the operator named, and its
            # capabilities resolve purely from the pinned role's own capability list
            # (capabilities_for_claims keys on role id, not on anything else the identity
            # carries), so an identity naming just that one role resolves correctly.
            identity = SimpleNamespace(
                user_id=_local_principal().user_id,
                roles=[resolved_role],
                acting_roles=[resolved_role],
            )
            # The loopback request's org, bound by _wrap_role_auth (REQ-1266).
            org_id = require_current_org()
        else:
            # A remote call's identity is the transport's, never bound by the HTTP middleware:
            # it acts as the role the call resolved to (a set's meta-role is a role too), and its
            # rights are read from that role alone, as an HTTP request's are.
            identity = SimpleNamespace(
                user_id=identity.user_id, roles=list(identity.roles), acting_roles=[resolved_role]
            )
        # The acting role too, as the HTTP middleware sets it: the catalog a tool reads and edits
        # is that role's (REQ-2152, catalog_scope).
        # No headers: a tool call carries no Idempotency-Key, so a decision applies as it always
        # did (REQ-2216).
        return SimpleNamespace(
            headers={},
            state=SimpleNamespace(identity=identity, active_org_id=org_id, role=resolved_role),
        )

    @_tool
    async def describe_role(role: str | None = None) -> dict:  # REQ-2082
        """The role you act as and what it is for, and the roles you may switch to."""
        return tools.describe_role(state, _role(role), switchable_roles(state))

    @_tool
    async def list_schemas(role: str | None = None) -> list[dict]:
        """List catalog schemas with description and table count."""
        return await tools.list_schemas(state, _role(role))

    @_tool
    async def list_tables(schema: str, role: str | None = None) -> list[dict]:
        """List tables in a schema with description and column count."""
        return await tools.list_tables(state, _role(role), schema)

    async def describe_table(schema: str, table: str, role: str | None = None) -> dict:
        return await tools.describe_table(state, _role(role), schema, table)

    # The one description Polly's chat and the status page give it too.
    describe_table.__doc__ = DESCRIBE_TABLE
    _tool(describe_table)

    @_tool
    async def list_commands(role: str | None = None) -> list[dict]:
        """List registered commands your role may invoke: name, domain, kind, arguments."""
        # REQ-1156.
        return tools.list_commands(state, _role(role))

    @_tool
    async def meta_resolve(
        uri: str, as_of: str | None = None, as_recorded: str | None = None, role: str | None = None
    ) -> dict:
        """The governed value a semantic address provisa://<org>/<domain>/tables/<table>/pk=<value>[/<column>]
        names. as_of (valid time) and as_recorded (record time): optional ISO 8601 instants.
        Physical addresses are refused."""
        # REQ-1365: the one resolve function; the tool is the meta domain's resolve, named as
        # the GraphQL field is: meta's.
        from provisa.api.data.resolve import resolve as resolve_address
        from provisa.core.request_context import require_current_org

        return await resolve_address(
            state,
            _role(role),
            uri,
            org_id=require_current_org(),
            as_of=as_of,
            as_recorded=as_recorded,
        )

    @mcp.tool()
    @_in_request_span(_tracer, "mcp.run_sql", transport="mcp")  # REQ-1910
    async def run_sql(
        sql: str,
        ctx: Context,
        role: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        redirect: bool = False,
        redirect_format: str | None = None,
    ) -> dict:
        """Run SQL under your role's governance; returns rows, capped per page (page with
        limit/offset; total_rows and truncated say whether more remain). redirect=true returns a
        results-store handle (presigned URL, row count, expiry) instead of rows, as
        redirect_format parquet or orc; a large result may be redirected that way unasked."""
        # A result over the operator's threshold is redirected when the operator has enabled it.
        # REQ-1882: governance and execution run on this call's request thread; the license nag
        # below writes to the MCP session, whose streams belong to the MCP loop, so it stays here.
        from provisa.core.statement_warnings import collecting

        # REQ-1350: one collector for the call (the request thread runs in a copy of this
        # context); what the answer says about itself goes in the result, under "warnings",
        # and as a warning log notification.
        with collecting() as found:
            result = await run_on_request_thread(
                lambda: _within_request(
                    lambda: _as_principal(
                        lambda: tools.run_sql(
                            state,
                            _role(role),
                            sql,
                            limit=limit,
                            offset=offset,
                            redirect=redirect,
                            redirect_format=redirect_format,
                        )
                    ),
                    "MCP run_sql",
                )
            )
        result = _with_warnings(result, found)
        for warning in found:
            await ctx.warning(warning.message)
        await _emit_mcp_nag(ctx)  # REQ-1137: out-of-band license nag, once per session
        return result

    @_tool
    async def explain_sql(sql: str, role: str | None = None) -> dict:
        """Check that SQL validates and plans under your role's governance, without running it."""
        return await tools.explain_sql(state, _role(role), sql)

    # --- statistics over a SQL-defined dataset (REQ-2090): results only, never the rows ---------

    @_tool
    async def describe(sql: str, role: str | None = None) -> dict:
        """Per-column statistics of the result of sql: count, missing, distinct, mean, sd,
        quantiles, most frequent values. Returns statistics, not rows."""
        return await tools.stats_describe(state, _role(role), sql)

    @_tool
    async def correlate(
        sql: str, columns: list[str], method: str = "pearson", role: str | None = None
    ) -> dict:
        """Correlation matrix (pearson or spearman) of numeric columns of the result of sql,
        with n. Returns the matrix, not rows."""
        return await tools.stats_correlate(state, _role(role), sql, columns, method)

    @_tool
    async def regress(
        sql: str,
        target: str,
        features: list[str],
        kind: str = "linear",
        role: str | None = None,
    ) -> dict:
        """Regress target on features over the result of sql: kind linear (R²) or logistic (AUC;
        two-valued target). Returns coefficients, standard errors, p-values, n."""
        return await tools.stats_regress(state, _role(role), sql, target, features, kind)

    @_tool
    async def compare(sql: str, value: str, group: str, role: str | None = None) -> dict:
        """Test whether value differs across the groups of column group in the result of sql:
        Welch's t-test (two groups) or one-way ANOVA (more), with effect size."""
        return await tools.stats_compare(state, _role(role), sql, value, group)

    @_tool
    async def forecast(
        sql: str,
        time: str,
        value: str,
        horizon: int,
        seasonal_periods: int | None = None,
        role: str | None = None,
    ) -> dict:
        """Forecast value horizon steps past the last time in the result of sql by exponential
        smoothing (ETS), with 95% prediction intervals."""
        return await tools.stats_forecast(
            state, _role(role), sql, time, value, horizon, seasonal_periods
        )

    @_tool
    async def search_catalog(query: str, role: str | None = None, k: int = 5) -> list[dict]:
        """Semantic search of the catalog for the k tables best matching a natural-language
        query, each with its columns, foreign keys and schema. Use when you don't know which
        table holds the data; for business vocabulary use search_terms."""
        return await tools.search_catalog(state, _role(role), query, k=k)

    @_tool
    async def search_terms(query: str, role: str | None = None, limit: int = 25) -> list[dict]:
        """Look up business-glossary terms by name or definition: each with its status, grounding,
        definition, physical refs, experts and paths down to data. Use to find which physical
        fields mean a business concept (e.g. "order date") before writing SQL, and cite it."""
        # REQ-1387: approved terms, and defined drafts on a path to data through an approved term,
        # are returned; grounded_pending when the path runs through terms not yet approved; an
        # undefined term appears only as an anonymous hop.
        return await tools.search_terms(state, _role(role), query, limit=limit)

    @_tool
    async def search_reference_library(
        question: str, role: str | None = None, k: int = 5
    ) -> list[dict]:
        """Search the org's reference documents (policies, regulations, contracts, specifications,
        standards) for passages answering a question: the k nearest, their neighbours, and
        passages cited by glossary terms the question reaches. Each carries title, source,
        section or page, quoted text, and how it was reached. Cite the passages an answer uses."""
        # REQ-2185: only documents the caller's role may read are searched. Neighbours are up to
        # k more: the passages just after and before each in its document, and those nearest it.
        # ``reached`` says how a passage was found, ``ordinal`` is its number in its document; a
        # term-cited passage carries the path, ``term`` and ``term_definition``.
        return await tools.search_reference_library(state, _role(role), question, k=k)

    @_tool
    async def list_metrics(role: str | None = None) -> list[dict]:
        """List governed metrics: name, description, ai_context (its definition, for AI),
        datatype, from_fact. Evaluate one with query_metric instead of writing its SQL."""
        # REQ-1319.
        return tools.list_metrics(state, _role(role))

    @_tool
    async def query_metric(
        metric: str,
        dimensions: list[str] | None = None,
        filters: str | None = None,
        role: str | None = None,
    ) -> list[dict]:
        """Evaluate a governed metric by name, grouped by the dimensions you choose (none: one
        grand-total row). Use instead of writing the aggregate in run_sql. filters: a SQL
        boolean condition."""
        # REQ-1319: reads the reserved ``metrics`` schema; the governed pipeline validates filters.
        return await tools.query_metric(
            state, _role(role), metric, dimensions or [], filters=filters
        )

    @_tool
    async def propose_source(source: dict, reason: str, role: str | None = None) -> dict:
        """Queue a newly found data source for a person to review and register; never creates it.
        source: at least {id, type}, plus any of host, port, database, username, password, path,
        description, allowed_domains. reason: what led to the proposal."""
        # REQ-1792: always the REQ-434 pending-request queue (the admin UI's Requests page),
        # whatever capability the session's credential carries.
        return await tools.propose_source(
            state, _role(role), source, reason, requested_by=_proposer(_role(role))
        )

    @_tool
    async def propose_table(table: dict, reason: str, role: str | None = None) -> dict:
        """Queue a table of a registered source for a person to review and register; never
        registers it. table: at least {source_id, domain_id, schema_name, table_name, columns},
        each column at least {name, visible_to}. reason: what led to the proposal."""
        # REQ-1792: the same pending-request queue as propose_source.
        return await tools.propose_table(
            state, _role(role), table, reason, requested_by=_proposer(_role(role))
        )

    @_tool
    async def graphql_field_names(schema: str, table: str, role: str | None = None) -> dict:
        """The actual GraphQL field names (and gRPC, JSON:API, OpenAPI identifiers) of a table and
        its columns. Call before writing such a query: they don't follow from the SQL names."""
        return await tools.graphql_field_names(state, _role(role), schema, table)

    @_tool
    async def cypher_field_names(schema: str, table: str, role: str | None = None) -> dict:
        """The actual Cypher node label, id property and property names of a table. Call before
        writing Cypher: they don't follow from the SQL names."""
        return await tools.cypher_field_names(state, _role(role), schema, table)

    @_tool
    async def run_cypher(
        cypher: str, params: dict | None = None, limit: int | None = None, role: str | None = None
    ) -> dict:
        """Run Cypher under your role; returns rows and the graph as Cytoscape elements
        {nodes, edges} (input to render_graph). Get labels and properties from
        cypher_field_names first."""
        # Elements: nodes [{data: {id, label, tableLabel, properties}}], edges [{data: {id, source,
        # target, label, properties}}], properties as the role sees them.
        return await tools.run_cypher(
            state, _role(role), cypher, params, limit, principal=_call_principal(state)
        )

    @_tool
    async def clear_graph_session() -> dict:
        """Drop the labels, nodes and relationships your Cypher statements created, on every
        client."""
        return await tools.clear_graph_session(state, _call_principal(state))

    @_tool
    async def render_graph(
        elements: dict,
        layout: str = "force",
        theme: str = "light",
        width: int = 800,
        height: int = 600,
    ):  # an Image: the SDK answers it as MCP image content (its annotation would not resolve here)
        """Draw run_cypher's elements as a PNG. layout: force, hierarchy or elk; theme: light or
        dark; width, height: 100 to 4000 pixels."""
        # Drawn as the Provisa graph page draws it: its stylesheet; force is fcose, hierarchy is
        # breadthfirst, elk the ERD's.
        return Image(data=tools.render_graph(elements, layout, theme, width, height), format="png")

    @_tool
    async def generate_explore_queries(question: str, role: str | None = None) -> dict:
        """Generate ready-to-run sql, graphql, cypher, grpc, jsonapi and openapi queries from one
        natural-language question. Use for aggregation, grouping, a business-term filter, or a
        question spanning several tables."""
        # The pipeline the NL Explore page runs.
        return await tools.generate_explore_queries(state, _role(role), question)

    @_tool
    async def list_native_tables(
        source_id: str, schema_name: str = "public", role: str | None = None
    ) -> list[dict]:
        """List a source's tables read from the source itself, outside the governed catalog: for a
        source with nothing registered yet."""
        return await tools.list_native_tables(state, _role(role), source_id, schema_name)

    @_tool
    async def describe_native_table(
        source_id: str, schema_name: str, table_name: str, role: str | None = None
    ) -> list[dict]:
        """Columns and types of a table read from the source itself, outside the governed
        catalog; use before propose_table on an unregistered source."""
        return await tools.describe_native_table(
            state, _role(role), source_id, schema_name, table_name
        )

    @_tool
    async def list_glossary_terms(
        q: str | None = None, include_deprecated: bool = True, role: str | None = None
    ) -> list[dict]:
        """The org's glossary terms for curation, filtered by q if given. To find data by
        meaning use search_terms."""
        resolved = _role(role)
        return await tools.list_glossary_terms(
            state, resolved, _capability_request(resolved), q, include_deprecated
        )

    @_tool
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

    @_tool
    async def update_glossary_term(
        term_id: int,
        name: str | None = None,
        definition: str | None = None,
        export_excluded: bool | None = None,
        role: str | None = None,
    ) -> dict:
        """Update a glossary term's fields; its status has its own tools."""
        resolved = _role(role)
        return await tools.update_glossary_term(
            state,
            resolved,
            _capability_request(resolved),
            term_id,
            name=name,
            definition=definition,
            export_excluded=export_excluded,
        )

    @_tool
    async def approve_glossary_term(term_id: int, role: str | None = None) -> dict:
        """Approve a draft glossary term (it needs a definition). Only when a person decides."""
        resolved = _role(role)
        return await tools.change_glossary_term_status(
            state, resolved, _capability_request(resolved), term_id, "approve", {}
        )

    @_tool
    async def deprecate_glossary_term(
        term_id: int, reason: str, retire_on: str | None = None, role: str | None = None
    ) -> dict:
        """Deprecate an approved glossary term; retire_on: optional ISO date it is retired on."""
        resolved = _role(role)
        return await tools.change_glossary_term_status(
            state,
            resolved,
            _capability_request(resolved),
            term_id,
            "deprecate",
            {"reason": reason, "retire_on": retire_on},
        )

    @_tool
    async def retire_glossary_term(
        term_id: int,
        reason: str,
        successor: int | None = None,
        confirm_tag_loss: list[str] | None = None,
        carry_tags: bool = False,
        role: str | None = None,
    ) -> dict:
        """Retire a glossary term. Its columns and tables move to successor (required if it has
        any), which gets a REPLACES edge. A move taking pii off a column is refused unless
        confirm_tag_loss names it or carry_tags puts it on the successor."""
        resolved = _role(role)
        return await tools.change_glossary_term_status(
            state,
            resolved,
            _capability_request(resolved),
            term_id,
            "retire",
            {
                "reason": reason,
                "successor": successor,
                "confirm_tag_loss": confirm_tag_loss or [],
                "carry_tags": carry_tags,
            },
        )

    @_tool
    async def restore_glossary_term(term_id: int, role: str | None = None) -> dict:
        """Restore a glossary term: deprecated to approved, retired to draft."""
        resolved = _role(role)
        return await tools.change_glossary_term_status(
            state, resolved, _capability_request(resolved), term_id, "restore", {}
        )

    @_tool
    async def list_glossary_findings(role: str | None = None) -> list[dict]:
        """Glossary terms needing a curator's attention, each with its findings
        ({kind, message, params})."""
        # A plain draft term is not a finding.
        resolved = _role(role)
        return await tools.list_glossary_findings(state, resolved, _capability_request(resolved))

    @_tool
    async def delete_glossary_term(term_id: int, role: str | None = None) -> dict:
        """Delete a glossary term. Irreversible."""
        resolved = _role(role)
        return await tools.delete_glossary_term(
            state, resolved, _capability_request(resolved), term_id
        )

    @_tool
    async def add_glossary_term_edge(
        term_id: int, to_term_id: int, rel_type: str, role: str | None = None
    ) -> dict:
        """Add a relationship edge between two glossary terms."""
        resolved = _role(role)
        return await tools.add_glossary_term_edge(
            state, resolved, _capability_request(resolved), term_id, to_term_id, rel_type
        )

    @_tool
    async def remove_glossary_term_edge(
        term_id: int, to_term_id: int, rel_type: str, role: str | None = None
    ) -> dict:
        """Remove a relationship edge between two glossary terms."""
        resolved = _role(role)
        return await tools.remove_glossary_term_edge(
            state, resolved, _capability_request(resolved), term_id, to_term_id, rel_type
        )

    @_tool
    async def ground_glossary_term_to_table(
        term_id: int, table_id: int, role: str | None = None
    ) -> dict:
        """Make a glossary term the class term of a registered table or view (one per table; it
        needs a primary key)."""
        resolved = _role(role)
        return await tools.ground_glossary_term_to_table(
            state, resolved, _capability_request(resolved), term_id, table_id
        )

    @_tool
    async def unground_glossary_term_from_table(
        term_id: int, table_id: int, role: str | None = None
    ) -> dict:
        """Withdraw a glossary term as a table's class term."""
        resolved = _role(role)
        return await tools.unground_glossary_term_from_table(
            state, resolved, _capability_request(resolved), term_id, table_id
        )

    @_tool
    async def tag_glossary_term(
        term_id: int, tag_id: str, reason: str | None = None, role: str | None = None
    ) -> dict:
        """Tag (e.g. pii) a Property glossary term: every column it holds, now and later, carries
        the tag."""
        resolved = _role(role)
        return await tools.tag_glossary_term(
            state, resolved, _capability_request(resolved), term_id, tag_id, reason
        )

    @_tool
    async def untag_glossary_term(term_id: int, tag_id: str, role: str | None = None) -> dict:
        """Take a tag off a glossary term."""
        resolved = _role(role)
        return await tools.untag_glossary_term(
            state, resolved, _capability_request(resolved), term_id, tag_id
        )

    @_tool
    async def list_data_products(role: str | None = None) -> list[dict]:
        """The org's data products: id, domain, name, purpose, owner and team roles, status."""
        resolved = _role(role)
        return await tools.list_data_products(state, resolved, _capability_request(resolved))

    @_tool
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
        support: str | None = None,
        role: str | None = None,
    ) -> dict:
        """Create, or fully replace, a data product by id. domain_id must be an existing domain.
        Status is kept (new: draft); the publish/deprecate/retire/restore tools change it."""
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
            support=support,
        )

    @_tool
    async def set_data_product_members(
        id: str, table_ids: list[int], command_names: list[str], role: str | None = None
    ) -> dict:
        """Set a data product's members to exactly these tables and commands, all in its
        domain."""
        resolved = _role(role)
        return await tools.set_data_product_members(
            state, resolved, _capability_request(resolved), id, table_ids, command_names
        )

    @_tool
    async def delete_data_product(id: str, role: str | None = None) -> dict:
        """Delete a data product by id. Irreversible."""
        resolved = _role(role)
        return await tools.delete_data_product(state, resolved, _capability_request(resolved), id)

    @_tool
    async def publish_data_product(id: str, role: str | None = None) -> dict:
        """Publish a draft data product: it goes live."""
        resolved = _role(role)
        return await tools.change_data_product_status(
            state, resolved, _capability_request(resolved), id, "publish", {}
        )

    @_tool
    async def deprecate_data_product(
        id: str, reason: str, retire_on: str | None = None, role: str | None = None
    ) -> dict:
        """Deprecate a live data product; it stays in service until retire_on (optional ISO
        date), when it is retired."""
        resolved = _role(role)
        return await tools.change_data_product_status(
            state,
            resolved,
            _capability_request(resolved),
            id,
            "deprecate",
            {"reason": reason, "retire_on": retire_on},
        )

    @_tool
    async def retire_data_product(
        id: str, reason: str, successor: str | None = None, role: str | None = None
    ) -> dict:
        """Retire a live or deprecated data product. Its members move to successor (a product in
        the same domain), required if it has any."""
        resolved = _role(role)
        return await tools.change_data_product_status(
            state,
            resolved,
            _capability_request(resolved),
            id,
            "retire",
            {"reason": reason, "successor": successor},
        )

    @_tool
    async def restore_data_product(id: str, role: str | None = None) -> dict:
        """Restore a data product: deprecated to live, retired to draft (moved members stay with
        the successor)."""
        resolved = _role(role)
        return await tools.change_data_product_status(
            state, resolved, _capability_request(resolved), id, "restore", {}
        )

    @_tool
    async def list_data_product_findings(role: str | None = None) -> list[dict]:
        """Data products needing a curator's attention, each with its findings."""
        resolved = _role(role)
        return await tools.list_data_product_findings(
            state, resolved, _capability_request(resolved)
        )

    @_tool
    async def upsert_metric(
        name: str,
        expression: str,
        datatype: str | None = None,
        description: str | None = None,
        ai_context: str | None = None,
        visible_to: list[str] | None = None,
        role: str | None = None,
    ) -> dict:
        """Create, or fully replace, a governed metric by name. expression: ANSI SQL over
        table.column references, with at least one aggregate function."""
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

    @_tool
    async def delete_metric(name: str, role: str | None = None) -> dict:
        """Delete a governed metric by name. Irreversible."""
        resolved = _role(role)
        return await tools.delete_metric(state, resolved, _capability_request(resolved), name)

    # REQ-1934/1494/1939/1919: the profiler, fakes, synthetic datasets and the config export.
    from provisa.api.mcp import model_tool_server

    model_tool_server.register(_tool, _role, _capability_request, state)

    # Optional: only registered when a Jev credential is configured, so an agent never sees
    # a tool it cannot use — no fallback, the tool simply does not exist without the key.
    if os.environ.get("TYPESAFEAI_API_KEY", "").strip():

        @_tool
        async def jev_evaluate(
            questions: list[dict],
            jev_state: Any = None,
            role: str | None = None,
        ) -> dict:
            """Calibrated typed decisions (not text) via TypeSafe's Jev API, all judged against
            jev_state (any JSON: the context). Each question is {id, type, instructions,
            criteria}: noul (yes/no; criteria {true, false}; returns P(yes)), choice (up to 255
            options; criteria {option: description}) or score (2-10 levels; criteria [level
            description, ...]). Each answer has a distribution and a confidence: send
            low-confidence ones to a person or further reasoning."""
            return await tools.jev_evaluate(state, _role(role), jev_state, questions)

    _install_tool_groups(mcp)  # REQ-2246: sip mode; before the titles go, so its tool loses its own
    # A client is sent every tool's schemas on every model call. Pydantic titles each schema and
    # property after its own name ("Role", "list_tablesArguments"), which says nothing the names
    # do not. A title is an annotation: arguments are validated by each tool's argument model and
    # results by the same schema without it.
    for info in mcp._tool_manager.list_tools():
        _drop_titles(info.parameters)
        _drop_titles(info.output_schema)
    return mcp


def _session_mode(ctx: Any) -> str:
    """The tool mode of the connection an MCP request came on, which ``_wrap_role_auth`` read
    from its address (REQ-2246)."""
    from provisa.api.mcp import tool_groups

    request = ctx.request_context.request
    if request is None:
        raise RuntimeError("an MCP request with no HTTP request carries no tool mode")
    return getattr(request.state, tool_groups.STATE_KEY)


def _install_tool_groups(mcp: Any) -> None:
    """REQ-2246: register ``tool_groups``, check every tool has one group, and list each session
    the tools of its mode. ``mcp.list_tools()`` stays the whole registry; the protocol's
    tools/list answers a session the tools its mode and the groups it added list. A call is
    dispatched as before, listed or not: what the caller may call is decided by its rights."""
    import functools as _functools

    from mcp import types
    from mcp.server.fastmcp import Context
    from mcp.server.lowlevel import NotificationOptions

    from provisa.api.mcp import tool_groups

    async def _registered() -> set[str]:
        return {t.name for t in await mcp.list_tools()}

    @mcp.tool(name=tool_groups.CATALOG_TOOL, description=tool_groups.CATALOG_DESCRIPTION)
    @_in_request_span(_tracer, f"mcp.{tool_groups.CATALOG_TOOL}", transport="mcp")
    async def _tool_groups(ctx: Context, add: str | None = None) -> dict:
        answer = tool_groups.answer(_session_mode(ctx), ctx.session, add, await _registered())
        if add is not None:
            # On this call's own response stream, so the client reads it with the answer.
            await ctx.session.send_notification(
                types.ServerNotification(types.ToolListChangedNotification()),
                related_request_id=ctx.request_context.request_id,
            )
        return answer

    tool_groups.check_registered({t.name for t in mcp._tool_manager.list_tools()})

    async def _listed() -> types.ListToolsResult:
        ctx = mcp.get_context()
        mode = _session_mode(ctx)
        every = await mcp.list_tools()
        return types.ListToolsResult(
            tools=[t for t in every if tool_groups.listed(mode, ctx.session, t.name)]
        )

    low = mcp._mcp_server
    low.list_tools()(_listed)
    # The capability that tells a client its tool list changes (tools.listChanged).
    low.create_initialization_options = _functools.partial(
        low.create_initialization_options, NotificationOptions(tools_changed=True)
    )


# Schema keywords whose values are data, not subschemas: a "title" inside them is not a title.
_SCHEMA_DATA = frozenset({"default", "const", "enum", "examples"})


def _drop_titles(schema: Any) -> None:
    """Remove every ``title`` annotation from a JSON schema, in place."""
    if isinstance(schema, list):
        for item in schema:
            _drop_titles(item)
        return
    if not isinstance(schema, dict):
        return
    if isinstance(schema.get("title"), str):
        del schema["title"]
    for key, value in schema.items():
        if key in _SCHEMA_DATA:
            continue
        if key in ("properties", "$defs", "definitions") and isinstance(value, dict):
            # Keys here are property or definition names, so a property named "title" stays.
            for sub in value.values():
                _drop_titles(sub)
        else:
            _drop_titles(value)


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
        from provisa.security.redaction import redact

        body = _json.dumps({"error": "unauthorized", "detail": redact(detail)}).encode("utf-8")
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
        session = (headers.get(b"mcp-session-id") or b"").decode("latin-1") or None
        if session is not None and _session_ended(session):
            # REQ-2076: the session ended on a security event; the client must start another.
            body = _json.dumps({"error": "session ended"}).encode("utf-8")
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        from provisa.api.mcp import tool_groups

        try:
            # REQ-2246: the tools this connection is listed, by the mode its address names.
            mode = tool_groups.mode_of_address(scope.get("query_string") or b"")
        except ValueError as exc:
            body = _json.dumps({"error": str(exc)}).encode("utf-8")
            await send(
                {
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        scope = {**scope, "state": {**(scope.get("state") or {}), tool_groups.STATE_KEY: mode}}
        session_reset = _request_session.set(session)
        try:
            await _authorized(scope, receive, send, headers)
        finally:
            _request_session.reset(session_reset)

    async def _authorized(scope: dict, receive: Any, send: Any, headers: dict) -> None:
        auth = headers.get(b"authorization")
        token = None
        if auth and auth.lower().startswith(b"bearer "):
            token = auth[len(b"bearer ") :].decode("utf-8").strip()
        if not token:
            if require_token:
                await _send_401(send, "bearer token required")
                return
            # Loopback stdio-style: the pinned role applies, in the deployment's own org -- the
            # unsecured HTTP path's org (REQ-1266), acting for the principal the operator named,
            # which each tool call resolves (REQ-1105) and records its statements as. What is
            # not a tool call is recorded as the anonymous user's (REQ-074/REQ-1386).
            from provisa.audit.context import ANONYMOUS_USER, audit_identity_scope

            loopback_token = set_current_org(state.org_id)
            try:
                with audit_identity_scope(ANONYMOUS_USER, "mcp"):
                    await app(scope, receive, send)
            finally:
                reset_current_org(loopback_token)
            return

        async def _resolve_principal() -> tuple[Any, str, str]:
            identity = await _validate_mcp_token(token, state)
            role = _role_for_identity(identity, state)
            org_id = await _org_for_identity(identity, state, requested_org_from_scope(scope))
            from provisa.api.app import ensure_serving_runtime

            await ensure_serving_runtime(org_id)
            return identity, role, org_id

        try:
            # REQ-1882: credential validation, org resolution and the org runtime build run on
            # this request's own thread; the MCP loop only receives and relays.
            identity, role, org_id = await run_on_request_thread(_resolve_principal)
        except (PermissionError, ValueError) as exc:
            # Token present but rejected (bad/expired token, or no mapped role) — fail closed.
            await _send_401(send, str(exc))
            return
        except OrgResolutionError as exc:
            # Authenticated but not resolvable to one permitted org — fail closed, no default.
            await _send_401(send, str(exc))
            return
        from provisa.security import role_probe

        # REQ-2076: an IdP-signed principal whose token access to the org is suspended.
        if await run_on_request_thread(lambda: role_probe.suspended(identity, org_id, "mcp")):
            await _send_401(send, role_probe.NOT_PERMITTED)
            return
        reset = _request_role.set(role)
        # REQ-1857: see the ContextVar's own docstring — needed by the capability-gated tools.
        identity_reset = _request_identity.set(identity)
        org_id_reset = _request_org_id.set(org_id)
        # REQ-1266: bind the org's data-plane runtime for the request so the tools' state.X reads
        # route to it.
        org_token = set_current_org(org_id)
        # REQ-074/REQ-1386: attribute the tools' governed statements to the token's principal. The
        # MCP transport runs on its own event loop in-process, so a plain scope binds it for the
        # request; the pipeline's terminals write the audit row.
        from provisa.audit.context import audit_identity_scope

        from provisa.core.request_context import current_acting_role

        acting_reset = current_acting_role.set(role)
        from provisa.auth.derived_roles import principal_scope

        try:
            # REQ-1682/REQ-1382: what a row filter reads for this caller, as on every transport.
            with (
                audit_identity_scope(identity.user_id, "mcp"),
                principal_scope(state, identity, role),
            ):
                await app(scope, receive, send)
        finally:
            current_acting_role.reset(acting_reset)
            _request_role.reset(reset)
            _request_identity.reset(identity_reset)
            _request_org_id.reset(org_id_reset)
            reset_current_org(org_token)

    return _middleware


def _uvicorn_config(app: Any, fd: int, ssl_kwargs: dict[str, Any]) -> Any:
    """The MCP server's uvicorn config, one that leaves the process's loggers alone.

    ``Config.__init__`` configures logging for the whole process: a ``log_config`` rewrites the
    ``uvicorn`` loggers' handlers and a ``log_level`` sets ``uvicorn.error`` and ``uvicorn.access``.
    This server runs in a thread beside the main one, so either would change the main server's
    startup and access lines. Both stay ``None``; the loggers keep the level the main server set."""
    import uvicorn

    return uvicorn.Config(app, fd=fd, log_config=None, log_level=None, **ssl_kwargs)


def start_mcp_server(state: Any, log_: logging.Logger | None = None) -> Any | None:
    """Start the MCP Streamable HTTP transport in a background thread.

    Opt-in via ``PROVISA_MCP_PORT`` (mirrors the bolt/pgwire optional-server
    pattern in app_startup). Returns the FastMCP instance, or None when disabled.
    Isolated here so app startup wiring stays a one-line call.
    """
    from provisa.security.high_security import mcp_start_allowed

    _log = log_ or log
    from provisa.core import settings_registry

    port = settings_registry.value("mcp.port")  # REQ-1913; 0 = not started
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
    host = settings_registry.value("mcp.host")

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
    if settings_registry.value("mcp.tls"):
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

    # REQ-1900/1901: `uvicorn.run(host=, port=)` binds its own socket with no SO_REUSEPORT (unlike
    # the bolt/pgwire fix in this same requirement), so under `--workers N` every worker but the
    # first crashed here with "address already in use" -- confirmed live on a real `--workers 8`
    # boot. `uvicorn.run` has no reuse_port kwarg, but it does accept an already-open file
    # descriptor (`fd=`), so the socket is pre-created here with SO_REUSEPORT set and handed to it
    # that way instead -- the same fix shape as bolt/pgwire, just via the fd seam rather than a
    # library-level flag. Kept alive for the server's whole lifetime (never closed here): closing
    # it would pull the listening socket out from under uvicorn's own wrapping of the same fd.
    import socket as _socket

    # PROVISA_MCP_HOST may be a raw IPv6 literal ("::1") -- an AF_INET socket cannot bind that
    # address at all, so pick the family the configured host actually needs rather than assuming
    # IPv4 (the overwhelmingly common case: "0.0.0.0" default, "127.0.0.1", or a hostname).
    _family = _socket.AF_INET6 if ":" in host else _socket.AF_INET
    _sock = _socket.socket(_family, _socket.SOCK_STREAM)
    _sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    _sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
    _sock.bind((host, port))  # nosec B104 - same wildcard host this server always advertised

    def _serve() -> None:
        uvicorn.Server(_uvicorn_config(app, _sock.fileno(), ssl_kwargs)).run()

    threading.Thread(target=_serve, daemon=True).start()
    _log.info("MCP Streamable %s server listening on %s:%d", scheme.upper(), host, port)
    return mcp
