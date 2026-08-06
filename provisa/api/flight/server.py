# Copyright (c) 2026 Kenneth Stott
# Canary: 2f87c2de-a092-4613-b94c-3899f4b2b39a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC Arrow Flight server for Provisa (REQ-045, REQ-126).

Clients send a GraphQL query as the Flight ticket, receive Arrow record batches.
When the Zaychik Flight SQL proxy is available, results stream end-to-end
without materializing the full result in Provisa memory.

The catalog path exposes the semantic layer as a read-only JDBC catalog.
"""

# Requirements: REQ-045, REQ-051, REQ-126, REQ-143, REQ-144, REQ-145, REQ-146, REQ-267, REQ-345, REQ-369

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, cast

import jwt
import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.flight.catalog import (
    CatalogTable,
    build_catalog_tables,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
    command_to_flight_info,
)
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.security.high_security import high_security_wire_reject
from provisa.transpiler.router import Route, decide_route

_tracer = _get_tracer(__name__)

if TYPE_CHECKING:
    from graphql import DocumentNode, GraphQLSchema

    from provisa.api.app import AppState
    from provisa.compiler.sql_gen import CompilationContext, CompiledQuery
    from provisa.transpiler.router import RouteDecision

log = logging.getLogger(__name__)

_SQL_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_CYPHER_PREFIX = re.compile(
    r"^\s*(MATCH|OPTIONAL\s+MATCH|CALL|WITH|MERGE|CREATE|RETURN)\b", re.IGNORECASE
)


async def _run_with_org(org_id: str | None, coro):
    """Bind ``current_org`` inside a loop coroutine (REQ-1266).

    ``run_coroutine_threadsafe`` does NOT carry the flight worker thread's ContextVar into the
    main-loop coroutine, so the org must be re-bound here, on the loop, around the awaited work."""
    if org_id is None:
        return await coro
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await coro
    finally:
        reset_current_org(token)


async def _validate_flight_credential(state, token: str):
    """Validate a Flight client's credential and return its identity (REQ-1263).

    Flight carries exactly one credential presentation — a bearer token in the handshake or the
    ticket — so the bearer validator is selected by name rather than calling ``validate_token``,
    whose meaning differs per provider (under ``basic`` it expects base64 ``user:password``, and
    every bearer credential, personal access token included, would fail there). The platform pool
    is passed through so a PAT resolves here exactly as it does on every other surface.
    """
    from provisa.auth.models import validator_for_scheme
    from provisa.auth.throttle import throttled
    from provisa.auth.wiring import build_auth_provider

    provider = build_auth_provider(state.auth_config, admin_pool=getattr(state, "admin_db", None))
    validator = validator_for_scheme(provider, "bearer")
    if validator is None:
        raise PermissionError(
            f"auth provider {provider.provider_name!r} accepts no bearer credential, "
            "so it cannot authenticate a Flight client"
        )
    # REQ-1393: Flight names no principal, so the throttle keys on the credential digest.
    return await throttled(validator, token, principal=None)


async def _resolve_identity_org(state, identity, request: dict[str, object]) -> str | None:
    """The org an authenticated Flight session binds (REQ-1266, REQ-1337).

    The same membership rule MCP and pgwire use: the principal's own memberships decide, and a
    ticket's ``org`` is a REQUEST honored only for a principal holding the cross-org right.
    """
    from provisa.api.org_resolve import resolve_session_org
    from provisa.security.rights import can_act_cross_org, capabilities_for_claims

    caps = capabilities_for_claims(identity.roles or [], getattr(state, "roles", {}))
    requested = request.get("org")
    return await resolve_session_org(
        state,
        user_id=identity.user_id,
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested if isinstance(requested, str) else identity.active_org_id,
    )


def _is_sql(query: str) -> bool:
    return bool(_SQL_PREFIX.match(query))


def _is_cypher(query: str) -> bool:
    return bool(_CYPHER_PREFIX.match(query))


_WHERE_PRED_RE = re.compile(
    r"(\w+)\s*=\s*(?:'([^']*)'|([-]?\d+\.\d+)|([-]?\d+))",
    re.IGNORECASE,
)


def _parse_where_variables(sql: str) -> dict[str, int | float | str]:
    """Extract col=val predicates from a WHERE clause (REQ-302)."""
    where_match = re.search(r"\bWHERE\b(.+?)(?:\bLIMIT\b|$)", sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return {}
    clause = where_match.group(1)
    result: dict[str, int | float | str] = {}
    for m in _WHERE_PRED_RE.finditer(clause):
        col = m.group(1)
        if m.group(2) is not None:
            result[col] = m.group(2)
        elif m.group(3) is not None:
            result[col] = float(m.group(3))
        else:
            result[col] = int(m.group(4))
    return result


def _parse_limit_value(value: int | bool | None) -> int | None:
    """Validate and return a row-limit integer, or None for unlimited."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]
            "limit must be a non-negative integer"
        )
    return value


class ProvisaFlightServer(
    flight.FlightServerBase
):  # REQ-045, REQ-051, REQ-143, REQ-369  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Arrow Flight server that executes GraphQL queries and streams Arrow data."""

    def __init__(
        self,
        state: AppState,
        location: str = "grpc://0.0.0.0:8815",
        *,
        main_loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: object,  # object-ok: forwarded verbatim to FlightServerBase.__init__ which accepts arbitrary keyword args  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> None:
        super().__init__(location, **kwargs)
        self._state = state
        # The main event loop owns the asyncpg pools; dispatch coroutines to it.
        self._main_loop = main_loop or asyncio.get_event_loop()
        # Keep a local loop for non-pool async work.
        self._loop = asyncio.new_event_loop()

    # ------------------------------------------------------------------
    # Per-org routing (REQ-1266)
    # ------------------------------------------------------------------

    def _run_on_loop(self, coro, *, timeout: float | None = None):
        """Dispatch *coro* to the main loop with the worker thread's active org bound inside it.

        Reads ``current_org`` on THIS (flight worker) thread — where the caller has bound it — and
        re-binds it inside the loop coroutine, since ``run_coroutine_threadsafe`` won't carry it."""
        from provisa.api.org_runtime import current_org

        org_id = current_org.get(None)
        fut = asyncio.run_coroutine_threadsafe(_run_with_org(org_id, coro), self._main_loop)
        return fut.result(timeout=timeout) if timeout is not None else fut.result()

    def _resolve_and_bind_org(self, request: dict[str, object], identity=None):
        """Resolve the org for this ticket and bind it on this worker thread; return the reset token.

        When the connection is authenticated the org comes from the validated principal's
        membership (the same rule MCP and pgwire use), so a ticket cannot name someone else's org.
        Unsecured deployments have no principal to resolve, so the org is taken from an explicit
        ``org`` in the ticket; under multitenancy it is REQUIRED — a missing org raises rather than
        silently binding the default (no cross-tenant default). Single-org deployments return
        ``None`` and leave the ContextVar unset (default runtime)."""
        if not getattr(self._state, "multitenancy", False):
            return None
        if identity is not None:
            org_id = self._run_on_loop(_resolve_identity_org(self._state, identity, request))
        else:
            org_id = request.get("org")
            if not org_id or not isinstance(org_id, str):
                raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                    "org is required under multitenancy"
                )
        from provisa.api.app import ensure_org_runtime
        from provisa.api.org_runtime import set_current_org

        # Build the org runtime (idempotent) on the main loop, then bind it on this thread.
        asyncio.run_coroutine_threadsafe(ensure_org_runtime(org_id), self._main_loop).result()
        return set_current_org(org_id)

    # ------------------------------------------------------------------
    # Authentication (REQ-1263)
    # ------------------------------------------------------------------

    def _auth_active(self) -> bool:
        """Whether this deployment authenticates Flight clients.

        Mirrors pgwire's fail-closed reading of the same state: a live auth middleware with no
        resolved ``auth_config`` is a misconfiguration, and a secured server must never degrade
        to trust mode because its config went missing."""
        if getattr(self._state, "auth_config", None) is not None:
            return True
        if getattr(self._state, "auth_middleware_active", False):
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                "flight auth_config not configured"
            )
        return False

    def _authenticate(self, credential: str | None):
        """Validate a bearer credential and return its identity, or None when auth is off.

        The credential is a provider token or a personal access token; both resolve through the
        one bearer validator, so Flight needs no knowledge of which was presented."""
        if not self._auth_active():
            return None
        if not credential:
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                "a bearer credential is required"
            )
        try:
            return self._run_on_loop(_validate_flight_credential(self._state, credential))
        except (ValueError, jwt.PyJWTError) as e:
            # Every rejection reads the same on the wire: a caller must not learn from the
            # response whether the credential was unknown, expired or revoked.
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                "credential rejected"
            ) from e

    def _authorize_role(self, identity, request: dict[str, object]) -> str:
        """The role this ticket executes as — derived from the validated identity, never asserted.

        A ticket may REQUEST a role, and it is honored only when the identity's own assignments
        carry it; anything else is a privilege claim by the client and is refused. With no request,
        the identity's claims map to a role through the same rules every other surface uses."""
        from provisa.auth.role_mapping import resolve_assignments, resolve_role

        auth_config = self._state.auth_config
        assert auth_config is not None  # an identity exists ⇒ auth is active ⇒ config is resolved
        default_role = auth_config.get("default_role")
        if not default_role:
            # No admin default: an identity matching no mapping rule is refused, not escalated.
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                "identity matched no role and no default_role is configured"
            )
        mapped = resolve_role(identity, auth_config.get("role_mapping", []), default_role)
        requested = request.get("role")
        if not requested:
            return mapped
        permitted = {a.role_id for a in resolve_assignments(identity)} | {mapped}
        if str(requested) not in permitted:
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                f"role {str(requested)!r} is not assigned to this identity"
            )
        return str(requested)

    # ------------------------------------------------------------------
    # Flight SQL handshake
    # ------------------------------------------------------------------

    def do_handshake(  # REQ-608
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        payload: Iterable[bytes],
    ) -> tuple[bytes, list[object]]:
        """Validate the handshake credential and return the session's authenticated role (REQ-1263).

        The handshake carries a bearer token — a provider token or a personal access token. The
        role it comes back with is derived from the validated identity, so a client learns what it
        is allowed to be rather than declaring it; a ``role`` in the payload is a request, honored
        only when the identity's assignments carry it. On an unsecured deployment there is no
        credential to validate and the requested role passes through, matching the ticket path.
        """
        buf = b""
        for chunk in payload:
            buf += chunk
        try:
            data = json.loads(buf.decode("utf-8")) if buf else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}
        credential = data.get("token")
        identity = self._authenticate(credential if isinstance(credential, str) else None)
        role_id = data.get("role", "") if identity is None else self._authorize_role(identity, data)
        token = json.dumps({"role": role_id}).encode("utf-8")
        return token, []

    # ------------------------------------------------------------------
    # list_flights — enumerate available data
    # ------------------------------------------------------------------

    def list_flights(  # REQ-126, REQ-127
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        criteria: bytes,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportUnusedParameter]
    ) -> Iterator[flight.FlightInfo]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """List available flights: catalog tables, then registered commands (REQ-1156).

        Commands are listed alongside tables (descriptor path ``["commands", domain, name]``) so a
        Flight client discovers a registered command instead of it being invocable-but-invisible;
        the listing is role-agnostic, matching the table catalog's broadest view."""
        from provisa.api.data.action_exec import list_visible_commands

        tables = build_catalog_tables(self._state)
        for table in tables:
            yield catalog_table_to_flight_info(table)
        for command in list_visible_commands(self._state, None):
            yield command_to_flight_info(command)

    # ------------------------------------------------------------------
    # get_flight_info — metadata for a specific flight
    # ------------------------------------------------------------------

    def get_flight_info(  # REQ-608
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return FlightInfo for a catalog table descriptor.  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        Descriptor path: [domain_id, table_name].
        """
        path = [p.decode("utf-8") if isinstance(p, bytes) else p for p in descriptor.path]

        # REQ-1156: a command descriptor is ["commands", domain, name] — resolve it to the command
        # FlightInfo so a client can fetch a registered command's shape, not only a table's.
        if len(path) == 3 and path[0] == "commands":
            from provisa.api.data.action_exec import list_visible_commands

            for cmd in list_visible_commands(self._state, None):
                if cmd["domain"] == path[1] and cmd["name"] == path[2]:
                    return command_to_flight_info(cmd)
            raise flight.FlightServerError(f"Command not found: {path[1]}.{path[2]}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        # REQ-1319: a metric descriptor is ["metrics", <name>, <dim>...] — the metric shape
        # at the requested grain, discoverable alongside tables and commands. Execution rides
        # the governed SQL-ticket path via the semantic metrics.<name> form.
        if len(path) >= 2 and path[0] == "metrics":
            from provisa.api.flight.catalog import metric_to_flight_info

            name, dims = path[1], list(path[2:])
            registry = getattr(self._state, "metrics", {})
            m = registry.get(name)
            if m is None:
                raise flight.FlightServerError(f"Metric not found: {name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            return metric_to_flight_info(name, dims, description=m.description or m.ai_context)

        if len(path) == 2:
            domain_id, table_name = path[0], path[1]
            tables = build_catalog_tables(self._state)
            for t in tables:
                if t.domain_id == domain_id and t.table_name == table_name:
                    return catalog_table_to_flight_info(t)
            raise flight.FlightServerError(f"Table not found: {domain_id}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        raise flight.FlightServerError(f"Invalid descriptor path: {path}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # get_schema — Arrow schema for a catalog table
    # ------------------------------------------------------------------

    def get_schema(  # REQ-608
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.SchemaResult:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return the Arrow schema for a catalog table.

        Descriptor path: [domain_id, table_name].
        """
        path = list(descriptor.path)
        if len(path) != 2:
            raise flight.FlightServerError(f"get_schema requires path [domain, table], got {path}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
        table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]

        tables = build_catalog_tables(self._state)
        for t in tables:
            if t.domain_id == domain_id and t.table_name == table_name:
                schema = catalog_table_to_arrow_schema(t)
                return flight.SchemaResult(schema)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        raise flight.FlightServerError(f"Table not found: {domain_id}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # do_get — execute query or return catalog data
    # ------------------------------------------------------------------

    def do_get(  # REQ-051, REQ-143, REQ-145, REQ-267, REQ-345, REQ-369
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a query from the ticket and return Arrow record batches.

        Dispatch logic:
          1. If ticket contains 'query' → execute it through the governed pipeline.
          2. No 'query' → catalog metadata fetch (table/column listing).
        """
        try:
            request = json.loads(ticket.ticket.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}") from e  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        # REQ-1263: authenticate before anything reads the ticket. The role the rest of this call
        # runs under is the one the validated identity permits — the client's `role` string is a
        # request, never the identity — so it is substituted into the request here and every
        # downstream reader sees the authorized value.
        credential = request.get("token")
        identity = self._authenticate(credential if isinstance(credential, str) else None)
        if identity is not None:
            request["role"] = self._authorize_role(identity, request)

        # REQ-1266: bind the ticket's org on this worker thread so every self._state.X read (here and
        # in the nested helpers) resolves the org's runtime; _run_on_loop re-binds it inside each
        # dispatched loop coroutine. reset in finally below.
        _org_token = self._resolve_and_bind_org(request, identity)
        try:
            return self._do_get_inner(request, ticket)
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    def _do_get_inner(
        self,
        request: dict[str, object],
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        query_text = request.get("query", "")
        # REQ-693: Flight stays open in high-security mode — it is one of the two transports an
        # encrypting client actually uses — but a ticket that returns row data must carry the same
        # client-side decryption key the HTTP data endpoints demand. The catalog branch below
        # returns table/column names only, so it stays reachable exactly as /data/sdl does.
        if query_text:
            kms_key = request.get("kms_key")
            refusal = high_security_wire_reject(
                self._state, str(kms_key) if isinstance(kms_key, str) else None
            )
            if refusal is not None:
                raise flight.FlightServerError(refusal)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        ticket_type = "sql" if _is_sql(str(query_text)) else "graphql"
        with _tracer.start_as_current_span("flight.do_get") as span:
            span.set_attribute("flight.ticket_type", ticket_type)
            if ticket_type == "sql":
                span.set_attribute("flight.sql", str(query_text)[:200])
            else:
                span.set_attribute("flight.gql_query", str(query_text)[:200])

        if request.get("query"):
            # REQ-369: cap concurrent Arrow Flight query streams per role. The slot is
            # held for the execution window (results are materialized in _execute_query).
            limiter = getattr(self._state, "rate_limiter", None)
            # role scopes the rate-limit bucket; defaulting to admin would bypass authz.
            if not request.get("role"):
                raise flight.FlightServerError("role is required")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            role_id = str(request["role"])
            role = self._state.roles.get(role_id) or {}
            cap = (role.get("rate_limit") or {}).get("max_flight_streams")
            if limiter and cap:
                key = f"rl:flight:{role_id}"
                ok = asyncio.run_coroutine_threadsafe(
                    limiter.acquire(key, cap), self._main_loop
                ).result()
                if not ok:
                    raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                        "max concurrent Arrow Flight streams reached"
                    )
                try:
                    return self._execute_query(request)
                finally:
                    asyncio.run_coroutine_threadsafe(limiter.release(key), self._main_loop).result()
            return self._execute_query(request)

        return self._do_get_catalog(ticket)

    def do_action(  # REQ-608
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        action: flight.Action,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> list[flight.Result]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Handle a Flight action request."""
        try:
            body = json.loads(action.body.to_pybytes().decode("utf-8")) if action.body else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = {}
        query_text = body.get("query", "")
        ticket_type = "sql" if _is_sql(str(query_text)) else "graphql"
        with _tracer.start_as_current_span("flight.do_action") as span:
            span.set_attribute("flight.ticket_type", ticket_type)
            if ticket_type == "sql":
                span.set_attribute("flight.sql", str(query_text)[:200])
            else:
                span.set_attribute("flight.gql_query", str(query_text)[:200])
        return []

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _do_get_catalog(self, ticket: flight.Ticket) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return catalog metadata as Arrow record batches."""
        request = json.loads(ticket.ticket.decode("utf-8"))
        domain = request.get("domain")
        table_name = request.get("table")

        tables = build_catalog_tables(self._state)

        if domain and table_name:
            # Return schema info for a specific table as rows
            for t in tables:
                if t.domain_id == domain and t.table_name == table_name:
                    return flight.RecordBatchStream(self._build_columns_table(t))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            raise flight.FlightServerError(f"Table not found: {domain}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        # Return all tables as rows
        return flight.RecordBatchStream(self._build_catalog_table(tables, domain))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    @staticmethod
    def _build_catalog_table(
        tables: list[CatalogTable],
        domain_filter: str | None = None,
    ) -> pa.Table:
        """Build Arrow table listing catalog tables."""
        domains = []
        names = []
        descriptions = []
        for t in tables:
            if domain_filter and t.domain_id != domain_filter:
                continue
            domains.append(t.domain_id)
            names.append(t.table_name)
            descriptions.append(t.description)
        return pa.table(
            {
                "schema_name": pa.array(domains, type=pa.utf8()),
                "table_name": pa.array(names, type=pa.utf8()),
                "description": pa.array(descriptions, type=pa.utf8()),
            }
        )

    @staticmethod
    def _build_columns_table(cat_table: CatalogTable) -> pa.Table:
        """Build Arrow table of column metadata for a catalog table."""
        col_names = []
        col_types = []
        col_nullable = []
        col_descs = []
        for col in cat_table.columns:
            col_names.append(col.name)
            col_types.append(col.data_type)
            col_nullable.append(col.is_nullable)
            col_descs.append(col.description)
        return pa.table(
            {
                "column_name": pa.array(col_names, type=pa.utf8()),
                "data_type": pa.array(col_types, type=pa.utf8()),
                "is_nullable": pa.array(col_nullable, type=pa.bool_()),
                "description": pa.array(col_descs, type=pa.utf8()),
            }
        )

    def _compile_query(
        self, ticket_bytes: bytes
    ) -> tuple[
        DocumentNode,
        CompilationContext,
        RLSContext,
        dict[str, object] | None,
        CompiledQuery,
        RouteDecision,
        dict[str, object] | None,
    ]:
        """Parse ticket, compile GraphQL to SQL, apply security pipeline."""
        try:
            request = json.loads(ticket_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}") from e  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        query_text = request.get("query")
        role_id = request.get("role", "admin")
        variables = request.get("variables")

        if not query_text:
            raise flight.FlightServerError("Ticket must include 'query'")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        if role_id not in self._state.schemas:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        schema = cast("GraphQLSchema", self._state.schemas[role_id])
        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        document = parse_query(schema, query_text, variables)
        compiled_queries = compile_query(document, ctx, variables)
        if not compiled_queries:
            raise flight.FlightServerError("No query fields found")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        compiled = compiled_queries[0]

        decision = decide_route(
            sources=compiled.sources,
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
            source_dsns=getattr(self._state, "source_dsns", None),
        )

        return document, ctx, rls, role, compiled, decision, variables

    def _do_get_cypher(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream:  # REQ-345, REQ-347, REQ-352  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a Cypher query ticket and return Arrow record batches."""
        import concurrent.futures

        from provisa.cypher.assembler import assemble_rows, to_serializable
        from provisa.cypher.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.label_map import CypherLabelMap
        from provisa.cypher.params import (
            CypherParamError,
            bind_params,
            collect_param_names,
        )
        from provisa.cypher.parser import CypherParseError, parse_cypher
        from provisa.cypher.translator import (
            CypherCrossSourceError,
            CypherTranslateError,
            cypher_to_sql,
        )
        from provisa.pgwire._pipeline import _govern_and_route_compiled, require_governed_plan

        query_text = str(request.get("query", ""))
        # role drives governance/RLS routing; defaulting to admin would bypass authz.
        if not request.get("role"):
            raise flight.FlightServerError("role is required")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        role_id = str(request["role"])
        params_obj = request.get("params") or {}
        params: dict[str, object] = params_obj if isinstance(params_obj, dict) else {}

        if role_id not in self._state.contexts:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        ctx = self._state.contexts[role_id]

        try:
            ast = parse_cypher(query_text)
        except CypherParseError as exc:
            raise flight.FlightServerError(f"Cypher parse error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        label_map = CypherLabelMap.from_schema(ctx)

        param_names = collect_param_names(query_text)
        try:
            bind_params(param_names, params)
        except CypherParamError as exc:
            raise flight.FlightServerError(f"Cypher param error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, params)
        except (CypherCrossSourceError, CypherTranslateError) as exc:
            raise flight.FlightServerError(f"Cypher translate error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

        try:
            sql_str = sql_ast.sql(dialect="postgres")
        except Exception as exc:
            raise flight.FlightServerError(f"Cypher SQL render failed: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        from provisa.compiler.sql_rewrite import make_semantic_sql

        semantic_sql = make_semantic_sql(sql_str, ctx)

        resolved_params = [params.get(name) for name in ordered_params]

        try:
            plan = self._run_on_loop(
                _govern_and_route_compiled(
                    semantic_sql, role_id, exec_params=resolved_params or None, state=self._state
                )
            )
        except PermissionError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        except ValueError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        engine = getattr(self._state, "federation_engine", None)
        if engine is None:
            raise flight.FlightServerError("Federation engine not connected")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        physical_sql = plan.physical_sql
        if physical_sql is None:
            raise flight.FlightServerError(
                f"Route {plan.route!r} is not supported for Cypher via Flight"
            )  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        require_governed_plan(plan)  # REQ-1176: verify at the last moment, before the engine executes

        def _run() -> list[dict[str, object]]:
            # On a worker thread — go through the sync engine terminal, not a raw cursor.
            res = engine.execute_engine_sync(physical_sql, resolved_params or [])
            return [dict(zip(res.column_names, row, strict=False)) for row in res.rows]

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            raw_rows = pool.submit(_run).result()

        assembled = assemble_rows(raw_rows, graph_vars)
        serialized = [to_serializable(r) for r in assembled]

        if not serialized:
            columns = list(graph_vars.keys()) if graph_vars else []
            empty = {col: pa.array([], type=pa.utf8()) for col in columns}
            return flight.RecordBatchStream(pa.table(empty))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        col_names = list(serialized[0].keys())
        col_data: dict[str, list[object]] = {c: [] for c in col_names}
        for row in serialized:
            for col in col_names:
                val = row.get(col)
                col_data[col].append(json.dumps(val) if isinstance(val, (dict, list)) else val)
        return flight.RecordBatchStream(pa.table(col_data))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _execute_query(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Dispatch a query to the correct handler based on language."""
        query_text = str(request.get("query", ""))
        if _is_cypher(query_text):
            return self._do_get_cypher(request)
        if _is_sql(query_text):
            return self._do_get_sql_governed(request)
        return self._do_get_graphql(request)

    def _license_stream(self, table: "pa.Table", role_id: str):  # REQ-1137
        """Return a Flight stream for ``table``, attaching the license nag as app_metadata on the
        first batch when nagging (out-of-band — the row data is untouched). Once per role/session."""
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"flight:{role_id}")
        except Exception:
            text = None
        if not text:
            return flight.RecordBatchStream(table)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        meta = pa.py_buffer(text.replace("\n", " ").encode("utf-8"))

        def _gen():
            first = True
            for batch in table.to_batches():
                if first:
                    first = False
                    yield (batch, meta)  # app_metadata rides the first chunk
                else:
                    yield batch

        return flight.GeneratorStream(table.schema, _gen())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _license_stream_gen(self, schema, batch_gen, role_id: str):  # REQ-1137, REQ-1214
        """Return a Flight GeneratorStream over a LAZY record-batch generator, attaching the license
        nag as app_metadata on the first batch when nagging (out-of-band — row data untouched). The
        streaming counterpart of :meth:`_license_stream`: the result never materializes as a Table."""
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"flight:{role_id}")
        except Exception:
            text = None
        meta = pa.py_buffer(text.replace("\n", " ").encode("utf-8")) if text else None

        def _gen():
            first = True
            for batch in batch_gen:
                if first and meta is not None:
                    first = False
                    yield (batch, meta)  # app_metadata rides the first chunk
                else:
                    yield batch

        return flight.GeneratorStream(schema, _gen())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _do_get_sql_governed(
        self, request: dict[str, object]
    ) -> (
        flight.RecordBatchStream | flight.GeneratorStream
    ):  # REQ-267, REQ-266  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute SQL through the shared governance pipeline and return Arrow record batches."""
        from provisa.compiler.sql_gen import ColumnRef
        from provisa.pgwire._pipeline import govern_batch_final_plan, require_governed_plan
        from provisa.pgwire.function_call import maybe_invoke_registered_function

        sql = str(request.get("query", ""))
        role_id = str(request.get("role", "admin"))

        # REQ-1156: a `SELECT fn(...)` naming a registered command invokes it through the single
        # governed executor, matching pgwire/MCP — otherwise commands are dark over Flight SQL.
        fn_result = self._run_on_loop(
            maybe_invoke_registered_function(sql, role_id, self._state)
        )
        if fn_result is not None:
            columns = [
                ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
                for c in fn_result.column_names
            ]
            table = rows_to_arrow_table(fn_result.rows, columns)
            return self._license_stream(table, role_id)  # REQ-1137

        try:
            plan = self._run_on_loop(govern_batch_final_plan(sql, role_id, self._state))
        except PermissionError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        except ValueError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        require_governed_plan(plan)  # REQ-1176: verify at the last moment, before the engine executes
        if plan.route == Route.ENGINE:
            assert plan.physical_sql is not None
            # Streamed Arrow Flight is an advertised, engine-specific transport (REQ-825, REQ-145,
            # REQ-1214): drain the engine's LAZY record-batch terminal so a large user result set
            # never fully materializes on this transport (bounded by one batch, not total size).
            try:
                arrow_schema, batch_gen = self._state.federation_engine.execute_engine_stream(
                    plan.physical_sql, []
                )
            except RuntimeError as exc:
                raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            return self._license_stream_gen(arrow_schema, batch_gen, role_id)  # REQ-1137
        elif plan.route == Route.DIRECT:
            if self._state.source_pools.has(plan.source_id) and self._state.source_pools.supports_stream(
                plan.source_id
            ):
                # REQ-1190: a single-reachable-source scan streams via the source's server-side cursor,
                # adapted to a lazy Arrow record-batch generator — never materialized on this transport
                # (streaming-uniformity Defect 1). Mirrors the ENGINE streaming terminal above.
                from provisa.federation.runtime_support import arrow_batches_from_rows

                stream = self._state.federation_engine.execute_native_stream(
                    self._state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or [],
                    loop=self._main_loop,
                )
                arrow_schema, batch_gen = arrow_batches_from_rows(stream)
                return self._license_stream_gen(arrow_schema, batch_gen, role_id)  # REQ-1137
            result = self._run_on_loop(
                self._state.federation_engine.execute_native(
                    self._state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or [],
                )
            )
            columns = [
                ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
                for c in result.column_names
            ]
            table = rows_to_arrow_table(result.rows, columns)
            return self._license_stream(table, role_id)  # REQ-1137
        else:
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                f"Route {plan.route!r} is not supported for SQL via Flight"
            )

    def _do_get_graphql(  # REQ-143, REQ-144, REQ-145, REQ-146
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a GraphQL query ticket and return Arrow record batches."""
        from provisa.pgwire._pipeline import _govern_and_route_compiled, require_governed_plan

        role_id = str(request.get("role", ""))
        ticket_bytes = json.dumps(request).encode("utf-8")
        _, _, _, _, compiled, _, _ = self._compile_query(ticket_bytes)

        try:
            plan = self._run_on_loop(
                _govern_and_route_compiled(compiled.sql, role_id, state=self._state)
            )
        except PermissionError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        except ValueError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        require_governed_plan(plan)  # REQ-1176: verify at the last moment, before the engine executes
        if plan.route == Route.DIRECT:
            result = self._run_on_loop(
                self._state.federation_engine.execute_native(
                    self._state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or compiled.params,
                )
            )
            table = rows_to_arrow_table(result.rows, compiled.columns)
            return flight.RecordBatchStream(table)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        assert plan.physical_sql is not None
        # Streamed Arrow Flight is an advertised, engine-specific transport (REQ-825, REQ-145).
        try:
            arrow_schema, batch_gen = self._state.federation_engine.execute_engine_stream(
                plan.physical_sql,
                compiled.params,
            )
        except RuntimeError as exc:
            raise flight.FlightServerError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        return flight.GeneratorStream(arrow_schema, batch_gen)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
