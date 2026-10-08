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

import json
import logging
import re
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, TypeVar, cast

from provisa.federation.execution_auth import plan_authorization

import jwt
import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.flight.compression import generator_stream, record_batch_stream
from provisa.api.flight.catalog import (
    CatalogTable,
    build_catalog_tables,
    role_sees_metric,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
    command_to_flight_info,
)
from provisa.compiler.directives import cache_hint_for
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.core import request_deadline
from provisa.core.connection_loop import current_connection_loop, run_on_connection_loop
from provisa.core.limits import request_timeout_for  # REQ-1905: Flight's own request timeout
from provisa.core.rpc_loop import hold_loop_for_stream as _hold_loop_for_stream
from provisa.core.rpc_loop import run_rpc as _run_rpc
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import in_request_span as _in_request_span
from provisa.otel_compat import request_span as _request_span
from provisa.security.high_security import high_security_wire_reject
from provisa.fakes.read_sql import reads_fakes
from provisa.transpiler.router import Route, decide_route

_tracer = _get_tracer(__name__)

if TYPE_CHECKING:
    from graphql import DocumentNode, GraphQLSchema

    from provisa.api.app import AppState
    from provisa.compiler.sql_gen import CompilationContext, CompiledQuery
    from provisa.transpiler.router import RouteDecision

log = logging.getLogger(__name__)

T = TypeVar("T")

# REQ-1882 (amended 2026-09-29): the entire request runs on its handler thread. pyarrow.flight
# serves each RPC on a gRPC handler thread (recycled between calls — thread-local state does not
# survive from one call to the next), so each RPC checks a ConnectionLoop out for its duration and
# runs every coroutine — auth, org resolution, rate limiting, governance, residency, audit — on it
# via ``loop.run_until_complete`` on that same thread. A do_get whose result streams from the
# loop after the handler returns (a DIRECT server-side cursor) keeps the loop until the stream
# ends; the stream is drained on the same handler thread.


_SQL_PREFIX = re.compile(r"\s*(SELECT|WITH)\b", re.IGNORECASE)
_CYPHER_PREFIX = re.compile(
    r"\s*(MATCH|OPTIONAL\s+MATCH|CALL|WITH|MERGE|CREATE|RETURN)\b", re.IGNORECASE
)
# Leading comments a statement may carry before its first keyword — e.g. the REQ-544 response-cache
# opt-in `-- @provisa cache=true` (SQL) or a `// @provisa ...` hint (Cypher) — skipped when the
# statement's language is detected.
_SQL_LEADING_COMMENTS = re.compile(r"(?:\s*(?:--[^\n]*(?:\n|$)|/\*.*?\*/))*", re.DOTALL)
_CYPHER_LEADING_COMMENTS = re.compile(r"(?:\s*(?://[^\n]*(?:\n|$)|/\*.*?\*/))*", re.DOTALL)


async def _prepare_engine_residency(state, plan) -> None:
    """Land ENGINE-route residency in ONE coroutine (REQ-1887).

    Thin wrapper around the shared ``provisa.federation.query_residency.prepare_engine_residency``
    (moved there so pgwire's own ENGINE-route dispatch can reuse the identical fold without a
    Flight<->pgwire cross-import — see that function's docstring)."""
    from provisa.federation.query_residency import prepare_engine_residency

    await prepare_engine_residency(state, plan)


async def _run_with_org(org_id: str | None, coro):
    """Bind ``current_org`` inside the RPC's connection-loop coroutine (REQ-1266).

    The loop's task copies the handler thread's context, so this makes the org the caller read
    off the thread explicit around the awaited work."""
    if org_id is None:
        return await coro
    from provisa.core.request_context import reset_current_org, set_current_org

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


async def _resolve_identity_org(state, identity, request: dict[str, object]) -> str:
    """The org an authenticated Flight session binds (REQ-1266, REQ-1337).

    The same membership rule MCP and pgwire use: the principal's own memberships decide, and a
    ticket's ``org`` is a REQUEST honored only for a principal holding the cross-org right.
    """
    from provisa.api.org_resolve import resolve_session_org
    from provisa.security.rights import can_act_cross_org, capabilities_for_claims

    caps = capabilities_for_claims(identity.roles or [], state.platform_roles)
    requested = request.get("org")
    return await resolve_session_org(
        state,
        user_id=identity.user_id,
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested if isinstance(requested, str) else None,
        credential_org=identity.active_org_id,  # REQ-1235
        named_by='set "org" in the ticket',
    )


def _after_leading_comments(comments: re.Pattern[str], query: str) -> int:
    m = comments.match(query)
    assert m is not None  # every part of the pattern is optional: it matches any string
    return m.end()


def _is_sql(query: str) -> bool:
    return bool(_SQL_PREFIX.match(query, _after_leading_comments(_SQL_LEADING_COMMENTS, query)))


def _is_cypher(query: str) -> bool:
    return bool(
        _CYPHER_PREFIX.match(query, _after_leading_comments(_CYPHER_LEADING_COMMENTS, query))
    )


def _report_table(table: "pa.Table") -> None:
    """Meter a materialized Flight result as egress (REQ-1452).

    ``nbytes`` is the Arrow buffer size, not the IPC frame size: pyarrow's writer exposes no Python
    byte seam, so this is an approximation missing framing metadata.
    """
    from provisa.core.request_context import current_org
    from provisa.core.egress import report

    report(current_org.get(), table.nbytes)


def _metered_batches(batches):
    """Meter a lazy Flight result batch by batch (REQ-1452).

    The org is captured eagerly because pyarrow drains this generator after ``do_get`` returned and
    reset the ticket's org.
    """
    from provisa.core.request_context import current_org
    from provisa.core.egress import report

    org_id = current_org.get()
    for batch in batches:
        report(org_id, batch.nbytes)
        yield batch


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


_FLIGHT_ERROR_MAX_LEN = 8000


def _audited_arrow(plan: Any, batch_gen: Any) -> Any:
    """A streamed result's record batches, with the statement's audit row written when the drain
    ends (REQ-074: ``finalize_audit(defer_to_drain=True)`` + ``audit_on_drain``) — pyarrow pulls
    them after do_get returns, so only then is the row count known."""
    from provisa.pgwire._pipeline import audit_on_drain

    return audit_on_drain(plan, batch_gen, rows_in=lambda batch: batch.num_rows)


def _flight_error(msg: str, cause: Exception | None = None) -> flight.FlightServerError:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Wrap *msg* as a FlightServerError, capped well under gRPC's 16KB metadata-size limit.

    pyarrow propagates a FlightServerError's message as gRPC trailing metadata; an uncapped
    message (e.g. a validation error embedding a large SQL statement) exceeds grpc's default
    16KB max_metadata_size and surfaces to the client as an opaque RESOURCE_EXHAUSTED error
    instead of the real message.
    """
    if len(msg) > _FLIGHT_ERROR_MAX_LEN:
        msg = msg[:_FLIGHT_ERROR_MAX_LEN] + "...(truncated)"
    err = flight.FlightServerError(msg)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if cause is not None:
        err.__cause__ = cause
    return err


def _ticket_delivery(request: dict[str, object], role_id: str):  # REQ-1194
    """The forced redirect a ticket asks for with its ``redirect`` / ``redirect_format`` options,
    or None. Flight is a streaming transport: it is never delivered automatically (REQ-1224
    amended 2026-10-04). An option that cannot be read is refused by name."""
    from provisa.executor.redirect import (
        RedirectFormatUnknown,
        delivery_from_request,
        parse_redirect_format,
    )

    redirect = request.get("redirect", False)  # an absent option asks for no redirect
    if not isinstance(redirect, bool):
        raise _flight_error(f"ticket option redirect must be true or false, not {redirect!r}")
    fmt = request.get("redirect_format")
    if fmt is not None and not isinstance(fmt, str):
        raise _flight_error(f"ticket option redirect_format must be a format name, not {fmt!r}")
    try:
        redirect_format = parse_redirect_format(fmt) if fmt else None
    except RedirectFormatUnknown as exc:
        raise _flight_error(str(exc), exc) from exc
    return delivery_from_request(
        force_redirect=redirect, redirect_format=redirect_format, threshold=None, role=role_id
    )


def _redirect_table(handle: dict | None, delivery: Any) -> "pa.Table":  # REQ-1194
    """The one-row table a delivered ticket answers: url, format, row_count, expires_at."""
    from provisa.executor.redirect import redirect_row

    if handle is None:
        raise _flight_error("the materialize terminal answered rows instead of a delivery handle")
    url, fmt, row_count, expires_at = redirect_row(handle, delivery)
    return pa.table(
        {
            "url": pa.array([url], pa.string()),
            "format": pa.array([fmt], pa.string()),
            "row_count": pa.array([row_count], pa.int64()),
            "expires_at": pa.array([expires_at], pa.timestamp("us", tz="UTC")),
        }
    )


def _parse_limit_value(value: int | bool | None) -> int | None:
    """Validate and return a row-limit integer, or None for unlimited."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _flight_error("limit must be a non-negative integer")
    return value


# The do_action type that answers the worker's health report.
_HEALTHCHECK_ACTION = "healthcheck"


# The middleware key the call's headers are kept under.
_HEADERS = "headers"


class _CallHeaders(flight.ServerMiddleware):  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The headers a call arrived with."""

    def __init__(self, headers: dict[str, list[str]]) -> None:
        self.headers = headers


class _CallHeadersFactory(flight.ServerMiddlewareFactory):  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    def start_call(self, info: object, headers: dict[str, list[str]]) -> _CallHeaders:  # noqa: ARG002  # required by the override signature
        return _CallHeaders(headers)


def _header(headers: dict[str, list[str]], name: str) -> str | None:
    """The first value of call header ``name`` (gRPC lowercases header names), if it was sent."""
    values = headers.get(name)
    return values[0] if values else None


def _bearer(headers: dict[str, list[str]]) -> str | None:
    """The bearer credential in the call's ``authorization`` header, if it carries one."""
    raw = _header(headers, "authorization")
    if raw is None:
        return None
    scheme, _, token = raw.partition(" ")
    return token if scheme.lower() == "bearer" and token else None


class ProvisaFlightServer(
    flight.FlightServerBase
):  # REQ-045, REQ-051, REQ-143, REQ-369  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Arrow Flight server that executes GraphQL queries and streams Arrow data."""

    def __init__(
        self,
        state: AppState,
        location: str = "grpc://0.0.0.0:8815",
        **kwargs: object,  # object-ok: forwarded verbatim to FlightServerBase.__init__ which accepts arbitrary keyword args  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> None:
        # The call's headers, for the RPCs that carry no ticket (list_flights, get_flight_info,
        # get_schema): their credential and requested role arrive there (_catalog_role).
        super().__init__(location, middleware={_HEADERS: _CallHeadersFactory()}, **kwargs)  # type: ignore[arg-type]
        self._state = state

    # ------------------------------------------------------------------
    # Per-org routing (REQ-1266)
    # ------------------------------------------------------------------

    def _run_on_loop(self, coro, *, timeout: float | None = None):
        """Run *coro* on this RPC's connection loop, on this handler thread (REQ-1882).

        Reads ``current_org`` and the audit identity on this thread — where the caller bound them —
        and binds them explicitly inside the coroutine (``_run_with_org`` /
        ``with_audit_identity``)."""
        from provisa.core.request_context import current_org
        from provisa.audit.context import current_audit_identity, with_audit_identity

        org_id = current_org.get(None)
        # REQ-074/REQ-1386: the acting principal crosses the same thread boundary as the org, and
        # for the same reason — the pipeline's audit write runs inside this loop coroutine.
        ident = current_audit_identity()
        inner = coro if ident is None else with_audit_identity(ident.user_id, ident.surface, coro)
        return run_on_connection_loop(_run_with_org(org_id, inner), timeout=timeout)

    def _finalize_audit(self, plan, status_code: int, *, defer_to_drain: bool = False) -> None:
        """Write the governed plan's audit row (REQ-074/REQ-1386).

        Flight governs on its connection loop and then drains the engine's terminal on this handler
        thread, so the plan never reaches ``_execute_plan`` and the row is written here.
        ``finalize_audit`` is idempotent per plan, so a later failure cannot double-write."""
        from provisa.pgwire._pipeline import finalize_audit

        self._run_on_loop(
            finalize_audit(plan, status_code, self._state, defer_to_drain=defer_to_drain)
        )

    def _in_catalog_org(self, fn: Callable[[], Any]) -> Any:
        """Run a metadata RPC's ``fn`` in the org whose catalog it describes (REQ-1266).

        list_flights, get_flight_info and get_schema carry no ticket, so they name no org. A
        single-org deployment answers them from its one org; under multitenancy they are refused
        by name -- the catalog is an org's, and no org's catalog is everyone's. A do_get catalog
        ticket names its org and answers the same question. Who may ask, and what they are shown,
        is :meth:`_catalog_role`'s."""
        from provisa.core.request_context import reset_current_org, set_current_org

        if getattr(self._state, "multitenancy", False):
            raise _flight_error(
                "catalog metadata names no org under multitenancy; fetch it with a do_get "
                "catalog ticket that names the org"
            )
        token = set_current_org(self._state.org_id)
        try:
            return fn()
        finally:
            reset_current_org(token)

    def _resolve_and_bind_org(self, request: dict[str, object], identity=None):
        """Resolve the org for this ticket and bind it on this worker thread; return the reset token.

        When the connection is authenticated the org comes from the validated principal's
        membership (the same rule MCP and pgwire use), so a ticket cannot name someone else's org.
        Unsecured deployments have no principal to resolve, so the org is taken from an explicit
        ``org`` in the ticket; under multitenancy it is REQUIRED — a missing org raises rather than
        silently binding the default (no cross-tenant default). A single-org deployment binds its
        one org."""
        from provisa.core.request_context import set_current_org

        if not getattr(self._state, "multitenancy", False):
            return set_current_org(self._state.org_id)
        if identity is not None:
            org_id = self._run_on_loop(_resolve_identity_org(self._state, identity, request))
        else:
            org_id = request.get("org")
            if not org_id or not isinstance(org_id, str):
                raise _flight_error("org is required under multitenancy")
        from provisa.api.app import ensure_serving_runtime

        # Build the org runtime (idempotent) on this RPC's connection loop, on this thread.
        run_on_connection_loop(ensure_serving_runtime(org_id))
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
            raise _flight_error("flight auth_config not configured")
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

    def _catalog_role(self, context: flight.ServerCallContext) -> str | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The role a ticketless metadata RPC lists the catalog as (REQ-1263), or None when this
        deployment authenticates nobody.

        list_flights, get_flight_info and get_schema carry no ticket, so their credential is the
        call's ``authorization: Bearer <token>`` header and the role they ask for its
        ``x-provisa-role`` header (one held role, or a comma-separated set acting as its
        meta-role) — the headers the native gRPC transport reads. With authentication on a call
        without a valid credential is refused, as a ticket without one is, and the catalog it is
        answered with is the authorized role's own: the tables, columns and commands that role is
        served on every other surface."""
        if not self._auth_active():
            return None
        middleware = context.get_middleware(_HEADERS)
        headers = middleware.headers if middleware is not None else {}
        identity = self._authenticate(_bearer(headers))
        assert identity is not None  # auth is active: _authenticate returned or raised
        return self._authorize_role(identity, {"role": _header(headers, "x-provisa-role")})

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
        from provisa.security.meta_role import resolve_requested_role

        # One role, or a comma-separated set of held roles acting as their meta-role.
        try:
            return resolve_requested_role(self._state, permitted, str(requested))
        except PermissionError as exc:
            raise flight.FlightUnauthenticatedError(str(exc)) from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

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

        def _body() -> tuple[bytes, list[object]]:
            credential = data.get("token")
            identity = self._authenticate(credential if isinstance(credential, str) else None)
            role_id = (
                data.get("role", "") if identity is None else self._authorize_role(identity, data)
            )
            return json.dumps({"role": role_id}).encode("utf-8"), []

        return _run_rpc(_body)

    # ------------------------------------------------------------------
    # list_flights — enumerate available data
    # ------------------------------------------------------------------

    def list_flights(  # REQ-126, REQ-127
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        criteria: bytes,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportUnusedParameter]
    ) -> Iterator[flight.FlightInfo]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """List available flights: catalog tables, then registered commands (REQ-1156).

        Commands are listed alongside tables (descriptor path ``["commands", domain, name]``) so a
        Flight client discovers a registered command instead of it being invocable-but-invisible.
        With authentication on the call carries a credential and lists what its role is served
        (:meth:`_catalog_role`); with none the listing is the whole catalog."""
        from provisa.api.data.action_exec import list_visible_commands

        def _infos() -> list[flight.FlightInfo]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            role = self._catalog_role(context)
            return self._in_catalog_org(
                lambda: [
                    *(
                        catalog_table_to_flight_info(t)
                        for t in build_catalog_tables(self._state, role)
                    ),
                    *(command_to_flight_info(c) for c in list_visible_commands(self._state, role)),
                ]
            )

        yield from _run_rpc(_infos)

    # ------------------------------------------------------------------
    # get_flight_info — metadata for a specific flight
    # ------------------------------------------------------------------

    def get_flight_info(  # REQ-608
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return FlightInfo for a catalog table descriptor.  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        Descriptor path: [domain_id, table_name]. What the caller's role is not served is not
        found (:meth:`_catalog_role`).
        """

        def _info() -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            role = self._catalog_role(context)
            return self._in_catalog_org(lambda: self._flight_info(descriptor, role))

        return _run_rpc(_info)

    def _flight_info(
        self,
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        role: str | None,
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        path = [p.decode("utf-8") if isinstance(p, bytes) else p for p in descriptor.path]

        # REQ-1156: a command descriptor is ["commands", domain, name] — resolve it to the command
        # FlightInfo so a client can fetch a registered command's shape, not only a table's.
        if len(path) == 3 and path[0] == "commands":
            from provisa.api.data.action_exec import list_visible_commands

            for cmd in list_visible_commands(self._state, role):
                if cmd["domain"] == path[1] and cmd["name"] == path[2]:
                    return command_to_flight_info(cmd)
            raise _flight_error(f"Command not found: {path[1]}.{path[2]}")

        # REQ-1319: a metric descriptor is ["metrics", <name>, <dim>...] — the metric shape
        # at the requested grain, discoverable alongside tables and commands. Execution rides
        # the governed SQL-ticket path via the semantic metrics.<name> form.
        if len(path) >= 2 and path[0] == "metrics":
            from provisa.api.flight.catalog import metric_to_flight_info

            name, dims = path[1], list(path[2:])
            registry = getattr(self._state, "metrics", {})
            m = registry.get(name)
            if m is None or (role is not None and not role_sees_metric(self._state, role, m)):
                raise _flight_error(f"Metric not found: {name}")
            return metric_to_flight_info(name, dims, description=m.description or m.ai_context)

        if len(path) == 2:
            domain_id, table_name = path[0], path[1]
            tables = build_catalog_tables(self._state, role)
            for t in tables:
                if t.domain_id == domain_id and t.table_name == table_name:
                    return catalog_table_to_flight_info(t)
            raise _flight_error(f"Table not found: {domain_id}.{table_name}")

        raise _flight_error(f"Invalid descriptor path: {path}")

    # ------------------------------------------------------------------
    # get_schema — Arrow schema for a catalog table
    # ------------------------------------------------------------------

    def get_schema(  # REQ-608
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.SchemaResult:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return the Arrow schema for a catalog table: the columns the caller's role is served
        (:meth:`_catalog_role`).

        Descriptor path: [domain_id, table_name].
        """
        path = list(descriptor.path)
        if len(path) != 2:
            raise _flight_error(f"get_schema requires path [domain, table], got {path}")

        domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
        table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]

        def _tables() -> list:
            role = self._catalog_role(context)
            return self._in_catalog_org(lambda: build_catalog_tables(self._state, role))

        tables = _run_rpc(_tables)
        for t in tables:
            if t.domain_id == domain_id and t.table_name == table_name:
                schema = catalog_table_to_arrow_schema(t)
                return flight.SchemaResult(schema)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        raise _flight_error(f"Table not found: {domain_id}.{table_name}")

    # ------------------------------------------------------------------
    # do_get — execute query or return catalog data
    # ------------------------------------------------------------------

    @_in_request_span(_tracer, "flight.do_get", transport="flight")  # REQ-1910
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
            raise _flight_error(f"Invalid ticket: {e}", e) from e
        # REQ-1882: the whole RPC — and any stream it returns — runs on this handler thread's loop.
        return _run_rpc(lambda: self._do_get_on_loop(request, ticket))

    def _do_get_on_loop(
        self,
        request: dict[str, object],
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
        _shield = request_deadline.shielded()
        try:
            from provisa.audit.context import ANONYMOUS_USER, audit_identity_scope

            # REQ-074/REQ-1386: the validated principal, or — on a deployment that authenticates
            # nobody — the anonymous one: the request is audited either way.
            user_id = identity.user_id if identity is not None else ANONYMOUS_USER
            with audit_identity_scope(user_id, "flight"):
                return self._do_get_inner(request, ticket)
        finally:
            # REQ-1905: do_get is over on this handler thread; a stream it returned binds the
            # deadline again around each batch it pulls (provisa/api/flight/deadline.py).
            with _shield.lock:
                _shield.settle()
                _shield.quiesce()
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

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
                raise _flight_error(refusal)
        ticket_type = "sql" if _is_sql(str(query_text)) else "graphql"
        with _request_span(_tracer, "flight.do_get", transport="flight") as span:
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
                raise _flight_error("role is required")
            role_id = str(request["role"])
            role = self._state.roles.get(role_id) or {}
            cap = (role.get("rate_limit") or {}).get("max_flight_streams")

            # REQ-1905: ONE deadline covers the whole ticket — the wait for a stream slot, the
            # execution and the stream that pyarrow pulls after this returns
            # (provisa/api/flight/deadline.py): the server's request_timeout, as on every other
            # transport. The server-wide stream slot itself is taken further in, where the engine
            # or a source is actually reached (see _acquire_stream_slot): after the response-cache
            # check, and held until the stream is drained.
            from provisa.api.flight.deadline import FlightDeadlineExceeded, request_budget

            try:
                with request_budget(request_timeout_for("flight")):
                    result = self._execute_with_role_cap(request, limiter, role_id, cap)
            except FlightDeadlineExceeded as exc:
                raise _flight_error(str(exc), exc) from exc
            # REQ-1910: the statement text is a debug-detail fact, and the request's detail is
            # known only once the pipeline has resolved its trace scope (a window, or the ticket's
            # own hint): recorded here, after that, rather than only at the ticket's arrival.
            from provisa.otel_compat import annotate_request

            text_attr = "flight__sql" if ticket_type == "sql" else "flight__gql_query"
            annotate_request(**{text_attr: str(query_text)[:200]})
            return result

        # With authentication on, request["role"] is the authorized role (_do_get_on_loop) and the
        # catalog is that role's; with none there is no role to narrow it by.
        return self._do_get_catalog(ticket, str(request["role"]) if self._auth_active() else None)

    def _acquire_stream_slot(self) -> Callable[[], None]:  # REQ-1905
        """Take one of this worker's Flight stream slots, waiting for it within the request's
        deadline, and return what gives it back. Called at every point a ticket reaches the engine
        or a source — never for a response-cache HIT — so the server-wide limit bounds what it is
        there to protect: the engine and sources the other transports share with Flight. Refused,
        with an error naming the limit, only when no slot frees within the deadline."""
        global_cap = getattr(self._state, "flight_global_cap", None)
        if not global_cap:
            return lambda: None
        from provisa.api.flight.stream_slots import StreamLimitTimeout, slots_for

        try:
            return slots_for(global_cap).acquire(request_timeout_for("flight"))
        except StreamLimitTimeout as exc:
            raise _flight_error(str(exc), exc) from exc

    def _execute_with_role_cap(  # REQ-369
        self, request: dict[str, object], limiter: object | None, role_id: str, cap: int | None
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute the query ticket under the role's own ``max_flight_streams`` gate, if any."""
        if limiter and cap:
            key = f"rl:flight:{role_id}"
            ok = run_on_connection_loop(limiter.acquire(key, cap))  # type: ignore[attr-defined]
            if not ok:
                raise _flight_error("max concurrent Arrow Flight streams reached")
            try:
                return self._execute_query(request)
            finally:
                run_on_connection_loop(limiter.release(key))  # type: ignore[attr-defined]
        return self._execute_query(request)

    def list_actions(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
    ) -> list[tuple[str, str]]:
        """The actions this server answers by name."""
        return [
            (
                _HEALTHCHECK_ACTION,
                "The health of the worker answering, as GET /health reports it (JSON).",
            )
        ]

    def do_action(  # REQ-608
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage, reportUnusedParameter]  # lib omits __all__
        action: flight.Action,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> list[flight.Result]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Handle a Flight action request.

        ``healthcheck``: one result, the JSON report ``GET /health`` answers with
        (``provisa.api.health_report``) — for a client or a load balancer that reaches this
        server over Flight only. It needs no credential, as ``/health`` needs none."""
        if action.type == _HEALTHCHECK_ACTION:
            from provisa.api.health_report import health_report

            report = _run_rpc(lambda: run_on_connection_loop(health_report(self._state)))
            return [flight.Result(json.dumps(report).encode("utf-8"))]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        try:
            body = json.loads(action.body.to_pybytes().decode("utf-8")) if action.body else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = {}
        query_text = body.get("query", "")
        ticket_type = "sql" if _is_sql(str(query_text)) else "graphql"
        with _request_span(_tracer, "flight.do_action", transport="flight") as span:
            span.set_attribute("flight.ticket_type", ticket_type)
            if ticket_type == "sql":
                span.set_attribute("flight.sql", str(query_text)[:200])
            else:
                span.set_attribute("flight.gql_query", str(query_text)[:200])
        return []

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _do_get_catalog(
        self,
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        role: str | None,
    ) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return catalog metadata as Arrow record batches: ``role``'s catalog, or the whole one
        when the deployment authenticates nobody (None)."""
        request = json.loads(ticket.ticket.decode("utf-8"))
        domain = request.get("domain")
        table_name = request.get("table")

        tables = build_catalog_tables(self._state, role)

        if domain and table_name:
            # Return schema info for a specific table as rows
            for t in tables:
                if t.domain_id == domain and t.table_name == table_name:
                    _catalog = self._build_columns_table(t)
                    _report_table(_catalog)
                    return record_batch_stream(_catalog)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            raise _flight_error(f"Table not found: {domain}.{table_name}")

        # Return all tables as rows
        _catalog = self._build_catalog_table(tables, domain)
        _report_table(_catalog)
        return record_batch_stream(_catalog)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

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
            raise _flight_error(f"Invalid ticket: {e}", e) from e

        query_text = request.get("query")
        role_id = request.get("role", "org_admin")
        variables = request.get("variables")

        if not query_text:
            raise _flight_error("Ticket must include 'query'")

        if role_id not in self._state.schemas:
            raise _flight_error(f"No schema for role {role_id!r}")

        schema = cast("GraphQLSchema", self._state.schemas[role_id])
        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        document = parse_query(schema, query_text, variables, ctx=ctx)
        compiled_queries = compile_query(document, ctx, variables)
        if not compiled_queries:
            raise _flight_error("No query fields found")

        compiled = compiled_queries[0]

        from provisa.federation.registry_view import operator_floor

        decision = decide_route(
            sources=compiled.sources,
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
            source_dsns=getattr(self._state, "source_dsns", None),
            operator_floor=operator_floor(self._state, compiled.table_ids),
            reads_fakes=reads_fakes(compiled.sql),  # REQ-1494
        )

        return document, ctx, rls, role, compiled, decision, variables

    def _do_get_cypher(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream:  # REQ-345, REQ-347, REQ-352  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a Cypher query ticket and return Arrow record batches."""
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
            raise _flight_error("role is required")
        role_id = str(request["role"])
        params_obj = request.get("params") or {}
        params: dict[str, object] = params_obj if isinstance(params_obj, dict) else {}

        if role_id not in self._state.contexts:
            raise _flight_error(f"No schema for role {role_id!r}")

        ctx = self._state.contexts[role_id]

        # REQ-1877: a statement this role already translated under this schema generation is not
        # parsed or translated again — only its values are bound.
        from provisa.api.rest.cypher_plan import (
            CypherTranslation,
            TranslationRequest,
            kept_label_map,
        )

        kept = TranslationRequest(
            self._state,
            role_id,
            surface="flight",
            domain_access=None,
            cypher=query_text,
            params=params,
        )
        translation = kept.cached()
        if translation is None:
            try:
                ast = parse_cypher(query_text)
            except CypherParseError as exc:
                raise _flight_error(f"Cypher parse error: {exc}", exc) from exc

            from provisa.security.rights import require_role

            # The acting role's own list: it decides whether the catalog is a MATCH root.
            _role_domain_access = require_role(self._state.roles, role_id)["domain_access"]
            label_map = kept_label_map(
                self._state,
                role_id,
                domain_access=_role_domain_access,
                cross_domain=False,
                business_view=False,
                build=lambda: CypherLabelMap.from_schema(ctx, domain_access=_role_domain_access),
            )

            param_names = collect_param_names(query_text)
            try:
                bind_params(param_names, params)
            except CypherParamError as exc:
                raise _flight_error(f"Cypher param error: {exc}", exc) from exc

            try:
                sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, params)
            except (CypherCrossSourceError, CypherTranslateError) as exc:
                raise _flight_error(f"Cypher translate error: {exc}", exc) from exc

            sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

            try:
                sql_str = sql_ast.sql(dialect="postgres")
            except Exception as exc:
                raise _flight_error(f"Cypher SQL render failed: {exc}", exc) from exc

            from provisa.compiler.sql_rewrite import make_semantic_sql

            translation = CypherTranslation(
                semantic_sql=make_semantic_sql(sql_str, ctx),
                ordered_params=tuple(ordered_params),
                param_names=tuple(param_names),
                graph_vars=graph_vars,
            )
            kept.record(translation)

        semantic_sql, graph_vars = translation.semantic_sql, translation.graph_vars
        try:
            resolved_params = translation.bind(params)
        except CypherParamError as exc:
            raise _flight_error(f"Cypher param error: {exc}", exc) from exc

        try:
            plan = self._run_on_loop(
                _govern_and_route_compiled(
                    semantic_sql,
                    role_id,
                    exec_params=resolved_params or None,
                    state=self._state,
                    # REQ-544: the Cypher request's own `// @provisa cache` opt-in.
                    cache_hint=cache_hint_for("cypher", query_text),
                    # REQ-1897: an opted-in read is looked up in the response cache before it
                    # is routed; a Route.CACHE plan has no engine SQL and is served below.
                    serve_cached=True,
                    sdl_joins=False,
                    deliver=_ticket_delivery(request, role_id),
                )
            )
        except PermissionError as exc:
            raise _flight_error(str(exc), exc) from exc
        except ValueError as exc:
            raise _flight_error(str(exc), exc) from exc

        engine = getattr(self._state, "federation_engine", None)
        if engine is None:
            raise _flight_error("Federation engine not connected")

        require_governed_plan(
            plan
        )  # REQ-1176: verify at the last moment, before the engine executes
        if plan.materialize is not None:
            return self._delivered_stream(plan)
        physical_sql = plan.physical_sql
        if physical_sql is None:
            # A statement the router did not send to the engine (DIRECT: one reachable source,
            # or CACHE: answered before routing) carries no engine-physical SQL. Cypher
            # assembles a buffered row result, so it runs through the pipeline terminal every
            # buffered surface reaches — response cache, audit, egress cap and the source's own
            # driver included, in one loop dispatch.
            from provisa.pgwire._pipeline import _execute_plan

            # REQ-1905: the pipeline terminal checks the response cache inside itself, so the
            # slot is taken around the whole call here (a HIT holds one for the lookup only).
            _release_slot = self._acquire_stream_slot()
            try:
                direct = self._run_on_loop(_execute_plan(plan, self._state))
            finally:
                _release_slot()
            return self._cypher_stream(
                [dict(zip(direct.column_names, row, strict=False)) for row in direct.rows],
                graph_vars,
                plan.warnings,
            )
        # REQ-1661: this govern-then-execute terminal never reaches _execute_plan, so its own
        # ensure_resident call is the ONLY place a MATERIALIZED source this plan reads gets landed
        # before the engine executes — mirrors the identical ENGINE-route bypass fixes in
        # _do_get_sql_governed (this file) and provisa/pgwire/server.py.
        # REQ-1865: a directly-bound row_materialize table must be keyed-fetched before the
        # pushdown probe below runs, or a join where every table is row_materialize probes an
        # empty replica end to end (see provisa/pgwire/server.py's identical fix for why).
        # REQ-1887: folded into one _run_on_loop dispatch — see prepare_residency_and_check_cache.
        # REQ-1897: this terminal bypasses _execute_plan_in_org entirely (that's the whole point --
        # Flight drains the engine directly), so it needs its own cache-HIT check too — folded
        # into this SAME dispatch (not a second hop) via prepare_residency_and_check_cache, which
        # checks the cache FIRST and skips residency prep entirely on a HIT (nothing to land if the
        # engine is never dialled). A hit is served in Flight's own native shape (the row-dict list
        # below, same as a live execution builds) without ever touching the engine -- but still
        # audited/egress-accounted, via check_response_cache's own finalize_audit call.
        from provisa.pgwire._pipeline import prepare_residency_and_check_cache

        cached = self._run_on_loop(prepare_residency_and_check_cache(plan, self._state))
        if cached is not None:
            raw_rows = [dict(zip(cached.column_names, row, strict=False)) for row in cached.rows]
        else:
            from provisa.federation.live_concurrency import acquire_plan_permits

            # REQ-1905: a MISS reaches the engine — hold a stream slot for the read.
            _release_slot = self._acquire_stream_slot()
            _permits = None
            try:
                # REQ-1909: this buffered drain holds the plan's live-read permits end to end.
                _permits = acquire_plan_permits(self._state, plan)
                # REQ-1882: the sync engine terminal runs on this handler thread (not a raw cursor,
                # and not a second thread).
                res = engine.execute_engine_sync(
                    physical_sql,
                    resolved_params or [],
                    authorization=plan_authorization(plan),
                )
                # REQ-1897: write-through to the raw-SQL response cache (stored as the drain ends,
                # on this RPC's loop, still bound inside do_get).
                from provisa.pgwire._pipeline import response_cache_tee

                tee = response_cache_tee(plan, self._state, run=self._run_on_loop)
                if tee is not None:
                    res = tee.rows(res)
                raw_rows = [dict(zip(res.column_names, row, strict=False)) for row in res.rows]
            except Exception:
                self._finalize_audit(plan, 500)  # REQ-074/REQ-1386
                raise
            finally:
                if _permits is not None:
                    _permits.release()
                _release_slot()
            plan.row_count = len(raw_rows)
            self._finalize_audit(plan, 200)  # REQ-074/REQ-1386

        return self._cypher_stream(raw_rows, graph_vars, plan.warnings)

    @staticmethod
    def _cypher_stream(
        raw_rows: list[dict[str, object]], graph_vars: dict[str, Any], warnings: Any = ()
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Assemble a Cypher result's rows into graph values and return them as an Arrow stream."""
        from provisa.cypher.assembler import assemble_rows, to_serializable

        assembled = assemble_rows(raw_rows, graph_vars)
        serialized = [to_serializable(r) for r in assembled]

        if not serialized:
            columns = list(graph_vars.keys()) if graph_vars else []
            empty = {col: pa.array([], type=pa.utf8()) for col in columns}
            _catalog = pa.table(empty)
            _report_table(_catalog)
            return record_batch_stream(_catalog, warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        col_names = list(serialized[0].keys())
        col_data: dict[str, list[object]] = {c: [] for c in col_names}
        for row in serialized:
            for col in col_names:
                val = row.get(col)
                col_data[col].append(json.dumps(val) if isinstance(val, (dict, list)) else val)
        _catalog = pa.table(col_data)
        _report_table(_catalog)
        return record_batch_stream(_catalog, warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _execute_query(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Dispatch a query to the correct handler based on language."""
        query_text = str(request.get("query", ""))
        # Before the text's language is guessed: a definition statement (CREATE VIEW, DROP TABLE,
        # a Cypher CREATE INDEX …) is answered with the pipeline's one refusal — CREATE would
        # otherwise read as Cypher and DROP / ALTER as GraphQL, each with an error of its own.
        from provisa.compiler.definitions import DefinitionNotAvailable, refuse_definition_text

        try:
            refuse_definition_text(query_text)
        except DefinitionNotAvailable as exc:
            raise _flight_error(str(exc), exc) from exc
        if _is_cypher(query_text):
            return self._do_get_cypher(request)
        if _is_sql(query_text):
            return self._do_get_sql_governed(request)
        return self._do_get_graphql(request)

    def _license_stream(self, table: "pa.Table", role_id: str, warnings: Any = ()):  # REQ-1137
        """Return a Flight stream for ``table``, attaching the license nag as app_metadata on the
        first batch when nagging (out-of-band — the row data is untouched). Once per role/session."""
        _report_table(table)
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"flight:{role_id}")
        except Exception:
            text = None
        if not text:
            return record_batch_stream(table, warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        meta = pa.py_buffer(text.replace("\n", " ").encode("utf-8"))

        def _gen():
            first = True
            for batch in table.to_batches():
                if first:
                    first = False
                    yield (batch, meta)  # app_metadata rides the first chunk
                else:
                    yield batch

        return generator_stream(table.schema, _gen(), warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _license_stream_gen(
        self, schema, batch_gen, role_id: str, warnings: Any = ()
    ):  # REQ-1137, REQ-1214
        """Return a Flight GeneratorStream over a LAZY record-batch generator, attaching the license
        nag as app_metadata on the first batch when nagging (out-of-band — row data untouched). The
        streaming counterpart of :meth:`_license_stream`: the result never materializes as a Table."""
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"flight:{role_id}")
        except Exception:
            text = None
        meta = pa.py_buffer(text.replace("\n", " ").encode("utf-8")) if text else None
        # REQ-1905: the batches are pulled after do_get returns; they stay under its deadline.
        from provisa.api.flight.deadline import stream_within_deadline

        counted = _metered_batches(stream_within_deadline(batch_gen))

        def _gen():
            first = True
            for batch in counted:
                if first and meta is not None:
                    first = False
                    yield (batch, meta)  # app_metadata rides the first chunk
                else:
                    yield batch

        return generator_stream(schema, _gen(), warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _delivered_stream(self, plan) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """REQ-1194: run a plan that asked for delivery through the one materialize terminal and
        answer the one row naming where its result was landed."""
        from provisa.executor.redirect import DeliveryFailed
        from provisa.pgwire._pipeline import _execute_plan

        _release_slot = self._acquire_stream_slot()  # REQ-1905: the engine is reached
        try:
            result = self._run_on_loop(_execute_plan(plan, self._state))
        except DeliveryFailed as exc:
            raise _flight_error(f"redirect delivery failed: {exc}", exc) from exc
        finally:
            _release_slot()
        return record_batch_stream(
            _redirect_table(result.redirect, plan.materialize), plan.warnings
        )  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _do_get_sql_governed(
        self, request: dict[str, object]
    ) -> (
        flight.RecordBatchStream | flight.GeneratorStream
    ):  # REQ-267, REQ-266  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute SQL through the shared governance pipeline and return Arrow record batches."""
        from provisa.compiler.sql_gen import ColumnRef
        from provisa.executor.result import QueryResult
        from provisa.pgwire._pipeline import govern_batch_final_plan_with_fn, require_governed_plan

        sql = str(request.get("query", ""))
        role_id = str(request.get("role", "org_admin"))
        delivery = _ticket_delivery(request, role_id)

        # REQ-1156: a `SELECT fn(...)` naming a registered command invokes it through the single
        # governed executor, matching pgwire/MCP — otherwise commands are dark over Flight SQL.
        # REQ-1887: the function-invocation check and governance now run in ONE coroutine
        # (govern_batch_final_plan_with_fn, matching pgwire's govern_pgwire_plan pattern), cutting
        # this from two _run_on_loop dispatches to one.
        try:
            # REQ-1897: this terminal serves a Route.CACHE plan (below), so an opted-in read is
            # looked up in the response cache before it is routed.
            result = self._run_on_loop(
                govern_batch_final_plan_with_fn(
                    sql, role_id, self._state, serve_cached=True, deliver=delivery
                )
            )
        except PermissionError as exc:
            raise _flight_error(str(exc), exc) from exc
        except ValueError as exc:
            raise _flight_error(str(exc), exc) from exc

        if isinstance(result, QueryResult):
            columns = [
                ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
                for c in result.column_names
            ]
            table = rows_to_arrow_table(result.rows, columns)
            return self._license_stream(table, role_id)  # REQ-1137

        plan = result
        require_governed_plan(
            plan
        )  # REQ-1176: verify at the last moment, before the engine executes
        if plan.materialize is not None:
            return self._delivered_stream(plan)
        # REQ-074/REQ-1386: this govern-then-stream terminal never reaches _execute_plan, so the
        # audit row is written here — once the terminal is established, or on the way out.
        try:
            if plan.route == Route.CACHE:
                # REQ-1897: answered from the response cache before routing — nothing was lowered,
                # routed or landed. Served as the Arrow table Flight streams (an arrow_ipc entry
                # verbatim, a rows entry converted losslessly); the hit is audited in the read.
                from provisa.pgwire._pipeline import check_response_cache_arrow

                table = self._run_on_loop(check_response_cache_arrow(plan, self._state))
                return self._license_stream(table, role_id, plan.warnings)  # REQ-1137
            if plan.route == Route.ENGINE:
                assert plan.physical_sql is not None
                # REQ-1661: this govern-then-stream terminal never reaches _execute_plan (see the
                # comment above), so its own ensure_resident call is the ONLY place a MATERIALIZED
                # source this plan reads gets landed before the engine executes — mirrors pgwire's
                # identical ENGINE-route bypass (provisa/pgwire/server.py). Confirmed live: a
                # cross-engine federated_join touching a never-yet-landed ClickHouse table failed
                # "Binder Error: Catalog ... does not exist" on both transports on a fresh boot.
                # REQ-1887/REQ-1897: see _engine_arrow_through_cache.
                cached_table, arrow_schema, batch_gen = self._engine_arrow_through_cache(plan, [])
                if cached_table is not None:
                    return self._license_stream(cached_table, role_id, plan.warnings)  # REQ-1137
                # The stream is drained after do_get returns: the row is written when it ends.
                self._finalize_audit(plan, 200, defer_to_drain=True)
                batch_gen = _audited_arrow(plan, batch_gen)
                return self._license_stream_gen(
                    arrow_schema, batch_gen, role_id, plan.warnings
                )  # REQ-1137
            elif plan.route == Route.DIRECT:
                if self._state.source_pools.has(
                    plan.source_id
                ) and self._state.source_pools.supports_stream(plan.source_id):
                    # REQ-1190: a single-reachable-source scan streams via the source's server-side cursor,
                    # adapted to a lazy Arrow record-batch generator — never materialized on this transport
                    # (streaming-uniformity Defect 1). Mirrors the ENGINE streaming terminal above.
                    from provisa.federation.runtime_support import arrow_batches_from_rows
                    from provisa.pgwire._pipeline import serve_stream_through_cache

                    # REQ-1897: a decoded HIT is served through the same rows->Arrow adapter the
                    # live stream uses (same path shape); a MISS streams the source teed into
                    # the raw-SQL cache, stored on this RPC's held loop when the drain ends.
                    # REQ-1905: opening the source's cursor (a cache MISS) takes a stream slot,
                    # held until the stream is drained.
                    from provisa.api.flight.stream_slots import SlotHeldBatches

                    _slot_releases: list[Callable[[], None]] = []

                    def _open_rows():
                        _slot_releases.append(self._acquire_stream_slot())
                        return self._state.federation_engine.execute_native_stream(
                            self._state.source_pools,
                            plan.source_id,
                            plan.sql,
                            plan.exec_params or [],
                            run=current_connection_loop().run,
                        )

                    def _release_slot() -> None:
                        for _release in _slot_releases:
                            _release()

                    try:
                        stream = serve_stream_through_cache(
                            plan,
                            self._state,
                            run=self._run_on_loop,
                            check_rows=True,
                            passthrough=None,
                            open_rows=_open_rows,
                        )
                        arrow_schema, batch_gen = arrow_batches_from_rows(stream)
                    except BaseException:
                        _release_slot()
                        raise
                    batch_gen = SlotHeldBatches(_release_slot, batch_gen)
                    self._finalize_audit(plan, 200, defer_to_drain=True)
                    batch_gen = _audited_arrow(plan, batch_gen)
                    # REQ-1882: the cursor is pumped on this RPC's loop as pyarrow drains the
                    # stream after do_get returns, so the stream holds the loop until it ends.
                    return self._license_stream_gen(
                        arrow_schema, _hold_loop_for_stream(batch_gen), role_id, plan.warnings
                    )  # REQ-1137
                from provisa.pgwire._pipeline import serve_buffered_through_cache

                # REQ-1897: a decoded HIT, or the buffered read stored in the raw-SQL cache — in
                # the same single loop dispatch the read always took (REQ-1887).
                result = self._run_on_loop(
                    serve_buffered_through_cache(
                        plan,
                        self._state,
                        lambda: self._native_read_in_slot(plan, plan.exec_params or []),
                    )
                )
                columns = [
                    ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
                    for c in result.column_names
                ]
                table = rows_to_arrow_table(result.rows, columns)
                plan.row_count = len(result.rows)
                self._finalize_audit(plan, 200)
                return self._license_stream(table, role_id, plan.warnings)  # REQ-1137
            else:
                raise _flight_error(f"Route {plan.route!r} is not supported for SQL via Flight")
        except Exception:
            self._finalize_audit(plan, 500)
            raise

    async def _native_read_in_slot(self, plan, params: list):  # REQ-1905
        """A buffered read of the plan's own source, holding a stream slot for the read. Called by
        the response cache only on a MISS, so a HIT takes no slot."""
        release = self._acquire_stream_slot()
        try:
            return await self._state.federation_engine.execute_native(
                self._state.source_pools, plan.source_id, plan.sql, params
            )
        finally:
            release()

    def _engine_arrow_through_cache(self, plan, params: list):
        """The ENGINE route's Arrow terminal through the raw-SQL response cache (REQ-1897):
        ``(cached_table, None, None)`` on a HIT, else ``(None, schema, batches)`` teed into the
        cache. The HIT is checked FIRST in the same loop dispatch as residency prep (REQ-1887) —
        a HIT (accounted inside check_response_cache_arrow) never dials the engine, so residency
        prep is skipped entirely. REQ-1661: this govern-then-stream terminal never reaches
        _execute_plan, so its own residency call is the ONLY place a MATERIALIZED source this plan
        reads gets landed before the engine executes. Streamed Arrow Flight (REQ-825, REQ-145,
        REQ-1214) drains the engine's LAZY record-batch terminal; each batch is forwarded as it
        arrives and the Arrow IPC entry is stored only when the stream drains within the bound —
        pyarrow pulls the batches after do_get returns, so a teed stream holds this RPC's loop
        (REQ-1882) for the store the drain's end runs on it."""
        from provisa.pgwire._pipeline import check_response_cache_arrow, response_cache_tee

        async def _hit_or_prepare():
            table = await check_response_cache_arrow(plan, self._state)
            if table is None:
                await _prepare_engine_residency(self._state, plan)
            return table

        cached_table = self._run_on_loop(_hit_or_prepare())
        if cached_table is not None:
            return cached_table, None, None
        # REQ-1905: a MISS reaches the engine — take a stream slot now, and hold it until the
        # lazy stream below is drained (pyarrow pulls it after do_get has returned).
        from provisa.api.flight.stream_slots import SlotHeldBatches

        from provisa.federation.live_concurrency import acquire_plan_permits

        release = self._acquire_stream_slot()
        try:
            # REQ-1909: the permits ride the batch generator until pyarrow finishes pulling it.
            permits = acquire_plan_permits(self._state, plan)
        except BaseException:
            release()
            raise
        try:
            arrow_schema, batch_gen = self._state.federation_engine.execute_engine_stream(
                plan.physical_sql, params
            )
        except RuntimeError as exc:
            permits.release()
            release()
            raise _flight_error(str(exc), exc) from exc
        except BaseException:
            permits.release()
            release()
            raise
        if permits.count:  # an uncapped read keeps the engine's own stream object (and its close)
            batch_gen = permits.guard(batch_gen)
        batch_gen = SlotHeldBatches(release, batch_gen)
        tee = response_cache_tee(plan, self._state, run=run_on_connection_loop)
        if tee is not None:
            batch_gen = _hold_loop_for_stream(tee.arrow(arrow_schema, batch_gen))
        return None, arrow_schema, batch_gen

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
                _govern_and_route_compiled(
                    compiled.sql,
                    role_id,
                    # The bound values are part of the plan (and so of its cache key, REQ-1897).
                    exec_params=list(compiled.params) or None,
                    state=self._state,
                    # REQ-544: the GraphQL request's own @cached opt-in.
                    cache_hint=cache_hint_for("graphql", str(request.get("query", ""))),
                    sdl_joins=True,
                    deliver=_ticket_delivery(request, role_id),
                )
            )
        except PermissionError as exc:
            raise _flight_error(str(exc), exc) from exc
        except ValueError as exc:
            raise _flight_error(str(exc), exc) from exc

        require_governed_plan(
            plan
        )  # REQ-1176: verify at the last moment, before the engine executes
        if plan.materialize is not None:
            return self._delivered_stream(plan)
        # REQ-074/REQ-1386: govern-then-stream terminal — the audit row is written here.
        try:
            if plan.route == Route.DIRECT:
                from provisa.pgwire._pipeline import serve_buffered_through_cache

                # REQ-1897: a decoded HIT, or the buffered read stored — one loop dispatch.
                result = self._run_on_loop(
                    serve_buffered_through_cache(
                        plan,
                        self._state,
                        lambda: self._native_read_in_slot(
                            plan, plan.exec_params or compiled.params
                        ),
                    )
                )
                table = rows_to_arrow_table(result.rows, compiled.columns)
                plan.row_count = len(result.rows)
                self._finalize_audit(plan, 200)
                return record_batch_stream(table, plan.warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

            assert plan.physical_sql is not None
            # REQ-1661/REQ-1887/REQ-1897: see _engine_arrow_through_cache.
            cached_table, arrow_schema, batch_gen = self._engine_arrow_through_cache(
                plan, compiled.params
            )
            if cached_table is not None:
                return record_batch_stream(cached_table, plan.warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            self._finalize_audit(plan, 200, defer_to_drain=True)
            batch_gen = _audited_arrow(plan, batch_gen)
            # REQ-1905: pulled after do_get returns; the stream stays under its deadline.
            from provisa.api.flight.deadline import stream_within_deadline

            return generator_stream(arrow_schema, stream_within_deadline(batch_gen), plan.warnings)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        except Exception:
            self._finalize_audit(plan, 500)
            raise
