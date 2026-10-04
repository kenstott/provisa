# Copyright (c) 2026 Kenneth Stott
# Canary: 83cf4f75-fddb-45f8-9149-cf1df7e86403
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt session: state machine, auth, query execution, result buffering."""

from __future__ import annotations

import logging
import re
from enum import Enum, auto
from typing import Any

from sqlalchemy import select

import provisa.bolt.messages as msg
from provisa.auth.throttle import LockedOut
from provisa.bolt.packstream import pack_message
from provisa.bolt.websocket import BoltWriter
from provisa.compiler.directives import NO_CACHE_HINT, cache_hint_for
from provisa.core import request_deadline
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import in_request_span as _in_request_span
from provisa.security.rights import can_act_cross_org, capabilities_for_claims

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

# Records sent between two looks at the request's deadline while a PULL streams (REQ-1905).
_DEADLINE_CHECK_ROWS = 256

_BOLT_VERSION = "5.4"
_SERVER_AGENT = f"Neo4j/{_BOLT_VERSION} (Provisa)"


# Server→client hint: how long a client should wait for a server response before
# giving up. Federated reads (multi-source, cold Kafka/Iceberg) can be slow, so
# this is generous and configurable via PROVISA_BOLT_RECV_TIMEOUT (seconds).
def _recv_timeout() -> int:
    from provisa.core import settings_registry  # REQ-1913: the operator setting

    return settings_registry.value("bolt.recv_timeout")


# REQ-1393: Neo4j's own code for a rejected-because-throttled login, so a driver reports the
# lockout as a lockout rather than as one more wrong password.
_RATE_LIMIT_CODE = "Neo.ClientError.Security.AuthenticationRateLimit"
_ARGUMENT_ERROR = "Neo.ClientError.Statement.ArgumentError"
# An HTTP status from the shared command executor, as the Neo4j error a Bolt client reads.
_BOLT_CODE_FOR_STATUS = {
    403: "Neo.ClientError.Security.Forbidden",
    404: "Neo.ClientError.Procedure.ProcedureNotFound",
}


def _scheme_of(meta: dict) -> str:
    """The credential presentation the driver declared in HELLO/LOGON.

    Bolt drivers send ``scheme`` alongside the credential — ``basic`` for principal+password,
    ``bearer`` for a token. Neo4j's own default when the field is absent is ``basic``, and every
    driver that omits it is sending a username and password.
    """
    scheme = meta.get("scheme")
    return scheme.lower() if isinstance(scheme, str) and scheme else "basic"


class State(Enum):
    NEGOTIATION = auto()
    AUTHENTICATION = auto()
    READY = auto()
    STREAMING = auto()
    TX_READY = auto()
    TX_STREAMING = auto()
    FAILED = auto()
    DEFUNCT = auto()


class BoltSession:
    def __init__(self, writer: BoltWriter, bolt_version: tuple[int, int]) -> None:
        self.writer = writer
        self.bolt_version = bolt_version
        self.state = State.AUTHENTICATION
        self.user_id: str | None = None
        # REQ-1266: the org this session's queries route to. Resolved once, lazily, at the first RUN
        # (auth identity is settled by then) and bound around execution via current_org. None → the
        # default-org runtime (single-org deployments).
        self.org_id: str | None = None
        self._org_resolved: bool = False
        # REQ-1235: the org the presented credential was issued for (a personal access token's),
        # or None for a credential that names no org. It is the only org such a credential opens.
        self._credential_org: str | None = None
        # All role_ids the authenticated user holds; each surfaces as a "provisa_<role>" database.
        self.roles: list[str] = []
        # Active role for the current tx/run, chosen via the Bolt `db` field. Defaults to roles[0].
        self.role_id: str | None = None
        # db selected by the most recent BEGIN (explicit-tx path).
        self._tx_db: str | None = None
        # Buffered result from last RUN: list of column-ordered value lists
        self._result_columns: list[str] = []
        self._result_rows: list[list[Any]] = []
        # REQ-1194/REQ-1195: a materialize handle when the last RUN redirected to a sink instead of
        # buffering rows. Surfaced in the trailing PULL SUCCESS metadata — Bolt's side-channel.
        self._result_redirect: dict | None = None
        # REQ-1905: the request deadline of the RUN in flight — Bolt's own request timeout, ONE
        # for the request: opened when RUN starts executing, held while its records are pulled,
        # ended with the last PULL/DISCARD, a RESET or a failure.
        self._deadline: request_deadline.Deadline | None = None
        self._pull_offset: int = 0

    # ── Response helpers ───────────────────────────────────────────────────────

    def _send(self, data: bytes, *, meter: bool = True) -> int:
        """Write one framed Bolt message. Returns bytes put on the wire.

        ``meter=False`` skips the per-call ``report()`` (REQ-1885): a caller that writes many
        messages in a tight loop (row streaming) accumulates the returned byte counts itself and
        reports the batch total once, instead of taking the egress-meter lock per message.
        """
        from provisa.bolt.framing import write_message
        from provisa.core.egress import report

        # REQ-1885: was an unconditional WARNING on every framed write (send_record fires once per
        # streamed row) — downgraded to DEBUG and gated so a normal successful send costs nothing
        # at production log levels.
        if log.isEnabledFor(logging.DEBUG):
            log.debug("[BOLT] send tag=0x%02X len=%d", data[1] if len(data) >= 2 else 0, len(data))
        n = write_message(self.writer, data)
        if meter:
            # REQ-1452/REQ-1455: metered here rather than at the socket because this is the one
            # seam both Bolt transports (raw TCP and the WebSocket surface) pass through, so
            # neither can be counted twice or missed. Pre-auth messages report no org and are
            # dropped by the meter.
            report(self.org_id, n)
        return n

    def send_success(self, meta: dict | None = None) -> None:
        self._send(pack_message(msg.SUCCESS, meta or {}))

    def send_failure(self, code: str, message: str) -> None:
        self._send(pack_message(msg.FAILURE, {"code": code, "message": message}))
        self.state = State.FAILED
        self._end_request()

    def _end_request(self) -> None:
        deadline, self._deadline = self._deadline, None
        if deadline is not None:
            deadline.stop()

    def _fail_timed_out(self, deadline: request_deadline.Deadline) -> None:
        """REQ-1905: the request outran Bolt's request timeout — a FAILURE naming the transport
        and the setting, and nothing more of its result."""
        self._result_rows = []
        self._pull_offset = 0
        self.send_failure(
            "Neo.ClientError.Transaction.TransactionTimedOut", str(deadline.expired_error())
        )

    def send_ignored(self) -> None:
        self._send(pack_message(msg.IGNORED))

    def send_record(self, values: list[Any], *, meter: bool = True) -> int:
        return self._send(pack_message(msg.RECORD, values), meter=meter)

    # ── Auth ───────────────────────────────────────────────────────────────────

    def _assert_peer_binding(self, principal: str) -> None:  # REQ-1228
        """Bind the TLS client certificate to the principal HELLO/LOGON declares, when configured.

        The certificate is on the asyncio transport underneath the writer. A plaintext or WebSocket
        connection exposes no ``ssl_object``, and the check is then a no-op — mTLS is only wired
        onto the listener when a CA is configured, so there is nothing to bind against.
        """
        from provisa.security.mtls import assert_principal_binding, resolve_client_auth

        auth = resolve_client_auth(
            "PROVISA_BOLT_CLIENT_CA",
            "PROVISA_BOLT_MTLS_MODE",
            "PROVISA_BOLT_MTLS_BIND_PRINCIPAL",
        )
        if auth is None or not auth.bind_principal:
            return
        get_extra_info = getattr(self.writer, "get_extra_info", None)
        ssl_object = get_extra_info("ssl_object") if get_extra_info is not None else None
        peer_cert = ssl_object.getpeercert() if ssl_object is not None else None
        assert_principal_binding(auth, peer_cert, principal)

    async def _resolve_user(
        self, scheme: str, principal: str, credentials: str
    ) -> tuple[str, list[str]] | None:
        """Return (user_id, role_ids) on success, None on failure (REQ-124, REQ-1263).

        The role set becomes the user's selectable databases (provisa_<role>). Selecting
        a role narrows to that role's domain rights; the user can never exceed this set.

        Bolt used to authenticate under the ``simple`` provider alone and refuse every other
        deployment outright. The credential now goes to whichever provider is configured, chosen
        by the ``scheme`` the driver declared — ``basic`` presents principal+credentials, ``bearer``
        presents a token, and a personal access token is just a bearer credential like any other.
        """
        # An import failure is a server fault, not an auth failure — propagate it.
        from provisa.api.app import state as app_state

        if app_state.auth_config is None:
            if getattr(app_state, "auth_middleware_active", False):
                # A real provider is active but its config is absent — misconfiguration.
                # Fail closed: never silently degrade a secured server to no-auth.
                raise RuntimeError("bolt auth_config not configured")
            # Explicit unsecured mode (provider: none / no auth section) — treat as no-auth.
            provider = "none"
        else:
            provider = app_state.auth_config["provider"]
        from provisa.security.meta_role import is_meta_role_id

        # A meta-role is made from roles a session names, never selected by its own id.
        all_roles = [r for r in app_state.contexts if not is_meta_role_id(r)]

        if provider == "none" or not getattr(app_state, "auth_middleware_active", False):
            # No auth — every role is available; default to principal if it names a real role.
            ordered = (
                [principal, *[r for r in all_roles if r != principal]]
                if principal and principal in app_state.contexts
                else all_roles
            )
            return (principal or "anonymous", ordered) if ordered else None

        identity = await self._authenticate(app_state, scheme, principal, credentials)
        if identity is None:
            return None
        roles = self._selectable_roles(app_state, identity)
        if not roles:
            return None
        self._credential_org = identity.active_org_id
        return identity.user_id, roles

    @staticmethod
    async def _authenticate(app_state, scheme: str, principal: str, credentials: str):
        """Validate the presented credential against the configured provider, or None.

        ``scheme`` names the presentation, so it selects the validator rather than being trusted:
        a provider that accepts no such presentation refuses the connection instead of having the
        credential retried against a different validator, which would turn one rejection into a
        second guess.
        """
        import base64

        import jwt

        from provisa.auth.models import validator_for_scheme
        from provisa.auth.throttle import throttled
        from provisa.auth.wiring import build_auth_provider

        auth_provider = build_auth_provider(
            app_state.auth_config, admin_pool=getattr(app_state, "admin_db", None)
        )
        validator = validator_for_scheme(auth_provider, scheme)
        if validator is None:
            return None
        if scheme == "basic":
            token = base64.b64encode(f"{principal}:{credentials}".encode()).decode()
        else:
            token = credentials
        try:
            # REQ-1393: counted against the same subject HTTP and pgwire count against, so an
            # account cannot be guessed at by moving between protocols. LockedOut is a
            # PermissionError, so it propagates rather than reading as one more bad password.
            return await throttled(
                validator, token, principal=principal if scheme == "basic" else None
            )
        except (ValueError, jwt.PyJWTError):
            return None

    @staticmethod
    def _selectable_roles(app_state, identity) -> list[str]:
        """The roles this identity may select, mapped role first (REQ-273, REQ-551).

        The server derives them from the validated identity — the claim-mapped role plus whatever
        the identity's own assignments carry — and keeps only those that exist as compiled
        contexts. A role the identity does not hold is not selectable, whatever database the
        client names.
        """
        from provisa.auth.role_mapping import resolve_assignments, resolve_role

        default_role = app_state.auth_config.get("default_role")
        if not default_role:
            # No admin default: an identity matching no mapping rule is refused, not escalated.
            raise RuntimeError("bolt auth requires auth.default_role to be configured")
        mapped = resolve_role(identity, app_state.auth_config.get("role_mapping", []), default_role)
        held = [a.role_id for a in resolve_assignments(identity)]
        ordered = [mapped, *[r for r in held if r != mapped]]
        return [r for r in ordered if r in app_state.contexts]

    def _meta_role(self, app_state: Any, named: str) -> str:
        from provisa.core.request_context import reset_current_org, set_current_org
        from provisa.security.meta_role import resolve_requested_role

        token = set_current_org(self.org_id) if self.org_id is not None else None
        try:
            return resolve_requested_role(app_state, set(self.roles), named)
        finally:
            if token is not None:
                reset_current_org(token)

    def _resolve_db(self, db: Any) -> tuple[str, bool] | None:
        """Map a Bolt `db` value to (role_id, include_ops), or None if unauthorized.

        The db name encodes two axes:
          provisa_ops_<role> → role + ops/meta domains included
          provisa_<role>     → role + business domains only (ops/meta excluded)
        Empty / "system" / "provisa" → default role, business view.
        ``<role>`` may be a comma-separated set of held roles, which acts as their meta-role
        (security/meta_role.py; resolved once the session's org is bound). Every role must be in
        the user's set; anything else → None.
        """
        if not self.roles:
            return None
        default = self.roles[0]
        if not db or db in ("system", "provisa"):
            return default, False
        for prefix, include_ops in (("provisa_ops_", True), ("provisa_", False)):
            if isinstance(db, str) and db.startswith(prefix):
                named = [r.strip() for r in db[len(prefix) :].split(",") if r.strip()]
                if named and all(r in self.roles for r in named):
                    return ",".join(named), include_ops
                return None
        return None

    # ── Message handlers ───────────────────────────────────────────────────────

    async def handle_hello(self, fields: list[Any]) -> None:
        # Bolt 4.x: HELLO carries credentials; Bolt 5.x: HELLO has no credentials (LOGON follows)
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        major, _ = self.bolt_version
        if major < 5:
            # Auth inline with HELLO
            principal = meta.get("principal", "")
            credentials = meta.get("credentials", "")
            try:
                # REQ-1228: a mismatched client certificate is not a credential question, so it is
                # answered before the password is examined and without counting against the throttle.
                self._assert_peer_binding(principal)
            except PermissionError as exc:
                self.send_failure("Neo.ClientError.Security.Unauthorized", str(exc))
                return
            try:
                resolved = await self._resolve_user(_scheme_of(meta), principal, credentials)
            except LockedOut as locked:
                self.send_failure(_RATE_LIMIT_CODE, str(locked))
                return
            if resolved is None:
                self.send_failure(
                    "Neo.ClientError.Security.Unauthorized",
                    f"Invalid credentials for principal {principal!r}",
                )
                return
            self.user_id, self.roles = resolved
            self.role_id = self.roles[0]
            self.state = State.READY
        else:
            # Bolt 5.x: wait for LOGON
            self.state = State.AUTHENTICATION

        self.send_success(
            {
                "server": _SERVER_AGENT,
                "connection_id": "bolt-provisa-1",
                "hints": {"connection.recv_timeout_seconds": _recv_timeout()},
            }
        )

    async def handle_logon(self, fields: list[Any]) -> None:
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        principal = meta.get("principal", "")
        credentials = meta.get("credentials", "")
        try:
            # REQ-1228: same certificate-to-principal binding HELLO applies on Bolt 4.x.
            self._assert_peer_binding(principal)
        except PermissionError as exc:
            self.send_failure("Neo.ClientError.Security.Unauthorized", str(exc))
            return
        try:
            resolved = await self._resolve_user(_scheme_of(meta), principal, credentials)
        except LockedOut as locked:
            self.send_failure(_RATE_LIMIT_CODE, str(locked))
            return
        if resolved is None:
            self.send_failure(
                "Neo.ClientError.Security.Unauthorized",
                f"Invalid credentials for principal {principal!r}",
            )
            return
        self.user_id, self.roles = resolved
        self.role_id = self.roles[0]
        self.state = State.READY
        self.send_success({})

    def handle_logoff(self) -> None:
        self.user_id = None
        self._credential_org = None
        self.roles = []
        self.role_id = None
        self.state = State.AUTHENTICATION
        self.send_success({})

    def handle_reset(self) -> None:
        self._end_request()
        self._result_columns = []
        self._result_rows = []
        self._result_redirect = None
        self._pull_offset = 0
        if self.state != State.DEFUNCT:
            self.state = State.READY if self.role_id else State.AUTHENTICATION
        self.send_success({})

    def handle_begin(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        self._tx_db = meta.get("db")
        self.state = State.TX_READY
        self.send_success({})

    def handle_commit(self) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        self._tx_db = None
        self.state = State.READY
        self.send_success({})

    def handle_rollback(self) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        self._tx_db = None
        self.state = State.READY
        self.send_success({})

    def _requested_org(self) -> str | None:  # REQ-1234
        """The org this connection's TLS SNI hostname named, or None.

        The SSLObject is under the writer's transport, the same place the client certificate is. A
        plaintext or WebSocket connection exposes none, and a driver that dialed an IP address sent
        no servername — both mean no org was requested, which is every connection on a single-org
        deployment.
        """
        from provisa.security.sni import indicated_host, org_from_host

        get_extra_info = getattr(self.writer, "get_extra_info", None)
        ssl_object = get_extra_info("ssl_object") if get_extra_info is not None else None
        return org_from_host(indicated_host(ssl_object))

    async def _member_orgs(self) -> list[str]:
        """The orgs this session's user belongs to (for SHOW DATABASES under multi-tenancy)."""
        from provisa.api.app import state as app_state
        from provisa.core.org_membership import bindable_memberships

        # Called only for an authenticated session on a multi-tenant deployment, which always has
        # a platform plane and a user.
        assert app_state.admin_db is not None and self.user_id is not None
        async with app_state.admin_db.acquire() as conn:
            result = await conn.execute_core(bindable_memberships(self.user_id))
            return sorted(dict(r._mapping)["org_id"] for r in result.fetchall())

    async def _ensure_org(self, database_org: str | None = None) -> None:
        """Resolve+build this session's org runtime once (REQ-1266).

        Bolt's org request is the hostname the driver dialed, carried in TLS SNI (REQ-1234) — the
        same string an HTTP client puts in ``Host``. It names an org without granting one: an org
        the principal is not a member of is refused below, so the org is still derived from
        membership (REQ-1235: an org nobody named is refused, a lone membership included; platform
        admin → default runtime — no silent cross-tenant default). Runs on the event loop, so a plain set/reset
        around handle_run's execution binds it (no thread hop, unlike pgwire).

        REQ-1235: ``database_org`` is the org the database name names (``<org>.provisa_<role>``).
        A session is bound to one org: a later database naming another is refused."""
        from provisa.api.org_resolve import OrgResolutionError

        if self._org_resolved:
            if database_org is not None and database_org != self.org_id:
                raise OrgResolutionError(
                    f"this session is bound to org {self.org_id!r}; "
                    f"open a new session for {database_org!r}"
                )
            return
        from provisa.api.app import ensure_org_runtime, state as app_state
        from provisa.api.org_resolve import org_named_by_host_or_database, resolve_session_org

        # REQ-1337: resolve the claims to RIGHTS and test cross_org — never the role name.
        caps = capabilities_for_claims(self.roles, getattr(app_state, "roles", {}))
        org_id = await resolve_session_org(
            app_state,
            user_id=self.user_id,
            can_act_any_org=can_act_cross_org(caps),
            requested_org=org_named_by_host_or_database(self._requested_org(), database_org),
            credential_org=self._credential_org,
            named_by=(
                "use the database <org>.provisa_<role>, or connect over TLS to the org's own "
                "hostname (<org>.<domain>)"
            ),
        )
        if org_id is not None:
            await ensure_org_runtime(org_id)
        self.org_id = org_id
        self._org_resolved = True

    @_in_request_span(_tracer, "bolt.run", transport="bolt")  # REQ-1910
    async def handle_run(self, fields: list[Any]) -> None:
        cypher: str = fields[0] if fields else ""
        parameters: dict = fields[1] if len(fields) > 1 and isinstance(fields[1], dict) else {}
        extra: dict = fields[2] if len(fields) > 2 and isinstance(fields[2], dict) else {}

        # db selection: autocommit RUN carries `db` in extra; explicit-tx inherits BEGIN's db.
        db = extra.get("db", self._tx_db)
        # REQ-1235: under multi-tenancy the database name carries the org, <org>.provisa_<role>.
        from provisa.api.app import state as _app_state
        from provisa.api.org_resolve import OrgResolutionError

        multitenant = bool(getattr(_app_state, "multitenancy", False))
        try:
            database_org, db = select_database(db, multitenancy=multitenant)
        except OrgResolutionError as exc:
            self.send_failure("Neo.ClientError.Database.DatabaseNotFound", str(exc))
            return
        resolved = self._resolve_db(db) if self.roles else None

        import logging as _logging

        _logging.getLogger("uvicorn.error").warning(
            "[BOLT] RUN cypher=%r db=%r resolved=%r state=%s",
            cypher,
            db,
            resolved,
            self.state.name,
        )

        if self.state == State.FAILED:
            self.send_ignored()
            return

        if not self.roles:
            self.send_failure("Neo.ClientError.Security.Unauthorized", "Not authenticated")
            return

        if resolved is None:
            self.send_failure(
                "Neo.ClientError.Database.DatabaseNotFound",
                f"Database {db!r} does not exist or is not accessible",
            )
            return

        role_id, include_ops = resolved

        # REQ-1266: resolve this session's org (once) and bind it around execution so every
        # state.X read below routes to the org's runtime. Ambiguous membership fails the RUN.
        try:
            await self._ensure_org(database_org)
            member_orgs = await self._member_orgs() if multitenant else None
        except OrgResolutionError as exc:
            self.send_failure("Neo.ClientError.Security.Forbidden", str(exc))
            return

        if "," in role_id:
            # A set of held roles acts as their meta-role, built in this session's org.
            try:
                role_id = self._meta_role(_app_state, role_id)
            except PermissionError as exc:
                self.send_failure("Neo.ClientError.Security.Forbidden", str(exc))
                return

        # REQ-1194/REQ-1195: a caller requests materialization via Bolt transaction metadata — the
        # side-channel that rides RUN's `extra` map without touching the record stream. The handle is
        # surfaced in the trailing PULL SUCCESS metadata.
        from provisa.executor.redirect import delivery_from_request

        tx_meta = extra.get("tx_metadata") or {}
        _redir_thr = tx_meta.get("provisa_redirect_threshold")
        delivery = delivery_from_request(
            force_redirect=str(tx_meta.get("provisa_redirect", "")).lower() == "true",
            redirect_format=tx_meta.get("provisa_redirect_format"),
            threshold=int(_redir_thr) if _redir_thr is not None else None,
            role=role_id,
        )

        from provisa.core.request_context import reset_current_org, set_current_org
        from provisa.audit.context import ANONYMOUS_USER, audit_identity_scope

        _org_token = set_current_org(self.org_id) if self.org_id is not None else None
        # REQ-074/REQ-1386: attribute this RUN's governed statements to the authenticated principal.
        # Bolt executes on the event loop (no thread hop), so a plain scope binds it. A connection
        # that named no principal (an unsecured deployment) is audited as the anonymous one.
        _audit_scope = audit_identity_scope(self.user_id or ANONYMOUS_USER, "bolt")
        self._end_request()  # a RUN over a result that was never drained ends that request
        shield = request_deadline.shielded()
        deadline = request_deadline.open_request("bolt")
        from provisa.core.statement_warnings import collecting

        try:
            # REQ-1350: one collector for the RUN; what its statements' answers say about
            # themselves is sent as notifications with the RUN's SUCCESS.
            with _audit_scope, request_deadline.bound(deadline), collecting() as run_warnings:
                columns, rows, redirect = await _execute_cypher(
                    cypher,
                    parameters,
                    role_id,
                    include_ops=include_ops,
                    roles=self.roles,
                    orgs=member_orgs,
                    deliver=delivery,
                )
                # The result is ready only now: not handed over once the deadline has passed.
                deadline.check()
        except Exception as exc:
            deadline.stop()
            request_deadline.let_go(exc)  # what the interrupted frames held is released now
            if deadline.fired and not deadline.ended_early:
                # REQ-1905: whatever the RUN failed with, its deadline has passed.
                self._fail_timed_out(deadline)
                return
            if isinstance(exc, PermissionError):
                self.send_failure("Neo.ClientError.Security.Forbidden", str(exc))
                return
            from starlette.exceptions import HTTPException as _HTTPException

            from provisa.cypher.command_call import CommandCallRefused

            if isinstance(exc, _HTTPException):
                # The shared executor's answer (a command call): said in Neo4j's terms.
                self.send_failure(
                    _BOLT_CODE_FOR_STATUS.get(exc.status_code, _ARGUMENT_ERROR), exc.detail
                )
                return
            if isinstance(exc, CommandCallRefused):
                self.send_failure(_ARGUMENT_ERROR, str(exc))
                return
            import logging as _logging
            import traceback as _tb

            _logging.getLogger("uvicorn.error").warning(
                "[BOLT] RUN failed: %s\n%s", exc, _tb.format_exc()
            )
            self.send_failure("Neo.ClientError.Statement.SyntaxError", str(exc))
            return
        finally:
            with shield.lock:  # execution is over: this thread is inside no deadline's scope
                shield.settle()
                shield.quiesce()
            if _org_token is not None:
                reset_current_org(_org_token)

        self._result_columns = columns
        self._result_rows = rows
        self._result_redirect = redirect
        self._pull_offset = 0
        self._deadline = deadline  # held until the records have been pulled

        in_tx = self.state in (State.TX_READY, State.TX_STREAMING)
        self.state = State.TX_STREAMING if in_tx else State.STREAMING
        meta: dict = {"fields": columns, "t_first": 0}
        notifications = [
            {
                "code": f"Provisa.{w.code}",
                "severity": "WARNING",
                "category": "GENERIC",
                "title": w.code,
                "description": w.message,
            }
            for w in run_warnings
        ]
        note = self._license_nag_notification()  # REQ-1137
        if note is not None:
            notifications.append(note)
        if notifications:
            meta["notifications"] = notifications
        self.send_success(meta)

    def _license_nag_notification(self) -> dict | None:
        """The REQ-1137 license nag as a Bolt SUCCESS-metadata notification, once per connection.

        Returned in the RUN SUCCESS ``notifications`` field — an out-of-band advisory channel that
        Neo4j clients surface without touching the record stream. None when not nagging / already
        emitted for this connection."""
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"bolt:{id(self)}")
        except Exception:  # nag must never break a session (REQ-1137)
            return None
        if not text:
            return None
        return {
            "code": "Provisa.License.TrialExpired",
            "severity": "WARNING",
            "category": "GENERIC",
            "title": "Provisa license",
            "description": text.replace("\n", " "),
        }

    def handle_pull(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        if self.state not in (State.STREAMING, State.TX_STREAMING):
            self.send_failure(
                "Neo.ClientError.Request.Invalid",
                f"Cannot PULL in state {self.state.name}",
            )
            return

        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        n = meta.get("n", -1)

        _dbg = logging.getLogger("uvicorn.error")
        _dbg.warning(
            "[BOLT] PULL n=%d offset=%d total_rows=%d", n, self._pull_offset, len(self._result_rows)
        )
        # REQ-1885: send_record's per-row report() call is batched into one call per PULL below —
        # egress metering (REQ-1452/REQ-1455) is a summed byte count with no per-row granularity
        # requirement, so summing here and reporting once is equally correct and avoids taking the
        # egress-meter lock per row. The per-row DEBUG log is gated the same way _send gates its own.
        rows_sent = 0
        batch_bytes = 0
        deadline = self._deadline
        timed_out = False
        try:
            while self._pull_offset < len(self._result_rows):
                if n != -1 and rows_sent >= n:
                    break
                # REQ-1905: the record stream is part of the request. Checked as each run of
                # records starts, so a PULL that arrives late sends none.
                if (
                    deadline is not None
                    and rows_sent % _DEADLINE_CHECK_ROWS == 0
                    and deadline.fired
                ):
                    timed_out = True
                    break
                row = self._result_rows[self._pull_offset]
                if _dbg.isEnabledFor(logging.DEBUG):
                    _dbg.debug("[BOLT] PULL sending record row=%r", row)
                batch_bytes += self.send_record(row, meter=False)
                self._pull_offset += 1
                rows_sent += 1
        finally:
            # Reported even on a mid-batch exception, so bytes for rows actually written are never
            # dropped from the meter (matches the prior per-row report-immediately behavior).
            if batch_bytes:
                from provisa.core.egress import report

                report(self.org_id, batch_bytes)

        if timed_out and deadline is not None:
            self._fail_timed_out(deadline)
            return

        has_more = self._pull_offset < len(self._result_rows)
        in_tx = self.state == State.TX_STREAMING
        if not has_more:
            self.state = State.TX_READY if in_tx else State.READY
            self._end_request()
        summary: dict = {"has_more": has_more, "t_last": 0, "type": "r"}
        if not has_more and self._result_redirect is not None:
            summary["redirect"] = self._result_redirect  # REQ-1194/REQ-1195
        self.send_success(summary)

    def handle_discard(self, fields: list[Any]) -> None:
        if self.state == State.FAILED:
            self.send_ignored()
            return
        meta: dict = fields[0] if fields and isinstance(fields[0], dict) else {}
        n = meta.get("n", -1)
        if n == -1:
            self._pull_offset = len(self._result_rows)
        else:
            self._pull_offset = min(self._pull_offset + n, len(self._result_rows))
        in_tx = self.state == State.TX_STREAMING
        has_more = self._pull_offset < len(self._result_rows)
        if not has_more:
            self.state = State.TX_READY if in_tx else State.READY
            self._end_request()
        self.send_success({"has_more": has_more})

    def handle_route(self) -> None:
        import logging as _logging

        _logging.getLogger("uvicorn.error").warning("[BOLT] ROUTE received")
        self.send_success(
            {
                "rt": {
                    "ttl": 300,
                    "db": "neo4j",
                    "servers": [
                        {"addresses": ["localhost:17687"], "role": "WRITE"},
                        {"addresses": ["localhost:17687"], "role": "READ"},
                        {"addresses": [], "role": "ROUTE"},
                    ],
                }
            }
        )

    def handle_telemetry(self) -> None:
        self.send_success({})


# ── Cypher execution ───────────────────────────────────────────────────────────


def _bolt_label_map(ctx: Any, role_id: str, include_ops: bool, app_state: Any) -> Any:
    """Build a CypherLabelMap scoped to the role's domain rights (the hard ceiling).

    include_ops=False additionally drops system/meta/ops domains (the "provisa_<role>"
    business view); include_ops=True keeps them ("provisa_ops_<role>"). The role's
    domain_access is always applied first, so no db name can exceed the role's rights.
    """
    from provisa.api.rest.cypher_plan import kept_label_map
    from provisa.cypher.label_map import CypherLabelMap

    domain_access = _bolt_domain_access(app_state, role_id)
    cache = app_state.schema_build_cache

    def _role_map() -> CypherLabelMap:
        return CypherLabelMap.from_schema(
            ctx,
            domain_access=domain_access,
            all_tables=cache.get("tables"),
            all_relationships=cache.get("relationships"),
            all_column_types=cache.get("column_types"),
            source_catalogs=app_state.source_catalogs,
        )

    # REQ-1877: a pure function of the registry — built once per schema generation and kept.
    base = kept_label_map(
        app_state,
        role_id,
        domain_access=domain_access,
        cross_domain=True,
        business_view=False,
        build=_role_map,
    )
    if include_ops:
        return base
    return kept_label_map(
        app_state,
        role_id,
        domain_access=domain_access,
        cross_domain=True,
        business_view=True,
        build=lambda: _business_view(base),
    )


def _bolt_domain_access(app_state: Any, role_id: str) -> list[str]:
    """The domain access a Bolt database's graph is scoped to: the selected role's own. A role
    that is not loaded raises — there is no scope to read off a missing role."""
    from provisa.security.rights import require_role

    return require_role(app_state.roles, role_id)["domain_access"]


def _business_view(base: Any) -> Any:
    """``base`` without system/meta/ops-domain nodes (and any relationship touching them).

    from_schema's domain_access only gates cross-domain node addition — it does not filter the
    base node set — so the exclusion happens here, post-build."""
    from provisa.core import domain_policy
    from provisa.cypher.label_map import CypherLabelMap

    sys_ids = set(domain_policy.system_domain_ids())
    biz_nodes = {tn: nm for tn, nm in base.nodes.items() if (nm.domain_id or "") not in sys_ids}
    biz_rels = {
        k: rm
        for k, rm in base.relationships.items()
        if rm.source_label in biz_nodes and rm.target_label in biz_nodes
    }
    nodes_by_table: dict[str, list[str]] = {}
    for tn, nm in biz_nodes.items():
        nodes_by_table.setdefault(nm.table_label, []).append(tn)
    biz_domains = {
        dl: [tn for tn in tns if tn in biz_nodes]
        for dl, tns in base.domains.items()
        if any(tn in biz_nodes for tn in tns)
    }
    biz_aliases = {
        rt: [rm for rm in rms if rm.source_label in biz_nodes and rm.target_label in biz_nodes]
        for rt, rms in base.aliases.items()
    }
    biz_aliases = {rt: rms for rt, rms in biz_aliases.items() if rms}
    return CypherLabelMap(
        nodes=biz_nodes,
        relationships=biz_rels,
        domains=biz_domains,
        nodes_by_table=nodes_by_table,
        aliases=biz_aliases,
    )


def select_database(db: Any, *, multitenancy: bool) -> tuple[str | None, Any]:  # REQ-1235
    """Split a Bolt database name into (the org it names, the role database).

    Multi-tenant names carry the org: ``<org>.provisa_<role>``; a bare role database is refused,
    naming the form. The system database (empty, ``system``) names no org. Single-tenant names
    have no org part.
    """
    from provisa.api.org_resolve import OrgResolutionError

    if not isinstance(db, str) or db in ("", "system"):
        return None, db
    org, dot, rest = db.partition(".")
    if not multitenancy:
        if dot:
            raise OrgResolutionError(
                f"database {db!r} does not exist: this deployment has no org part in its "
                "database names"
            )
        return None, db
    if not dot or not org:
        raise OrgResolutionError(f"database {db!r} names no org; use <org>.{db}, e.g. acme.{db}")
    return org, rest


def _show_databases_rows(
    roles: list[str], orgs: list[str] | None = None
) -> tuple[list[str], list[list[Any]]]:
    """One database per (view × role): provisa_<role> (business) and provisa_ops_<role>.

    REQ-1235: ``orgs`` (multi-tenant) prefixes each with every org the user belongs to; None is a
    single-tenant deployment, whose names have no org part."""
    cols = [
        "name",
        "type",
        "aliases",
        "access",
        "address",
        "role",
        "writer",
        "requestedStatus",
        "currentStatus",
        "statusMessage",
        "default",
        "home",
        "constituents",
    ]
    default_role = roles[0] if roles else None
    rows: list[list[Any]] = []
    prefixes = [f"{o}." for o in orgs] if orgs is not None else [""]
    home_prefix = prefixes[0] if prefixes else ""
    for prefix in prefixes:
        for r in roles:
            for name in (f"{prefix}provisa_{r}", f"{prefix}provisa_ops_{r}"):
                is_home = name == f"{home_prefix}provisa_{default_role}"
                rows.append(
                    [
                        name,
                        "standard",
                        [],
                        "read-write",
                        "localhost:17687",
                        "primary",
                        True,
                        "online",
                        "online",
                        "",
                        is_home,
                        is_home,
                        [],
                    ]
                )
    return cols, rows


def _system_query(
    cypher: str,
    ctx: Any,
    role_id: str,
    include_ops: bool,
    app_state: Any,
    roles: list[str] | None = None,
    orgs: list[str] | None = None,
) -> tuple[list[str], list[list[Any]]] | None:
    """Handle Neo4j Browser system/catalog queries. Return None to fall through."""
    import logging as _logging

    _dbg = _logging.getLogger("uvicorn.error")
    q = cypher.strip()

    q_upper = q.upper()

    # SHOW DATABASES / SHOW DEFAULT DATABASE — one db per (view × role) the user holds.
    if q_upper.startswith("SHOW DATABASE") or q_upper.startswith("SHOW DEFAULT DATABASE"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW DATABASES roles=%r", roles)
        return _show_databases_rows(roles or ([role_id] if role_id else []), orgs)

    # SHOW ALIASES
    if q_upper.startswith("SHOW ALIASES"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW ALIASES")
        return ["name", "database", "location", "url", "user"], []

    # SHOW PROCEDURES / SHOW FUNCTIONS — list registered commands so a Bolt client (Neo4j
    # Browser/Bloom) discovers what `CALL <command>(...)` can invoke, not just run it blind (REQ-1156).
    if q_upper.startswith("SHOW PROCEDURES") or q_upper.startswith("SHOW FUNCTIONS"):
        _dbg.warning("[BOLT] _system_query: intercepted SHOW PROCEDURES/FUNCTIONS")
        return ["name", "description", "signature"], [
            # REQ-1319: the built-in metric procedure is discoverable alongside commands.
            [
                "provisa.metric",
                "Grain-closed read of a registered metric: dimension columns + value",
                "provisa.metric(name :: STRING, dimensions :: LIST OF STRING) :: (ROWS)",
            ],
            *[
                [c["name"], c["description"], _command_signature(c)]
                for c in _list_commands(app_state, role_id)
            ],
        ]

    # SHOW TRANSACTIONS / SHOW SETTINGS / SHOW INDEXES / SHOW CONSTRAINTS
    if (
        q_upper.startswith("SHOW TRANSACTIONS")
        or q_upper.startswith("SHOW SETTINGS")
        or q_upper.startswith("SHOW INDEXES")
        or q_upper.startswith("SHOW CONSTRAINTS")
    ):
        _dbg.warning(
            "[BOLT] _system_query: intercepted SHOW %s", q.split()[1] if len(q.split()) > 1 else ""
        )
        return [], []

    # db.labels / db.relationshipTypes / db.propertyKeys compound query
    if "db.labels()" in q or "db.relationshipTypes()" in q or "db.propertyKeys()" in q:
        label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
        labels = sorted(
            {nm.table_label for nm in label_map.nodes.values()}
            | {nm.domain_label for nm in label_map.nodes.values() if nm.domain_label}
            # REQ-1320: role-tagged tables (modeling_role fact/dimension) additionally
            # expose the star-schema labels Fact / Dimension.
            | {rl for nm in label_map.nodes.values() if (rl := nm.role_label)}
        )
        rel_types = sorted({rm.rel_type for rm in label_map.relationships.values()})
        prop_keys = sorted({prop for nm in label_map.nodes.values() for prop in nm.properties})
        _dbg.warning("[BOLT] _system_query: db.labels=%r rel_types=%r", labels, rel_types)
        rows: list[list[Any]] = []
        if "db.labels()" in q:
            rows.append([{"name": "labels", "data": labels}])
        if "db.relationshipTypes()" in q:
            rows.append([{"name": "relationshipTypes", "data": rel_types}])
        if "db.propertyKeys()" in q:
            rows.append([{"name": "propertyKeys", "data": prop_keys}])
        return ["result"], rows

    # dbms.components() — version info
    if "dbms.components()" in q:
        _dbg.warning("[BOLT] _system_query: intercepted dbms.components()")
        return ["name", "versions", "edition"], [
            ["Neo4j Kernel (Provisa)", ["5.3.0"], "community"],
        ]

    # CALL dbms.showCurrentUser() — Browser uses this to identify the logged-in user
    if "dbms.showCurrentUser()" in q:
        _dbg.warning("[BOLT] _system_query: intercepted dbms.showCurrentUser()")
        return ["username", "roles", "flags"], [[role_id, list(roles or [role_id]), []]]

    # CALL dbms.* catch-all (must be after specific handlers above)
    if q_upper.startswith("CALL DBMS.") or q_upper.startswith("CALL DB."):
        _dbg.warning("[BOLT] _system_query: intercepted CALL DBMS/DB.*")
        return [], []

    return None


async def _graph_counts(
    ctx: Any, role_id: str, include_ops: bool, app_state: Any
) -> tuple[int, int]:
    """Total node/rel counts: sum of per-label count(n) and per-rel-type count(r).

    Mirrors /data/graph-counts (REQ-392); view-labels over the same physical rows
    are counted per-label, matching the internal graph browser.
    """

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    # A parameterized node (native-filter columns) is a function with no snapshot — count-all is
    # undefined without its arg, so exclude it and any relationship touching it from the count sweep.
    node_labels = [nm.label for nm in label_map.nodes.values() if not nm.native_filter_columns]
    seen: set[str] = set()
    rel_types: list[str] = []
    for rel in label_map.relationships.values():
        src_nm = label_map.nodes.get(rel.source_label)
        tgt_nm = label_map.nodes.get(rel.target_label)
        if (src_nm and src_nm.native_filter_columns) or (tgt_nm and tgt_nm.native_filter_columns):
            continue
        if rel.rel_type not in seen:
            seen.add(rel.rel_type)
            rel_types.append(rel.rel_type)

    async def _count(cypher: str) -> int:
        try:
            _cols, rows, _ = await _execute_cypher(cypher, {}, role_id, include_ops=include_ops)
            return int(rows[0][0]) if rows and rows[0] else 0
        except Exception:
            return 0

    # Sequential, not asyncio.gather: a native engine (DuckDB) runs on ONE non-reentrant connection,
    # so concurrent count queries race and return sporadic zeros for attached/materialized sources.
    node_count = 0
    for lbl in node_labels:
        node_count += await _count(f"MATCH (n:{lbl}) RETURN count(n) AS cnt")
    rel_count = 0
    for rt in rel_types:
        rel_count += await _count(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt")
    return node_count, rel_count


async def _impute_relationships(
    parameters: dict, ctx: Any, role_id: str, include_ops: bool, app_state: Any
) -> tuple[list[str], list[list[Any]]]:
    """Impute edges among the Browser's visible nodes (REQ-345).

    The Browser sends integer node IDs in $existingNodeIds/$newNodeIds. Resolve them
    to (label, pk) via node_ids, then for every relationship pair whose endpoints are
    both visible run MATCH (a)-[r]->(b) WHERE a.pk IN [...] AND b.pk IN [...] RETURN r.
    Each returned edge carries integer node IDs (re-registered idempotently), so they
    line up with the nodes already on the Browser canvas.
    """
    from provisa.compiler.naming import apply_cql_property as _cql_prop

    model_db = getattr(app_state, "model_db", None)
    if model_db is None:
        return ["r"], []

    raw_ids = list(parameters.get("existingNodeIds") or []) + list(
        parameters.get("newNodeIds") or []
    )
    int_ids: list[int] = []
    for v in raw_ids:
        try:
            int_ids.append(int(v))
        except (ValueError, TypeError):
            pass
    if not int_ids:
        return ["r"], []

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    nm_by_label = {nm.label: nm for nm in label_map.nodes.values()}

    from provisa.core.schema_org import node_ids

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(node_ids.c.id, node_ids.c.label, node_ids.c.composite_id).where(
                node_ids.c.id.in_(int_ids)
            )
        )
        pg_rows = [dict(r._mapping) for r in result.fetchall()]

    by_label: dict[str, list[Any]] = {}
    for r in pg_rows:
        if nm_by_label.get(r["label"]) is None:
            continue
        pk_str = r["composite_id"].rsplit("|", 1)[-1]
        val: Any = int(pk_str) if pk_str.lstrip("-").isdigit() else pk_str
        by_label.setdefault(r["label"], []).append(val)

    visible = set(by_label.keys())

    def _cql_literal(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    edges: list[list[Any]] = []
    seen: set[Any] = set()
    for rel in label_map.relationships.values():
        src_label = label_map.nodes[rel.source_label].label
        tgt_label = label_map.nodes[rel.target_label].label
        if src_label not in visible or tgt_label not in visible:
            continue
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        src_prop = _cql_prop(src_nm.id_column)
        tgt_prop = _cql_prop(tgt_nm.id_column)
        src_ids = ", ".join(_cql_literal(i) for i in by_label[src_label])
        tgt_ids = ", ".join(_cql_literal(i) for i in by_label[tgt_label])
        query = (
            f"MATCH (a:{src_label})-[r:{rel.rel_type}]->(b:{tgt_label})"
            f" WHERE a.{src_prop} IN [{src_ids}] AND b.{tgt_prop} IN [{tgt_ids}] RETURN r"
        )
        try:
            _cols, rrows, _ = await _execute_cypher(query, {}, role_id, include_ops=include_ops)
        except Exception:
            continue
        for row in rrows:
            edge = row[0] if row else None
            if isinstance(edge, dict):
                eid = edge.get("identity")
                if eid not in seen:
                    seen.add(eid)
                    edges.append([edge])
    return ["r"], edges


def _parse_call_arg(tok: str) -> Any:
    """Coerce one CALL argument literal to a Python value (string/number/bool/null)."""
    tok = tok.strip()
    if (tok.startswith("'") and tok.endswith("'")) or (tok.startswith('"') and tok.endswith('"')):
        return tok[1:-1]
    low = tok.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low == "null":
        return None
    try:
        return int(tok)
    except ValueError:
        try:
            return float(tok)
        except ValueError:
            return tok


def _list_commands(app_state, role_id: str | None) -> list[dict]:
    """Commands visible to *role_id*, for SHOW PROCEDURES discovery (REQ-1156)."""
    from provisa.api.data.action_exec import list_visible_commands

    return list_visible_commands(app_state, role_id)


def _command_signature(cmd: dict) -> str:
    """A Neo4j-style procedure signature for a command: ``name(arg :: TYPE) :: (ROWS)`` (REQ-1156)."""
    args = ", ".join(
        f"{a['name']} :: {str(a.get('type', 'String')).upper()}" for a in cmd["arguments"]
    )
    ret = "LIST OF MAP" if cmd["set_returning"] else "MAP"
    return f"{cmd['name']}({args}) :: ({ret})"


async def _maybe_invoke_command_call(
    cypher: str, parameters: dict, role_id: str, app_state
) -> tuple[list[str], list[list[Any]]] | None:
    """If *cypher* is ``CALL <command>(args)``, invoke it (REQ-1156): read by the one Cypher
    command-call reader (cypher/command_call.py, shared with Cypher over HTTP), admitted and bound
    by the shared executor, shaped by its YIELD and RETURN. None falls through to Cypher."""
    from provisa.api.data.action_exec import bind_command_args, invoke_tracked_function
    from provisa.cypher.command_call import parse_command_call, project

    call = parse_command_call(cypher, parameters or {})
    if call is None:
        return None
    args = bind_command_args(call.name, call.values, app_state, role_id)
    rows = await invoke_tracked_function(call.name, args, app_state, role_id)
    cols, shaped = project(call, rows)
    return cols, [[r.get(c) for c in cols] for r in shaped]


_CALL_METRIC_RE = re.compile(
    r"^\s*CALL\s+provisa\.metric\s*\((.*)\)\s*(?:YIELD\b.*)?;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _split_top_level_args(raw: str) -> list[str]:
    """Split a CALL argument string on top-level commas (quotes and [] nesting respected)."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    quote: str | None = None
    for ch in raw:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf))
    return [p.strip() for p in parts]


def _parse_metric_dimensions(tok: str) -> list[str]:
    """Coerce the dimensions CALL argument: list literal, single string, or null."""
    tok = tok.strip()
    if not tok or tok.lower() == "null":
        return []
    if tok.startswith("[") and tok.endswith("]"):
        inner = tok[1:-1].strip()
        if not inner:
            return []
        return [str(_parse_call_arg(t)) for t in _split_top_level_args(inner)]
    return [str(_parse_call_arg(tok))]


async def _maybe_invoke_metric_call(
    cypher: str, role_id: str, app_state
) -> tuple[list[str], list[list[Any]]] | None:
    """If *cypher* is ``CALL provisa.metric(name, dimensions)``, run the metric read (REQ-1319).

    Builds the semantic addressing form ``SELECT <dims>, value FROM metrics.<name> GROUP BY
    <dims>`` and executes it through the same governed path Cypher-compiled SQL uses
    (_govern_and_route_compiled → _execute_plan), so REQ-1317 expansion, RLS, and masking all
    apply. Returns (columns, rows) or None when the statement is not a provisa.metric CALL.
    An unknown metric — including one not visible to *role_id* — is a hard ValueError.
    """
    m = _CALL_METRIC_RE.match(cypher.strip())
    if m is None:
        return None
    args = _split_top_level_args(m.group(1))
    if not args or len(args) > 2:
        raise ValueError("provisa.metric expects (name :: STRING, dimensions :: LIST OF STRING)")
    name = _parse_call_arg(args[0])
    if not isinstance(name, str):
        raise ValueError(f"provisa.metric: name must be a string, got {name!r}")
    dims = _parse_metric_dimensions(args[1]) if len(args) == 2 else []

    metric = (getattr(app_state, "metrics", {}) or {}).get(name)
    # Visibility is part of existence for the caller — an invisible metric reads as unknown,
    # matching the pgwire catalog's visible_to gate (never leak that the name exists).
    if metric is None or not ("*" in metric.visible_to or role_id in metric.visible_to):
        raise ValueError(f"Unknown metric: {name!r}")

    from provisa.compiler.metric_expand import metric_semantic_sql
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    sql = metric_semantic_sql(name, dims)
    plan = await _govern_and_route_compiled(
        sql, role_id, buffered=True, cache_hint=NO_CACHE_HINT, sdl_joins=False
    )
    result = await _execute_plan(plan)
    return list(result.column_names), [list(row) for row in result.rows]


async def _execute_cypher(
    cypher: str,
    parameters: dict,
    role_id: str,
    include_ops: bool = True,
    roles: list[str] | None = None,
    deliver: Any = None,
    orgs: list[str] | None = None,
) -> tuple[list[str], list[list[Any]], dict | None]:
    """Run Cypher through the Provisa pipeline; return (columns, rows-of-values, redirect-handle).

    ``deliver`` (a ``Delivery`` or None) requests the read result be materialized to a sink instead of
    buffered; when it fires the returned rows are empty and the third element is the sink handle. Only
    the read path honours it — catalog/system, command, and write branches never redirect (REQ-1194).
    """
    from provisa.api.app import state as app_state
    from provisa.cypher.assembler import (
        assemble_rows,
        register_node_ids,
        register_rel_ids,
        to_serializable,
    )
    from provisa.cypher.params import CypherParamError, bind_params, collect_param_names
    from provisa.cypher.parser import CypherParseError, parse_cypher
    from provisa.cypher.translator import (
        CypherCrossSourceError,
        CypherTranslateError,
        cypher_to_sql,
    )
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    ctx = app_state.contexts.get(role_id)
    if ctx is None:
        raise PermissionError("Schema not loaded")

    result = _system_query(cypher, ctx, role_id, include_ops, app_state, roles, orgs)
    if result is not None:
        return (*result, None)

    # REQ-1319: `CALL provisa.metric(name, dimensions)` — grain-closed metric read through the
    # same governed path as Cypher-compiled SQL. Checked before command dispatch: the dotted
    # name can never collide with a registered command (command names are bare identifiers).
    metric_rows = await _maybe_invoke_metric_call(cypher, role_id, app_state)
    if metric_rows is not None:
        return (*metric_rows, None)

    # REQ-1156: `CALL <command>(args)` naming a registered command invokes it through the single
    # governed executor and returns its rows — so Bolt/Cypher clients (Neo4j Browser/Bloom) can run
    # a command exactly like GraphQL/SQL. Placed after _system_query so `CALL dbms.*` still wins.
    cmd = await _maybe_invoke_command_call(cypher, parameters, role_id, app_state)
    if cmd is not None:
        return (*cmd, None)

    # Browser sysinfo node/rel totals — compute real counts (matches the internal graph browser).
    _q = cypher.strip()
    if "count(*)" in _q and "'nodes'" in _q and "'relationships'" in _q:
        node_count, rel_count = await _graph_counts(ctx, role_id, include_ops, app_state)
        return (
            ["result"],
            [
                [{"name": "nodes", "data": node_count}],
                [{"name": "relationships", "data": rel_count}],
            ],
            None,
        )

    # Browser auto-complete-relationships probe — impute edges among visible nodes (REQ-345).
    #   MATCH (a)-[r]->(b) WHERE id(a) IN $existingNodeIds AND id(b) IN $newNodeIds RETURN r
    if "$existingNodeIds" in cypher and "$newNodeIds" in cypher:
        return (
            *await _impute_relationships(parameters, ctx, role_id, include_ops, app_state),
            None,
        )

    # REQ-1877: a read this role already translated under this schema generation is not parsed
    # or translated again — only its values are bound. A write is never kept, so a hit is a read.
    from provisa.api.rest.cypher_plan import CypherTranslation, TranslationRequest

    kept = TranslationRequest(
        app_state,
        role_id,
        surface="bolt-ops" if include_ops else "bolt",
        domain_access=_bolt_domain_access(app_state, role_id),
        cypher=cypher,
        params=parameters,
    )
    translation = kept.cached()
    if translation is None:
        # Try write path first; fall through to read path if it doesn't parse as a write.
        from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

        try:
            parse_cypher_write(cypher)
            return (
                *await _execute_write_cypher(
                    cypher, parameters, role_id, ctx, include_ops, app_state
                ),
                None,
            )
        except CypherWriteParseError:
            pass

        try:
            ast = parse_cypher(cypher)
        except CypherParseError as exc:
            raise ValueError(str(exc)) from exc

        label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)

        param_names = collect_param_names(cypher)
        try:
            bind_params(param_names, parameters)
        except CypherParamError as exc:
            raise ValueError(str(exc)) from exc

        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, parameters)
        except (CypherCrossSourceError, CypherTranslateError) as exc:
            raise ValueError(str(exc)) from exc

        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

        try:
            sql_str = sql_ast.sql(dialect="postgres")
        except Exception as exc:
            raise RuntimeError(f"SQL generation failed: {exc}") from exc

        translation = CypherTranslation(
            semantic_sql=make_semantic_sql(sql_str, ctx),
            ordered_params=tuple(ordered_params),
            param_names=tuple(param_names),
            graph_vars=graph_vars,
        )
        kept.record(translation)

    semantic_sql, graph_vars = translation.semantic_sql, translation.graph_vars
    try:
        resolved_params = translation.bind(parameters)
    except CypherParamError as exc:
        raise ValueError(str(exc)) from exc

    # One governed plan, through the one terminal. Bolt is a buffered transport (REQ-1224): the
    # terminal inlines a result under the threshold and lands one over it. A single-source read
    # stays on its source -- the pipeline probes it there, at threshold+1 rows -- so this no longer
    # needs a first, un-buffered pass to keep it off the engine (github.com/kenstott/provisa/
    # issues/123), nor a DIRECT terminal of its own with its own cache read and write.
    plan = await _govern_and_route_compiled(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
        deliver=deliver,
        buffered=True,
        # REQ-544: the Cypher request's own `// @provisa cache` opt-in.
        cache_hint=cache_hint_for("cypher", cypher),
        # REQ-1897: an opted-in read is looked up in the response cache before it is routed.
        serve_cached=True,
        sdl_joins=False,
    )
    result = await _execute_plan(plan)
    if result.redirect is not None:
        # Materialized to a sink — no records stream; the handle rides the trailing SUCCESS metadata.
        return [], [], result.redirect
    raw_rows = [dict(zip(result.column_names, row)) for row in result.rows]
    assembled = assemble_rows(raw_rows, graph_vars)
    serializable = [to_serializable(r) for r in assembled]

    _model_db = getattr(app_state, "model_db", None)  # REQ-1922: graph ids are org-wide
    await register_node_ids(serializable, _model_db)
    await register_rel_ids(serializable, _model_db)

    columns = list(raw_rows[0].keys()) if raw_rows else []
    rows = [[row.get(col) for col in columns] for row in serializable]
    return columns, rows, None


async def _execute_write_cypher(
    cypher: str, parameters: dict, role_id: str, ctx: Any, include_ops: bool, app_state: Any
) -> tuple[list[str], list[list[Any]]]:
    from provisa.cypher.params import CypherParamError
    from provisa.cypher.write_translator import (
        CypherWriteParseError,
        WriteTranslator,
        bind_write_params,
        parse_cypher_write,
    )
    from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

    try:
        write_ast = parse_cypher_write(cypher)
    except CypherWriteParseError as exc:
        raise ValueError(str(exc)) from exc

    label_map = _bolt_label_map(ctx, role_id, include_ops, app_state)
    translator = WriteTranslator(label_map)
    try:
        # The run's parameters are bound into the statement ($name → $k): the admission and the
        # source see the values, never their names.
        sql, bound = bind_write_params(translator.translate(write_ast), parameters)
    except CypherParamError as exc:
        raise ValueError(str(exc)) from exc

    plan = await _govern_and_route_compiled(
        sql, role_id, exec_params=bound or None, cache_hint=NO_CACHE_HINT, sdl_joins=False
    )
    result = await _execute_plan(plan)
    rows = [list(row) for row in result.rows]
    return result.column_names, rows
