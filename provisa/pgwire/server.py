# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL wire protocol server for Provisa.

Builds on buenavista's socketserver-based handler, adding:
- TLS via ssl.SSLContext wrap
- Cleartext password auth bridged to SimpleAuthProvider/bcrypt
- Catalog intercept (information_schema + pg_catalog via DuckDB)
- Full Provisa governance pipeline for user queries
- Multi-statement simple-query support
"""
# Requirements: REQ-001, REQ-002, REQ-120, REQ-124, REQ-125, REQ-266, REQ-273
# complexity-gate: allow-ble=5 reason="wire-protocol request-handler boundary: an arbitrary user query / DDL / COPY / CTAS / describe can raise any exception type from the pluggable engine (DuckDB/buenavista/extensions) — each is caught and converted to a PostgreSQL SQLSTATE error response (send_error / _send_pg_error) so one bad statement returns a protocol error instead of crashing the connection handler; catching a narrower set would let an unmapped type kill the session"

from __future__ import annotations

import asyncio
import datetime
import decimal
import logging
import os
import re
import socketserver
import ssl
import struct
import threading
from typing import TYPE_CHECKING, Iterator, Optional, Tuple

import jwt

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from provisa.core.egress import CountingWriter
from provisa.executor.result import ResultStream
from provisa.security.rights import can_act_cross_org, capabilities_for_claims

log = logging.getLogger(__name__)

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()


async def _run_with_org(org_id: str | None, coro):  # REQ-1266
    """Await ``coro`` on the event loop with ``current_org`` bound to ``org_id``.

    pgwire governs/executes on the main loop via run_coroutine_threadsafe; the ContextVar set on the
    socketserver worker thread does NOT propagate into that loop-side coroutine, so the org must be
    bound inside it. ``None`` (single-org / default) awaits unbound → the default-org runtime."""
    if org_id is None:
        return await coro
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await coro
    finally:
        reset_current_org(token)


async def _resolve_and_build_org(state_, identity, requested_org: str | None) -> str | None:
    """Resolve the org for an authenticated pgwire identity and materialize its runtime (REQ-1266).

    Runs on the main event loop (membership lookup + build touch loop-bound DB handles). Returns the
    org id to bind on the session, or None for a single-org deployment / default-org principal.

    REQ-1234: ``requested_org`` is the org the TLS SNI hostname named, when the client dialed one.
    It is a request and nothing more — ``resolve_session_org`` refuses an org the principal is not
    a member of, so dialing acme.provisa.dev does not put anyone inside acme."""
    from provisa.api.app import ensure_org_runtime
    from provisa.api.org_resolve import resolve_session_org

    # REQ-1337: resolve the claims to RIGHTS and test cross_org — never the role name.
    caps = capabilities_for_claims(
        getattr(identity, "roles", []) or [], getattr(state_, "roles", {})
    )
    org_id = await resolve_session_org(
        state_,
        user_id=getattr(identity, "user_id", None),
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested_org or getattr(identity, "active_org_id", None),
    )
    if org_id is not None:
        await ensure_org_runtime(org_id)
    return org_id


_TXN_TAG_RE = re.compile(
    r"^\s*(SET|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)
# CTAS: CREATE TABLE ... AS SELECT — a physical data move (REQ-996), NOT plain DDL. Routed to the
# CTAS handler ahead of _DDL_RE, whose column-def path cannot parse an AS-SELECT body.
_CTAS_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?.+?\bAS\b\s+(?:WITH\b|SELECT\b|\()",
    re.IGNORECASE | re.DOTALL,
)
_DDL_RE = re.compile(
    r"^\s*(CREATE\s+(TABLE|VIEW|INDEX|UNIQUE\s+INDEX|SEQUENCE|SCHEMA)"
    r"|ALTER\s+(TABLE|INDEX|SEQUENCE|VIEW)"
    r"|DROP\s+(TABLE|VIEW|INDEX|SEQUENCE|SCHEMA))\b",
    re.IGNORECASE,
)


state = None  # module-level reference; replaced by tests via patch()

# PostgreSQL startup-message protocol codes (the uint32 following the length prefix). Magic values
# fixed by the wire protocol — named here so handle_startup reads as protocol dispatch, not integers.
_SSL_REQUEST_CODE = 80877103  # SSLRequest (1234 << 16 | 5679)
_CANCEL_REQUEST_CODE = 80877102  # CancelRequest (1234 << 16 | 5678)
_PROTOCOL_VERSION_3 = 196608  # StartupMessage protocol 3.0 (3 << 16)

if TYPE_CHECKING:  # REQ-1394 — the exchange is imported lazily at runtime, named here for typing.
    from provisa.auth.scram import ScramExchange

# REQ-890: bearer/JWT provider names whose cleartext password payload is an OIDC access token.
_OIDC_PROVIDERS = frozenset({"oidc", "oauth", "keycloak", "firebase"})

# REQ-1394: the authentication-request subcodes the SASL exchange uses. 3 is the cleartext request
# this server sent before SCRAM existed and still sends when SCRAM is off.
_AUTH_SASL = 10
_AUTH_SASL_CONTINUE = 11
_AUTH_SASL_FINAL = 12

# REQ-1394: the seed behind mock authentication. Per process and never persisted, so a username
# with no verifier gets a stable-looking salt within a connection and an unguessable one across
# deployments — the point being that an unknown user is answered exactly like a known one.
_MOCK_SEED = os.urandom(32)


def _pg_literal(v) -> str:
    """Render a Python value as a safe PG literal string."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return "E'\\\\x" + v.hex() + "'"
    if isinstance(v, (list, tuple)):
        return "'{" + ",".join(str(x) for x in v) + "}'"
    s = str(v)
    return "'" + s.replace("'", "''") + "'"


def _substitute_params(sql: str, params: list | None) -> str:
    """Replace $1, $2, ... with literal values (highest index first to avoid $1 matching $10)."""
    if not params:
        return sql
    result = sql
    for i in range(len(params), 0, -1):
        result = result.replace(f"${i}", _pg_literal(params[i - 1]))
    return result


def _tag_from_sql(sql: str) -> str:
    m = _TXN_TAG_RE.match(sql)
    if m:
        return m.group(1).upper().split()[0]
    return ""


_TYPE_TO_BVTYPE: dict[str, BVType] = {
    "INTEGER[]": BVType.INTEGERARRAY,
    "VARCHAR[]": BVType.STRINGARRAY,
    "BOOLEAN": BVType.BOOL,
    "FLOAT": BVType.FLOAT,
    "DOUBLE": BVType.FLOAT,
    "DECIMAL": BVType.DECIMAL,
    "TIMESTAMP": BVType.TIMESTAMP,
    "DATE": BVType.DATE,
    "TIME": BVType.TIME,
    # PostgreSQL result-type names (DIRECT sources now report real column types, REQ-883) —
    # without these, an int/float column would fall through to TEXT and mistype the client.
    "BOOL": BVType.BOOL,
    "FLOAT4": BVType.FLOAT,
    "FLOAT8": BVType.FLOAT,
    "NUMERIC": BVType.DECIMAL,
    "TIMESTAMPTZ": BVType.TIMESTAMP,
    "TIMETZ": BVType.TIME,
}
_INT_TYPES = {
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    # PostgreSQL integer type names
    "INT2",
    "INT4",
    "INT8",
}


def _sql_type_to_bvtype(type_str: str) -> BVType:
    if type_str in _TYPE_TO_BVTYPE:
        return _TYPE_TO_BVTYPE[type_str]
    if type_str in _INT_TYPES:
        return BVType.BIGINT
    return BVType.TEXT


def _infer_bvtype(rows: list[tuple], col_idx: int) -> BVType:
    for row in rows:
        v = row[col_idx] if col_idx < len(row) else None
        if v is None:
            continue
        if isinstance(v, bool):
            return BVType.BOOL
        if isinstance(v, int):
            return BVType.BIGINT
        if isinstance(v, float):
            return BVType.FLOAT
        if isinstance(v, decimal.Decimal):
            return BVType.DECIMAL
        if isinstance(v, datetime.datetime):
            return BVType.TIMESTAMP
        if isinstance(v, datetime.date):
            return BVType.DATE
        if isinstance(v, datetime.time):
            return BVType.TIME
        if isinstance(v, list):
            if v and isinstance(v[0], int):
                return BVType.INTEGERARRAY
            if v and isinstance(v[0], str):
                return BVType.STRINGARRAY
            return BVType.JSON
        if isinstance(v, dict):
            return BVType.JSON
        return BVType.TEXT
    return BVType.TEXT


class ProvisaQueryResult(BVQueryResult):  # REQ-529, REQ-028
    """Adapts a :class:`ResultStream` (streaming ENGINE terminal, materialized DIRECT/admin
    result, or DuckDB catalog result) to the buenavista QueryResult ABC.

    Rows are pulled lazily: a streaming result's batches are drained only as buenavista emits
    DataRow messages, so a large user result set never fully materializes. The wire protocol
    needs column types up front (RowDescription precedes DataRow); when the engine supplies no
    per-column types, exactly ONE batch is buffered to infer them — a bounded peek, not the
    whole result."""

    def __init__(self, engine_result: ResultStream, original_sql: str = ""):
        super().__init__()
        self._cols = engine_result.column_names
        self._status = _tag_from_sql(original_sql)
        self._batch_iter: Iterator[list] = engine_result.batches()  # type: ignore[assignment]
        self._head: list | None = None
        ctypes = engine_result.column_types
        # A None entry (or absent types) means the type must be inferred from data, which
        # requires the first batch on hand before RowDescription is sent.
        if not ctypes or any(t is None for t in ctypes):
            self._head = next(self._batch_iter, [])
        if ctypes:
            self._types = [
                _sql_type_to_bvtype(t) if t else _infer_bvtype(self._head or [], i)
                for i, t in enumerate(ctypes)
            ]
        else:
            self._types = [_infer_bvtype(self._head or [], i) for i in range(len(self._cols))]

    def has_results(self) -> bool:
        return len(self._cols) > 0

    def column_count(self) -> int:
        return len(self._cols)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self._cols[index], self._types[index])

    def rows(self) -> Iterator[list]:
        if self._head is not None:
            yield from self._head
        for batch in self._batch_iter:
            yield from batch

    def status(self) -> str:
        return self._status or "OK"


class ProvisaSession(Session):  # REQ-001, REQ-002, REQ-266
    def __init__(self) -> None:
        super().__init__()
        self.role_id: str | None = None
        # REQ-074/REQ-1386: the authenticated principal this session acts as — the audit log's
        # user_id. Set wherever role_id is set (trust mode: the startup packet's user; secured
        # modes: the validated identity), so an authenticated session always has both.
        self.user_id: str | None = None
        # REQ-1266: the org this session is bound to (multitenant OIDC sessions only). None → the
        # single-org default runtime (trust/simple modes, or a platform admin with no single org).
        self.org_id: str | None = None

    def cursor(self):
        return None

    def close(self):
        pass

    def in_transaction(self) -> bool:
        return False

    def load_df_function(self, table: str):
        del table
        return None

    def execute_sql(self, sql: str, params=None) -> ProvisaQueryResult:
        # REQ-1266: bind this session's org on the worker thread so the sync state.X reads below
        # (answer/INTERCEPT, execute_engine_sync, source_pools) route to its runtime. The loop-side
        # governance/execute coroutines are separately bound via _run_with_org (ContextVars do not
        # cross the run_coroutine_threadsafe boundary). None → default runtime (no bind).
        if self.org_id is None:
            return self._execute_sql_bound(sql, params)
        from provisa.api.org_runtime import reset_current_org, set_current_org

        token = set_current_org(self.org_id)
        try:
            return self._execute_sql_bound(sql, params)
        finally:
            reset_current_org(token)

    def _execute_sql_bound(self, sql: str, params=None) -> ProvisaQueryResult:
        from provisa.pgwire.catalog import answer, classify

        stripped = _substitute_params(sql.strip(), params)
        disposition = classify(stripped)
        if disposition == "INTERCEPT":
            from provisa.api.app import state

            result = answer(stripped, self.role_id or "", state)
            log.debug(
                "[RESULT] cols=%r rows=%r",
                result.column_names,
                result.rows[:3] if result.rows else [],
            )
            return ProvisaQueryResult(result, stripped)

        if self.role_id is None:
            raise RuntimeError("Not authenticated")
        if self.user_id is None:
            # Both are set together at authentication; a role without a principal means the
            # session was admitted by a path that never identified its caller.
            raise RuntimeError("Authenticated session has no principal")

        global _loop
        with _loop_lock:
            loop = _loop
        if loop is None:
            raise RuntimeError("Event loop not available")

        from provisa.pgwire._pipeline import (
            _execute_plan,
            _Plan,
            govern_pgwire_plan,
            require_governed_plan,
        )

        # Govern on the event loop, then — for the ENGINE route — drain the engine's SYNC
        # streaming terminal HERE on the socketserver worker thread (REQ-028). Mirrors Flight
        # SQL's govern-then-stream split: the private engine cursor is created and drained on
        # this one thread, and rows flow lazily as buenavista emits DataRow (never buffered on
        # the loop). DIRECT/admin/govdata routes are async-native and materialize via the loop.
        try:
            # REQ-074/REQ-1386: the acting principal is bound INSIDE the loop coroutine (ContextVars
            # do not cross run_coroutine_threadsafe), so the governor's audit/denial write records
            # who ran the statement and that it arrived over pgwire.
            from provisa.audit.context import with_audit_identity

            governed = asyncio.run_coroutine_threadsafe(
                _run_with_org(
                    self.org_id,
                    with_audit_identity(
                        self.user_id, "pgwire", govern_pgwire_plan(stripped, self.role_id)
                    ),
                ),
                loop,
            ).result(timeout=120)
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        from provisa.api.app import state
        from provisa.transpiler.router import Route

        try:
            if isinstance(governed, _Plan) and governed.route == Route.ENGINE:
                # REQ-1176: this streaming sink runs physical_sql on the engine directly (like
                # Flight SQL), so it MUST verify the governed-provenance stamp before the engine
                # executes — the single-chokepoint guarantee is not satisfied by _execute_plan alone.
                require_governed_plan(governed)
                if governed.physical_sql is None:
                    raise RuntimeError("ENGINE plan missing physical_sql")
                result = state.federation_engine.execute_engine_sync(
                    governed.physical_sql,
                    governed.exec_params,
                    session_hints=governed.session_hints,
                )
            elif (
                isinstance(governed, _Plan)
                and governed.route == Route.DIRECT
                and governed.source_id
                and state.source_pools.has(governed.source_id)
                and state.source_pools.supports_stream(governed.source_id)
            ):
                # REQ-1190: a single-reachable-source scan STREAMS via the source's server-side cursor,
                # drained on this worker thread just like the ENGINE terminal — never materialized on the
                # loop (streaming-uniformity Defect 1). REQ-1176: verify the stamp before the source runs.
                require_governed_plan(governed)
                result = state.federation_engine.execute_native_stream(
                    state.source_pools,
                    governed.source_id,
                    governed.sql,
                    governed.exec_params,
                    loop=loop,
                )
            elif isinstance(governed, _Plan):
                result = asyncio.run_coroutine_threadsafe(
                    _run_with_org(self.org_id, _execute_plan(governed)), loop
                ).result(timeout=120)
            else:
                result = governed  # registered-function call: bounded, already materialized
        except PermissionError as exc:
            self._finalize_audit(governed, 500, loop)
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            self._finalize_audit(governed, 500, loop)
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        # REQ-074/REQ-1386: the ENGINE/DIRECT streaming terminals above never reach _execute_plan,
        # so the audit row is written here. Idempotent — the _execute_plan branch already wrote it.
        self._finalize_audit(governed, 200, loop)
        return ProvisaQueryResult(result, stripped)

    def _finalize_audit(self, governed, status_code: int, loop) -> None:
        """Write the governed plan's audit row from this worker thread, under the session's org."""
        from provisa.pgwire._pipeline import _Plan, finalize_audit

        if not isinstance(governed, _Plan):
            return  # a registered-function call carries no plan
        asyncio.run_coroutine_threadsafe(
            _run_with_org(self.org_id, finalize_audit(governed, status_code)), loop
        ).result(timeout=30)


class ProvisaConnection(Connection):  # REQ-529
    def new_session(self) -> ProvisaSession:
        return ProvisaSession()

    def parameters(self) -> dict[str, str]:
        # Startup ParameterStatus set. server_version declares PG 14, so we
        # report the full PG-14 hard-wired set (PG protocol §54.2), including
        # the PG-14 additions default_transaction_read_only and in_hot_standby.
        # Values are sourced from _KNOWN_SETTINGS so the handshake and
        # SHOW/current_setting stay consistent. Casing follows what PG sends.
        from provisa.pgwire.catalog_data import _KNOWN_SETTINGS as s

        return {
            "server_version": s["server_version"],
            "server_encoding": s["server_encoding"],
            "client_encoding": s["client_encoding"],
            "application_name": s["application_name"],
            "is_superuser": s["is_superuser"],
            "session_authorization": s["session_authorization"],
            "DateStyle": s["datestyle"],
            "IntervalStyle": s["intervalstyle"],
            "TimeZone": s["timezone"],
            "integer_datetimes": s["integer_datetimes"],
            "standard_conforming_strings": s["standard_conforming_strings"],
            "default_transaction_read_only": s["default_transaction_read_only"],
            "in_hot_standby": s["in_hot_standby"],
        }


class ProvisaHandler(BuenaVistaHandler):  # REQ-120, REQ-124, REQ-125, REQ-273
    """Extends BuenaVistaHandler with TLS, cleartext auth, and catalog intercept."""

    # REQ-1394: what was advertised in the authentication request, and the exchange in flight.
    # Class-level so the state exists from the first byte — a connection that never reached
    # send_auth_request has been offered nothing and must not be read as mid-SASL.
    _sasl_offered: bool = False
    _sasl: "ScramExchange | None" = None

    def setup(self) -> None:
        # REQ-1452/REQ-1455: meter what this connection writes to its client. Wrapping the socket
        # writer is the only truthful place to count a pgwire result set — the rows are streamed
        # DataRow by DataRow long after the query was finalized, so the audit seam never sees the
        # byte total. Starts unattributed and is bound to an org once auth resolves one; the bytes
        # of the startup and auth exchange belong to no org and are dropped rather than guessed.
        super().setup()
        self.wfile = CountingWriter(self.wfile, None)

    def _send_pg_error(self, severity: str, sqlstate: str, message: str) -> None:
        buf = BVBuffer()
        for field, value in (
            (b"S", severity),
            (b"V", severity),
            (b"C", sqlstate),
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def _send_pg_notice(self, message: str) -> None:
        """Send a NoticeResponse (a non-fatal, out-of-band message) — never touches result rows."""
        buf = BVBuffer()
        for field, value in (
            (b"S", "NOTICE"),
            (b"V", "NOTICE"),
            (b"C", "01000"),  # SQLSTATE warning class
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.NOTICE_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def handle_post_auth(self, ctx):  # type: ignore[override]
        """After a successful auth, emit the REQ-1137 license nag once per connection as a
        NoticeResponse (out-of-band; the query results are never modified or gated)."""
        super().handle_post_auth(ctx)
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"pgwire:{getattr(ctx, 'process_id', id(ctx))}")
            if text:
                self._send_pg_notice(text.replace("\n", " "))
        except Exception:  # nag must never break a connection (REQ-1137)
            log.debug("pgwire license nag emission skipped", exc_info=True)

    def handle_startup(self, conn: Connection) -> Optional[BVContext]:  # type: ignore[override]
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == _SSL_REQUEST_CODE:
            ssl_ctx: ssl.SSLContext | None = getattr(self.server, "ssl_ctx", None)
            if ssl_ctx:
                self.wfile.write(b"S")
                self.wfile.flush()
                self.request = ssl_ctx.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile("rb")
                # Re-wrap: the TLS upgrade replaces the socket, and with it the writer setup()
                # metered. Leaving it unwrapped would silently stop metering every TLS pgwire
                # session, i.e. all of them on the hosted deployment.
                self.wfile = CountingWriter(self.request.makefile("wb", 0), None)
                self.r = BVBuffer(self.rfile)
            else:
                self.wfile.write(b"N")
                self.wfile.flush()
            return self.handle_startup(conn)
        elif code == _CANCEL_REQUEST_CODE:
            process_id = self.r.read_uint32()
            secret_key = self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            return None
        elif code == _PROTOCOL_VERSION_3:
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(b"\x00")]
            params = dict(zip(msg[::2], msg[1::2]))
            log.info(
                "[PGWIRE] connect params: %s", {k: v for k, v in params.items() if k != "password"}
            )
            ctx = BVContext(conn.create_session(), None, params)
            self.send_auth_request(ctx)
            return ctx
        else:
            raise Exception(f"Unsupported startup message code: {code}")

    def send_auth_request(self, ctx: BVContext) -> None:
        del ctx
        # REQ-1394: SCRAM when the deployment asked for it, cleartext-over-TLS otherwise. The
        # choice is made once, here, and the state machine below follows what was advertised.
        self._sasl_offered = self._scram_offered()
        self._sasl = None
        if self._sasl_offered:
            from provisa.auth.scram import MECHANISM

            # AuthenticationSASL carries the mechanism list as NUL-terminated names ended by an
            # empty one. Only SCRAM-SHA-256 is offered; -PLUS would promise channel binding that
            # the exchange does not implement.
            body = MECHANISM.encode("ascii") + b"\x00\x00"
            self.wfile.write(
                struct.pack(
                    "!cii", ServerResponse.AUTHENTICATION_REQUEST, 8 + len(body), _AUTH_SASL
                )
            )
            self.wfile.write(body)
        else:
            self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
        self.wfile.flush()

    def _app_state(self):
        """The running application state, whichever module holds it."""
        import provisa.pgwire.server as _m

        _state = _m.state
        if _state is None:
            from provisa.api.app import state as _state  # type: ignore[assignment]
        return _state

    def _scram_offered(self) -> bool:  # REQ-1394
        """Whether this connection is offered SASL rather than a cleartext password.

        SCRAM authenticates a local password and nothing else: it proves knowledge of a verifier
        this deployment derived, so it is offered only under the basic provider. A bearer provider
        or a personal access token arrives as an opaque secret in the password field, which SCRAM
        has no way to carry — those deployments keep the cleartext request, protected by TLS.
        """
        _state = self._app_state()
        auth_config = _state.auth_config
        if auth_config is None or not getattr(_state, "auth_middleware_active", False):
            return False
        if auth_config["provider"] != "basic":
            return False
        return bool(auth_config.get("scram"))

    def handle_md5_password(self, ctx: BVContext, payload: bytes) -> None:
        if self._sasl_offered:
            # REQ-1394: every SASL message arrives as a PASSWORD_MESSAGE, so the negotiation is
            # dispatched from here rather than from the vendored pre-auth loop.
            self._handle_sasl(ctx, payload)
            return
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")

        _state = self._app_state()

        auth_config = _state.auth_config
        if auth_config is None:
            if getattr(_state, "auth_middleware_active", False):
                # A real provider is active but its config is absent — misconfiguration.
                # Fail closed: never silently degrade a secured server to no-auth/trust.
                raise RuntimeError("pgwire auth_config not configured")
            # Explicit unsecured mode (provider: none / no auth section) — treat as trust mode.
            provider = "none"
        else:
            provider = auth_config["provider"]

        if provider == "none" or not _state.auth_middleware_active:
            # Trust mode: username maps directly to role_id, password ignored. The startup
            # packet's user is the only principal there is — it is what the audit row records.
            ctx.session.role_id = username  # type: ignore[attr-defined]
            ctx.session.user_id = username  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
            return

        assert auth_config is not None  # provider != "none" ⇒ auth_config is present
        with _loop_lock:
            loop = _loop
        if loop is None:
            self._send_pg_error("FATAL", "08004", "pgwire event loop not available")
            return

        from provisa.auth.throttle import LockedOut

        try:
            # REQ-1228: under PROVISA_MTLS_BIND_PRINCIPAL the client certificate's common name and
            # the startup packet's user must be the same person. Checked before the password is
            # examined — a mismatched certificate is not a credential question.
            self._assert_peer_binding(username)
        except PermissionError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
            return

        auth_provider = self._build_provider(_state, auth_config)
        if auth_provider is None:
            return
        try:
            identity = self._validate_credential(loop, auth_provider, provider, username, password)
        except LockedOut as locked:
            # REQ-1393: a distinct answer from a wrong password. 28000 is invalid_authorization_
            # specification — the attempt was refused before the credential was examined at all.
            self._send_pg_error("FATAL", "28000", str(locked))
            return
        if identity is None:
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        self._complete_auth(ctx, identity, auth_config)

    def _send_auth_message(self, code: int, body: bytes) -> None:  # REQ-1394
        """One AuthenticationRequest message carrying a SASL payload."""
        self.wfile.write(
            struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8 + len(body), code)
        )
        self.wfile.write(body)
        self.wfile.flush()

    def _handle_sasl(self, ctx: BVContext, payload: bytes) -> None:  # REQ-1394
        """One step of the SCRAM exchange, driven by whichever message just arrived.

        Two round trips, and which one this is follows from whether an exchange already exists.
        The first message names the mechanism and carries client-first; the second carries the
        proof. A protocol error ends the connection with FATAL rather than being retried — SCRAM
        has no resynchronisation point, and a client that sent the wrong thing will send it again.
        """
        from provisa.auth.scram import MECHANISM, ScramError

        username = ctx.params.get("user", "")
        if self._sasl is None:
            mechanism, sep, rest = payload.partition(b"\x00")
            if not sep or mechanism.decode("utf-8") != MECHANISM:
                self._send_pg_error(
                    "FATAL", "28000", f"unsupported SASL mechanism: {mechanism.decode('utf-8')!r}"
                )
                return
            (length,) = struct.unpack("!i", rest[:4])
            if length < 0:
                # -1 means "no initial response". SCRAM's first message is not optional, so there
                # is nothing to answer with.
                self._send_pg_error("FATAL", "28000", "SASL initial response is required")
                return
            self._sasl_start(username, rest[4 : 4 + length].decode("utf-8"))
            return

        try:
            final = self._sasl.server_final(payload.decode("utf-8").rstrip("\x00"))
        except ScramError as exc:
            # REQ-1393: a failed proof is a failed password and counts against the account exactly
            # as a wrong one over any other surface.
            from provisa.auth.throttle import login_throttle, subject_key

            login_throttle().record_failure(subject_key(username, ""))
            log.info("[PGWIRE] SCRAM authentication failed for %r: %s", username, exc)
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return

        self._send_auth_message(_AUTH_SASL_FINAL, final.encode("utf-8"))
        self._sasl_complete(ctx, username)

    def _sasl_start(self, username: str, client_first: str) -> None:  # REQ-1394
        """Answer client-first with the account's salt, or a mock account's when it has none."""
        from provisa.auth.scram import ScramError, ScramExchange, mock_verifier
        from provisa.auth.scram_store import read_verifier
        from provisa.auth.throttle import LockedOut, login_throttle, subject_key

        loop = self._sasl_loop()
        if loop is None:
            return
        try:
            # REQ-1393: the lockout is checked before any work is done on the account's behalf,
            # so a locked-out name cannot be used to make the server derive verifiers all day.
            login_throttle().check(subject_key(username, ""))
        except LockedOut as locked:
            self._send_pg_error("FATAL", "28000", str(locked))
            return

        _state = self._app_state()
        admin_db = _state.admin_db
        assert admin_db is not None  # the basic provider is DB-backed; _scram_offered required it
        verifier = asyncio.run_coroutine_threadsafe(read_verifier(admin_db, username), loop).result(
            timeout=60
        )
        if verifier is None:
            # PostgreSQL's mock authentication. A user who has never set a password under SCRAM —
            # and a user who does not exist — gets a well-formed exchange that no proof satisfies,
            # so the handshake never becomes a name oracle.
            verifier = mock_verifier(username, _MOCK_SEED)

        exchange = ScramExchange(verifier)
        try:
            first = exchange.server_first(client_first)
        except ScramError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
            return
        self._sasl = exchange
        self._send_auth_message(_AUTH_SASL_CONTINUE, first.encode("utf-8"))

    def _sasl_loop(self):
        """The API event loop, or None after telling the client why authentication cannot run."""
        with _loop_lock:
            loop = _loop
        if loop is None:
            self._send_pg_error("FATAL", "08004", "pgwire event loop not available")
        return loop

    def _sasl_complete(self, ctx: BVContext, username: str) -> None:  # REQ-1394
        """Turn a verified proof into a session.

        The proof says the password was right; it says nothing about whether the account is still
        active or what it may do. Both of those come from reading the account, which is why this
        goes through the provider rather than trusting the exchange.
        """
        from provisa.auth.throttle import login_throttle, subject_key

        loop = self._sasl_loop()
        if loop is None:
            return
        _state = self._app_state()
        auth_config = _state.auth_config
        assert auth_config is not None  # _scram_offered required it
        auth_provider = self._build_provider(_state, auth_config)
        if auth_provider is None:
            return
        try:
            identity = asyncio.run_coroutine_threadsafe(
                auth_provider.identity_for(username), loop
            ).result(timeout=60)
        except ValueError:
            # The verifier matched but the account is gone or deactivated. Answered as a failed
            # password: a deactivated account must not be able to tell that its password is right.
            login_throttle().record_failure(subject_key(username, ""))
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        login_throttle().record_success(subject_key(username, ""))
        self._complete_auth(ctx, identity, auth_config)

    def _requested_org(self) -> str | None:  # REQ-1234
        """The org this connection's TLS SNI hostname named, or None.

        None on a plaintext connection and on one dialed by IP address, which is every connection
        on a single-org deployment — those resolve their org from the principal alone, unchanged.
        The socket is the wrapped one; ``handle_startup`` replaced ``self.request`` during the
        SSLRequest exchange, and the servername callback stashed the name on it during the
        handshake.
        """
        from provisa.security.sni import indicated_host, org_from_host

        return org_from_host(indicated_host(self.request))

    def _assert_peer_binding(self, username: str) -> None:  # REQ-1228
        """Bind the TLS client certificate to the startup packet's user, when configured.

        A plaintext connection has no peer certificate to inspect; ``resolve_client_auth`` returns
        None there because mTLS is only wired onto the context when a CA is configured, and the
        binding check is then a no-op. The socket is the wrapped one — ``handle_startup`` replaced
        ``self.request`` during the SSLRequest exchange.
        """
        from provisa.security.mtls import assert_principal_binding, resolve_client_auth

        auth = resolve_client_auth(
            "PROVISA_PGWIRE_CLIENT_CA",
            "PROVISA_PGWIRE_MTLS_MODE",
            "PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL",
        )
        if auth is None or not auth.bind_principal:
            return
        peer_cert = self.request.getpeercert() if isinstance(self.request, ssl.SSLSocket) else None
        assert_principal_binding(auth, peer_cert, username)

    def _build_provider(self, _state, auth_config: dict):  # REQ-124
        """The configured AuthProvider, or None after answering the client with FATAL.

        A provider that cannot be constructed — an unknown name, a missing signing key — can
        authenticate nobody. The client is told so on the wire; dropping the connection with an
        unhandled exception would leave it guessing.
        """
        from provisa.auth.wiring import build_auth_provider

        try:
            return build_auth_provider(auth_config, admin_pool=getattr(_state, "admin_db", None))
        except ValueError as exc:
            self._send_pg_error("FATAL", "28P01", f"pgwire auth provider unavailable: {exc}")
            return None

    def _validate_credential(  # REQ-124, REQ-890, REQ-1263
        self, loop, auth_provider, provider_name: str, username: str, password: str
    ):
        """Validate the startup credential against the provider, or None.

        pgwire carries no scheme field — the startup packet holds a username and one cleartext
        secret — so the presentation is decided once, from what the secret is. A personal access
        token names itself by prefix and is a bearer credential (REQ-1263); a bearer/JWT provider
        is told to expect a token in the password field (REQ-890); everything else is a password,
        presented as ``basic``. One decision, one validator: a credential the chosen validator
        refuses is not retried against another, which would turn one rejection into a second guess.

        Validators run on the main loop, never a private ``asyncio.run`` — the PAT store and any
        DB-backed provider hold loop-bound handles.
        """
        import base64

        from provisa.auth.models import validator_for_scheme
        from provisa.auth.pat import is_personal_access_token
        from provisa.auth.throttle import throttled

        if is_personal_access_token(password) or provider_name in _OIDC_PROVIDERS:
            scheme, token = "bearer", password
        else:
            scheme = "basic"
            token = base64.b64encode(f"{username}:{password}".encode()).decode()

        validator = validator_for_scheme(auth_provider, scheme)
        if validator is None:
            return None
        # REQ-1393: the startup packet names the account, so failed guesses count against it here
        # and on every other surface alike. LockedOut propagates — the caller answers 28000.
        attempt = throttled(validator, token, principal=username if scheme == "basic" else None)
        try:
            return asyncio.run_coroutine_threadsafe(attempt, loop).result(timeout=60)
        except (ValueError, jwt.PyJWTError):
            return None

    def _complete_auth(  # REQ-273, REQ-551, REQ-890, REQ-1266
        self, ctx: BVContext, identity, auth_config: dict
    ) -> None:
        """Map the validated identity to a role, bind its org, and admit the connection."""
        from provisa.auth.role_mapping import resolve_role

        default_role = auth_config.get("default_role")
        if not default_role:
            # No admin default: an identity matching no mapping rule is refused, not escalated
            # onto whatever role the deployment happens to have named first.
            raise RuntimeError("pgwire auth requires auth.default_role to be configured")
        role = resolve_role(identity, auth_config.get("role_mapping", []), default_role)
        # REQ-1266: bind the session to the identity's org (multitenant) so its queries route to that
        # org's data-plane runtime. Resolution + build run on the main loop (loop-bound DB handles);
        # an unresolvable principal fails the connection rather than silently landing on the default.
        import provisa.pgwire.server as _m

        _state = _m.state
        if _state is None:
            from provisa.api.app import state as _state  # type: ignore[assignment]
        if getattr(_state, "multitenancy", False):
            from provisa.api.org_resolve import OrgResolutionError

            with _loop_lock:
                loop = _loop
            if loop is None:
                self._send_pg_error("FATAL", "08004", "pgwire event loop not available")
                return
            try:
                ctx.session.org_id = asyncio.run_coroutine_threadsafe(  # type: ignore[attr-defined]
                    _resolve_and_build_org(_state, identity, self._requested_org()), loop
                ).result(timeout=60)
            except OrgResolutionError as exc:
                self._send_pg_error("FATAL", "28000", f"org selection failed: {exc}")
                return
        # REQ-1452: attribute this connection's writes from here on.
        self.wfile.bind_org(getattr(ctx.session, "org_id", None))
        ctx.session.role_id = role  # type: ignore[attr-defined]
        ctx.session.user_id = identity.user_id  # type: ignore[attr-defined]  # REQ-074
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def handle_describe(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        if ba[0] == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
            if stmt_name is not None and not ctx.stmts.get(stmt_name, ("x",))[0].strip():
                self.send_no_data()
                return
        elif ba[0] == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            sql = ctx.stmts[stmt][0]
            if not sql.strip():
                self.send_paramter_description([])
                self.send_no_data()
                return
            indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
            if "typeinfo_tree" in sql.lower() and indices:
                param_oids = [1028]
            elif "set_config" in sql.lower() and indices:
                param_oids = [25] * len(indices)
            else:
                stored_oids = ctx.stmts[stmt][1]
                if stored_oids:
                    param_oids = stored_oids
                elif indices:
                    _CAST_OID = {
                        "text": 25,
                        "varchar": 25,
                        "int": 23,
                        "int4": 23,
                        "int8": 20,
                        "bigint": 20,
                        "bool": 16,
                        "float8": 701,
                    }
                    cast_map = {
                        int(m): _CAST_OID.get(t.lower(), 25)
                        for m, t in re.findall(r"\$(\d+)::(\w+)", sql)
                    }
                    param_oids = [cast_map.get(i, 20) for i in range(1, max(indices) + 1)]
                else:
                    param_oids = []
            # Update stored param_oids so describe_statement substitutes example values
            # instead of executing the SQL with unresolved $N placeholders.
            ctx.stmts[stmt] = (sql, param_oids)
            try:
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            self.send_paramter_description(param_oids)
            if query_result.has_results():
                self.send_row_description(query_result)
            else:
                self.send_no_data()
            return
        super().handle_describe(ctx, payload)

    def handle_execute(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        portal_idx = ba.index(0)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
        if stmt_name is not None and not ctx.stmts.get(stmt_name, ("x",))[0].strip():
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            return
        super().handle_execute(ctx, payload)

    def handle_query(self, ctx: BVContext, payload: bytes) -> None:
        from provisa.compiler.sql_rewrite import split_sql_statements

        decoded = payload.decode("utf-8").rstrip("\x00")

        # Statement-aware split: a ';' inside a string literal / comment / dollar-quote must NOT
        # mis-split, so governance and execution see identical statement boundaries (no parser
        # differential — replaces the old naive decoded.split(';')).
        stmts = split_sql_statements(decoded)
        if not stmts:
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return

        # REQ-1266: bind the session's org on this worker thread so the DDL/COPY handlers' sync
        # state.X reads route to its runtime (SELECT re-binds inside execute_sql; nesting is safe).
        _org_id = ctx.session.org_id  # type: ignore[attr-defined]
        _org_token = None
        if _org_id is not None:
            from provisa.api.org_runtime import set_current_org

            _org_token = set_current_org(_org_id)
        try:
            self._process_query_stmts(ctx, stmts)
        finally:
            if _org_token is not None:
                from provisa.api.org_runtime import reset_current_org

                reset_current_org(_org_token)

    def _process_query_stmts(self, ctx: BVContext, stmts: list[str]) -> None:
        for stmt in stmts:
            if _COPY_RE.match(stmt):
                from provisa.pgwire.copy_handler import CopyHandler

                try:
                    nrows = CopyHandler(self).handle(ctx, stmt)  # type: ignore[arg-type]
                    self.send_command_complete(f"COPY {nrows}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _CTAS_RE.match(stmt):
                from provisa.executor.ctas import run_ctas

                # role_id lives on the session, not the handler; the loop may be unset.
                role = ctx.session.role_id  # type: ignore[attr-defined]
                if not role:
                    self._send_pg_error("ERROR", "28000", "Not authenticated")
                    ctx.mark_error()
                    break
                with _loop_lock:
                    _ctas_loop = _loop
                if _ctas_loop is None:
                    self._send_pg_error("ERROR", "58000", "Event loop not available")
                    ctx.mark_error()
                    break
                user = ctx.session.user_id  # type: ignore[attr-defined]
                if not user:
                    self._send_pg_error("ERROR", "28000", "Not authenticated")
                    ctx.mark_error()
                    break
                from provisa.audit.context import with_audit_identity

                try:
                    tag = asyncio.run_coroutine_threadsafe(
                        _run_with_org(
                            ctx.session.org_id,  # type: ignore[attr-defined]
                            # REQ-074/REQ-1386: the CTAS SELECT runs through the governed pipeline;
                            # bind its principal inside the loop coroutine so the row is attributed.
                            with_audit_identity(user, "pgwire", run_ctas(stmt, role)),
                        ),
                        _ctas_loop,
                    ).result(timeout=120)
                    self.send_command_complete(f"{tag}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _DDL_RE.match(stmt):
                from provisa.pgwire.ddl_handler import DdlHandler

                try:
                    tag = DdlHandler(self).handle(ctx, stmt)
                    self.send_command_complete(f"{tag}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            try:
                from buenavista.core import Extension

                if req := Extension.check_json(stmt):
                    method = req.get("method")
                    extension = self.server.extensions.get(method)  # type: ignore[attr-defined]
                    if not extension:
                        raise Exception("Unknown method: " + str(method))
                    query_result = extension.apply(req.get("params"), ctx.session)
                else:
                    query_result = ctx.execute_sql(stmt)
            except PermissionError as exc:
                self._send_pg_error("ERROR", "42501", str(exc))
                ctx.mark_error()
                break
            except Exception as exc:
                self.send_error(exc, ctx)
                break

            if not query_result:
                raise Exception("No query result for: " + stmt)

            if query_result.has_results():
                self.send_row_description(query_result)
                row_count = self.send_data_rows(query_result)
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                status = query_result.status()
                self.send_command_complete(f"{status}\x00")

        self.send_ready_for_query(ctx)


class ProvisaServer(BuenaVistaServer):  # REQ-001, REQ-266
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        conn: ProvisaConnection,
        ssl_ctx: ssl.SSLContext | None = None,
    ) -> None:
        socketserver.ThreadingTCPServer.__init__(self, server_address, ProvisaHandler)  # type: ignore[arg-type]
        self.conn = conn
        self.rewriter = None
        self.extensions: dict = {}
        self.ctxts: dict = {}
        self.auth = None
        self.ssl_ctx = ssl_ctx

    def verify_request(self, request, client_address) -> bool:
        del request, client_address
        return True


def start_pgwire_server(  # REQ-527
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None,
    loop: asyncio.AbstractEventLoop,
) -> ProvisaServer:
    """Start the pgwire server in a daemon thread. Returns the server instance."""
    import os

    global _loop
    with _loop_lock:
        _loop = loop

    _debug_log = os.path.expanduser("~/pgwire_debug.log")
    _fh = logging.FileHandler(_debug_log)
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger("provisa.pgwire").addHandler(_fh)
    logging.getLogger("provisa.pgwire").setLevel(logging.DEBUG)
    logging.getLogger("buenavista").addHandler(_fh)
    logging.getLogger("buenavista").setLevel(logging.DEBUG)

    conn = ProvisaConnection()
    server = ProvisaServer((host, port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[PGWIRE] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    return server
