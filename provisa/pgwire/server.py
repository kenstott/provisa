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
import re
import socketserver
import ssl
import struct
import threading
from typing import Iterator, Optional, Tuple

import jwt

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from provisa.executor.result import ResultStream

log = logging.getLogger(__name__)

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()

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

# REQ-890: bearer/JWT provider names whose cleartext password payload is an OIDC access token.
_OIDC_PROVIDERS = frozenset({"oidc", "oauth", "keycloak", "firebase"})


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


_DUCKDB_TYPE_TO_BVTYPE: dict[str, BVType] = {
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
_DUCKDB_INT_TYPES = {
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


def _duckdb_type_to_bvtype(type_str: str) -> BVType:
    if type_str in _DUCKDB_TYPE_TO_BVTYPE:
        return _DUCKDB_TYPE_TO_BVTYPE[type_str]
    if type_str in _DUCKDB_INT_TYPES:
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
                _duckdb_type_to_bvtype(t) if t else _infer_bvtype(self._head or [], i)
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
            governed = asyncio.run_coroutine_threadsafe(
                govern_pgwire_plan(stripped, self.role_id), loop
            ).result(timeout=120)
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        from provisa.transpiler.router import Route

        try:
            if isinstance(governed, _Plan) and governed.route == Route.ENGINE:
                from provisa.api.app import state

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
            elif isinstance(governed, _Plan):
                result = asyncio.run_coroutine_threadsafe(
                    _execute_plan(governed), loop
                ).result(timeout=120)
            else:
                result = governed  # registered-function call: bounded, already materialized
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        return ProvisaQueryResult(result, stripped)


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
                self.wfile = self.request.makefile("wb", 0)
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
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
        self.wfile.flush()

    def handle_md5_password(self, ctx: BVContext, payload: bytes) -> None:
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")

        import provisa.pgwire.server as _m

        _state = _m.state
        if _state is None:
            from provisa.api.app import state as _state  # type: ignore[assignment]

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
            # Trust mode: username maps directly to role_id, password ignored.
            ctx.session.role_id = username  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
            return

        if provider in _OIDC_PROVIDERS:
            assert auth_config is not None  # provider != "none" ⇒ auth_config is present
            self._authenticate_oidc(ctx, username, password, auth_config)
            return

        if provider != "simple":
            self._send_pg_error(
                "FATAL",
                "28P01",
                f"pgwire auth provider not supported: {provider!r}",
            )
            return

        from provisa.auth.providers.simple import _provider_instance as auth_provider

        if auth_provider is None:
            self._send_pg_error("FATAL", "28P01", "Auth provider not initialized")
            return

        try:
            auth_provider.login(username, password)
        except ValueError:
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return

        ctx.session.role_id = username  # type: ignore[attr-defined]
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def _authenticate_oidc(  # REQ-890
        self, ctx: BVContext, username: str, token: str, auth_config: dict
    ) -> None:
        """Validate an OIDC bearer token (sent as the cleartext password) and map it to a role.

        Reuses the same AuthProvider the REST/GraphQL path uses (built via build_auth_provider)
        and the same claim→role mapping (resolve_role). validate_token bodies are synchronous, so
        asyncio.run drives them from this socketserver thread.
        """
        from provisa.auth.role_mapping import resolve_role
        from provisa.auth.wiring import build_auth_provider

        provider = build_auth_provider(auth_config)
        try:
            identity = asyncio.run(provider.validate_token(token))
        except (ValueError, jwt.PyJWTError):
            self._send_pg_error(
                "FATAL", "28P01", f'token authentication failed for user "{username}"'
            )
            return

        role = resolve_role(
            identity,
            auth_config.get("role_mapping", []),
            auth_config.get("default_role", "analyst"),
        )
        ctx.session.role_id = role  # type: ignore[attr-defined]
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
                try:
                    tag = asyncio.run_coroutine_threadsafe(run_ctas(stmt, role), _ctas_loop).result(
                        timeout=120
                    )
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
