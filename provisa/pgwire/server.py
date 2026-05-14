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

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from provisa.executor.trino import QueryResult as TrinoResult

log = logging.getLogger(__name__)

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()

_TXN_TAG_RE = re.compile(
    r"^\s*(SET|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)


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
        if isinstance(v, (dict, list)):
            return BVType.JSON
        return BVType.TEXT
    return BVType.TEXT


class ProvisaQueryResult(BVQueryResult):
    """Adapts TrinoResult (or DuckDB catalog result) to the buenavista QueryResult ABC."""

    def __init__(self, trino_result: TrinoResult, original_sql: str = ""):
        super().__init__()
        self._rows = trino_result.rows
        self._cols = trino_result.column_names
        self._status = _tag_from_sql(original_sql)
        self._types = [_infer_bvtype(self._rows, i) for i in range(len(self._cols))]

    def has_results(self) -> bool:
        return len(self._cols) > 0

    def column_count(self) -> int:
        return len(self._cols)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self._cols[index], self._types[index])

    def rows(self) -> Iterator[list]:
        return iter(self._rows)  # type: ignore[return-value]

    def status(self) -> str:
        return self._status or "OK"


class ProvisaSession(Session):
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
        return None

    def execute_sql(self, sql: str, params=None) -> ProvisaQueryResult:
        from provisa.pgwire.catalog import answer, classify

        stripped = _substitute_params(sql.strip(), params)
        if classify(stripped) == "INTERCEPT":
            from provisa.api.app import state

            result = answer(stripped, self.role_id or "", state)
            return ProvisaQueryResult(result, stripped)

        if self.role_id is None:
            raise RuntimeError("Not authenticated")

        global _loop
        with _loop_lock:
            loop = _loop
        if loop is None:
            raise RuntimeError("Event loop not available")

        from provisa.pgwire._pipeline import execute_pgwire_sql

        try:
            future = asyncio.run_coroutine_threadsafe(
                execute_pgwire_sql(stripped, self.role_id), loop
            )
            result = future.result(timeout=120)
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

        return ProvisaQueryResult(result, stripped)


class ProvisaConnection(Connection):
    def new_session(self) -> ProvisaSession:
        return ProvisaSession()

    def parameters(self) -> dict[str, str]:
        return {
            "server_version": "14.0.provisa",
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "TimeZone": "UTC",
            "integer_datetimes": "on",
            "standard_conforming_strings": "on",
            "IntervalStyle": "postgres",
        }


class ProvisaHandler(BuenaVistaHandler):
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

    def handle_startup(self, conn: Connection) -> Optional[BVContext]:  # type: ignore[override]
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == 80877103:  # SSL request
            ssl_ctx: ssl.SSLContext | None = getattr(self.server, "ssl_ctx", None)
            if ssl_ctx:
                self.wfile.write(b"S")
                self.wfile.flush()
                self.request = ssl_ctx.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile("rb")
                self.wfile = self.request.makefile("wb")
                self.r = BVBuffer(self.rfile)
            else:
                self.wfile.write(b"N")
                self.wfile.flush()
            return self.handle_startup(conn)
        elif code == 80877102:  # Cancel request
            process_id = self.r.read_uint32()
            secret_key = self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            return None
        elif code == 196608:  # Protocol 3.0
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
        # PG auth type 3 = cleartext password
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
        self.wfile.flush()

    def handle_md5_password(self, ctx: BVContext, payload: bytes) -> None:
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")

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

    def handle_query(self, ctx: BVContext, payload: bytes) -> None:
        decoded = payload.decode("utf-8").rstrip("\x00")

        stmts = [s.strip() for s in decoded.split(";") if s.strip()]
        if not stmts:
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return

        for stmt in stmts:
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


class ProvisaServer(BuenaVistaServer):
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        conn: ProvisaConnection,
        ssl_ctx: ssl.SSLContext | None = None,
    ) -> None:
        socketserver.ThreadingTCPServer.__init__(self, server_address, ProvisaHandler)
        self.conn = conn
        self.rewriter = None
        self.extensions: dict = {}
        self.ctxts: dict = {}
        self.auth = None
        self.ssl_ctx = ssl_ctx

    def verify_request(self, request, client_address) -> bool:
        return True


def start_pgwire_server(
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None,
    loop: asyncio.AbstractEventLoop,
) -> ProvisaServer:
    """Start the pgwire server in a daemon thread. Returns the server instance."""
    global _loop
    with _loop_lock:
        _loop = loop

    conn = ProvisaConnection()
    server = ProvisaServer((host, port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[PGWIRE] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    return server
